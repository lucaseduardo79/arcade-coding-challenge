# CSE Financial Analysis - AI Engineering Challenge

Comprehensive solution for extracting, analyzing, and querying quarterly financial data from two companies listed on the **Colombo Stock Exchange (CSE)**:

- **Dipped Products PLC** (DIPD) - Security ID: 670
- **Richard Pieris Exports PLC** (REXP) - Security ID: 771

Built with **4 independent Docker containers**, each handling one step of the pipeline.

---

## Architecture

```
docker compose up
       |
       v
[1. Scraper] ──> shared/pdfs/ + shared/metadata/scrape_manifest.json
       |
       v
[2. ETL] ──> shared/db/financial_data.duckdb
       |
       v
[3. Dashboard :8501]  +  [4. Query System :8502]
```

| Container | Purpose | Technology |
|-----------|---------|------------|
| **scraper** | Scrapes quarterly financial report PDFs from CSE | Python, httpx, Pydantic |
| **etl** | Extracts P&L data from PDFs using LLM, stores in DuckDB | LangGraph, Groq API, pdfplumber, DuckDB |
| **dashboard** | Interactive financial dashboard | Streamlit, Plotly, DuckDB |
| **query-system** | Natural language query interface (Text2SQL) | LangGraph, Groq API, Streamlit, DuckDB |

All containers share data through a **bind mount** (`./shared`) with the following structure:

```
shared/
├── pdfs/                 # Downloaded PDFs organized by company
│   ├── DIPD/
│   └── REXP/
├── db/
│   └── financial_data.duckdb   # Structured financial database
└── metadata/
    └── scrape_manifest.json    # Tracks downloaded PDFs
```

---

## Quick Start

### Prerequisites

- **Docker** and **Docker Compose** (v2+)
- **Groq API Key** (free at https://console.groq.com) - used for LLM-based extraction and querying
- (Optional) **Fallback API Key** from Together AI, OpenRouter, or OpenAI - used when Groq rate limits are hit

### 1. Clone and configure

```bash
git clone https://github.com/lucaseduardo79/arcade-coding-challenge.git
cd arcade-coding-challenge

# Create .env from template
cp .env.example .env
# Edit .env and add your API keys
```

### 2. Configure API keys

Edit the `.env` file:

```env
GROQ_API_KEY=your_groq_api_key_here

# Optional: Fallback API for when Groq hits rate limits
FALLBACK_API_KEY=your_together_or_openrouter_key
FALLBACK_BASE_URL=https://api.together.xyz/v1
FALLBACK_MODEL=meta-llama/Llama-3.3-70B-Instruct-Turbo

# Optional: LangSmith observability
LANGCHAIN_TRACING_V2=true
LANGCHAIN_API_KEY=your_langsmith_key
LANGCHAIN_PROJECT=cse-financial-analysis
```

### 3. Run the full pipeline

```bash
docker compose up --build
```

This executes the containers sequentially:

1. **Scraper** downloads ~53 PDFs from CSE (takes ~2 minutes)
2. **ETL** extracts financial data using LLM (takes ~30-45 minutes depending on rate limits)
3. **Dashboard** starts at http://localhost:8501
4. **Query System** starts at http://localhost:8502

### 4. Run individual containers

```bash
# Run only the scraper
docker compose up scraper

# Run only the ETL (requires scraper to have completed)
docker compose up etl

# Run dashboard and query system (requires ETL to have completed)
docker compose up dashboard query-system
```

---

## Step 1: Data Scraping

**Container:** `scraper`

The scraper discovers and downloads financial report PDFs from the CSE using the internal API endpoint `POST /api/getFinancialAnnouncement`.

### How it works

1. Queries the CSE API with each company's `securityId` to get a list of financial announcements
2. Filters for quarterly/interim reports (skips annual reports and press releases)
3. Downloads PDFs to `shared/pdfs/{COMPANY_ID}/`
4. Generates `shared/metadata/scrape_manifest.json` with metadata for each PDF

### Key features

- **Direct API access**: Uses the discovered CSE API endpoint (no browser automation needed)
- **Retry logic**: Exponential backoff with `tenacity` for resilient downloads
- **Manifest tracking**: Saves progress after each download for resumability
- **Date range**: Fetches reports from 2020 onwards (5+ years of data)

### Output

- ~53 PDFs (~27 DIPD + ~26 REXP) covering quarterly and interim reports
- `scrape_manifest.json` mapping each PDF to company and period metadata

---

## Step 2: ETL Pipeline (Dataset Creation)

**Container:** `etl`

Extracts structured P&L data from PDF reports using a **LangGraph extraction pipeline** with LLM-based parsing.

### LangGraph Extraction Graph

```
START
  |
  v
[identify_pages] ─── LLM identifies which pages contain the P&L statement
  |
  v
[extract_data] ──── LLM extracts structured financial metrics from P&L pages
  |
  v
[validate] ──────── Programmatic arithmetic validation (GP = Revenue - COGS, etc.)
  |       \
  |     FAIL (retry < 3)
  |        \
  |         └──> [extract_data] with error feedback
  |
  v (PASS or max retries)
[normalize] ──── Converts all values to LKR Thousands
  |
  v
  END
```

### Extraction approach

1. **PDF Parsing**: `pdfplumber` extracts text and tables from each page, with `PyMuPDF` as fallback
2. **Page Identification**: `llama-3.1-8b-instant` (fast, small model) identifies P&L pages from page summaries
3. **Data Extraction**: `llama-3.3-70b-versatile` extracts structured JSON from P&L text using Groq's JSON mode
4. **Validation**: Arithmetic checks (gross_profit = revenue - COGS, etc.) with 2% tolerance
5. **Retry Loop**: Up to 3 attempts with error feedback for self-correction
6. **Normalization**: Converts millions/billions to thousands for consistent units

### Financial metrics extracted

| Metric | Description |
|--------|-------------|
| `revenue` | Total revenue / turnover |
| `cost_of_goods_sold` | Cost of sales (positive number) |
| `gross_profit` | Revenue - COGS |
| `operating_expenses` | Distribution + admin + other operating costs |
| `operating_income` | Gross profit + other income - operating expenses |
| `net_income` | Profit for the period |
| `other_income` | Other operating income |
| `finance_income` | Interest and investment income |
| `finance_costs` | Interest expense (positive number) |
| `profit_before_tax` | PBT |
| `income_tax_expense` | Tax charge (positive number) |

### DuckDB Schema

```sql
companies              -- Company master data (DIPD, REXP)
report_sources         -- PDF metadata and extraction confidence
income_statement       -- Raw extracted P&L data (cumulative and quarterly)
quarterly_standalone   -- Derived single-quarter figures
```

The `quarterly_standalone` table derives individual quarter figures from cumulative interim reports (e.g., Q2 = H1 - Q1).

### Key features

- **Resumable**: Tracks processed PDFs in DuckDB; skips already-extracted reports
- **Rate limit handling**: Automatic fallback to Together AI / OpenRouter when Groq hits TPD limits
- **DailyLimitExhausted**: Gracefully stops when both primary and fallback APIs are exhausted
- **Fiscal year mapping**: March fiscal year end (Q1 = Apr-Jun, Q2 = Jul-Sep, Q3 = Oct-Dec, Q4 = Jan-Mar)

---

## Step 3: Dashboard

**Container:** `dashboard` | **URL:** http://localhost:8501

Interactive Streamlit dashboard with three pages:

### Overview
- Key metrics cards (latest quarter revenue, net income, margins)
- Quarterly revenue bar chart (grouped by company)
- Profit margin trends (gross margin % and net margin %)
- Company comparison radar chart
- Year-over-year growth heatmap

### Company Detail
- Deep dive into a single company's financials
- P&L waterfall chart (Revenue -> COGS -> Gross Profit -> OpEx -> Net Income)
- Margin trend analysis
- Full quarterly data table

### Comparison
- Side-by-side delta analysis of DIPD vs REXP
- Revenue and net income comparison
- Quarterly trend overlay

### Charts built with Plotly

| Chart | Type | Description |
|-------|------|-------------|
| `quarterly_revenue_bar` | Grouped bar | Quarterly revenue by company |
| `profit_margin_trend` | Line | Gross and net margin % over time |
| `net_income_waterfall` | Waterfall | P&L breakdown for a single period |
| `company_comparison_radar` | Radar | Normalized metric comparison |
| `yoy_growth_heatmap` | Heatmap | Year-over-year growth rates |

---

## Step 4: LLM Query System

**Container:** `query-system` | **URL:** http://localhost:8502

Natural language interface for querying financial data using a **LangGraph Text2SQL agent**.

### LangGraph Agent Graph

```
START
  |
  v
[guardrails] ──── Checks for profanity and code/architecture disclosure attempts
  |         \
  |       BLOCKED → END (returns warning message)
  |
  v
[assistant] ──── LLM decides: call a tool (TOOL:) or give final answer (FINAL:)
  |         \
  |       FINAL: → END
  |
  v
[tools] ──── Executes the tool (execute_sql, get_table_schema, list_tables)
  |
  v
[assistant] ──── Analyzes results and formulates response
  |
  ...  (loops until FINAL:)
```

### Protocol

The agent uses a structured output protocol:

- `TOOL: {"name": "execute_sql", "args": {"query": "SELECT ..."}}` - Calls a tool
- `FINAL: The revenue for DIPD in Q1 2024 was...` - Returns final answer to user

### Available tools

| Tool | Description |
|------|-------------|
| `execute_sql` | Executes SELECT queries against DuckDB (read-only, SELECT only) |
| `get_table_schema` | Returns column types and sample data for a table |
| `list_tables` | Lists all tables with row counts |

### Guardrails

Two layers of protection:

1. **Profanity filter**: Blocks messages containing offensive words (Portuguese + English)
2. **Code disclosure filter**: Blocks attempts to extract system prompts, architecture details, Docker configs, LangGraph implementation, etc.

### Sample questions

- "What was DIPD's revenue in the latest quarter?"
- "Compare the net income of both companies over the last 3 years"
- "Which company has higher gross profit margins?"
- "Show me the quarterly revenue trend for REXP"
- "What is the year-over-year growth in revenue for DIPD?"

### Features

- **Conversation memory**: Maintains context across messages (up to 10 exchanges)
- **Schema injection**: Full database schema with sample data is provided to the LLM
- **Few-shot examples**: Pre-configured SQL examples for common financial queries
- **Error handling**: SQL errors are fed back to the LLM for self-correction

---

## Observability

### LangSmith Tracing

All LLM calls are traced via the `@traceable` decorator from the LangSmith SDK:

- ETL extraction calls: `groq_chat` traces in `etl/src/llm_extractor.py`
- Query system calls: `groq_chat` traces in `query_system/src/graph.py`

Configure in `.env`:

```env
LANGCHAIN_TRACING_V2=true
LANGCHAIN_API_KEY=your_langsmith_key
LANGCHAIN_PROJECT=cse-financial-analysis
```

View traces at https://smith.langchain.com.

---

## Rate Limit Handling

The Groq free tier has strict limits (100K tokens/day for `llama-3.3-70b-versatile`). The solution handles this with:

1. **Delay between calls**: 3s between API calls, 5s between PDFs
2. **Smaller model for classification**: `llama-3.1-8b-instant` for page identification (~40% token savings)
3. **Automatic fallback**: When Groq returns 429, falls back to a configurable OpenAI-compatible API (Together AI, OpenRouter, etc.)
4. **Resumable ETL**: Already-processed PDFs are skipped on subsequent runs
5. **Graceful shutdown**: `DailyLimitExhausted` exception stops the pipeline cleanly when both APIs fail

---

## Testing

Three test suites are provided in `tests/`:

```bash
# Install test dependencies (if running outside Docker)
pip install duckdb pandas plotly groq openai langsmith langchain-core langgraph python-dotenv

# Test DuckDB data integrity (no API key needed)
python tests/test_duckdb.py

# Test dashboard data readers and chart builders (no API key needed)
python tests/test_dashboard.py

# Test query system (requires GROQ_API_KEY or FALLBACK_API_KEY)
python tests/test_query_system.py
```

| Suite | Tests | What it covers |
|-------|-------|----------------|
| `test_duckdb.py` | 45 | Schema, data completeness, arithmetic consistency, referential integrity |
| `test_dashboard.py` | 39 | db_reader functions, chart builders, data quality |
| `test_query_system.py` | 33 | Guardrails, graph components, tool execution, agent end-to-end |

---

## Limitations and Data Inconsistencies

### PDF Parsing Challenges
- Financial report PDFs have **varied formats** across years and companies
- Some reports have tables that `pdfplumber` cannot parse correctly, requiring `PyMuPDF` text fallback
- Cumulative vs single-quarter distinction depends on report text description

### LLM Extraction Limitations
- The LLM occasionally misreads numbers when table alignment is ambiguous
- Some reports have revenue and COGS in different units (thousands vs millions), causing arithmetic mismatches
- The LLM sometimes attempts arithmetic expressions in JSON output (e.g., `100+200+300`), which Groq's JSON mode rejects
- Validation catches most errors, but some extractions are accepted with warnings after max retries

### Rate Limits
- Groq free tier: 100K tokens/day for `llama-3.3-70b-versatile`
- Full ETL of ~40 quarterly reports requires ~2-3 daily token cycles or a fallback API
- The ETL is fully resumable and can be run across multiple days

### Data Coverage
- Some quarterly reports may produce standalone values computed from cumulative figures, which introduces small rounding differences
- Annual reports are skipped (too large for LLM context windows and quarterly data is the focus)

---

## Project Structure

```
arcade-coding-challenge/
├── docker-compose.yml          # Orchestrates all 4 containers
├── .env.example                # Environment variable template
├── .gitignore
├── README.md
│
├── scraper/                    # Container 1: PDF Scraper
│   ├── Dockerfile
│   ├── requirements.txt
│   └── src/
│       ├── main.py             # Entry point
│       ├── cse_client.py       # CSE API client
│       ├── pdf_downloader.py   # PDF download with retry
│       ├── manifest.py         # Manifest tracking
│       └── config.py           # Company configs
│
├── etl/                        # Container 2: LLM ETL Pipeline
│   ├── Dockerfile
│   ├── requirements.txt
│   └── src/
│       ├── main.py             # Entry point (resumable pipeline)
│       ├── graph.py            # LangGraph extraction graph
│       ├── llm_extractor.py    # Groq SDK + fallback + validation
│       ├── pdf_parser.py       # pdfplumber + PyMuPDF
│       ├── db_writer.py        # DuckDB schema + insertion
│       ├── schemas.py          # Pydantic models
│       └── config.py           # Model and path config
│
├── dashboard/                  # Container 3: Streamlit Dashboard
│   ├── Dockerfile
│   ├── requirements.txt
│   └── src/
│       ├── app.py              # Entry point (3-page navigation)
│       ├── db_reader.py        # DuckDB query helpers
│       ├── charts.py           # 5 Plotly chart builders
│       └── pages/
│           ├── overview.py     # Multi-company overview
│           ├── company_detail.py  # Single company deep-dive
│           └── comparison.py   # Side-by-side analysis
│
├── query_system/               # Container 4: LLM Query System
│   ├── Dockerfile
│   ├── requirements.txt
│   └── src/
│       ├── app.py              # Streamlit chat UI
│       ├── agent.py            # Stateful agent wrapper
│       ├── graph.py            # LangGraph Text2SQL graph
│       ├── guardrails.py       # Profanity + disclosure protection
│       ├── prompts.py          # System prompts + few-shot examples
│       └── config.py           # Model and limits config
│
├── tests/                      # Test suites
│   ├── test_duckdb.py          # Data integrity tests
│   ├── test_dashboard.py       # Dashboard component tests
│   └── test_query_system.py    # Query system tests
│
└── shared/                     # Bind-mounted shared volume (gitignored)
    ├── pdfs/
    ├── db/
    └── metadata/
```

---

## Technology Stack

| Category | Technology |
|----------|-----------|
| **Language** | Python 3.12 |
| **LLM** | Groq API (`llama-3.3-70b-versatile`, `llama-3.1-8b-instant`) |
| **Agent Framework** | LangGraph (StateGraph, ToolNode, tools_condition) |
| **Observability** | LangSmith SDK (`@traceable` decorator) |
| **Database** | DuckDB (embedded analytical database) |
| **PDF Parsing** | pdfplumber + PyMuPDF |
| **Dashboard** | Streamlit + Plotly |
| **Containerization** | Docker + Docker Compose |
| **Data Validation** | Pydantic v2 |
| **HTTP Client** | httpx (async) + tenacity (retry) |
