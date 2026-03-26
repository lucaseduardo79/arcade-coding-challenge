"""LangGraph Text2SQL agent following raw Groq SDK pattern with guardrails and fallback."""

import os
import json
import logging
from uuid import uuid4

from groq import Groq
from openai import OpenAI
from langsmith import traceable
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
from langchain_core.tools import tool
from langgraph.graph import START, END, StateGraph, MessagesState
from langgraph.prebuilt import ToolNode, tools_condition

from .config import GROQ_MODEL, GROQ_TEMPERATURE, DB_PATH, MAX_QUERY_ROWS
from .prompts import SYSTEM_PROMPT, FEW_SHOT_EXAMPLES
from .guardrails import run_guardrails

logger = logging.getLogger(__name__)

# ── Primary: Groq client ──
groq_client = Groq()

# ── Fallback: OpenAI-compatible client (OpenRouter, Together, OpenAI, etc.) ──
FALLBACK_API_KEY = os.environ.get("FALLBACK_API_KEY", "")
FALLBACK_BASE_URL = os.environ.get("FALLBACK_BASE_URL", "https://openrouter.ai/api/v1")
FALLBACK_MODEL = os.environ.get("FALLBACK_MODEL", "meta-llama/llama-3.3-70b-instruct")

fallback_client = OpenAI(
    api_key=FALLBACK_API_KEY,
    base_url=FALLBACK_BASE_URL,
) if FALLBACK_API_KEY else None

if fallback_client:
    logger.info(f"Fallback configured: {FALLBACK_BASE_URL} key={FALLBACK_API_KEY[:8]}... model={FALLBACK_MODEL}")
else:
    logger.warning("No fallback API configured (FALLBACK_API_KEY is empty)")


@traceable(run_type="llm", name="groq_chat")
def groq_chat(messages, model=GROQ_MODEL, temperature=GROQ_TEMPERATURE,
              max_completion_tokens=2000, top_p=1.0) -> str:
    """Call Groq API with fallback (traced by LangSmith via @traceable)."""
    kwargs = dict(
        model=model,
        messages=messages,
        temperature=temperature,
        max_completion_tokens=max_completion_tokens,
        top_p=top_p,
        stream=False,
    )

    try:
        completion = groq_client.chat.completions.create(**kwargs)
        return completion.choices[0].message.content or ""
    except Exception as e:
        error_str = str(e)
        if "429" in error_str or "rate_limit" in error_str or "tokens per" in error_str:
            logger.warning(f"Groq rate limited: {error_str[:120]}. Trying fallback...")
            if fallback_client:
                try:
                    fb_kwargs = {**kwargs, "model": FALLBACK_MODEL}
                    logger.info(f"Using fallback: {FALLBACK_BASE_URL} model={FALLBACK_MODEL}")
                    completion = fallback_client.chat.completions.create(**fb_kwargs)
                    return completion.choices[0].message.content or ""
                except Exception as fb_err:
                    logger.error(f"Fallback also failed: {fb_err}")
                    raise
            else:
                logger.error("No fallback configured. Set FALLBACK_API_KEY env var.")
                raise
        raise


# ── Tools ──
import duckdb


@tool
def execute_sql(query: str) -> str:
    """Execute a SELECT SQL query against the financial database and return results."""
    if not query.strip().upper().startswith("SELECT"):
        return "Error: Only SELECT queries are allowed."
    conn = duckdb.connect(DB_PATH, read_only=True)
    try:
        result = conn.execute(query).fetchdf()
        if len(result) == 0:
            return "Query returned no results."
        if len(result) > MAX_QUERY_ROWS:
            return (
                f"Showing first {MAX_QUERY_ROWS} of {len(result)} rows:\n"
                f"{result.head(MAX_QUERY_ROWS).to_string(index=False)}"
            )
        return result.to_string(index=False)
    except Exception as e:
        return f"SQL Error: {str(e)}"
    finally:
        conn.close()


@tool
def get_table_schema(table_name: str) -> str:
    """Get the schema (columns, types) and sample rows for a specific table.
    Available tables: companies, report_sources, income_statement, quarterly_standalone"""
    allowed = {"companies", "report_sources", "income_statement", "quarterly_standalone"}
    if table_name not in allowed:
        return f"Unknown table. Available: {', '.join(sorted(allowed))}"
    conn = duckdb.connect(DB_PATH, read_only=True)
    try:
        schema = conn.execute(f"DESCRIBE {table_name}").fetchdf().to_string(index=False)
        sample = conn.execute(f"SELECT * FROM {table_name} LIMIT 3").fetchdf().to_string(index=False)
        row_count = conn.execute(f"SELECT COUNT(*) as cnt FROM {table_name}").fetchone()[0]
        return f"Table: {table_name} ({row_count} rows)\n\nSchema:\n{schema}\n\nSample:\n{sample}"
    except Exception as e:
        return f"Error: {str(e)}"
    finally:
        conn.close()


@tool
def list_tables() -> str:
    """List all tables in the financial database with row counts."""
    conn = duckdb.connect(DB_PATH, read_only=True)
    try:
        tables = conn.execute("SHOW TABLES").fetchdf()
        result = []
        for _, row in tables.iterrows():
            name = row.iloc[0]
            count = conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
            result.append(f"- {name}: {count} rows")
        return "Database tables:\n" + "\n".join(result)
    except Exception as e:
        return f"Error: {str(e)}"
    finally:
        conn.close()


tools = [execute_sql, get_table_schema, list_tables]
TOOL_NAMES = {t.name for t in tools}


# ── Schema loader (cached) ──
_schema_cache = None


def _get_schema_text() -> str:
    global _schema_cache
    if _schema_cache is not None:
        return _schema_cache
    conn = duckdb.connect(DB_PATH, read_only=True)
    try:
        tables_df = conn.execute("SHOW TABLES").fetchdf()
        parts = []
        for _, row in tables_df.iterrows():
            name = row.iloc[0]
            schema = conn.execute(f"DESCRIBE {name}").fetchdf()
            cols = ", ".join(
                f"{r['column_name']} ({r['column_type']})"
                for _, r in schema.iterrows()
            )
            count = conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
            sample = conn.execute(f"SELECT * FROM {name} LIMIT 2").fetchdf().to_string(index=False)
            parts.append(f"**{name}** ({count} rows): {cols}\nSample:\n{sample}")
        _schema_cache = "\n\n".join(parts)
        return _schema_cache
    except Exception as e:
        return f"Schema unavailable: {e}"
    finally:
        conn.close()


# ── Message conversion (LangChain → Groq raw format) ──

def lc_messages_to_groq(messages):
    """Convert LangChain message objects to Groq API dict format."""
    out = []
    for m in messages:
        if isinstance(m, SystemMessage):
            out.append({"role": "system", "content": m.content})
        elif isinstance(m, HumanMessage):
            out.append({"role": "user", "content": m.content})
        elif isinstance(m, ToolMessage):
            tool_name = getattr(m, "name", "tool")
            out.append({
                "role": "assistant",
                "content": f"[TOOL_RESULT name={tool_name} tool_call_id={m.tool_call_id}] {m.content}",
            })
        elif isinstance(m, AIMessage):
            out.append({"role": "assistant", "content": m.content})
        else:
            out.append({"role": "assistant", "content": str(getattr(m, "content", m))})
    return out


# ── LLM output parser ──

def parse_llm_output(text: str):
    """Parse structured LLM output: TOOL: {...} or FINAL: ..."""
    if not text:
        return "error", None
    text = text.strip()
    if text.startswith("TOOL:"):
        try:
            return "tool", json.loads(text[len("TOOL:"):].strip())
        except json.JSONDecodeError:
            return "error", None
    if text.startswith("FINAL:"):
        return "final", text[len("FINAL:"):].strip()
    # Fallback: treat unstructured text as final answer
    return "final", text


# ── Graph nodes ──

def guardrails_node(state: MessagesState):
    """Check user message against guardrails before processing."""
    messages = state["messages"]
    # Find the last user message
    last_user = None
    for m in reversed(messages):
        if isinstance(m, HumanMessage):
            last_user = m.content
            break

    if last_user is None:
        return {"messages": []}

    block_msg = run_guardrails(last_user)
    if block_msg:
        logger.warning(f"Guardrail triggered for: {last_user[:50]}...")
        return {"messages": [AIMessage(content=block_msg)]}

    # Pass through - no new messages added
    return {"messages": []}


def assistant(state: MessagesState):
    """Main assistant node - calls Groq and decides TOOL or FINAL."""
    schema_text = _get_schema_text()
    system_prompt = SYSTEM_PROMPT.format(schema=schema_text)

    messages = [SystemMessage(content=system_prompt)] + state["messages"]
    groq_messages = lc_messages_to_groq(messages)

    llm_text = groq_chat(groq_messages)
    kind, payload = parse_llm_output(llm_text)

    if kind == "tool":
        name = payload.get("name")
        args = payload.get("args", {})
        if not name or name not in TOOL_NAMES:
            return {
                "messages": [
                    AIMessage(content=f"I tried to use an invalid tool. Let me try again with the correct approach.")
                ]
            }
        tc = {"name": name, "args": args, "id": str(uuid4())}
        return {"messages": [AIMessage(content="", tool_calls=[tc])]}

    if kind == "final":
        return {"messages": [AIMessage(content=payload)]}

    # Fallback
    return {"messages": [AIMessage(content=llm_text)]}


def guardrails_router(state: MessagesState) -> str:
    """Route after guardrails: if blocked (AI message added), go to END; otherwise to assistant."""
    last_msg = state["messages"][-1] if state["messages"] else None
    if isinstance(last_msg, AIMessage):
        return "end"
    return "assistant"


# ── Build graph ──

builder = StateGraph(MessagesState)

builder.add_node("guardrails", guardrails_node)
builder.add_node("assistant", assistant)
builder.add_node("tools", ToolNode(tools))

builder.add_edge(START, "guardrails")
builder.add_conditional_edges(
    "guardrails",
    guardrails_router,
    {"assistant": "assistant", "end": END},
)
builder.add_conditional_edges("assistant", tools_condition)
builder.add_edge("tools", "assistant")

graph = builder.compile()
