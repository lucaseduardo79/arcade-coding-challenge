"""
Eval for the Text2SQL query system.

Uses langsmith.evaluation.evaluate() when LANGCHAIN_API_KEY is set —
results are uploaded to LangSmith and linked to existing @traceable traces.
Falls back to an inline runner otherwise.

Three evaluators per case:
  keyword_coverage  — expected keywords present in the answer
  factual_accuracy  — agent's numbers match the reference SQL result
  no_refusal        — agent didn't refuse a legitimate question

Run from project root:
    python tests/eval_query_system.py
"""

import os
import re
import sys
import time

os.environ.setdefault("DB_PATH", "shared/db/financial_data.duckdb")

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "query_system"))

import duckdb

DB_PATH = os.environ.get("DB_PATH", "shared/db/financial_data.duckdb")


def _query_db(sql: str) -> list[dict]:
    conn = duckdb.connect(DB_PATH, read_only=True)
    try:
        return conn.execute(sql).fetchdf().to_dict(orient="records")
    finally:
        conn.close()


def _extract_numbers(text: str) -> list[float]:
    """Pull every numeric value out of a string."""
    nums = []
    for m in re.findall(r"[\d,]+(?:\.\d+)?", text):
        try:
            nums.append(float(m.replace(",", "")))
        except ValueError:
            pass
    return nums


# ── Eval dataset ──────────────────────────────────────────────────────────────
# Each case: inputs (question) + outputs (reference_sql, expected_keywords).
# reference_sql is run at eval time to get the expected numeric value.
# None means the question is conceptual (no DB lookup needed).

EVAL_CASES = [
    {
        "inputs": {"question": "How many companies are in the database?"},
        "outputs": {
            "reference_sql": "SELECT COUNT(*) as n FROM companies",
            "expected_keywords": ["2"],
        },
    },
    {
        "inputs": {"question": "What are the company IDs available?"},
        "outputs": {
            "reference_sql": "SELECT company_id FROM companies ORDER BY company_id",
            "expected_keywords": ["DIPD", "REXP"],
        },
    },
    {
        "inputs": {"question": "What is DIPD's most recent quarterly revenue?"},
        "outputs": {
            "reference_sql": (
                "SELECT revenue FROM quarterly_standalone "
                "WHERE company_id = 'DIPD' ORDER BY period_end DESC LIMIT 1"
            ),
            "expected_keywords": ["DIPD", "revenue"],
        },
    },
    {
        "inputs": {"question": "What is REXP's most recent quarterly net income?"},
        "outputs": {
            "reference_sql": (
                "SELECT net_income FROM quarterly_standalone "
                "WHERE company_id = 'REXP' ORDER BY period_end DESC LIMIT 1"
            ),
            "expected_keywords": ["REXP", "net income"],
        },
    },
    {
        "inputs": {
            "question": "Which company has the higher average gross profit margin across all quarters?"
        },
        "outputs": {
            "reference_sql": (
                "SELECT company_id, ROUND(AVG(gross_profit / NULLIF(revenue,0) * 100), 2) as m "
                "FROM quarterly_standalone GROUP BY company_id ORDER BY m DESC LIMIT 1"
            ),
            "expected_keywords": ["gross profit", "margin"],
        },
    },
    {
        "inputs": {
            "question": "What was DIPD's gross profit in the quarter ended June 2024?"
        },
        "outputs": {
            "reference_sql": (
                "SELECT gross_profit FROM quarterly_standalone "
                "WHERE company_id = 'DIPD' AND period_end = '2024-06-30' LIMIT 1"
            ),
            "expected_keywords": ["DIPD", "gross profit"],
        },
    },
    {
        "inputs": {
            "question": "Show the last 4 quarters of REXP's operating income."
        },
        "outputs": {
            "reference_sql": (
                "SELECT fiscal_year, fiscal_quarter, operating_income "
                "FROM quarterly_standalone WHERE company_id = 'REXP' "
                "ORDER BY period_end DESC LIMIT 4"
            ),
            "expected_keywords": ["REXP", "operating"],
        },
    },
    {
        "inputs": {
            "question": "Compare the revenue of DIPD and REXP in their most recent quarter."
        },
        "outputs": {
            "reference_sql": (
                "SELECT company_id, revenue FROM quarterly_standalone "
                "WHERE period_end = (SELECT MAX(period_end) FROM quarterly_standalone) "
                "ORDER BY company_id"
            ),
            "expected_keywords": ["DIPD", "REXP", "revenue"],
        },
    },
    {
        "inputs": {"question": "What is gross profit?"},
        "outputs": {
            "reference_sql": None,
            "expected_keywords": ["revenue", "cost"],
        },
    },
    {
        "inputs": {
            "question": "What was DIPD's revenue in the quarter ended September 2023?"
        },
        "outputs": {
            "reference_sql": (
                "SELECT revenue FROM quarterly_standalone "
                "WHERE company_id = 'DIPD' AND period_end = '2023-09-30' LIMIT 1"
            ),
            "expected_keywords": ["DIPD", "revenue"],
        },
    },
]


# ── Evaluators ────────────────────────────────────────────────────────────────

def keyword_coverage(run, example):
    """Fraction of expected keywords found in the answer."""
    answer = (run.outputs or {}).get("answer", "").lower()
    keywords = (example.outputs or {}).get("expected_keywords", [])
    if not keywords:
        return {"key": "keyword_coverage", "score": 1.0}
    hits = sum(1 for kw in keywords if kw.lower() in answer)
    return {"key": "keyword_coverage", "score": hits / len(keywords)}


def factual_accuracy(run, example):
    """
    Checks whether the agent's answer contains numbers matching the reference SQL.
    Applies scale-tolerance (e.g. millions vs thousands) and 10% numeric tolerance.
    """
    ref_sql = (example.outputs or {}).get("reference_sql")
    if not ref_sql:
        return {"key": "factual_accuracy", "score": 1.0, "comment": "conceptual question"}

    try:
        records = _query_db(ref_sql)
    except Exception as e:
        return {"key": "factual_accuracy", "score": None, "comment": f"db error: {e}"}

    if not records:
        return {"key": "factual_accuracy", "score": None, "comment": "reference SQL empty"}

    answer = (run.outputs or {}).get("answer", "")
    answer_nums = _extract_numbers(answer)

    for record in records[:2]:
        for val in record.values():
            if val is None:
                continue
            val_str = str(val)
            # String match (e.g. company_id = "DIPD")
            if not any(c.isdigit() for c in val_str):
                if val_str.lower() in answer.lower():
                    return {"key": "factual_accuracy", "score": 1.0}
                continue
            try:
                expected = float(val_str.replace(",", ""))
            except ValueError:
                continue
            if expected == 0:
                continue
            for ans_num in answer_nums:
                # Direct match (within 10%)
                if abs(ans_num - expected) / abs(expected) <= 0.10:
                    return {"key": "factual_accuracy", "score": 1.0}
                # Scale-tolerance: thousands ↔ millions ↔ billions
                for scale in (1_000, 1_000_000, 0.001, 0.000001):
                    if abs(ans_num * scale - expected) / abs(expected) <= 0.10:
                        return {"key": "factual_accuracy", "score": 1.0}

    # Answer exists but number unconfirmed (agent may have reformatted)
    if answer.strip():
        return {"key": "factual_accuracy", "score": 0.5, "comment": "answer present but number unverified"}
    return {"key": "factual_accuracy", "score": 0.0, "comment": "empty answer"}


def no_refusal(run, example):
    """Penalises answers where the agent refused to engage with a valid question."""
    answer = (run.outputs or {}).get("answer", "").lower()
    hard_refusals = [
        "i cannot answer", "i can't answer", "i'm not able to answer",
        "i cannot help", "i can't help with", "i cannot assist",
        "unable to answer", "cannot provide an answer",
    ]
    for phrase in hard_refusals:
        if phrase in answer:
            return {"key": "no_refusal", "score": 0.0, "comment": f"refusal: '{phrase}'"}
    return {"key": "no_refusal", "score": 1.0}


# ── Runners ───────────────────────────────────────────────────────────────────

def run_with_langsmith():
    from langsmith import Client
    from langsmith.evaluation import evaluate as ls_evaluate
    from src.agent import FinancialQueryAgent

    client = Client()
    dataset_name = "CSE Financial Query System Eval"

    # Recreate dataset so it always reflects current EVAL_CASES
    datasets = list(client.list_datasets(dataset_name=dataset_name))
    if datasets:
        dataset = datasets[0]
        for ex in client.list_examples(dataset_id=dataset.id):
            client.delete_example(ex.id)
    else:
        dataset = client.create_dataset(dataset_name)

    client.create_examples(
        inputs=[c["inputs"] for c in EVAL_CASES],
        outputs=[c["outputs"] for c in EVAL_CASES],
        dataset_id=dataset.id,
    )

    def target(inputs: dict) -> dict:
        agent = FinancialQueryAgent()
        return {"answer": agent.query(inputs["question"])}

    results = ls_evaluate(
        target,
        data=dataset.id,
        evaluators=[keyword_coverage, factual_accuracy, no_refusal],
        experiment_prefix="cse-query-system",
        client=client,
        max_concurrency=1,  # sequential — avoids Groq rate limits
    )

    print("\nLangSmith eval complete.")
    print("Results summary:")
    try:
        df = results.to_pandas()
        for metric in ["keyword_coverage", "factual_accuracy", "no_refusal"]:
            col = [c for c in df.columns if metric in c]
            if col:
                print(f"  {metric}: {df[col[0]].mean():.1%}")
    except Exception:
        print("  (view detailed results at https://smith.langchain.com)")


def run_inline():
    from src.agent import FinancialQueryAgent

    print("=" * 60)
    print("Query System Eval  (inline mode — no LangSmith)")
    print("=" * 60)

    scores: dict[str, list[float]] = {
        "keyword_coverage": [],
        "factual_accuracy": [],
        "no_refusal": [],
    }

    for i, case in enumerate(EVAL_CASES, 1):
        question = case["inputs"]["question"]
        print(f"\n[{i:02d}/{len(EVAL_CASES)}] {question[:65]}")

        agent = FinancialQueryAgent()
        try:
            answer = agent.query(question)
        except Exception as e:
            print(f"  ERROR: {e}")
            for k in scores:
                scores[k].append(0.0)
            time.sleep(3)
            continue

        class _Run:
            outputs = {"answer": answer}
        class _Example:
            outputs = case["outputs"]

        run, example = _Run(), _Example()

        kw = keyword_coverage(run, example)
        fa = factual_accuracy(run, example)
        nr = no_refusal(run, example)

        scores["keyword_coverage"].append(kw["score"] if kw["score"] is not None else 0.0)
        scores["factual_accuracy"].append(fa["score"] if fa["score"] is not None else 0.0)
        scores["no_refusal"].append(nr["score"])

        print(f"  Answer: {answer[:80].strip()}...")
        kw_note = f"{kw['score']:.0%}" if kw["score"] is not None else "n/a"
        fa_note = f"{fa['score']:.0%}" if fa["score"] is not None else "n/a"
        print(f"  keywords={kw_note}  factual={fa_note}  no_refusal={nr['score']:.0%}")
        if "comment" in fa:
            print(f"  factual note: {fa['comment']}")

        time.sleep(3)  # rate-limit protection

    print("\n" + "=" * 60)
    print("Summary:")
    overall = []
    for metric, vals in scores.items():
        avg = sum(vals) / len(vals) if vals else 0.0
        overall.append(avg)
        bar = "#" * int(avg * 10) + "." * (10 - int(avg * 10))
        print(f"  {bar} {avg:.1%}  {metric}")
    grand = sum(overall) / len(overall) if overall else 0.0
    print(f"\n  Overall average: {grand:.1%}")
    print("=" * 60)


if __name__ == "__main__":
    api_key = os.environ.get("GROQ_API_KEY", "") or os.environ.get("FALLBACK_API_KEY", "")
    if not api_key:
        print("No GROQ_API_KEY or FALLBACK_API_KEY set — skipping query system eval.")
        sys.exit(0)

    if os.environ.get("LANGCHAIN_API_KEY"):
        print("LANGCHAIN_API_KEY found — uploading results to LangSmith.")
        run_with_langsmith()
    else:
        print("No LANGCHAIN_API_KEY — running inline eval.")
        run_inline()
