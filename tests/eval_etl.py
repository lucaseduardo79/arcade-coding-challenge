"""
Eval for the LLM extractor (ETL pipeline).

Runs extract_pl_data() on three manually constructed fixture texts
and compares each extracted field against a verified ground truth.

Metrics:
  - Field accuracy per fixture (% fields within 2% tolerance)
  - Aggregate field accuracy across all fixtures
  - Per-field breakdown so you can spot systematic errors

Requires GROQ_API_KEY (or FALLBACK_API_KEY).

Run from project root:
    python tests/eval_etl.py
"""

import os
import sys
import time

os.environ.setdefault("DB_PATH", "shared/db/financial_data.duckdb")

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "etl"))


# ── Fixture texts + ground truth ──────────────────────────────────────────────
# All monetary values already in LKR Thousands unless noted.
# Ground truth verified by manual arithmetic from the fixture text.

FIXTURES = [
    {
        "description": "Single quarter Q1 (Rs. '000)",
        "company_id": "EVAL_CO",
        "text": """\
STATEMENT OF PROFIT OR LOSS
For the three months ended 30th June 2024
(All amounts in Rs. '000)

                                    Three months ended
                                    30.06.2024    30.06.2023
Revenue                               450,000       380,000
Cost of sales                        (310,000)     (265,000)
Gross Profit                          140,000       115,000
Distribution costs                    (15,000)      (12,000)
Administrative expenses               (35,000)      (30,000)
Other income                            5,000         4,000
Profit from operations                 95,000        77,000
Finance charges                        (12,000)      (10,000)
Profit before taxation                  83,000        67,000
Income tax expense                     (20,000)      (16,000)
Net profit for the period              63,000        51,000
""",
        "ground_truth": {
            "period_end_date": "2024-06-30",
            "period_months": 3,
            "is_cumulative": False,
            "revenue": 450_000.0,
            "cost_of_goods_sold": 310_000.0,
            "gross_profit": 140_000.0,
            "other_income": 5_000.0,
            "operating_expenses": 50_000.0,   # 15k dist + 35k admin
            "operating_income": 95_000.0,
            "finance_costs": 12_000.0,
            "profit_before_tax": 83_000.0,
            "income_tax_expense": 20_000.0,
            "net_income": 63_000.0,
        },
    },
    {
        "description": "Cumulative 6-month half-year (Rs. '000)",
        "company_id": "EVAL_CO",
        "text": """\
STATEMENT OF COMPREHENSIVE INCOME
For the six months ended 30th September 2024
(Amounts in Rs. '000)

                                    Six months ended    Six months ended
                                    30.09.2024          30.09.2023
Turnover                              920,000             780,000
Cost of goods sold                   (630,000)           (540,000)
Gross Profit                          290,000             240,000
Selling & distribution expenses       (32,000)            (27,000)
General & administration expenses     (68,000)            (58,000)
Other income                            8,500               7,200
Profit from operations                198,500             162,200
Finance income                          3,200               2,800
Finance charges                        (22,000)            (19,000)
Profit before taxation                179,700             146,000
Taxation                              (44,000)            (36,000)
Net profit for the period             135,700             110,000
""",
        "ground_truth": {
            "period_end_date": "2024-09-30",
            "period_months": 6,
            "is_cumulative": True,
            "revenue": 920_000.0,
            "cost_of_goods_sold": 630_000.0,
            "gross_profit": 290_000.0,
            "other_income": 8_500.0,
            "operating_expenses": 100_000.0,  # 32k + 68k
            "operating_income": 198_500.0,
            "finance_income": 3_200.0,
            "finance_costs": 22_000.0,
            "profit_before_tax": 179_700.0,
            "income_tax_expense": 44_000.0,
            "net_income": 135_700.0,
        },
    },
    {
        "description": "Annual statement (Rs. Millions — must be normalized to thousands)",
        "company_id": "EVAL_CO",
        "text": """\
INCOME STATEMENT
For the year ended 31st March 2024
(In Rupees Millions)

                                    Year ended      Year ended
                                    31.03.2024      31.03.2023
Revenue                                 2,450           2,100
Cost of sales                          (1,680)         (1,450)
Gross profit                              770             650
Distribution costs                        (85)            (72)
Administrative expenses                  (195)           (168)
Other income                               22              18
Operating profit                          512             428
Finance income                             15              12
Finance costs                             (65)            (55)
Profit before income tax                  462             385
Income tax expense                       (115)            (96)
Net profit for the year                   347             289
""",
        # Ground truth in thousands (millions × 1000)
        "ground_truth": {
            "period_end_date": "2024-03-31",
            "period_months": 12,
            "is_cumulative": True,
            "revenue": 2_450_000.0,
            "cost_of_goods_sold": 1_680_000.0,
            "gross_profit": 770_000.0,
            "other_income": 22_000.0,
            "operating_expenses": 280_000.0,  # 85k + 195k (in thousands after norm)
            "operating_income": 512_000.0,
            "finance_income": 15_000.0,
            "finance_costs": 65_000.0,
            "profit_before_tax": 462_000.0,
            "income_tax_expense": 115_000.0,
            "net_income": 347_000.0,
        },
    },
]

NUMERIC_FIELDS = [
    "revenue", "cost_of_goods_sold", "gross_profit", "other_income",
    "operating_expenses", "operating_income", "finance_income",
    "finance_costs", "profit_before_tax", "income_tax_expense", "net_income",
]

CATEGORICAL_FIELDS = ["period_end_date", "period_months", "is_cumulative"]

NUMERIC_TOLERANCE = 0.02  # 2%


def _compare_field(field: str, expected, actual) -> dict:
    """Return a result dict for one field comparison."""
    if expected is None:
        return {"expected": expected, "actual": actual, "correct": None, "skipped": True}
    if actual is None:
        return {"expected": expected, "actual": actual, "correct": False}

    if field in CATEGORICAL_FIELDS:
        match = str(actual) == str(expected)
        return {"expected": expected, "actual": actual, "correct": match}

    # Numeric
    try:
        exp_f, act_f = float(expected), float(actual)
    except (TypeError, ValueError):
        return {"expected": expected, "actual": actual, "correct": False}

    if exp_f == 0:
        correct = act_f == 0
        pct_err = 0.0 if correct else float("inf")
    else:
        pct_err = abs(act_f - exp_f) / abs(exp_f)
        correct = pct_err <= NUMERIC_TOLERANCE

    return {"expected": exp_f, "actual": act_f, "correct": correct, "pct_error": pct_err}


def evaluate_fixture(fixture: dict) -> dict:
    from src.llm_extractor import extract_pl_data, normalize_to_thousands

    extraction = extract_pl_data(fixture["text"], fixture["company_id"])
    extraction = normalize_to_thousands(extraction)

    gt = fixture["ground_truth"]
    results = {}

    for field in CATEGORICAL_FIELDS + NUMERIC_FIELDS:
        expected = gt.get(field)
        actual = getattr(extraction, field, None)
        results[field] = _compare_field(field, expected, actual)

    return results


def run_eval():
    api_key = os.environ.get("GROQ_API_KEY", "") or os.environ.get("FALLBACK_API_KEY", "")
    if not api_key:
        print("No GROQ_API_KEY or FALLBACK_API_KEY — skipping ETL eval.")
        sys.exit(0)

    print("=" * 60)
    print("ETL Extractor Eval")
    print("=" * 60)

    all_results: list[dict] = []

    for i, fixture in enumerate(FIXTURES, 1):
        print(f"\n[{i}/{len(FIXTURES)}] {fixture['description']}")

        try:
            results = evaluate_fixture(fixture)
        except Exception as e:
            print(f"  ERROR: {e}")
            time.sleep(3)
            continue

        all_results.append(results)

        n_correct = sum(1 for r in results.values() if r.get("correct") is True)
        n_total   = sum(1 for r in results.values() if r.get("correct") is not None)
        n_skip    = sum(1 for r in results.values() if r.get("skipped"))
        pct = n_correct / n_total if n_total else 0.0

        print(f"  Accuracy: {n_correct}/{n_total} fields correct ({pct:.0%})"
              + (f"  ({n_skip} skipped)" if n_skip else ""))

        for field, r in results.items():
            if r.get("correct") is False:
                pe = r.get("pct_error", 0)
                pe_str = f"  ({pe:.1%} off)" if pe and pe != float("inf") else ""
                print(f"  WRONG  {field:22s}  expected={r['expected']}  got={r['actual']}{pe_str}")

        time.sleep(3)  # rate-limit protection

    # ── Aggregate ─────────────────────────────────────────────────────────────
    if not all_results:
        print("\nNo results to aggregate.")
        return

    print("\n" + "=" * 60)
    print("Aggregate field accuracy:")

    field_stats: dict[str, dict] = {}
    for results in all_results:
        for field, r in results.items():
            if r.get("correct") is None:
                continue
            s = field_stats.setdefault(field, {"correct": 0, "total": 0, "errors": []})
            s["correct"] += int(r["correct"])
            s["total"] += 1
            if not r["correct"] and "pct_error" in r:
                s["errors"].append(r["pct_error"])

    overall_c = overall_t = 0
    for field in CATEGORICAL_FIELDS + NUMERIC_FIELDS:
        if field not in field_stats:
            continue
        s = field_stats[field]
        pct = s["correct"] / s["total"]
        bar = "#" * int(pct * 10) + "." * (10 - int(pct * 10))
        avg_err = f"  avg_err={sum(s['errors'])/len(s['errors']):.1%}" if s["errors"] else ""
        print(f"  {bar} {pct:.0%}  {field}{avg_err}")
        overall_c += s["correct"]
        overall_t += s["total"]

    overall = overall_c / overall_t if overall_t else 0.0
    print(f"\n  Overall field accuracy: {overall:.1%}  ({overall_c}/{overall_t})")
    print("=" * 60)


if __name__ == "__main__":
    run_eval()
