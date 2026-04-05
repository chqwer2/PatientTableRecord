#!/usr/bin/env python3
"""
Data Cleaning Agent — PatientTables

Architecture (token-efficient):
  Phase 1 — Preprocessing   Python/pandas   GBK → UTF-8, register DuckDB views
  Phase 2 — Analysis        Python/DuckDB   all standard checks (no Claude)
  Phase 3 — Cleaning        Python/DuckDB   apply rules deterministically
  Phase 4 — Interpretation  ONE Claude call receive full findings, write report
                            (+ optional run_sql tool if Claude needs to dig deeper)
  Phase 5 — Save            write timestamped Markdown report
"""
import json
import time
import threading
from datetime import datetime
from pathlib import Path

import anthropic
import duckdb
import pandas as pd

# ─── Paths ─────────────────────────────────────────────────────────────────────
PATH    = Path("/Users/haochen/Desktop/PatientTables")
TMP     = PATH / "tmp"
CLEANED = PATH / "cleaned"
TMP.mkdir(exist_ok=True)
CLEANED.mkdir(exist_ok=True)

con         = duckdb.connect()
agent_start = time.perf_counter()
timing_log: list[tuple[str, float]] = []

# Opus 4.6 list pricing
_PRICE_IN  = 5.00  / 1_000_000
_PRICE_OUT = 25.00 / 1_000_000


def tlog(label: str, elapsed: float) -> None:
    timing_log.append((label, elapsed))
    print(f"    ⏱  {label}: {elapsed:.2f}s")


def _fmt_time(s: float) -> str:
    return f"{s:.0f}s" if s < 60 else f"{int(s//60)}m {int(s%60)}s"


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 1 — Preprocessing
# ═══════════════════════════════════════════════════════════════════════════════
def setup_data() -> None:
    print("=" * 62)
    print("  DATA CLEANING AGENT — PatientTables")
    print(f"  Started : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 62)
    print("\n[Phase 1]  Preprocessing source files…")

    CSV_COLS = ["姓名_x", "性别_x", "年龄", "体检日期_x"]
    for year in ["2022", "2023", "2024"]:
        dst = TMP / f"{year}_keys.csv"
        if dst.exists():
            print(f"  {year}_keys.csv cached — skipped")
            continue
        t0 = time.perf_counter()
        chunks = [
            chunk for chunk in pd.read_csv(
                PATH / f"{year}.csv", encoding="gbk",
                usecols=CSV_COLS, chunksize=50_000
            )
        ]
        pd.concat(chunks, ignore_index=True).to_csv(dst, index=False, encoding="utf-8-sig")
        tlog(f"extract {year}.csv", time.perf_counter() - t0)

    stroke_csv = TMP / "stroke_registry.csv"
    if not stroke_csv.exists():
        t0 = time.perf_counter()
        df = pd.read_excel(PATH / "2022-2024.xlsx", sheet_name="脑卒中")
        df.to_csv(stroke_csv, index=False, encoding="utf-8-sig")
        tlog("convert xlsx", time.perf_counter() - t0)
    else:
        print("  stroke_registry.csv cached — skipped")

    for year in ["2022", "2023", "2024"]:
        con.execute(f"""
            CREATE OR REPLACE VIEW v{year} AS
            SELECT "姓名_x" AS name, "性别_x" AS gender,
                   TRY_CAST("年龄" AS INTEGER) AS age,
                   "体检日期_x" AS exam_date
            FROM read_csv_auto('{TMP}/{year}_keys.csv')
            WHERE "姓名_x" IS NOT NULL AND TRIM("姓名_x") != ''
        """)

    con.execute(f"""
        CREATE OR REPLACE VIEW stroke AS
        SELECT "姓名" AS name,
               CASE WHEN "性别" LIKE '%男%' THEN '男'
                    WHEN "性别" LIKE '%女%' THEN '女'
                    ELSE "性别" END AS gender,
               "出生日期"  AS dob,
               TRY_CAST(LEFT(CAST("出生日期" AS VARCHAR), 4) AS INTEGER) AS birth_year,
               "年度" AS year, "脑卒中诊断" AS stroke_diagnosis
        FROM read_csv_auto('{TMP}/stroke_registry.csv')
        WHERE "姓名" IS NOT NULL AND TRIM("姓名") != ''
    """)
    print("  Views ready: stroke, v2022, v2023, v2024\n")


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 2 — Programmatic analysis  (all Python/DuckDB, zero Claude tokens)
# ═══════════════════════════════════════════════════════════════════════════════
def run_analysis() -> dict:
    print("[Phase 2]  Running data analysis (Python/DuckDB)…")
    t0 = time.perf_counter()

    findings: dict = {}

    # ── 2a. Row counts ─────────────────────────────────────────────────────────
    findings["row_counts"] = {
        tbl: con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        for tbl in ["stroke", "v2022", "v2023", "v2024"]
    }
    findings["unique_name_gender"] = {
        tbl: con.execute(
            f"SELECT COUNT(DISTINCT name || '|' || COALESCE(gender,'')) FROM {tbl}"
        ).fetchone()[0]
        for tbl in ["stroke", "v2022", "v2023", "v2024"]
    }

    # ── 2b. Null / blank names ─────────────────────────────────────────────────
    findings["null_names"] = {}
    for tbl in ["stroke", "v2022", "v2023", "v2024"]:
        n = con.execute(
            f"SELECT COUNT(*) FROM {tbl} WHERE name IS NULL OR TRIM(name)=''"
        ).fetchone()[0]
        findings["null_names"][tbl] = n

    # ── 2c. Gender distribution ────────────────────────────────────────────────
    findings["gender_dist"] = {}
    for tbl in ["stroke", "v2022", "v2023", "v2024"]:
        dist = con.execute(f"""
            SELECT gender, COUNT(*) AS cnt
            FROM {tbl} GROUP BY gender ORDER BY cnt DESC
        """).fetchdf().to_dict("records")
        findings["gender_dist"][tbl] = dist

    # ── 2d. Duplicates on name + gender ────────────────────────────────────────
    findings["duplicates"] = {}
    for tbl in ["stroke", "v2022", "v2023", "v2024"]:
        row = con.execute(f"""
            SELECT COUNT(*) AS dup_groups, SUM(cnt-1) AS excess_rows
            FROM (SELECT name, gender, COUNT(*) AS cnt
                  FROM {tbl} GROUP BY name, gender HAVING COUNT(*) > 1)
        """).fetchone()
        findings["duplicates"][tbl] = {
            "duplicate_groups": row[0], "excess_rows": row[1]
        }

    # ── 2e. Age statistics and outliers (CSV tables only) ─────────────────────
    findings["age_stats"] = {}
    for tbl in ["v2022", "v2023", "v2024"]:
        row = con.execute(f"""
            SELECT MIN(age), MAX(age), ROUND(AVG(age),1), MEDIAN(age),
                   COUNT(*) FILTER (WHERE age IS NULL)        AS null_age,
                   COUNT(*) FILTER (WHERE age < 0 OR age > 120) AS out_of_range,
                   COUNT(*) FILTER (WHERE age < 18)           AS under_18,
                   COUNT(*) FILTER (WHERE age > 100)          AS over_100
            FROM {tbl}
        """).fetchone()
        findings["age_stats"][tbl] = {
            "min": row[0], "max": row[1], "avg": row[2], "median": row[3],
            "null_age": row[4], "out_of_range": row[5],
            "under_18": row[6], "over_100": row[7],
        }

    # ── 2f. Birth year validity (stroke registry) ──────────────────────────────
    row = con.execute("""
        SELECT COUNT(*) FILTER (WHERE birth_year IS NULL)          AS null_dob,
               COUNT(*) FILTER (WHERE birth_year < 1900)           AS before_1900,
               COUNT(*) FILTER (WHERE birth_year > 2010)           AS after_2010,
               MIN(birth_year), MAX(birth_year)
        FROM stroke
    """).fetchone()
    findings["dob_validity"] = {
        "null_dob": row[0], "before_1900": row[1], "after_2010": row[2],
        "birth_year_min": row[3], "birth_year_max": row[4],
    }

    # ── 2g. Age consistency: CSV age vs DOB-derived expected age ──────────────
    findings["age_consistency"] = {}
    for tbl, year in [("v2022", 2022), ("v2023", 2023), ("v2024", 2024)]:
        row = con.execute(f"""
            SELECT COUNT(*) AS matched,
                   COUNT(*) FILTER (WHERE ABS(c.age - ({year} - s.birth_year)) > 1) AS inconsistent
            FROM {tbl} c
            JOIN (SELECT DISTINCT name, gender, birth_year FROM stroke) s
              ON c.name = s.name AND c.gender = s.gender
            WHERE c.age IS NOT NULL AND s.birth_year IS NOT NULL
        """).fetchone()
        findings["age_consistency"][tbl] = {
            "matched_to_stroke": row[0], "age_inconsistent": row[1]
        }

    # ── 2h. Cross-table overlap matrix (name + gender) ─────────────────────────
    tables = ["stroke", "v2022", "v2023", "v2024"]
    findings["overlap_matrix"] = {}
    for t1 in tables:
        uniq1 = con.execute(
            f"SELECT COUNT(DISTINCT name || '|' || COALESCE(gender,'')) FROM {t1}"
        ).fetchone()[0]
        findings["overlap_matrix"][t1] = {}
        for t2 in tables:
            if t1 == t2:
                findings["overlap_matrix"][t1][t2] = {"overlap": uniq1, "pct": "100%"}
                continue
            overlap = con.execute(f"""
                SELECT COUNT(*) FROM (
                    SELECT DISTINCT a.name, a.gender
                    FROM {t1} a JOIN {t2} b ON a.name=b.name AND a.gender=b.gender
                )
            """).fetchone()[0]
            findings["overlap_matrix"][t1][t2] = {
                "overlap": overlap,
                "pct": f"{overlap/uniq1*100:.1f}%" if uniq1 else "0%"
            }

    tlog("Phase 2 analysis", time.perf_counter() - t0)
    print()
    return findings


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 3 — Deterministic cleaning  (Python/DuckDB, zero Claude tokens)
# ═══════════════════════════════════════════════════════════════════════════════
def run_cleaning() -> dict:
    print("[Phase 3]  Applying cleaning rules (Python/DuckDB)…")
    print(f"  {'Table':<12} {'Total':>10} {'Blank rows':>12} {'Age OOR':>9} {'Duplicates':>12} {'→ Kept':>10}")
    print(f"  {'-'*12} {'-'*10} {'-'*12} {'-'*9} {'-'*12} {'-'*10}")
    t0 = time.perf_counter()
    results: dict = {}

    for tbl in ["v2022", "v2023", "v2024"]:
        total = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]

        # Count each removal reason before cleaning
        blank = con.execute(
            f"SELECT COUNT(*) FROM {tbl} WHERE name IS NULL OR TRIM(name) = ''"
        ).fetchone()[0]
        age_oor = con.execute(
            f"SELECT COUNT(*) FROM {tbl} WHERE name IS NOT NULL AND TRIM(name) != '' "
            f"AND age IS NOT NULL AND (age < 0 OR age > 120)"
        ).fetchone()[0]
        dups = con.execute(f"""
            SELECT COALESCE(SUM(cnt-1), 0) FROM (
                SELECT name, gender, COUNT(*) AS cnt FROM {tbl}
                WHERE name IS NOT NULL AND TRIM(name) != ''
                  AND (age IS NULL OR (age >= 0 AND age <= 120))
                GROUP BY name, gender HAVING COUNT(*) > 1
            )
        """).fetchone()[0]

        df = con.execute(f"""
            WITH deduped AS (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY name, gender) AS _rn
                FROM {tbl}
                WHERE name IS NOT NULL AND TRIM(name) != ''
                  AND (age IS NULL OR (age >= 0 AND age <= 120))
            )
            SELECT * EXCLUDE (_rn) FROM deduped WHERE _rn = 1
        """).fetchdf()
        after = len(df)

        out = CLEANED / f"{tbl}_cleaned.csv"
        df.to_csv(out, index=False, encoding="utf-8-sig")
        con.execute(f"CREATE OR REPLACE VIEW {tbl}_clean AS SELECT * FROM read_csv_auto('{out}')")

        print(f"  {tbl:<12} {total:>10,} {blank:>12,} {age_oor:>9,} {dups:>12,} {after:>10,}")
        results[tbl] = {
            "total_rows":       total,
            "removed_blank":    blank,
            "removed_age_oor":  age_oor,
            "removed_dups":     dups,
            "kept":             after,
            "note_blank":       "Excel export artifact — rows padded to Excel max (1,048,575)",
            "file":             str(out),
        }

    # Stroke registry
    total = con.execute("SELECT COUNT(*) FROM stroke").fetchone()[0]
    blank = con.execute(
        "SELECT COUNT(*) FROM stroke WHERE name IS NULL OR TRIM(name) = ''"
    ).fetchone()[0]
    inv_dob = con.execute(
        "SELECT COUNT(*) FROM stroke WHERE name IS NOT NULL AND TRIM(name) != '' "
        "AND birth_year IS NOT NULL AND (birth_year < 1900 OR birth_year > 2010)"
    ).fetchone()[0]
    dups = con.execute("""
        SELECT COALESCE(SUM(cnt-1), 0) FROM (
            SELECT name, gender, COUNT(*) AS cnt FROM stroke
            WHERE name IS NOT NULL AND TRIM(name) != ''
              AND (birth_year IS NULL OR (birth_year >= 1900 AND birth_year <= 2010))
            GROUP BY name, gender HAVING COUNT(*) > 1
        )
    """).fetchone()[0]

    df = con.execute("""
        WITH deduped AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY name, gender) AS _rn
            FROM stroke
            WHERE name IS NOT NULL AND TRIM(name) != ''
              AND (birth_year IS NULL OR (birth_year >= 1900 AND birth_year <= 2010))
        )
        SELECT * EXCLUDE (_rn) FROM deduped WHERE _rn = 1
    """).fetchdf()
    after = len(df)

    out = CLEANED / "stroke_cleaned.csv"
    df.to_csv(out, index=False, encoding="utf-8-sig")
    con.execute(f"CREATE OR REPLACE VIEW stroke_clean AS SELECT * FROM read_csv_auto('{out}')")

    print(f"  {'stroke':<12} {total:>10,} {blank:>12,} {'N/A':>9} {dups:>12,} {after:>10,}")
    print(f"\n  Note: 'Blank rows' in CSVs are Excel export padding (max 1,048,575 rows).")
    print(f"  Note: stroke 'Age OOR' column = invalid birth year (< 1900 or > 2010): {inv_dob}")

    results["stroke"] = {
        "total_rows":         total,
        "removed_blank":      blank,
        "removed_invalid_dob": inv_dob,
        "removed_dups":       dups,
        "kept":               after,
        "file":               str(out),
    }

    tlog("Phase 3 cleaning", time.perf_counter() - t0)
    print()
    return results


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 4 — ONE Claude call: interpret findings and write report
#            Optional tool: run_sql (only if Claude genuinely needs to dig deeper)
# ═══════════════════════════════════════════════════════════════════════════════
_SQL_TOOL = {
    "name": "run_sql",
    "description": (
        "Run a read-only SQL SELECT on DuckDB views when you need to investigate "
        "something unexpected that isn't covered by the provided findings. "
        "Only call this if genuinely necessary — the findings summary should be sufficient."
        " Available views: stroke, v2022, v2023, v2024 "
        "(+ _clean variants after Phase 3). "
        "stroke cols: name, gender, dob, birth_year, year, stroke_diagnosis. "
        "vXXXX cols: name, gender, age, exam_date."
    ),
    "input_schema": {
        "type": "object",
        "properties": {
            "query": {"type": "string"},
            "limit": {"type": "integer"}
        },
        "required": ["query"],
        "additionalProperties": False,
    }
}


def _exec_sql(query: str, limit: int = 100) -> dict:
    try:
        forbidden = {"INSERT","UPDATE","DELETE","DROP","CREATE","ALTER","TRUNCATE"}
        if any(kw in query.upper().split() for kw in forbidden):
            return {"error": "Only SELECT allowed."}
        q = query.strip().rstrip(";")
        if "LIMIT" not in q.upper():
            q = f"SELECT * FROM ({q}) __q LIMIT {limit}"
        df = con.execute(q).fetchdf()
        return {"rows": len(df), "columns": df.columns.tolist(),
                "data": df.head(50).to_dict("records")}
    except Exception as e:
        return {"error": str(e)}


def run_interpretation(findings: dict, cleaning: dict) -> str:
    print("[Phase 4]  Claude interpreting findings & writing report…")

    client = anthropic.Anthropic()

    prompt = f"""You are a senior data analyst writing a data quality report for medical patient records.

I have already run all profiling, cleaning, and relationship analysis in Python.
Here are the complete findings — use these to write the report.
Only call `run_sql` if you spot something that genuinely cannot be answered from the data below.

## Analysis findings (pre-cleaning)
```json
{json.dumps(findings, ensure_ascii=False, indent=2, default=str)}
```

## Cleaning results
```json
{json.dumps(cleaning, ensure_ascii=False, indent=2, default=str)}
```

## Your task
Write a comprehensive Markdown data quality report with these sections:
1. Executive Summary
2. Data Sources Overview (row counts, unique patients per source)
3. Issues Found — per table, with exact numbers
4. Cleaning Actions Taken — what was removed and why
5. Table Relationships & Overlap Analysis — interpret the overlap matrix
6. Age Consistency Analysis — how well do CSV ages match stroke DOB-derived ages
7. Recommendations — actionable next steps for the data team

Be specific with numbers. Use Markdown tables where helpful."""

    messages = [{"role": "user", "content": prompt}]
    total_in  = 0
    total_out = 0
    report_parts: list[str] = []
    turn = 0
    MAX_TURNS = 5   # hard cap; typically finishes in 1-2

    while turn < MAX_TURNS:
        turn += 1

        # ── spinner while waiting for first token ──────────────────────────
        _stop  = threading.Event()
        _first = threading.Event()

        def _spinner():
            while not _first.is_set() and not _stop.is_set():
                e = time.perf_counter() - agent_start
                print(f"\r  ⌛  Claude thinking…  {_fmt_time(e)}", end="", flush=True)
                time.sleep(0.5)
            if _first.is_set():
                print("\r" + " " * 50 + "\r", end="", flush=True)

        t_sp = threading.Thread(target=_spinner, daemon=True)
        t_sp.start()
        # ──────────────────────────────────────────────────────────────────

        t0 = time.perf_counter()
        with client.messages.stream(
            model="claude-opus-4-6",
            max_tokens=8_000,
            thinking={"type": "adaptive"},
            tools=[_SQL_TOOL],
            messages=messages,
        ) as stream:
            for event in stream:
                if event.type == "content_block_start":
                    if event.content_block.type == "thinking":
                        _first.set()
                        print("\n  💭 [thinking…]", flush=True)
                elif event.type == "content_block_delta":
                    if event.delta.type == "text_delta":
                        _first.set()
                        print(event.delta.text, end="", flush=True)
                        report_parts.append(event.delta.text)
            response = stream.get_final_message()

        _stop.set()
        t_sp.join()

        total_in  += response.usage.input_tokens
        total_out += response.usage.output_tokens
        cost = total_in * _PRICE_IN + total_out * _PRICE_OUT
        tlog(f"Claude turn {turn}", time.perf_counter() - t0)
        print(f"\n  Tokens this turn: {response.usage.input_tokens:,} in / "
              f"{response.usage.output_tokens:,} out  |  "
              f"Cumulative cost ~${cost:.3f}")

        messages.append({"role": "assistant", "content": response.content})

        if response.stop_reason == "end_turn":
            break

        if response.stop_reason == "tool_use":
            tool_results = []
            for block in response.content:
                if block.type != "tool_use":
                    continue
                print(f"\n  🔧 run_sql: {block.input.get('query','')[:80]}…")
                result = _exec_sql(**block.input)
                print(f"     ↳ {len(result.get('data',[]))} rows returned")
                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": block.id,
                    "content": json.dumps(result, ensure_ascii=False, default=str),
                })
            messages.append({"role": "user", "content": tool_results})
        else:
            print(f"\n  ⚠  stop_reason: {response.stop_reason}")
            break

    print("\n")
    return "".join(report_parts)


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 5 — Save report
# ═══════════════════════════════════════════════════════════════════════════════
def save_report(report: str) -> None:
    total = time.perf_counter() - agent_start

    timing_md = (
        "\n\n---\n\n## Job Timing\n\n"
        f"**Total runtime: {_fmt_time(total)}**\n\n"
        "| Step | Duration |\n|---|---|\n"
    )
    for label, secs in timing_log:
        timing_md += f"| {label} | {secs:.2f}s |\n"

    full = report + timing_md
    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = PATH / f"cleaning_report_{ts}.md"
    path.write_text(full, encoding="utf-8")

    print("=" * 62)
    print(f"  Report saved  →  {path.name}")
    print(f"  Total runtime :  {_fmt_time(total)}")
    print("=" * 62)
    print("\nStep-by-step timing:")
    for label, secs in timing_log:
        print(f"  {label:<44} {secs:>7.2f}s")


# ═══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    setup_data()
    findings = run_analysis()
    cleaning = run_cleaning()
    report   = run_interpretation(findings, cleaning)
    save_report(report)
