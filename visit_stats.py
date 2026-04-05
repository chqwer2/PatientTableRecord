#!/usr/bin/env python3
"""
visit_stats.py — Visit statistics for fused.csv, with disease rate per row.

"Target disease" = patient appears in the stroke registry (in_stroke = Y in
patient_reference.csv).  Any such patient is counted as a case regardless of
which exam year they appear in (prevalent case definition).

Rules:
  • Year derived from 体检日期 (YYYY-MM-DD, normalised by step2).
    Falls back to 年度 when 体检日期 is blank for a row.
  • Same patient + same year = 1 visit (dedup by patient_id × year).
  • Disease rate per row = stroke_patients / total_patients in that group.

Outputs (printed to console + saved to analysis/):
  stats_per_year.csv        — per exam-year: patients, stroke cases, rate
  stats_patient_span.csv    — per span (1/2/3/4 years): patients, stroke cases, rate
  stats_summary.txt         — full printed output
"""
import re
import sys
import time
from pathlib import Path

import pandas as pd

FUSED_PATH = Path("/Users/haochen/Desktop/PatientTables/output/analysis/fused.csv")
REF_PATH   = Path("/Users/haochen/Desktop/PatientTables/output/reference/patient_reference.csv")
OUT_DIR    = FUSED_PATH.parent

t0 = time.perf_counter()

# ── Load fused (only columns needed) ─────────────────────────────────────────
if not FUSED_PATH.exists():
    sys.exit(f"ERROR: {FUSED_PATH} not found. Run the pipeline first.")
if not REF_PATH.exists():
    sys.exit(f"ERROR: {REF_PATH} not found. Run main_2018.py first.")

RISK_COLS = ("心电图_风险", "胸片_风险")

print(f"Loading fused.csv…", end=" ", flush=True)
df = pd.read_csv(
    FUSED_PATH, dtype=str, encoding="utf-8-sig", low_memory=False,
    usecols=lambda c: c in ("patient_id", "person_id", "体检日期", "年度") or c in RISK_COLS,
)
if "patient_id" not in df.columns and "person_id" in df.columns:
    df.rename(columns={"person_id": "patient_id"}, inplace=True)
print(f"{len(df):,} rows  ({time.perf_counter()-t0:.1f}s)")

id_col = "patient_id"
if id_col not in df.columns:
    sys.exit("ERROR: 'patient_id' column not found in fused.csv.")

# ── Load patient_reference.csv (all year flags) ───────────────────────────────
print(f"Loading patient_reference.csv…", end=" ", flush=True)
ref = pd.read_csv(REF_PATH, dtype=str, encoding="utf-8-sig",
                  usecols=lambda c: c in (
                      "person_id", "in_stroke", "in_2018", "in_2022", "in_2023", "in_2024"))
ref.rename(columns={"person_id": id_col}, inplace=True)
def _y(col): return ref[col].str.strip().str.upper() == "Y"
ref["is_stroke"] = _y("in_stroke")
stroke_ids = set(ref.loc[ref["is_stroke"], id_col].dropna())
print(f"{len(ref):,} patients  |  {len(stroke_ids):,} stroke cases")

# ── Extract year ──────────────────────────────────────────────────────────────
def _yr_from_date(v):
    if pd.isna(v): return None
    m = re.match(r'((?:19|20)\d{2})', str(v).strip())
    return m.group(1) if m else None

def _yr_fallback(v):
    if pd.isna(v): return None
    m = re.search(r'((?:19|20)\d{2})', str(v).strip())
    return m.group(1) if m else None

has_exam_date = "体检日期" in df.columns
has_nian_du   = "年度"    in df.columns

if not has_exam_date and not has_nian_du:
    sys.exit("ERROR: neither '体检日期' nor '年度' found in fused.csv.")

if has_exam_date:
    df["_yr"] = df["体检日期"].apply(_yr_from_date)
    if has_nian_du:
        mask = df["_yr"].isna()
        df.loc[mask, "_yr"] = df.loc[mask, "年度"].apply(_yr_fallback)
    year_source = "体检日期" + (" (fallback: 年度)" if has_nian_du else "")
else:
    df["_yr"] = df["年度"].apply(_yr_fallback)
    year_source = "年度"

# ── Tag stroke ────────────────────────────────────────────────────────────────
df = df[df[id_col].notna() & (df[id_col].str.strip() != "")]
df["_stroke"] = df[id_col].isin(stroke_ids)

# Dedup: one row per (patient, year)
pairs_all = df[[id_col, "_yr", "_stroke"]].drop_duplicates(subset=[id_col, "_yr"])
pairs_yr  = pairs_all[pairs_all["_yr"].notna()].copy()

n_total   = pairs_all[id_col].nunique()
n_yr      = pairs_yr[id_col].nunique()

# ─────────────────────────────────────────────────────────────────────────────
lines = []
def p(s=""):
    print(s)
    lines.append(s)

p("=" * 70)
p("  PatientTables — Visit Statistics with Disease Rate (Stroke)")
p("=" * 70)
p(f"\n  Fused   : {FUSED_PATH}")
p(f"  Ref     : {REF_PATH}")
p(f"  Year    : {year_source}")
p(f"  Total unique patients                    : {n_total:,}")
p(f"  Patients with at least one known year    : {n_yr:,}")
p(f"  Stroke cases (in_stroke=Y) in reference  : {len(stroke_ids):,}")

# ─────────────────────────────────────────────────────────────────────────────
# 0. Stroke patient overlap — how many of the stroke patients appear in each
#    source file (2018 / 2022 / 2023 / 2024), counted from patient_reference.
# ─────────────────────────────────────────────────────────────────────────────
stroke_ref = ref[ref["is_stroke"]].copy()
n_stroke_total = len(stroke_ref)

p("\n" + "─" * 70)
p(f"  Table 0 — Stroke patient presence across source files")
p(f"  (base: {n_stroke_total:,} stroke patients in registry)")
p("─" * 70)
p(f"  {'Source file':<12}  {'Stroke pts present':>20}  {'% of stroke pts':>16}  {'% of source pts':>16}")
p(f"  {'─'*12}  {'─'*20}  {'─'*16}  {'─'*16}")

source_cols = [("2018", "in_2018"), ("2022", "in_2022"),
               ("2023", "in_2023"), ("2024", "in_2024")]
for yr_label, col in source_cols:
    if col not in ref.columns:
        p(f"  {yr_label:<12}  {'(column missing)':>20}")
        continue
    in_src = ref[col].str.strip().str.upper() == "Y"
    # stroke patients who appear in this source
    n_in_src_stroke = int((stroke_ref[col].str.strip().str.upper() == "Y").sum())
    # total patients in this source (for denominator)
    n_in_src_all    = int(in_src.sum())
    pct_of_stroke   = n_in_src_stroke / n_stroke_total * 100
    pct_of_src      = n_in_src_stroke / n_in_src_all  * 100 if n_in_src_all else 0
    p(f"  {yr_label:<12}  {n_in_src_stroke:>20,}  {pct_of_stroke:>15.1f}%  {pct_of_src:>15.1f}%")

# Stroke patients with NO match in any source year
no_match = stroke_ref[
    ~(stroke_ref[["in_2018","in_2022","in_2023","in_2024"]]
      .apply(lambda col: col.str.strip().str.upper() == "Y").any(axis=1))
]
p(f"\n  Stroke-only (not in any health-check file) : {len(no_match):,}  "
  f"({len(no_match)/n_stroke_total*100:.1f}%)")

# ─────────────────────────────────────────────────────────────────────────────
# 1. Per-year table
#    Each patient counted once per year they appear in.
#    Stroke flag = patient is in stroke registry (regardless of which year).
# ─────────────────────────────────────────────────────────────────────────────
# For each year, count distinct patients + distinct stroke patients
per_year_rows = []
for yr, grp in pairs_yr.groupby("_yr"):
    patients = grp[id_col].unique()
    n_pat    = len(patients)
    n_stroke = sum(1 for pid in patients if pid in stroke_ids)
    per_year_rows.append({
        "year":            yr,
        "unique_patients": n_pat,
        "stroke_patients": n_stroke,
        "disease_rate":    round(n_stroke / n_pat * 100, 2) if n_pat else 0.0,
    })
per_year = pd.DataFrame(per_year_rows).sort_values("year")

p("\n" + "─" * 70)
p("  Table 1 — Per exam-year  (deduped by patient × year)")
p("  Disease = stroke (in_stroke = Y in reference; any appearance counts)")
p("─" * 70)
p(f"  {'Year':<8}  {'Patients':>10}  {'Stroke cases':>13}  {'Disease rate':>13}")
p(f"  {'─'*8}  {'─'*10}  {'─'*13}  {'─'*13}")
for _, r in per_year.iterrows():
    p(f"  {r['year']:<8}  {int(r['unique_patients']):>10,}  "
      f"{int(r['stroke_patients']):>13,}  {r['disease_rate']:>12.2f}%")
p(f"  {'─'*8}  {'─'*10}  {'─'*13}  {'─'*13}")
# Overall (count each patient once across all years)
all_patients = pairs_yr[id_col].unique()
n_all_stroke = sum(1 for pid in all_patients if pid in stroke_ids)
p(f"  {'Overall':<8}  {len(all_patients):>10,}  "
  f"{n_all_stroke:>13,}  {n_all_stroke/len(all_patients)*100:>12.2f}%")

# ─────────────────────────────────────────────────────────────────────────────
# 2. Span distribution table
#    For each patient: count distinct years → group by span → disease rate
# ─────────────────────────────────────────────────────────────────────────────
span = (
    pairs_yr.groupby(id_col)["_yr"]
    .nunique()
    .rename("n_years")
    .reset_index()
)
span["_stroke"] = span[id_col].isin(stroke_ids)

span_rows = []
for n_yrs, grp in span.groupby("n_years"):
    n_pat    = len(grp)
    n_stroke = grp["_stroke"].sum()
    span_rows.append({
        "years_appeared": int(n_yrs),
        "n_patients":     n_pat,
        "stroke_patients": int(n_stroke),
        "disease_rate":   round(n_stroke / n_pat * 100, 2) if n_pat else 0.0,
        "pct_of_total":   round(n_pat / n_yr * 100, 1),
    })
span_dist = pd.DataFrame(span_rows).sort_values("years_appeared")
span_dist["cumulative_pct"] = span_dist["pct_of_total"].cumsum().round(1)

p("\n" + "─" * 70)
p("  Table 2 — Patient span distribution  (distinct exam years per patient)")
p("─" * 70)
p(f"  {'Years':<7}  {'Patients':>10}  {'% of total':>11}  "
  f"{'Cumul%':>7}  {'Stroke cases':>13}  {'Disease rate':>13}")
p(f"  {'─'*7}  {'─'*10}  {'─'*11}  {'─'*7}  {'─'*13}  {'─'*13}")
for _, r in span_dist.iterrows():
    p(f"  {int(r['years_appeared']):<7}  {int(r['n_patients']):>10,}  "
      f"{r['pct_of_total']:>10.1f}%  {r['cumulative_pct']:>6.1f}%  "
      f"{int(r['stroke_patients']):>13,}  {r['disease_rate']:>12.2f}%")
p(f"  {'─'*7}  {'─'*10}  {'─'*11}  {'─'*7}  {'─'*13}  {'─'*13}")
p(f"  {'Total':<7}  {n_yr:>10,}  {'100.0%':>11}  "
  f"{'':>7}  {n_all_stroke:>13,}  {n_all_stroke/n_yr*100:>12.2f}%")

# ─────────────────────────────────────────────────────────────────────────────
# 3. Risk column distribution (心电图_风险 / 胸片_风险)
#    Counts per level across all rows (not deduped — each visit row counted).
#    Level meanings: -1 = not examined, 0 = normal/unclassified, 1 = low risk, 2 = high risk
# ─────────────────────────────────────────────────────────────────────────────
LEVEL_LABELS = {"-1": "not examined", "0": "normal", "1": "low risk", "2": "high risk"}

present_risk_cols = [c for c in RISK_COLS if c in df.columns]
if present_risk_cols:
    p("\n" + "─" * 70)
    p("  Table 3 — Risk column distribution  (all visit rows, not deduped)")
    p("─" * 70)
    for col in present_risk_cols:
        vc = df[col].value_counts(dropna=False).sort_index()
        total = len(df)
        p(f"\n  {col}  (total rows: {total:,})")
        p(f"  {'Level':<6}  {'Label':<16}  {'Count':>10}  {'%':>7}")
        p(f"  {'─'*6}  {'─'*16}  {'─'*10}  {'─'*7}")
        for level, cnt in vc.items():
            # Normalise "0.0" → "0", "-1.0" → "-1" etc. for label lookup
            lvl_str = str(level) if str(level) != "nan" else "NaN"
            try:
                lvl_key = str(int(float(lvl_str)))
            except (ValueError, OverflowError):
                lvl_key = lvl_str
            label = LEVEL_LABELS.get(lvl_key, "")
            p(f"  {lvl_str:<6}  {label:<16}  {cnt:>10,}  {cnt/total*100:>6.1f}%")

# ─────────────────────────────────────────────────────────────────────────────
p("\n" + "=" * 70)
p(f"  Done in {time.perf_counter()-t0:.1f}s")
p("=" * 70)

# ── Save ──────────────────────────────────────────────────────────────────────
per_year.to_csv(OUT_DIR / "stats_per_year.csv",     index=False, encoding="utf-8-sig")
span_dist.to_csv(OUT_DIR / "stats_patient_span.csv", index=False, encoding="utf-8-sig")
(OUT_DIR / "stats_summary.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")

print(f"\n  Saved:")
print(f"    {OUT_DIR}/stats_per_year.csv")
print(f"    {OUT_DIR}/stats_patient_span.csv")
print(f"    {OUT_DIR}/stats_summary.txt")
