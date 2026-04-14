#!/usr/bin/env python3
"""
step5_disease_registry_analysis.py
────────────────────────────────────────────────────────────────────────────
Cross-analysis of high-blood-pressure (高血压) and diabetes (糖尿病) disease
registries against the physical-exam database (fused_filled.csv).

Run AFTER step4_fill_missing.py. Does NOT modify fused_filled.csv.

────────────────────────────────────────────────────────────────────────────
Questions answered
────────────────────────────────────────────────────────────────────────────
A. Among 高血压 patients:
     A1. How many have ≥ 1 physical exam BEFORE their diagnosis date?
     A2. Distribution of exam counts (for those who do).

B. Among 糖尿病 patients:
     B1. How many have ≥ 1 physical exam BEFORE their diagnosis date?
     B2. Distribution of exam counts.

C. Unique patients across A + B combined (overlap counted once).

D. Data-quality cross-check:
     D1. Patients in 高血压 file flagged 是否为糖尿病=是  →  how many are
         actually found in the 糖尿病 file?
     D2. Patients in 糖尿病 file flagged 是否为高血压=是  →  same, flipped.

E. Age-gate filtering (applied before exam counting):
     Each physical-exam record is accepted only if the age recorded in that
     exam is consistent with the patient's age in the disease file, i.e.:
         |est_by_disease − exam_est_by| ≤ AGE_GATE_TOL  (default 1 year)
     where est_by = reference_year − age  (both sides).
     This guards against counting exams that belong to a different patient
     who shares the same person_id (upstream ID collision) or whose age
     was mis-recorded.

F. Cross-validation of patient identity:
     For each matched patient, compare the estimated birth year from the
     disease-registry record against the mean birth-year estimate across
     all age-gated exam records. Flags patients whose two estimates
     disagree by > XVAL_THRESHOLD years.

────────────────────────────────────────────────────────────────────────────
Patient-matching strategy
────────────────────────────────────────────────────────────────────────────
The disease files contain 姓名 + 性别 + 年龄 but NO patient_id.
We link them to the pipeline's patient_reference.csv via:
  1. Exact match on (姓名, 性别)   ← name + gender
  2. Birth-year proximity: est_by = ref_year − age (where ref_year is derived
     from 末次随访时间 → 建档日期 → 2025 in that priority).
     Candidates kept where |ref.birth_year − est_by| ≤ BIRTH_YEAR_TOL (1 yr).
  3. Tie-break among equally-close candidates:
       prefer the person_id whose fused exam records contain the most
       age-consistent rows (|est_by_disease − exam_est_by| ≤ AGE_GATE_TOL).
       Second tie-break: alphabetical person_id (reproducible).

Unmatched disease-registry rows are counted and reported separately.

────────────────────────────────────────────────────────────────────────────
Outputs  (all in output/analysis/)
────────────────────────────────────────────────────────────────────────────
  step5_report.txt                   — full text report (main output)
  step5_ht_exam_before_diag.csv      — per-patient HT exam counts before dx
  step5_dm_exam_before_diag.csv      — per-patient DM exam counts before dx
  step5_age_gate_excluded.csv        — exam rows excluded by the age gate
  step5_cross_validation_flags.csv   — patients with residual age discrepancy
"""

import time
from pathlib import Path

import pandas as pd

# ── Paths ─────────────────────────────────────────────────────────────────────
DESKTOP        = Path("/Users/haochen/Desktop/PatientTables")
HT_PATH        = DESKTOP / "高血压.xlsx"
DM_PATH        = DESKTOP / "糖尿病.xlsx"
FUSED_PATH     = DESKTOP / "output/analysis/fused_filled.csv"
REF_PATH       = DESKTOP / "output/reference/patient_reference.csv"
OUT_DIR        = DESKTOP / "output/analysis"
OUT_DIR.mkdir(parents=True, exist_ok=True)

BIRTH_YEAR_TOL = 1   # ±N years: initial ref matching (name+gender+birth_year)
AGE_GATE_TOL   = 1   # ±N years: per-exam age consistency gate
XVAL_THRESHOLD = 2   # flag patient if residual discrepancy exceeds this

t0 = time.perf_counter()

print("=" * 68)
print("  PatientTables — step5: Disease Registry × Physical Exam Analysis")
print("=" * 68)


# ═══════════════════════════════════════════════════════════════════════════════
# 1.  Load data
# ═══════════════════════════════════════════════════════════════════════════════
print("\n[Load] Reading source files…")

ht_raw = pd.read_excel(HT_PATH)
dm_raw = pd.read_excel(DM_PATH)
print(f"  高血压.xlsx       {len(ht_raw):>8,} rows × {len(ht_raw.columns)} cols")
print(f"  糖尿病.xlsx       {len(dm_raw):>8,} rows × {len(dm_raw.columns)} cols")

ref = pd.read_csv(REF_PATH, dtype=str)
ref["birth_year_int"] = pd.to_numeric(ref["birth_year"], errors="coerce")
print(f"  patient_reference {len(ref):>8,} rows")

fused = pd.read_csv(FUSED_PATH, dtype=str, low_memory=False)
fused["体检日期_pd"] = pd.to_datetime(fused["体检日期"], errors="coerce")
fused["exam_year"]   = fused["体检日期_pd"].dt.year
fused["exam_age"]    = pd.to_numeric(fused["年龄"], errors="coerce")
# birth-year estimate from this exam record: exam_year − exam_age
# (may differ from true birth year by ±1 due to birthday position in the year)
fused["exam_est_by"] = fused["exam_year"] - fused["exam_age"]
print(f"  fused_filled.csv  {len(fused):>8,} rows × {len(fused.columns)} cols")

# Pre-compute per-patient lookup: person_id → sorted list of exam_est_by values
# Used for age-gate tie-breaking inside _match_to_ref.
_fused_est_by_lookup: dict[str, list[float]] = (
    fused.dropna(subset=["exam_est_by"])
    .groupby("patient_id")["exam_est_by"]
    .apply(list)
    .to_dict()
)


# ═══════════════════════════════════════════════════════════════════════════════
# 2.  Prepare disease-registry DataFrames
# ═══════════════════════════════════════════════════════════════════════════════

def _prep(df: pd.DataFrame) -> pd.DataFrame:
    """Add derived columns to a disease-registry DataFrame."""
    df = df.copy()
    df["age_int"] = pd.to_numeric(df["年龄"], errors="coerce")

    # Reference year for age: use the most recent follow-up or registration date.
    ref_year = pd.Series([pd.NaT] * len(df), index=df.index, dtype="datetime64[ns]")
    for col in ["末次随访时间", "建档日期"]:
        if col in df.columns:
            ref_year = ref_year.combine_first(pd.to_datetime(df[col], errors="coerce"))
    df["ref_year"] = ref_year.dt.year.fillna(2025).astype(int)

    # est_by: estimated birth year from disease file
    df["est_by"]    = df["ref_year"] - df["age_int"]
    df["diag_date"] = pd.to_datetime(df["确诊时间"], errors="coerce")
    return df


ht = _prep(ht_raw)
dm = _prep(dm_raw)


# ═══════════════════════════════════════════════════════════════════════════════
# 3.  Match disease-registry patients to patient_reference
#
#     Step 1 — join on (姓名, 性别): name + gender
#     Step 2 — keep only candidates with |ref.birth_year − est_by| ≤ BIRTH_YEAR_TOL
#     Step 3 — among surviving ties, prefer the candidate whose fused exam records
#              are most age-consistent (highest count of exams with
#              |est_by_disease − exam_est_by| ≤ AGE_GATE_TOL).
#              Second tie-break: alphabetical person_id.
# ═══════════════════════════════════════════════════════════════════════════════

def _age_gate_score(pid: str, disease_est_by: float) -> int:
    """
    Count how many fused exam records for `pid` pass the age gate:
        |disease_est_by − exam_est_by| ≤ AGE_GATE_TOL
    Returns 0 if the patient has no exam records.
    """
    exams = _fused_est_by_lookup.get(pid, [])
    if not exams:
        return 0
    return sum(1 for e in exams if abs(disease_est_by - e) <= AGE_GATE_TOL)


def _match_to_ref(df: pd.DataFrame, label: str) -> pd.DataFrame:
    """
    Return a DataFrame of matched rows (one per original disease-file row that
    could be linked to patient_reference), with a 'person_id' column added.

    Matching: (姓名, 性别) exact + birth_year ±BIRTH_YEAR_TOL + age-gate tie-break.
    Prints a match-rate summary.
    """
    df2 = df.reset_index(drop=True).copy()
    df2["_row_key"] = df2.index

    # Step 1+2: join on name+gender, filter by birth-year proximity
    merged = df2.merge(
        ref[["name", "gender", "birth_year_int", "person_id"]],
        left_on=["姓名", "性别"],
        right_on=["name", "gender"],
        how="left",
    )
    merged["by_diff"] = (merged["birth_year_int"] - merged["est_by"]).abs()
    within_tol = merged[merged["by_diff"] <= BIRTH_YEAR_TOL].copy()

    # Step 3: tie-break with age-gate score (higher = more age-consistent exams)
    within_tol["_age_score"] = within_tol.apply(
        lambda r: _age_gate_score(r["person_id"], r["est_by"]), axis=1
    )
    # Sort: smallest by_diff first, then highest age_score, then person_id A→Z
    within_tol = within_tol.sort_values(
        ["by_diff", "_age_score", "person_id"],
        ascending=[True, False, True],
    )
    best = within_tol.drop_duplicates(subset=["_row_key"], keep="first").copy()

    n_matched   = best["_row_key"].nunique()
    n_total     = len(df2)
    n_unmatched = n_total - n_matched

    print(f"\n  [{label}] matching summary:")
    print(f"    Total rows in file                    : {n_total:>8,}")
    print(f"    Rows matched (name+gender+birth_year) : {n_matched:>8,}  ({n_matched/n_total:.1%})")
    print(f"    Rows unmatched                        : {n_unmatched:>8,}  ({n_unmatched/n_total:.1%})")
    print(f"    Unique person_ids matched             : {best['person_id'].nunique():>8,}")

    best = best.drop(
        columns=["_row_key", "name", "gender", "birth_year_int",
                 "by_diff", "_age_score"],
        errors="ignore",
    )
    return best


print("\n[Match] Linking disease-registry rows to patient_reference…")
ht_m = _match_to_ref(ht, "高血压")
dm_m = _match_to_ref(dm, "糖尿病")


# ═══════════════════════════════════════════════════════════════════════════════
# 4.  Physical-exam lookup with age gate
#
#     For each matched patient:
#       a. Retrieve all fused exam rows by person_id.
#       b. AGE GATE: keep only exam rows where
#              |est_by_disease − exam_est_by| ≤ AGE_GATE_TOL
#          This ensures we never count an exam record that belongs to a
#          different person sharing the same person_id (ID collision) or
#          whose age was mis-recorded.
#       c. DATE GATE: keep only exam rows before the earliest diagnosis date.
#       d. Count per patient.
# ═══════════════════════════════════════════════════════════════════════════════

def _exams_before_diag(matched: pd.DataFrame, label: str):
    """
    Returns:
      counts          — Series(person_id → exam_count_before_diag) for patients
                        with ≥ 1 age-gated exam before their diagnosis date.
      total_pids      — total unique matched patients (denominator).
      excluded_df     — DataFrame of exam rows that failed the age gate.
    """
    # One (person_id, diag_date, disease_est_by) per unique patient.
    # Multiple disease-file rows for the same person_id → earliest diag_date.
    pairs = (
        matched[["person_id", "diag_date", "est_by"]]
        .dropna(subset=["person_id", "diag_date"])
        .drop_duplicates()
        .sort_values("diag_date")
        .drop_duplicates(subset=["person_id"], keep="first")   # earliest diag
    )

    total_pids = pairs["person_id"].nunique()

    # Join with all fused exam rows for those patients
    all_exams = pairs.merge(
        fused[["patient_id", "体检日期_pd", "exam_est_by", "年龄", "体检日期"]],
        left_on="person_id",
        right_on="patient_id",
        how="inner",
    )

    # ── Age gate ──────────────────────────────────────────────────────────────
    all_exams["age_diff"] = (all_exams["est_by"] - all_exams["exam_est_by"]).abs()
    passed  = all_exams[all_exams["age_diff"] <= AGE_GATE_TOL].copy()
    excluded = all_exams[all_exams["age_diff"] >  AGE_GATE_TOL].copy()

    # ── Date gate: only exams BEFORE diagnosis ────────────────────────────────
    before = passed[passed["体检日期_pd"] < passed["diag_date"]]

    counts = before.groupby("person_id").size().rename("exam_count_before_diag")

    n_with_exams   = counts.shape[0]
    n_excl_rows    = len(excluded)
    n_excl_pids    = excluded["person_id"].nunique() if n_excl_rows else 0

    print(f"\n  [{label}] physical exams before diagnosis (with age gate ±{AGE_GATE_TOL} yr):")
    print(f"    Unique matched patients               : {total_pids:>8,}")
    print(f"    Exam rows excluded by age gate        : {n_excl_rows:>8,}  "
          f"(from {n_excl_pids:,} patients)")
    print(f"    Patients with ≥ 1 exam (age-gated)   : {n_with_exams:>8,}  "
          f"({n_with_exams/total_pids:.1%})")

    # Build excluded DataFrame for saving
    excl_out = excluded[["person_id", "体检日期", "年龄", "exam_est_by",
                          "est_by", "age_diff", "diag_date"]].copy()
    excl_out["source"] = label

    return counts, total_pids, excl_out


print("\n[Exams] Counting physical exams before diagnosis date (with age gate)…")
ht_counts, ht_total_pids, ht_excluded = _exams_before_diag(ht_m, "高血压")
dm_counts, dm_total_pids, dm_excluded = _exams_before_diag(dm_m, "糖尿病")

ht_overlap = set(ht_counts.index) & set(dm_counts.index)
ht_only    = set(ht_counts.index) - set(dm_counts.index)
dm_only    = set(dm_counts.index) - set(ht_counts.index)
unique_all = ht_only | dm_only | ht_overlap

print(f"\n  [Combined] patients with ≥ 1 age-gated exam before diagnosis:")
print(f"    HT only                               : {len(ht_only):>8,}")
print(f"    DM only                               : {len(dm_only):>8,}")
print(f"    Both HT and DM                        : {len(ht_overlap):>8,}")
print(f"    Total unique                          : {len(unique_all):>8,}")


# ═══════════════════════════════════════════════════════════════════════════════
# 5.  Data-quality cross-check: comorbidity flag consistency
# ═══════════════════════════════════════════════════════════════════════════════
print("\n[Quality] Cross-checking comorbidity flags…")

dm_m_pids = set(dm_m["person_id"].dropna().unique())
ht_m_pids = set(ht_m["person_id"].dropna().unique())

ht_dm_flag   = ht[ht["是否为糖尿病"] == "是"]
ht_dm_flag_m = _match_to_ref(ht_dm_flag, "HT→DM-flag")
ht_dm_flag_pids = set(ht_dm_flag_m["person_id"].dropna().unique())
d1_confirmed = ht_dm_flag_pids & dm_m_pids
d1_not_found = ht_dm_flag_pids - dm_m_pids

print(f"\n  D1. 高血压 file: 是否为糖尿病='是'")
print(f"    Rows with flag                        : {len(ht_dm_flag):>8,}")
print(f"    Unique matched patients               : {ht_dm_flag_m['person_id'].nunique():>8,}")
print(f"    Confirmed in 糖尿病 file ✓            : {len(d1_confirmed):>8,}  "
      f"({len(d1_confirmed)/max(ht_dm_flag_m['person_id'].nunique(),1):.1%})")
print(f"    NOT found in 糖尿病 file ✗            : {len(d1_not_found):>8,}  "
      f"({len(d1_not_found)/max(ht_dm_flag_m['person_id'].nunique(),1):.1%})")

dm_ht_flag   = dm[dm["是否为高血压"] == "是"]
dm_ht_flag_m = _match_to_ref(dm_ht_flag, "DM→HT-flag")
dm_ht_flag_pids = set(dm_ht_flag_m["person_id"].dropna().unique())
d2_confirmed = dm_ht_flag_pids & ht_m_pids
d2_not_found = dm_ht_flag_pids - ht_m_pids

print(f"\n  D2. 糖尿病 file: 是否为高血压='是'")
print(f"    Rows with flag                        : {len(dm_ht_flag):>8,}")
print(f"    Unique matched patients               : {dm_ht_flag_m['person_id'].nunique():>8,}")
print(f"    Confirmed in 高血压 file ✓            : {len(d2_confirmed):>8,}  "
      f"({len(d2_confirmed)/max(dm_ht_flag_m['person_id'].nunique(),1):.1%})")
print(f"    NOT found in 高血压 file ✗            : {len(d2_not_found):>8,}  "
      f"({len(d2_not_found)/max(dm_ht_flag_m['person_id'].nunique(),1):.1%})")


# ═══════════════════════════════════════════════════════════════════════════════
# 6.  Cross-validation: residual birth-year discrepancy after age gate
#
#     After the age gate filters out inconsistent exam rows, compare:
#       est_by_disease  = ref_year − age  (disease file)
#       mean(exam_est_by) over age-gated exams only
#     Flag if discrepancy > XVAL_THRESHOLD years.
# ═══════════════════════════════════════════════════════════════════════════════
print("\n[Cross-validate] Checking residual age/birth-year consistency…")

def _exam_by_gated(matched: pd.DataFrame) -> pd.DataFrame:
    """
    Return per-patient mean exam_est_by computed over age-GATED exams only.
    """
    pid_by = (
        matched[["person_id", "est_by"]]
        .dropna()
        .drop_duplicates("person_id")
    )
    all_ex = pid_by.merge(
        fused[["patient_id", "exam_est_by"]],
        left_on="person_id", right_on="patient_id", how="inner",
    )
    all_ex["age_diff"] = (all_ex["est_by"] - all_ex["exam_est_by"]).abs()
    gated = all_ex[all_ex["age_diff"] <= AGE_GATE_TOL]
    agg = (
        gated.groupby("person_id")["exam_est_by"]
        .agg(exam_by_mean="mean", exam_by_std="std", exam_count="count")
        .reset_index()
    )
    return pid_by.merge(agg, on="person_id", how="left")


def _xval(matched: pd.DataFrame, label: str) -> pd.DataFrame:
    chk = _exam_by_gated(matched)
    chk_has = chk.dropna(subset=["exam_by_mean"]).copy()
    chk_has["by_discrepancy"] = (chk_has["est_by"] - chk_has["exam_by_mean"]).abs()

    n        = len(chk_has)
    n_ok1    = (chk_has["by_discrepancy"] <= 1).sum()
    n_ok2    = (chk_has["by_discrepancy"] <= 2).sum()
    n_flagged= (chk_has["by_discrepancy"] > XVAL_THRESHOLD).sum()

    print(f"\n  [{label}] residual birth-year discrepancy (age-gated exams only):")
    print(f"    Patients with age-gated exam data     : {n:>7,}")
    print(f"    Discrepancy ≤ 1 yr  (tight)           : {n_ok1:>7,}  ({n_ok1/max(n,1):.1%})")
    print(f"    Discrepancy ≤ 2 yr  (acceptable)      : {n_ok2:>7,}  ({n_ok2/max(n,1):.1%})")
    print(f"    Discrepancy > {XVAL_THRESHOLD} yr  (flagged)        : {n_flagged:>7,}  "
          f"({n_flagged/max(n,1):.1%})")

    flagged = chk_has[chk_has["by_discrepancy"] > XVAL_THRESHOLD].copy()
    flagged["source"] = label
    return flagged


ht_flags = _xval(ht_m, "高血压")
dm_flags = _xval(dm_m, "糖尿病")
all_flags = pd.concat([ht_flags, dm_flags], ignore_index=True)


# ═══════════════════════════════════════════════════════════════════════════════
# 7.  Save output files
# ═══════════════════════════════════════════════════════════════════════════════
print("\n[Save] Writing output files…")

def _make_exam_df(counts: pd.Series, matched: pd.DataFrame) -> pd.DataFrame:
    pid_info = (
        matched[["person_id", "est_by", "diag_date"]]
        .dropna(subset=["person_id"])
        .drop_duplicates("person_id")
        .set_index("person_id")
    )
    out = counts.to_frame().join(pid_info, how="left")
    out.index.name = "person_id"
    out["estimated_birth_year"] = out["est_by"].round(0).astype("Int64")
    out["diagnosis_date"]       = out["diag_date"].dt.date
    return out[["estimated_birth_year", "diagnosis_date",
                "exam_count_before_diag"]].sort_values(
        "exam_count_before_diag", ascending=False
    )

ht_out = _make_exam_df(ht_counts, ht_m)
dm_out = _make_exam_df(dm_counts, dm_m)
excluded_all = pd.concat([ht_excluded, dm_excluded], ignore_index=True)

ht_out.to_csv(OUT_DIR / "step5_ht_exam_before_diag.csv",   encoding="utf-8-sig")
dm_out.to_csv(OUT_DIR / "step5_dm_exam_before_diag.csv",   encoding="utf-8-sig")
excluded_all.to_csv(OUT_DIR / "step5_age_gate_excluded.csv", index=False, encoding="utf-8-sig")
all_flags.to_csv(OUT_DIR / "step5_cross_validation_flags.csv", index=False, encoding="utf-8-sig")

print(f"  step5_ht_exam_before_diag.csv     {len(ht_out):>7,} rows")
print(f"  step5_dm_exam_before_diag.csv     {len(dm_out):>7,} rows")
print(f"  step5_age_gate_excluded.csv       {len(excluded_all):>7,} rows  (exam rows excluded by age gate)")
print(f"  step5_cross_validation_flags.csv  {len(all_flags):>7,} rows  (residual discrepancy > {XVAL_THRESHOLD} yr)")


# ═══════════════════════════════════════════════════════════════════════════════
# 8.  Text report
# ═══════════════════════════════════════════════════════════════════════════════

def _dist_table(counts: pd.Series, label: str, max_rows: int = 20) -> list[str]:
    vc    = counts.value_counts().sort_index()
    total = vc.sum()
    lines = [
        f"  {label} — exam count distribution before diagnosis (age-gated)",
        f"  {'Exams':>6}  {'Patients':>8}  {'%':>6}  {'Cumul%':>7}",
        f"  {'─'*35}",
    ]
    cum = 0
    for shown, (n_exams, n_pts) in enumerate(vc.items()):
        pct   = n_pts / total * 100
        cum  += n_pts
        cpct  = cum / total * 100
        lines.append(f"  {n_exams:>6}  {n_pts:>8,}  {pct:>5.1f}%  {cpct:>6.1f}%")
        if shown + 1 >= max_rows and len(vc) > max_rows:
            rest = vc.iloc[shown+1:].sum()
            lines.append(f"  {'...':>6}  {rest:>8,}  (remaining {len(vc)-shown-1} buckets)")
            break
    lines.append(f"  {'TOTAL':>6}  {total:>8,}  100.0%")
    return lines


def _xval_stats_gated(matched, label):
    chk = _exam_by_gated(matched)
    chk_h = chk.dropna(subset=["exam_by_mean"]).copy()
    chk_h["disc"] = (chk_h["est_by"] - chk_h["exam_by_mean"]).abs()
    n   = len(chk_h)
    ok1 = (chk_h["disc"] <= 1).sum()
    ok2 = (chk_h["disc"] <= 2).sum()
    fl  = (chk_h["disc"] > XVAL_THRESHOLD).sum()
    return n, ok1, ok2, fl


elapsed = time.perf_counter() - t0
ht_n, ht_ok1, ht_ok2, ht_fl = _xval_stats_gated(ht_m, "高血压")
dm_n, dm_ok1, dm_ok2, dm_fl = _xval_stats_gated(dm_m, "糖尿病")

report_lines = [
    "=" * 68,
    "  PatientTables — step5: Disease Registry × Physical Exam Report",
    f"  Elapsed: {elapsed:.1f}s",
    "=" * 68,
    "",
    "─" * 68,
    "  SOURCE FILES",
    "─" * 68,
    f"  高血压.xlsx      : {len(ht_raw):,} rows  "
    f"(unique name+gender: {ht_raw.drop_duplicates(['姓名','性别']).shape[0]:,})",
    f"  糖尿病.xlsx      : {len(dm_raw):,} rows  "
    f"(unique name+gender: {dm_raw.drop_duplicates(['姓名','性别']).shape[0]:,})",
    f"  patient_reference: {len(ref):,} patients",
    f"  fused_filled.csv : {len(fused):,} exam records  "
    f"({fused['patient_id'].nunique():,} unique patients)",
    "",
    "─" * 68,
    "  MATCHING STRATEGY",
    "─" * 68,
    f"  Step 1 — exact (姓名, 性别) match",
    f"  Step 2 — birth-year proximity: |ref.birth_year − est_by| ≤ {BIRTH_YEAR_TOL} yr",
    f"           where est_by = ref_year − age  (ref_year from 末次随访时间→建档日期→2025)",
    f"  Step 3 — age-gate tie-break: prefer candidate with most fused exams",
    f"           satisfying |est_by_disease − exam_est_by| ≤ {AGE_GATE_TOL} yr",
    f"  Exam gate: individual exam rows counted only if age gate passes (±{AGE_GATE_TOL} yr)",
    "",
    "─" * 68,
    "  A. 高血压 PATIENTS — PHYSICAL EXAMS BEFORE DIAGNOSIS",
    "─" * 68,
    f"  Unique matched patients : {ht_m['person_id'].nunique():,}",
    f"  With ≥ 1 exam before dx : {len(ht_counts):,}  ({len(ht_counts)/ht_total_pids:.1%})",
    f"  No exam before dx       : {ht_total_pids - len(ht_counts):,}",
    "",
] + _dist_table(ht_counts, "高血压") + [
    "",
    "─" * 68,
    "  B. 糖尿病 PATIENTS — PHYSICAL EXAMS BEFORE DIAGNOSIS",
    "─" * 68,
    f"  Unique matched patients : {dm_m['person_id'].nunique():,}",
    f"  With ≥ 1 exam before dx : {len(dm_counts):,}  ({len(dm_counts)/dm_total_pids:.1%})",
    f"  No exam before dx       : {dm_total_pids - len(dm_counts):,}",
    "",
] + _dist_table(dm_counts, "糖尿病") + [
    "",
    "─" * 68,
    "  C. COMBINED UNIQUE PATIENTS (HT ∪ DM, overlap = 1)",
    "─" * 68,
    f"  HT only                : {len(ht_only):,}",
    f"  DM only                : {len(dm_only):,}",
    f"  Both HT and DM         : {len(ht_overlap):,}",
    f"  Total unique           : {len(unique_all):,}",
    "",
    "─" * 68,
    "  D. DATA-QUALITY CROSS-CHECK — COMORBIDITY FLAGS",
    "─" * 68,
    "",
    "  D1. 高血压 file: 是否为糖尿病='是'",
    f"      Flagged rows                : {len(ht_dm_flag):,}",
    f"      Unique matched patients     : {ht_dm_flag_m['person_id'].nunique():,}",
    f"      Confirmed in 糖尿病 file ✓  : {len(d1_confirmed):,}  "
    f"({len(d1_confirmed)/max(ht_dm_flag_m['person_id'].nunique(),1):.1%})",
    f"      NOT in 糖尿病 file ✗        : {len(d1_not_found):,}  "
    f"({len(d1_not_found)/max(ht_dm_flag_m['person_id'].nunique(),1):.1%})",
    "",
    "  D2. 糖尿病 file: 是否为高血压='是'",
    f"      Flagged rows                : {len(dm_ht_flag):,}",
    f"      Unique matched patients     : {dm_ht_flag_m['person_id'].nunique():,}",
    f"      Confirmed in 高血压 file ✓  : {len(d2_confirmed):,}  "
    f"({len(d2_confirmed)/max(dm_ht_flag_m['person_id'].nunique(),1):.1%})",
    f"      NOT in 高血压 file ✗        : {len(d2_not_found):,}  "
    f"({len(d2_not_found)/max(dm_ht_flag_m['person_id'].nunique(),1):.1%})",
    "",
    "─" * 68,
    "  E. AGE GATE SUMMARY",
    "─" * 68,
    f"  Gate: |disease_est_by − exam_est_by| ≤ {AGE_GATE_TOL} yr per exam row.",
    f"  Exam rows excluded by age gate:",
    f"    高血压 : {len(ht_excluded):,} rows  from {ht_excluded['person_id'].nunique() if len(ht_excluded) else 0:,} patients",
    f"    糖尿病 : {len(dm_excluded):,} rows  from {dm_excluded['person_id'].nunique() if len(dm_excluded) else 0:,} patients",
    f"  Excluded rows saved to: step5_age_gate_excluded.csv",
    "",
    "─" * 68,
    "  F. CROSS-VALIDATION — RESIDUAL AGE DISCREPANCY (after age gate)",
    "─" * 68,
    f"  Method: compare disease_est_by vs mean(exam_est_by) over gated exams.",
    "",
    "  高血压:",
    f"    Patients with gated exam data : {ht_n:,}",
    f"    Discrepancy ≤ 1 yr  (tight)   : {ht_ok1:,}  ({ht_ok1/max(ht_n,1):.1%})",
    f"    Discrepancy ≤ 2 yr  (ok)      : {ht_ok2:,}  ({ht_ok2/max(ht_n,1):.1%})",
    f"    Discrepancy > {XVAL_THRESHOLD} yr  (flagged) : {ht_fl:,}  ({ht_fl/max(ht_n,1):.1%})",
    "",
    "  糖尿病:",
    f"    Patients with gated exam data : {dm_n:,}",
    f"    Discrepancy ≤ 1 yr  (tight)   : {dm_ok1:,}  ({dm_ok1/max(dm_n,1):.1%})",
    f"    Discrepancy ≤ 2 yr  (ok)      : {dm_ok2:,}  ({dm_ok2/max(dm_n,1):.1%})",
    f"    Discrepancy > {XVAL_THRESHOLD} yr  (flagged) : {dm_fl:,}  ({dm_fl/max(dm_n,1):.1%})",
    "",
    "  Flagged patients saved to: step5_cross_validation_flags.csv",
    "",
    "─" * 68,
    "  OUTPUT FILES",
    "─" * 68,
    f"  step5_report.txt                  — this report",
    f"  step5_ht_exam_before_diag.csv     — {len(ht_out):,} HT patients with ≥1 age-gated exam before dx",
    f"  step5_dm_exam_before_diag.csv     — {len(dm_out):,} DM patients with ≥1 age-gated exam before dx",
    f"  step5_age_gate_excluded.csv       — {len(excluded_all):,} exam rows excluded by age gate",
    f"  step5_cross_validation_flags.csv  — {len(all_flags):,} patients with residual discrepancy > {XVAL_THRESHOLD} yr",
    "",
    "  fused_filled.csv was NOT modified.",
    "=" * 68,
]

report_text = "\n".join(report_lines)
(OUT_DIR / "step5_report.txt").write_text(report_text, encoding="utf-8")
print(f"  step5_report.txt")

print()
print(report_text)
