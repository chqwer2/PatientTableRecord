#!/usr/bin/env python3
"""
main.py — Match 2022/2023/2024 annual check CSVs against golden patient IDs.

Run AFTER main_2018.py (which builds the golden patient_reference.csv from
2018.xlsx + stroke registry).

What it does:
  1. Loads patient_reference.csv (golden base: 2018 + stroke patients with exact DOBs)
  2. Loads 2022/2023/2024 CSVs
  3. For each (name, gender) group:
       — records whose estimated birth year (exam_year − age) falls within ±1
         of a golden patient's birth year → assigned that golden person_id
       — records with NO golden match → clustered among themselves → new person_id
  4. Cross-year monotonicity check (age must not decrease or jump > 2/yr across years)
  5. Updates patient_reference.csv: sets in_2022/in_2023/in_2024 flags,
     appends any new CSV-only patients
  6. Saves 2022_intermediate.csv, 2023_intermediate.csv, 2024_intermediate.csv

Output (all in /Users/haochen/Desktop/PatientTables/output/):
  origin/2022_origin.csv   …2023… …2024…  — raw, blank rows/cols removed
  intermediate/2022_intermediate.csv …     — person_id added, names dropped
  reference/patient_reference.csv         — updated with year flags + new patients
"""
import random
import re
import string
import time
from collections import defaultdict
from pathlib import Path

import pandas as pd

DROP_CSV = ["姓名_x"]   # name column in annual health check CSVs


PATH       = Path("/Users/haochen/Desktop/PatientTables")
OUT        = PATH / "output"
OUT_ORIGIN = OUT / "origin"
OUT_INTER  = OUT / "intermediate"
OUT_CLEAN  = OUT / "clean"
OUT_REF    = OUT / "reference"
for _d in [OUT, OUT_ORIGIN, OUT_INTER, OUT_CLEAN, OUT_REF]:
    _d.mkdir(parents=True, exist_ok=True)

t_start = time.perf_counter()


def fmt(s: float) -> str:
    return f"{s:.0f}s" if s < 60 else f"{int(s//60)}m {int(s%60)}s"


def _si(v):
    try: return int(float(str(v).strip()))
    except: return None

_DOB_YEAR_RE = re.compile(r'(\d{4})')

def _year_from_dob(dob_val) -> int | None:
    if pd.isna(dob_val):
        return None
    m = _DOB_YEAR_RE.search(str(dob_val).strip())
    y = int(m.group(1)) if m else None
    return y if y and 1900 < y < 2030 else None

def _exam_year(date_val) -> int | None:
    """Extract 4-digit year from an exam date value."""
    if pd.isna(date_val):
        return None
    m = _DOB_YEAR_RE.search(str(date_val).strip())
    y = int(m.group(1)) if m else None
    return y if y and 1990 < y < 2030 else None


def _norm_dob(dob_str) -> str | None:
    """Normalise a DOB string to 'YYYY-MM-DD' for exact comparison, or None."""
    if not dob_str:
        return None
    s = str(dob_str).strip()
    m = re.match(r'(\d{4})-(\d{2})-(\d{2})', s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    m2 = re.match(r'(\d{4})(\d{2})(\d{2})', s)
    if m2:
        return f"{m2.group(1)}-{m2.group(2)}-{m2.group(3)}"
    return None


def _cluster_records(records):
    """
    records: list of (by|None, is_exact, exam_yr|None, age|None, full_dob|None, source|None)

    Two records are compatible (same person) if ALL of:
      1. DOB / birth-year check:
           Both exact, SAME source, full DOBs → require IDENTICAL DOB string.
           Both exact, DIFFERENT sources       → require same birth year only.
           Both exact, year-only               → require same birth year.
           At least one estimated              → |by1 - by2| <= 1.
      2. Same exam year:
           At least one exact        → |age1 - age2| <= 1
           Both estimated            → ages must be equal (no tolerance)
      3. Cross-year monotonicity:
           At least one exact        → max age jump = yr_gap + 1
           Both estimated            → max age jump = yr_gap

    Greedy: each record joins the first cluster whose EVERY member it is compatible with.
    Returns: [(canonical_by|None, [indices])]
    """
    def _compat(a, b):
        by_a, ex_a, yr_a, age_a, dob_a, src_a = a
        by_b, ex_b, yr_b, age_b, dob_b, src_b = b
        # 1. DOB / birth-year proximity
        if ex_a and ex_b:
            if dob_a and dob_b:
                same_source = (src_a is not None and src_b is not None and src_a == src_b)
                if same_source and dob_a != dob_b:
                    return False
            if by_a is not None and by_b is not None and by_a != by_b:
                return False
        else:
            # At least one is estimated: allow ±1 year tolerance.
            if by_a is not None and by_b is not None:
                if abs(by_a - by_b) > 1:
                    return False
        # 2. Same exam year
        if yr_a is not None and yr_b is not None and yr_a == yr_b:
            if age_a is not None and age_b is not None:
                same_yr_tol = 1 if (ex_a or ex_b) else 0
                if abs(age_a - age_b) > same_yr_tol:
                    return False
        # 3. Cross-year monotonicity
        if (yr_a is not None and yr_b is not None and yr_a != yr_b
                and age_a is not None and age_b is not None):
            (lo_yr, lo_age), (hi_yr, hi_age) = sorted(
                [(yr_a, age_a), (yr_b, age_b)], key=lambda x: x[0])
            gap = hi_yr - lo_yr
            if hi_age < lo_age:
                return False
            max_jump = gap + 1 if (ex_a or ex_b) else gap
            if hi_age > lo_age + max_jump:
                return False
        return True

    order = sorted(range(len(records)), key=lambda i: (
        records[i][0] if records[i][0] is not None else float('inf'),
        records[i][2] if records[i][2] is not None else float('inf'),
        records[i][3] if records[i][3] is not None else 0,
    ))
    # (source field at index 5 is not used in sort/canonical — only in _compat)
    clusters: list[list[int]] = []
    for i in order:
        placed = False
        for clust in clusters:
            if all(_compat(records[i], records[j]) for j in clust):
                clust.append(i); placed = True; break
        if not placed:
            clusters.append([i])
    result = []
    for clust in clusters:
        bys = [records[i][0] for i in clust if records[i][0] is not None]
        cby = round(sum(bys) / len(bys)) if bys else None
        result.append((cby, clust))
    return result


def _expected_age_from_dob(dob_str, exam_date_str, exam_yr: int | None) -> tuple[int | None, int | None]:
    """
    Return (min_expected_age, max_expected_age) for an exam.
    - Full DOB + full exam date → exact single value (min == max).
    - Full DOB + exam year only → [exam_yr-by-1, exam_yr-by]  (birthday could be before/after).
    - Year-only DOB            → same two-value range.
    - DOB missing              → (None, None).
    """
    dob_clean = str(dob_str).strip() if dob_str else ""
    dob_m = re.match(r'(\d{4})-(\d{2})-(\d{2})', dob_clean)
    if dob_m:
        by = int(dob_m.group(1))
        bm = int(dob_m.group(2))
        bd = int(dob_m.group(3))
    else:
        by = _year_from_dob(dob_clean)
        bm = bd = None
    if by is None or exam_yr is None:
        return None, None
    # Try exact exam date
    exam_clean = str(exam_date_str).strip() if exam_date_str else ""
    exam_m = re.match(r'(\d{4})-(\d{2})-(\d{2})', exam_clean)
    if exam_m and bm is not None and bd is not None:
        ey = int(exam_m.group(1))
        em = int(exam_m.group(2))
        ed = int(exam_m.group(3))
        exact = ey - by - (0 if (em, ed) >= (bm, bd) else 1)
        return exact, exact
    # Year only
    return exam_yr - by - 1, exam_yr - by


# ─────────────────────────────────────────────────────────────────────────────
print("=" * 60)
print("  PatientTables — 2022/2023/2024 CSV Patient ID Assignment")
print("=" * 60)

# ─────────────────────────────────────────────────────────────────────────────
# Step 1: Load all 3 CSV files — keep ALL columns, drop only 100%-blank rows
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n{'Source':<12} {'Original':>12} {'Blank removed':>15} {'Kept':>10}  Reason")
print("-" * 70)

frames = {}

for year in ["2022", "2023", "2024"]:
    t0 = time.perf_counter()
    chunks_all, chunks_keep = [], []
    for chunk in pd.read_csv(
        PATH / f"{year}.csv",
        encoding="gbk",
        chunksize=50_000,
        dtype=str,
        low_memory=False,
    ):
        chunks_all.append(len(chunk))
        chunks_keep.append(chunk[chunk.notna().any(axis=1)])

    total_in = sum(chunks_all)
    df = pd.concat(chunks_keep, ignore_index=True)
    removed  = total_in - len(df)
    df.rename(columns={c: c + "围" for c in df.columns if c.endswith("-正常范")}, inplace=True)
    frames[year] = df
    print(f"  {year+'.csv':<10} {total_in:>12,} {removed:>15,} {len(df):>10,}  Excel padding")

print()
print("  NOTE: 'Blank removed' in CSVs = Excel export artifact (100%-blank rows).")


# ─────────────────────────────────────────────────────────────────────────────
# Step 1.5: Save origin versions — raw data, blank rows+columns removed only
# ─────────────────────────────────────────────────────────────────────────────
print("\n[Origin] Saving origin versions → output/origin/")
for year in ["2022", "2023", "2024"]:
    t0 = time.perf_counter()
    orig = frames[year].copy()
    blank_cols = [c for c in orig.columns if orig[c].isna().all()]
    orig.drop(columns=blank_cols, inplace=True)
    orig.to_csv(OUT_ORIGIN / f"{year}_origin.csv", index=False, encoding="utf-8-sig")
    print(f"  {year}_origin.csv   {len(orig):>8,} rows | {len(orig.columns):>4} cols "
          f"| {len(blank_cols)} blank cols dropped  ({fmt(time.perf_counter()-t0)})")


# ─────────────────────────────────────────────────────────────────────────────
# Step 2: Load golden reference and build patient_lookup
#
# Golden patients (2018 + stroke) have exact DOBs.
# For 2022/23/24 records, estimated birth year = exam_year − age (±1 error).
#
# Matching priority:
#   1. If abs(est_birth_year - golden_birth_year) <= 1 → same golden patient
#   2. Otherwise → cluster among non-golden records → new patient_id
#
# Cross-year consistency: because birth_year = exam_year - age, a person who
# ages correctly between years will have a stable birth_year estimate:
#   age 73 in 2022 → est_by = 1949
#   age 74 in 2023 → est_by = 1949  ✓ (same cluster)
#   age 72 in 2023 → est_by = 1951  ✗ (diff=2 > 1 → different cluster)
# Monotonicity violations are thus naturally prevented by the ±1 tolerance,
# but an explicit check is run at the end to catch any edge cases.
# ─────────────────────────────────────────────────────────────────────────────
print("\n[Patient list] Loading golden reference and matching 2022/23/24 records…")
t0 = time.perf_counter()

ref_path = OUT_REF / "patient_reference.csv"
if not ref_path.exists():
    raise FileNotFoundError(
        f"patient_reference.csv not found at {ref_path}\n"
        "Run main_2018.py first to build the golden patient reference."
    )

golden_df = pd.read_csv(ref_path, dtype=str)
_pid_pool: set = set(golden_df["person_id"].dropna().tolist())

# golden_lookup: (name, gender) → [(canonical_by|None, pid)]
# golden_dobs:  pid → full dob string (for exact expected-age computation)
golden_lookup: dict = {}
golden_dobs:   dict = {}
for _, r in golden_df.iterrows():
    name   = str(r["name"]).strip()
    gender = str(r.get("gender", "")).strip() if pd.notna(r.get("gender")) else ""
    cby    = _si(r.get("birth_year")) if pd.notna(r.get("birth_year", None)) else None
    pid    = str(r["person_id"])
    dob    = str(r.get("dob", "")).strip() if pd.notna(r.get("dob")) else ""
    golden_lookup.setdefault((name, gender), []).append((cby, pid))
    if dob:
        golden_dobs[pid] = dob

print(f"  Loaded {len(golden_df):,} golden patients  "
      f"({len(golden_dobs):,} with full DOB for exact age matching)")


def _new_pid() -> str:
    # First character is always a letter to prevent Excel scientific-notation
    # auto-formatting (e.g. "6E0024" → "6E+24").
    while True:
        p = random.choice(string.ascii_uppercase) + \
            "".join(random.choices(string.ascii_uppercase + string.digits, k=5))
        if p not in _pid_pool:
            _pid_pool.add(p)
            return p


# ── Collect all 2022/23/24 records by (name, gender) ─────────────────────────
# (by|None, is_exact, exam_yr|None, age|None) per record
_ng_records: dict = defaultdict(list)

for year in ["2022", "2023", "2024"]:
    df_y      = frames[year]
    yr_int    = int(year)
    exam_col  = next((c for c in df_y.columns if "体检时间" in c), None) or \
                next((c for c in df_y.columns if "检查时间" in c or "检查日期" in c), None)
    base_cols = ["姓名_x", "性别_x", "年龄", "年度_x"]
    extra     = [exam_col] if exam_col else []
    for _, row in df_y[base_cols + extra].iterrows():
        name   = str(row["姓名_x"]).strip() if pd.notna(row["姓名_x"]) else ""
        gender = str(row["性别_x"]).strip() if pd.notna(row["性别_x"]) else ""
        if not name:
            continue
        age       = _si(row.get("年龄"))
        exam_date = str(row.get(exam_col, "")).strip() if exam_col else ""
        exam_yr   = _exam_year(exam_date) if exam_date else None
        yr        = exam_yr or (_si(row.get("年度_x")) or yr_int)
        by        = (yr - age) if (age and age > 0) else None
        # (by, is_exact, exam_yr, age, exam_date_str)
        _ng_records[(name, gender)].append((by, False, exam_yr or yr, age, exam_date))

# ── patient_lookup: start with golden patients ───────────────────────────────
patient_lookup: dict = {k: list(v) for k, v in golden_lookup.items()}
new_patients:   list = []   # patients found only in 2022/23/24 CSVs

def _golden_age_ok(rec, gcby, gpid):
    """
    True if the record's actual age is consistent with the golden patient.
    Uses exact DOB + exam date when available for precise expected-age check;
    falls back to ±1 birth-year proximity otherwise.
    """
    by, is_exact, exam_yr, age, exam_date = rec
    # Birth year proximity (always required)
    if gcby is not None and by is not None and abs(gcby - by) > 1:
        return False
    # Exact age check: if golden has full DOB, compute expected age
    gdob = golden_dobs.get(gpid, "")
    if gdob and exam_yr is not None and age is not None:
        min_age, max_age = _expected_age_from_dob(gdob, exam_date, exam_yr)
        if min_age is not None and not (min_age - 1 <= age <= max_age + 1):
            return False
    return True


for (name, gender), records in sorted(_ng_records.items()):
    # records: (by, is_exact, exam_yr, age, exam_date_str)
    ng_key       = (name, gender)
    golden_cands = golden_lookup.get(ng_key, [])
    # CSV records have no full DOB — pass None as 5th (full_dob) and 6th (source)
    cluster_input = [(r[0], r[1], r[2], r[3], None, None) for r in records]

    if not golden_cands:
        # No golden anchor — cluster entirely from 2022/23/24 records
        clusters = _cluster_records(cluster_input)
        patient_lookup[ng_key] = []
        for cby, _ in clusters:
            pid = _new_pid()
            patient_lookup[ng_key].append((cby, pid))
            new_patients.append({
                "person_id": pid, "name": name, "gender": gender,
                "birth_year": cby if cby is not None else "", "dob": "",
                "source": "csv",
                "in_2018": "N", "in_stroke": "N",
                "in_2022": "N", "in_2023": "N", "in_2024": "N",
                "stroke_diagnosis": "",
            })
    else:
        # Has golden anchors.
        # A record matches golden only if birth year AND exact age (from DOB) agree.
        non_golden = [
            rec for rec in records
            if rec[0] is not None
            and not any(
                _golden_age_ok(rec, gcby, gpid)
                for gcby, gpid in golden_cands
            )
        ]
        if non_golden:
            clusters = _cluster_records([(r[0], r[1], r[2], r[3], None, None) for r in non_golden])
            for cby, _ in clusters:
                pid = _new_pid()
                patient_lookup[ng_key].append((cby, pid))
                new_patients.append({
                    "person_id": pid, "name": name, "gender": gender,
                    "birth_year": cby if cby is not None else "", "dob": "",
                    "source": "csv",
                    "in_2018": "N", "in_stroke": "N",
                    "in_2022": "N", "in_2023": "N", "in_2024": "N",
                    "stroke_diagnosis": "",
                })

total_patients = sum(len(v) for v in patient_lookup.values())
print(f"  {total_patients:,} total patients  "
      f"({len(golden_df):,} golden + {len(new_patients):,} CSV-only new)")
print(f"  Done  ({fmt(time.perf_counter() - t0)})")


def _get_pid(name: str, gender: str, yr: int | None, age: int | None,
             dob_year: int | None = None, exam_date: str | None = None) -> str:
    """Return person_id — closest birth-year match within tolerance wins.
    When the candidate has a full golden DOB, also verifies the record's actual
    age is consistent with that DOB + exam date (exact expected-age check).
    Returns "" if no candidate passes both checks.
    """
    by_est = dob_year if dob_year is not None else (
        (yr - age) if (yr and age and age > 0) else None
    )
    candidates = patient_lookup.get((name, gender), [])
    if not candidates:
        return ""
    if by_est is None:
        return candidates[0][1]
    best_pid, best_diff = None, float("inf")
    for cby, pid in candidates:
        if cby is None:
            if best_pid is None:
                best_pid = pid
            continue
        diff = abs(cby - by_est)
        if diff > 1:
            continue
        # Exact DOB age check: reject if the golden patient's DOB predicts a
        # different age than what this record shows.
        gdob = golden_dobs.get(pid, "")
        if gdob and yr is not None and age is not None:
            min_a, max_a = _expected_age_from_dob(gdob, exam_date, yr)
            if min_a is not None and not (min_a - 1 <= age <= max_a + 1):
                continue   # wrong age for this golden patient → different person
        if diff < best_diff:
            best_diff, best_pid = diff, pid
    return best_pid or ""


# ─────────────────────────────────────────────────────────────────────────────
# Step 3: Add person_id, drop names, save intermediate files
# ─────────────────────────────────────────────────────────────────────────────
print("\n[Intermediate] Adding person_id and saving intermediate files…")

frames_with_pid = {}

for year in ["2022", "2023", "2024"]:
    t0 = time.perf_counter()
    df = frames[year].copy()
    yr_int   = int(year)
    exam_col = next((c for c in df.columns if "体检时间" in c), None) or \
               next((c for c in df.columns if "检查时间" in c or "检查日期" in c), None)

    df.insert(0, "person_id", df.apply(
        lambda r, _yr=yr_int, _ec=exam_col: _get_pid(
            str(r["姓名_x"]).strip() if pd.notna(r["姓名_x"]) else "",
            str(r["性别_x"]).strip() if pd.notna(r["性别_x"]) else "",
            _si(r.get("年度_x")) or _yr,
            _si(r.get("年龄")),
            dob_year=None,
            exam_date=str(r.get(_ec, "")).strip() if _ec else None,
        ), axis=1
    ))
    df.drop(columns=[c for c in DROP_CSV if c in df.columns], inplace=True)
    df.to_csv(OUT_INTER / f"{year}_intermediate.csv", index=False, encoding="utf-8-sig")
    frames_with_pid[year] = df
    print(f"  intermediate/{year}_intermediate.csv   {len(df):,} rows | {len(df.columns)} cols  "
          f"({fmt(time.perf_counter()-t0)})")


# ─────────────────────────────────────────────────────────────────────────────
# Step 4: Update patient_reference.csv
#   a. Set in_2022/in_2023/in_2024 flags for ALL patients (golden + new)
#   b. Append new CSV-only patients
# ─────────────────────────────────────────────────────────────────────────────
print("\n[Reference] Updating patient_reference.csv…")
t0 = time.perf_counter()

# Collect pids seen per year
pids_per_year = {}
for year in ["2022", "2023", "2024"]:
    pids_per_year[year] = set(
        frames_with_pid[year]["person_id"].dropna().astype(str).tolist()
    )

ref_df = pd.read_csv(ref_path, dtype=str)

# Ensure in_20XX columns exist
for year in ["2022", "2023", "2024"]:
    col = f"in_{year}"
    if col not in ref_df.columns:
        ref_df[col] = "N"

# Update flags for existing (golden) patients
for year in ["2022", "2023", "2024"]:
    col  = f"in_{year}"
    mask = ref_df["person_id"].isin(pids_per_year[year])
    ref_df.loc[mask, col] = "Y"

# Append new CSV-only patients (with updated year flags)
if new_patients:
    new_df = pd.DataFrame(new_patients)
    for year in ["2022", "2023", "2024"]:
        col  = f"in_{year}"
        mask = new_df["person_id"].isin(pids_per_year[year])
        new_df.loc[mask, col] = "Y"
    for col in ref_df.columns:
        if col not in new_df.columns:
            new_df[col] = ""
    ref_df = pd.concat([ref_df, new_df[ref_df.columns]], ignore_index=True)
    print(f"  Added {len(new_patients):,} new CSV-only patients")

ref_df.to_csv(ref_path, index=False, encoding="utf-8-sig")
print(f"  reference/patient_reference.csv  {len(ref_df):,} total patients  "
      f"({fmt(time.perf_counter()-t0)})")


# ─────────────────────────────────────────────────────────────────────────────
# Step 5: Cross-year age monotonicity validation
#
# Same person must NOT have their age decrease between years.
# Must NOT increase by more than 2 per year (accounting for ±1 age estimation).
# The ±1 birth-year clustering already prevents most violations; this check
# catches any edge cases and reports them.
# ─────────────────────────────────────────────────────────────────────────────
print("\n[Monotonicity] Validating age progression across years…")

pid_year_age: dict = defaultdict(list)  # pid → [(year, age)]

for year in ["2022", "2023", "2024"]:
    df  = frames_with_pid[year]
    yr_int = int(year)
    for _, row in df[["person_id", "年龄", "年度_x"]].iterrows():
        pid = str(row["person_id"])
        age = _si(row.get("年龄"))
        yr  = _si(row.get("年度_x")) or yr_int
        if pid and age:
            pid_year_age[pid].append((yr, age))

violations = 0
for pid, records in pid_year_age.items():
    # Deduplicate same-year records, take median age if multiple
    by_year: dict = defaultdict(list)
    for yr, age in records:
        by_year[yr].append(age)
    sorted_recs = sorted(
        (yr, round(sum(ages)/len(ages))) for yr, ages in by_year.items()
    )
    for i in range(1, len(sorted_recs)):
        yr1, age1 = sorted_recs[i-1]
        yr2, age2 = sorted_recs[i]
        yr_gap = yr2 - yr1
        if age2 < age1:
            print(f"  WARN pid={pid}: age decreased {age1}→{age2}  ({yr1}→{yr2})")
            violations += 1
        elif yr_gap > 0 and age2 > age1 + 2 * yr_gap:
            print(f"  WARN pid={pid}: age jumped too fast {age1}→{age2}  ({yr1}→{yr2}, gap={yr_gap}yr)")
            violations += 1

if violations == 0:
    print("  All records pass age-monotonicity check.")
else:
    print(f"  {violations} violation(s) found — review clustering logic or source data.")


# ─────────────────────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────────────────────
total = time.perf_counter() - t_start

n_golden   = len(golden_df)
n_csv_new  = len(new_patients)
in_both_22_23_24 = ref_df[
    (ref_df.get("in_2022", "N") == "Y") &
    (ref_df.get("in_2023", "N") == "Y") &
    (ref_df.get("in_2024", "N") == "Y")
]

print(f"""
{'=' * 60}
  Done in {fmt(total)}

  Golden patients (2018+stroke) : {n_golden:,}
  New CSV-only patients          : {n_csv_new:,}
  Total unique patients          : {len(ref_df):,}
  In all 3 CSV years (22+23+24)  : {len(in_both_22_23_24):,}

  Output layout:
    output/origin/        ← raw data, blank rows/cols removed
    output/intermediate/  ← person_id added, names dropped
    output/reference/     ← patient_reference.csv (id ↔ name/gender/DOB)

  Run main_2018.py was already run (golden base).
  Run main2.py next (API-based lab triplet processing).
{'=' * 60}""")
