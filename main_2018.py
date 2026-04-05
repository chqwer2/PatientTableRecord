#!/usr/bin/env python3
"""
main_2018.py — Build golden patient reference from 2018.xlsx + stroke registry.

Run this FIRST, before main.py.

What it does:
  1. Loads stroke registry (2022-2024.xlsx, sheet "脑卒中") — exact 出生日期
  2. Loads 2018.xlsx — exact DOB from 身份证 / 出生日期 column
  3. Clusters patients by (name, gender, birth_year):
       tolerance = 0 if BOTH records have exact DOBs
       tolerance = 1 if either is age-estimated
  4. Saves patient_reference.csv — the golden base for all patient IDs
  5. Saves 2018_intermediate.csv and stroke_intermediate.csv with person_id

Output (all in /Users/haochen/Desktop/PatientTables/output/):
  origin/2018_origin.csv            column-mapped, blank rows/cols removed
  origin/stroke_origin.csv          blank rows/cols removed
  intermediate/2018_intermediate.csv    person_id added, names dropped
  intermediate/stroke_intermediate.csv  person_id added, names dropped
  reference/patient_reference.csv       golden patient base
"""
import difflib
import random
import re
import string
import time
from collections import defaultdict
from pathlib import Path

import pandas as pd

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


# ── Shared utilities ──────────────────────────────────────────────────────────

def _si(v):
    try: return int(float(str(v).strip()))
    except: return None

_DOB_YEAR_RE = re.compile(r'(\d{4})')

def _year_from_dob(dob_val) -> int | None:
    """Extract 4-digit year from any date string."""
    if pd.isna(dob_val):
        return None
    m = _DOB_YEAR_RE.search(str(dob_val).strip())
    y = int(m.group(1)) if m else None
    return y if y and 1900 < y < 2030 else None

def _year_from_id(id_val) -> int | None:
    """Extract birth year from Chinese ID card number."""
    if pd.isna(id_val):
        return None
    s = re.sub(r'\s', '', str(id_val).strip())
    if len(s) == 18:
        y = _si(s[6:10])
        return y if y and 1900 < y < 2030 else None
    if len(s) == 15:
        y = _si(s[6:8])
        return (1900 + y) if y is not None and 0 <= y <= 99 else None
    return None

def _dob_from_id(id_val) -> str:
    """Extract full birth date string (YYYY-MM-DD) from Chinese ID card number."""
    if pd.isna(id_val):
        return ""
    s = re.sub(r'\s', '', str(id_val).strip())
    if len(s) == 18:
        raw = s[6:14]   # YYYYMMDD
        if raw.isdigit():
            return f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"
    if len(s) == 15:
        yy = s[6:8]; mm = s[8:10]; dd = s[10:12]
        if yy.isdigit() and mm.isdigit() and dd.isdigit():
            return f"19{yy}-{mm}-{dd}"
    return ""

def _exam_year(date_val) -> int | None:
    """Extract 4-digit year from an exam/onset date value."""
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
               (prevents merging different people with same name/gender/birth-year
                within the same data source, e.g. two different stroke patients)
           Both exact, DIFFERENT sources (cross-source: 2018 vs stroke) → same birth year.
               (allows slight DOB discrepancies caused by data-entry differences
                between the 2018 health-check file and the stroke registry)
           Both exact, year-only (no full DOB)  → require same birth year.
           At least one estimated                → |by1 - by2| <= 1.
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
                    # Same source, both have full DOBs, they differ → different people.
                    return False
                # Cross-source (2018 vs stroke): DOB may differ slightly due to data
                # entry — fall through to birth-year check below.
            # Year-only exact or cross-source: require same birth year.
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


# ─────────────────────────────────────────────────────────────────────────────
print("=" * 60)
print("  PatientTables — Golden Reference Builder (2018 + Stroke)")
print("=" * 60)

# ── Load stroke data ──────────────────────────────────────────────────────────
print("\n[Load] Reading stroke data from 2022-2024.xlsx…")
t0 = time.perf_counter()
df_stroke = pd.read_excel(PATH / "2022-2024.xlsx", sheet_name="脑卒中", dtype=str)
df_stroke["性别"] = df_stroke["性别"].str.extract(r"([男女])")
df_stroke = df_stroke[df_stroke.notna().any(axis=1)].copy()
print(f"  {len(df_stroke):,} stroke records  ({fmt(time.perf_counter()-t0)})")

# ── Manual semantic overrides: csv_col → col_in_2018 ─────────────────────────
MANUAL_COL_MAP: dict[str, str] = {
    "心电图异常":    "心电图描述",
    "胸部X片异常":   "胸部X线片-异常说明",
    "腹部B超异常":   "腹部B超-异常说明",
}

# ── Step 1: Get union of columns from 2022/2023/2024 CSVs ────────────────────
print("\n[Columns] Reading column headers from 2022/2023/2024 CSVs…")
csv_cols_ordered: list[str] = []
csv_cols: set = set()
for year in ["2022", "2023", "2024"]:
    df_hdr = pd.read_csv(PATH / f"{year}.csv", nrows=0, encoding="gbk", dtype=str)
    renamed = {c: c + "围" for c in df_hdr.columns if c.endswith("-正常范")}
    df_hdr.rename(columns=renamed, inplace=True)
    for c in df_hdr.columns:
        if c not in csv_cols:
            csv_cols_ordered.append(c)
            csv_cols.add(c)
csv_list = csv_cols_ordered
print(f"  Union of CSV columns: {len(csv_cols):,}  (2022-first order)")


def _normalise(col: str) -> list[str]:
    UNIT_RE   = re.compile(r'[/／].{1,6}$')
    PREFIX_RE = re.compile(r'^(?:体检|开始|每次|坚持)')
    s1 = UNIT_RE.sub("", col).strip()
    s2 = PREFIX_RE.sub("", col).strip()
    s3 = PREFIX_RE.sub("", s1).strip()
    return list(dict.fromkeys([col, s1, s2, s3]))


def _find_18col_for_csv(csv_col: str, cols18_set: set, cols18_list: list) -> str | None:
    FUZZY = 0.76
    if csv_col in cols18_set:
        return csv_col
    no_x = re.sub(r"_x$", "", csv_col)
    if no_x != csv_col and no_x in cols18_set:
        return no_x
    for c18 in cols18_list:
        for norm in _normalise(c18):
            if norm == csv_col:
                return c18
    if len(csv_col) >= 3:
        subs = [c for c in cols18_list if csv_col in c]
        if len(subs) == 1:
            return subs[0]
        if len(subs) > 1:
            return min(subs, key=len)
    subs = [c for c in cols18_list if len(c) >= 3 and c in csv_col]
    if len(subs) == 1:
        return subs[0]
    if len(subs) > 1:
        return max(subs, key=len)
    csv_chars = set(csv_col)
    for c18 in cols18_list:
        if set(c18) == csv_chars and len(c18) == len(csv_col):
            return c18
    for cand in dict.fromkeys([csv_col, no_x]):
        m = difflib.get_close_matches(cand, cols18_list, n=1, cutoff=FUZZY)
        if m:
            return m[0]
    return None


# ── Step 2: Load 2018.xlsx ────────────────────────────────────────────────────
print("\n[Load] Reading 2018.xlsx…")
t0 = time.perf_counter()
df18_raw = pd.read_excel(PATH / "2018.xlsx", dtype=str)
total_in = len(df18_raw)

df18_raw.rename(columns={c: c + "围" for c in df18_raw.columns if c.endswith("-正常范")},
                inplace=True)

df18 = df18_raw[df18_raw.notna().any(axis=1)].copy()
removed = total_in - len(df18)
print(f"  Loaded {total_in:,} rows | {removed:,} blank rows removed → {len(df18):,} rows kept")
print(f"  2018.xlsx columns: {len(df18_raw.columns):,}  ({fmt(time.perf_counter()-t0)})")

# Extract 身份证 and 出生日期 BEFORE column mapping — they may not survive the schema restriction
_id18_raw_col  = next((c for c in df18.columns if "身份证" in c or "证件" in c), None)
_dob18_raw_col = next((c for c in df18.columns if "出生" in c), None)
_id18_vals  = df18[_id18_raw_col].reset_index(drop=True)  if _id18_raw_col  else None
_dob18_vals = df18[_dob18_raw_col].reset_index(drop=True) if _dob18_raw_col else None
print(f"  Pre-mapping: ID-card col='{_id18_raw_col}', DOB col='{_dob18_raw_col}'")

# ── Step 3: Column matching — 2018 → CSV schema ───────────────────────────────
print("\n[Filter] Matching CSV schema columns → 2018.xlsx columns…")

cols18_list = sorted(df18.columns)
cols18_set  = set(df18.columns)

csv_to_18:   dict[str, str]  = {}
exact_cols:  list[str]       = []
renamed_map: list[tuple]     = []
empty_cols:  list[str]       = []
claimed18:   set[str]        = set()

for csv_col in csv_list:
    if csv_col in MANUAL_COL_MAP:
        col18 = MANUAL_COL_MAP[csv_col]
        if col18 not in cols18_set:
            empty_cols.append(csv_col)
            continue
    else:
        col18 = _find_18col_for_csv(csv_col, cols18_set, cols18_list)

    if col18 is None:
        empty_cols.append(csv_col)
    elif col18 in claimed18:
        empty_cols.append(csv_col)
    else:
        claimed18.add(col18)
        csv_to_18[csv_col] = col18
        if col18 == csv_col:
            exact_cols.append(csv_col)
        else:
            renamed_map.append((col18, csv_col))

rows = len(df18)
ordered_data: dict[str, pd.Series] = {}
for csv_col in csv_list:
    if csv_col in csv_to_18:
        col18 = csv_to_18[csv_col]
        ordered_data[csv_col] = df18[col18].reset_index(drop=True)
    else:
        ordered_data[csv_col] = pd.Series([None] * rows, dtype=object)

df18 = pd.DataFrame(ordered_data)

# Inject _dob18: prefer direct 出生日期, fall back to 身份证-derived date
def _make_dob18(i):
    dob = str(_dob18_vals.iloc[i]).strip() if (_dob18_vals is not None and pd.notna(_dob18_vals.iloc[i])) else ""
    if dob and re.search(r'\d{4}', dob):
        return dob
    return _dob_from_id(_id18_vals.iloc[i]) if _id18_vals is not None else ""

df18["_dob18"] = [_make_dob18(i) for i in range(len(df18))]
n_dob = (df18["_dob18"] != "").sum()
print(f"  DOB resolved for {n_dob:,} / {len(df18):,} rows (from 出生日期 or 身份证)")

print(f"\n  {'2018 column':<38}  {'CSV column':<38}  Action")
print(f"  {'-'*38}  {'-'*38}  {'-'*12}")
for col18, csv_col in sorted(renamed_map):
    print(f"  {col18:<38}  {csv_col:<38}  renamed")
for c in sorted(exact_cols):
    print(f"  {c:<38}  {'(exact match)':<38}  kept")
for c in sorted(empty_cols):
    print(f"  {'(absent in 2018)':<38}  {c:<38}  added empty")

print(f"\n  Matched+kept : {len(exact_cols) + len(renamed_map)}")
print(f"  Renamed      : {len(renamed_map)}")
print(f"  Added empty  : {len(empty_cols)}")

if df18.get("年度_x") is None or df18["年度_x"].isna().all():
    df18["年度_x"] = "2018"
    print("  Set '年度_x' = 2018 (was absent in source file)")

# ── Step 4: Detect name / gender columns ─────────────────────────────────────
name_col   = next((c for c in df18.columns if "姓名" in c), None)
gender_col = next((c for c in df18.columns if "性别" in c), None)
if not name_col:
    raise ValueError("No name column (含'姓名') found in 2018.xlsx after column matching.")
print(f"\n  Name col: '{name_col}'  |  Gender col: '{gender_col}'")

year_col = next((c for c in df18.columns if "年度" in c), None)
print(f"  Year col: '{year_col}'")


# ─────────────────────────────────────────────────────────────────────────────
# Step 5: Build golden patient reference from 2018 + stroke
# ─────────────────────────────────────────────────────────────────────────────
print("\n[Golden] Building golden patient reference from 2018 + stroke…")
t0 = time.perf_counter()

_pid_pool: set = set()

def _new_pid() -> str:
    # First character is always a letter to prevent Excel scientific-notation
    # auto-formatting (e.g. "6E0024" → "6E+24").
    while True:
        p = random.choice(string.ascii_uppercase) + \
            "".join(random.choices(string.ascii_uppercase + string.digits, k=5))
        if p not in _pid_pool:
            _pid_pool.add(p)
            return p

# (name, gender) → [(by|None, is_exact, exam_yr|None, age|None, dob_str, source)]
golden_ng: dict = defaultdict(list)

# Find exam-date column in the mapped 2018 schema (for accurate by_est)
_exam_col_18 = next((c for c in df18.columns if "体检时间" in c), None) or \
               next((c for c in df18.columns if "检查时间" in c or "检查日期" in c or "体检日期" in c), None)
print(f"  Exam-date col for 2018: '{_exam_col_18}'")

# Add 2018 records
print("  Collecting 2018 records…")
for i in range(len(df18)):
    row      = df18.iloc[i]
    name     = str(row[name_col]).strip()   if pd.notna(row[name_col])                      else ""
    gender_v = str(row[gender_col]).strip() if (gender_col and pd.notna(row[gender_col]))    else ""
    if not name:
        continue
    dob_str  = _make_dob18(i)
    by       = _year_from_dob(dob_str) if dob_str else None
    is_exact = by is not None
    age_v    = _si(row.get("年龄"))
    # Use actual exam year from 体检时间 for birth year estimation;
    # fall back to 年度 (which may be "2018" even for exams done in 2019-2022).
    exam_yr = _exam_year(row.get(_exam_col_18)) if _exam_col_18 else None
    yr_v    = exam_yr or (_si(row.get(year_col)) or 2018)
    if not is_exact:
        by = (yr_v - age_v) if (age_v and age_v > 0) else None
    golden_ng[(name, gender_v)].append((by, is_exact, exam_yr or yr_v, age_v, dob_str, "2018"))

n_2018_recs = sum(len(v) for v in golden_ng.values())
print(f"    {n_2018_recs:,} 2018 person-rows collected")

# Build stroke diagnostic map (first diagnosis per patient)
stroke_diag: dict = {}

# Add stroke records
print("  Collecting stroke records…")
for _, row in df_stroke[df_stroke["姓名"].notna()].iterrows():
    name     = str(row["姓名"]).strip()
    gender_v = str(row["性别"]).strip() if pd.notna(row.get("性别")) else ""
    if not name:
        continue
    ng_key = (name, gender_v)
    if ng_key not in stroke_diag:
        stroke_diag[ng_key] = str(row.get("脑卒中诊断", "")).strip()
    dob_str  = str(row["出生日期"]).strip() if pd.notna(row.get("出生日期")) else ""
    by       = _year_from_dob(dob_str) if dob_str else None
    is_exact = by is not None
    age_v    = _si(row.get("发病年龄"))
    yr_str   = str(row.get("发病日期", ""))
    yr_v     = _si(yr_str[:4]) if (pd.notna(row.get("发病日期", None)) and len(yr_str) >= 4) else None
    exam_yr  = yr_v
    if not is_exact:
        by = (yr_v - age_v) if (yr_v and age_v and age_v > 0) else None
    golden_ng[(name, gender_v)].append((by, is_exact, exam_yr, age_v, dob_str, "stroke"))

n_stroke_recs = sum(len(v) for v in golden_ng.values()) - n_2018_recs
print(f"    {n_stroke_recs:,} stroke person-rows collected")

# Cluster each (name, gender) group and assign person_ids
patient_lookup: dict = {}    # (name, gender) → [(canonical_by, pid)]
reference_rows: list = []

for (name, gender), records in sorted(golden_ng.items()):
    # records: (by, is_exact, exam_yr, age, dob_str, source)
    # (by, is_exact, exam_yr, age, full_dob) — full_dob used for exact dedup
    by_info  = [(r[0], r[1], r[2], r[3], _norm_dob(r[4]), r[5]) for r in records]
    dob_strs = [r[4] for r in records]
    sources  = [r[5] for r in records]

    clusters = _cluster_records(by_info)
    patient_lookup[(name, gender)] = []
    ng_key = (name, gender)

    for cby, cluster_idxs in clusters:
        pid = _new_pid()
        patient_lookup[(name, gender)].append((cby, pid))
        best_dob = next((dob_strs[i] for i in cluster_idxs if dob_strs[i]), "")
        src_set  = {sources[i] for i in cluster_idxs}
        reference_rows.append({
            "person_id":        pid,
            "name":             name,
            "gender":           gender,
            "birth_year":       cby if cby is not None else "",
            "dob":              best_dob,
            "source":           "+".join(sorted(src_set)),
            "in_2018":          "Y" if "2018"   in src_set else "N",
            "in_stroke":        "Y" if "stroke" in src_set else "N",
            "in_2022":          "N",
            "in_2023":          "N",
            "in_2024":          "N",
            "stroke_diagnosis": stroke_diag.get(ng_key, ""),
        })

n_total  = len(reference_rows)
n_both   = sum(1 for r in reference_rows if r["in_2018"] == "Y" and r["in_stroke"] == "Y")
n_18only = sum(1 for r in reference_rows if r["in_2018"] == "Y" and r["in_stroke"] == "N")
n_sk_only= sum(1 for r in reference_rows if r["in_2018"] == "N" and r["in_stroke"] == "Y")
print(f"  {n_total:,} golden patients total")
print(f"    {n_both:,} in both 2018 and stroke")
print(f"    {n_18only:,} 2018-only")
print(f"    {n_sk_only:,} stroke-only")
print(f"  Done  ({fmt(time.perf_counter() - t0)})")


# ─────────────────────────────────────────────────────────────────────────────
# Step 6: Assign person_ids and save output files
# ─────────────────────────────────────────────────────────────────────────────
print("\n[Save] Writing output files…")
t0 = time.perf_counter()

def _get_pid_golden(name: str, gender: str, by_est: int | None) -> str:
    """Return person_id from golden patient_lookup — closest birth year match."""
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
        else:
            diff = abs(cby - by_est)
            if diff <= 1 and diff < best_diff:
                best_diff, best_pid = diff, pid
    return best_pid or candidates[0][1]

# ── Save patient reference (golden base) ─────────────────────────────────────
ref_path = OUT_REF / "patient_reference.csv"
ref_df = pd.DataFrame(reference_rows)
ref_df.to_csv(ref_path, index=False, encoding="utf-8-sig")
print(f"  reference/patient_reference.csv  {len(ref_df):,} golden patients  "
      f"({fmt(time.perf_counter()-t0)})")

# ── Save 2018 origin ──────────────────────────────────────────────────────────
t0 = time.perf_counter()
df_orig = df18.copy()
origin_drop = [c for c in df_orig.columns if "姓名" in c or c == "_dob18"]
df_orig.drop(columns=origin_drop, errors="ignore", inplace=True)
blank_orig = [c for c in df_orig.columns if df_orig[c].isna().all()]
df_orig.drop(columns=blank_orig, inplace=True)
df_orig.to_csv(OUT_ORIGIN / "2018_origin.csv", index=False, encoding="utf-8-sig")
print(f"  origin/2018_origin.csv   {len(df_orig):,} rows | {len(df_orig.columns)} cols  "
      f"({fmt(time.perf_counter()-t0)})")

# ── Assign person_ids to 2018 rows and save intermediate ─────────────────────
t0 = time.perf_counter()

def _dob_year18(i):
    dob = _make_dob18(i)
    m = _DOB_YEAR_RE.search(dob)
    return int(m.group(1)) if m else None

df18_out = df18.copy()
df18_out.insert(0, "person_id", [
    _get_pid_golden(
        str(df18.iloc[i][name_col]).strip()   if pd.notna(df18.iloc[i][name_col])                    else "",
        str(df18.iloc[i][gender_col]).strip() if (gender_col and pd.notna(df18.iloc[i][gender_col])) else "",
        _dob_year18(i),
    )
    for i in range(len(df18))
])
df18_out.drop(columns=["_dob18"] + [c for c in df18_out.columns if "姓名" in c],
              errors="ignore", inplace=True)
df18_out.to_csv(OUT_INTER / "2018_intermediate.csv", index=False, encoding="utf-8-sig")
print(f"  intermediate/2018_intermediate.csv   {len(df18_out):,} rows | {len(df18_out.columns)} cols  "
      f"({fmt(time.perf_counter()-t0)})")

# ── Save stroke origin ────────────────────────────────────────────────────────
t0 = time.perf_counter()
df_stroke_orig = df_stroke.copy()
blank_s = [c for c in df_stroke_orig.columns if df_stroke_orig[c].isna().all()]
df_stroke_orig.drop(columns=blank_s, inplace=True)
df_stroke_orig.to_csv(OUT_ORIGIN / "stroke_origin.csv", index=False, encoding="utf-8-sig")
print(f"  origin/stroke_origin.csv   {len(df_stroke_orig):,} rows | {len(df_stroke_orig.columns)} cols  "
      f"({fmt(time.perf_counter()-t0)})")

# ── Assign person_ids to stroke rows and save intermediate ────────────────────
t0 = time.perf_counter()
df_stroke_out = df_stroke.copy()
df_stroke_out.insert(0, "person_id", df_stroke_out.apply(
    lambda r: _get_pid_golden(
        str(r["姓名"]).strip() if pd.notna(r["姓名"]) else "",
        str(r["性别"]).strip() if pd.notna(r.get("性别")) else "",
        _year_from_dob(r.get("出生日期")),
    ), axis=1
))
df_stroke_out.drop(columns=["姓名"], errors="ignore", inplace=True)
df_stroke_out.to_csv(OUT_INTER / "stroke_intermediate.csv", index=False, encoding="utf-8-sig")
print(f"  intermediate/stroke_intermediate.csv   {len(df_stroke_out):,} rows | "
      f"{len(df_stroke_out.columns)} cols  ({fmt(time.perf_counter()-t0)})")


total = time.perf_counter() - t_start
print(f"""
{'=' * 60}
  Done in {fmt(total)}

  Golden reference : {len(ref_df):,} patients (2018 + stroke)
  Output:
    output/origin/2018_origin.csv
    output/origin/stroke_origin.csv
    output/intermediate/2018_intermediate.csv
    output/intermediate/stroke_intermediate.csv
    output/reference/patient_reference.csv  (golden base)

  Run main.py next to match 2022/2023/2024 CSVs against golden IDs.
{'=' * 60}""")
