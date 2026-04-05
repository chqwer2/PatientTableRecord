#!/usr/bin/env python3
"""
step2_fuse_clean_files.py — Fuse all clean CSVs, deduplicate, analyse.

Steps:
  1. Load *_with_id.csv (main.py output) + *_clean.csv (main_2018.py output),
     fuse on shared columns only (inner join). Skip incompatible schemas.
  2. Normalise column names: strip _x suffix, rename _person_key → patient_id,
     drop _source. Sort rows by 体检日期 then patient_id.
  3. Drop exact duplicate rows.
  4. Drop rows where vacant fraction > VACANT_THRESHOLD.
  5. Text summaries for B超 / X片 / 心电图 columns: split on all delimiters,
     strip measurements (44mm, 33*19mm, etc.), deduplicate terms, one file
     per column in output/text_summaries/.
  6. Outlier report:
       a. Patient-entry distribution (how many patients have 1/2/3/… visits)
       b. Per-file dropped column counts
       c. Numeric IQR outliers + categorical rarity per column

Outputs in /Users/haochen/Desktop/PatientTables/output/:
  fused.csv                       — fused, sorted, deduped table
  dropped_rows.csv                — rows dropped by vacant threshold
  text_summaries/<col>.txt        — term frequency for each medical text column
  outlier_report.txt              — outlier diagnosis
"""
import re
import time
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd

# ── Configuration ─────────────────────────────────────────────────────────────
OUT_ROOT          = Path("/Users/haochen/Desktop/PatientTables/output")
OUT_DIR           = OUT_ROOT / "clean"      # processed files from main.py / main_2018.py
OUT_ANALYSIS      = OUT_ROOT / "analysis"   # fused.csv, reports, text_summaries
OUT_ANALYSIS.mkdir(parents=True, exist_ok=True)
VACANT_THRESHOLD  = 0.70
OUTLIER_IQR_MULT  = 2.5
RARE_FREQ_FRAC    = 0.01
RARE_FREQ_ABS     = 3
MIN_SHARED_COLS   = 20
MEDICAL_TEXT_COLS = []   # leave [] to auto-detect

t_start = time.perf_counter()


def fmt(s: float) -> str:
    return f"{s:.1f}s" if s < 60 else f"{int(s//60)}m {int(s%60)}s"


# ─────────────────────────────────────────────────────────────────────────────
# Step 1 — Load and fuse
# ─────────────────────────────────────────────────────────────────────────────
print("=" * 65)
print("  step2_fuse_clean_files — Fuse · Deduplicate · Analyse")
print("=" * 65)

# All *_clean.csv in output/clean/ (written by main.py and main_2018.py)
clean_files = sorted(OUT_DIR.glob("*_clean.csv"))
if not clean_files:
    raise FileNotFoundError(
        f"No *_clean.csv found in {OUT_DIR}\n"
        "Run main.py then main_2018.py first."
    )

print(f"\n[Load] Found {len(clean_files)} files:")
raw_frames: list[pd.DataFrame] = []
file_cols:  list[set[str]]     = []

for fp in clean_files:
    t0 = time.perf_counter()
    df = pd.read_csv(fp, encoding="utf-8-sig", dtype=str, low_memory=False)
    source = fp.stem.replace("_clean", "")
    df["_source"] = source

    # Normalise known rename patterns before stripping _x globally
    for old, new in [("姓名_x", "姓名"), ("性别_x", "性别"),
                     ("年度_x", "年度"), ("体检日期_x", "体检日期")]:
        if old in df.columns:
            df.rename(columns={old: new}, inplace=True)
    if "年龄" not in df.columns and "发病年龄" in df.columns:
        df.rename(columns={"发病年龄": "年龄"}, inplace=True)
    # Strip remaining _x suffixes
    df.rename(columns={c: c[:-2] for c in df.columns if c.endswith("_x")}, inplace=True)

    file_cols.append(set(df.columns))
    raw_frames.append(df)
    print(f"  {fp.name:<28} {len(df):>6,} rows × {len(df.columns):>3} cols  ({fmt(time.perf_counter()-t0)})")

# ── Shared-column inner join — skip schema-incompatible files ─────────────
active_fps    = list(clean_files)
active_frames = list(raw_frames)
active_cols   = list(file_cols)
skipped_files: list[tuple[str, int]] = []   # (name, shared_cols_when_included)

while len(active_cols) > 1:
    candidate = set.intersection(*active_cols)
    if len(candidate) >= MIN_SHARED_COLS:
        break
    worst_idx = max(range(len(active_cols)),
                    key=lambda i: len(set.intersection(
                        *[c for j, c in enumerate(active_cols) if j != i])))
    worst_fp = active_fps[worst_idx]
    print(f"  [skip] {worst_fp.name} — only {len(candidate)} shared cols if included "
          f"(threshold {MIN_SHARED_COLS}); excluded from fuse.")
    skipped_files.append((worst_fp.name, len(candidate)))
    active_fps.pop(worst_idx)
    active_frames.pop(worst_idx)
    active_cols.pop(worst_idx)

shared_cols   = set.intersection(*active_cols)
ordered_shared = [c for c in active_frames[0].columns if c in shared_cols]

# Per-file dropped-column summary (stored for outlier report)
file_drop_summary: list[tuple[str, list[str]]] = []
print(f"\n  Shared columns : {len(shared_cols)}  (across {len(active_fps)} files)")
for fp, cols in zip(active_fps, active_cols):
    dropped = sorted(cols - shared_cols - {"_source"})
    file_drop_summary.append((fp.name, dropped))
    if dropped:
        print(f"  {fp.name} drops {len(dropped)} cols: {', '.join(dropped)}")
    else:
        print(f"  {fp.name} drops nothing")

raw_frames = [df[ordered_shared] for df in active_frames]
fused = pd.concat(raw_frames, ignore_index=True, sort=False)
print(f"\n  Fused: {len(fused):,} rows × {len(fused.columns)} cols")


# ─────────────────────────────────────────────────────────────────────────────
# Step 2 — Assign patient_id, normalise dates, sort
# ─────────────────────────────────────────────────────────────────────────────
def safe_int(v) -> int | None:
    try:
        return int(float(str(v).strip()))
    except (ValueError, TypeError):
        return None

# patient_id from person_id (same hash across years)
if "person_id" in fused.columns:
    fused["patient_id"] = fused["person_id"].fillna("").astype(str)
    fused.drop(columns=["person_id"], inplace=True)
else:
    fused["patient_id"] = fused.index.astype(str)
    print("  WARNING: no person_id — cross-year linking disabled.")

# Normalise all date columns → YYYY-MM-DD
# Handles: 2022年9月2日 / 2022-09-02 / 2022/9/2 / 2022.9.2 / 2022-09-02 00:00:00.0
_CN_DATE_RE = re.compile(r'(\d{4})\s*年\s*(\d{1,2})\s*月\s*(\d{1,2})\s*日?')
_CN_YM_RE   = re.compile(r'(\d{4})\s*年\s*(\d{1,2})\s*月$')

def _norm_date(val) -> str | None:
    if pd.isna(val):
        return None
    s = str(val).strip()
    # Chinese full date: 2022年9月2日
    m = _CN_DATE_RE.match(s)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    # Chinese year-month only: 2022年9月
    m = _CN_YM_RE.match(s)
    if m:
        return f"{m.group(1)}-{int(m.group(2)):02d}-01"
    # Let pandas handle the rest (ISO, slash, dot, with/without time)
    try:
        parsed = pd.to_datetime(s, errors="raise")
        return parsed.strftime("%Y-%m-%d")
    except Exception:
        return None

date_cols = [c for c in fused.columns if "日期" in c or "时间" in c]
for dc in date_cols:
    fused[dc] = fused[dc].apply(_norm_date)

# Sort: patient_id → 体检日期 → 年度
sort_keys = ["patient_id"]
if "体检日期" in fused.columns:
    sort_keys.append("体检日期")
sort_keys.append("年度")
fused["_year_sort"] = fused["年度"].apply(lambda v: safe_int(v) or 9999)
sort_keys_actual = ["patient_id"] + (["体检日期"] if "体检日期" in fused.columns else []) + ["_year_sort"]
fused.sort_values(sort_keys_actual, inplace=True, kind="stable", na_position="last")
fused.drop(columns=["_year_sort"], inplace=True)
fused.reset_index(drop=True, inplace=True)

# Drop _source; move patient_id to front; keep tidy column order
fused.drop(columns=["_source"], errors="ignore", inplace=True)
front_cols = ["patient_id", "性别", "年龄", "年度"] + (["体检日期"] if "体检日期" in fused.columns else [])
front_cols = [c for c in front_cols if c in fused.columns]
rest_cols  = [c for c in fused.columns if c not in front_cols]
fused = fused[front_cols + rest_cols]

unique_persons = fused["patient_id"].nunique()
print(f"\n[Group] {unique_persons:,} unique patients identified")


# ─────────────────────────────────────────────────────────────────────────────
# Step 3 — Drop exact duplicates
# ─────────────────────────────────────────────────────────────────────────────
print("\n[Dedup] Removing exact duplicate rows…")
n_before = len(fused)
fused.drop_duplicates(keep="first", inplace=True)
fused.reset_index(drop=True, inplace=True)
print(f"  Removed {n_before - len(fused):,} exact duplicates  ({len(fused):,} rows remain)")


# ─────────────────────────────────────────────────────────────────────────────
# Step 4 — Vacant-threshold filter (columns)
# ─────────────────────────────────────────────────────────────────────────────
print(f"\n[Vacant] Dropping columns with > {VACANT_THRESHOLD:.0%} missing values…")
FORCE_KEEP   = {"胸部X片", "胸部X片异常"}   # never drop regardless of null rate
# Also keep all date/time columns
FORCE_KEEP  |= {c for c in fused.columns if "日期" in c or "时间" in c}
check_cols   = [c for c in fused.columns if c != "patient_id"]
col_null_frac = fused[check_cols].isna().sum() / len(fused)
drop_cols    = [c for c in col_null_frac[col_null_frac > VACANT_THRESHOLD].index
                if c not in FORCE_KEEP]
kept_cols    = [c for c in fused.columns if c not in drop_cols]
fused        = fused[kept_cols]

print(f"  Threshold : {VACANT_THRESHOLD:.0%}  |  Cols checked: {len(check_cols)}")
print(f"  Dropped   : {len(drop_cols)} cols  |  Kept: {len(fused.columns)} cols")
if drop_cols:
    for i in range(0, len(drop_cols), 6):
        print("    " + ", ".join(drop_cols[i:i+6]))

pd.DataFrame({"column": drop_cols,
              "null_frac": [round(col_null_frac[c], 4) for c in drop_cols]
             }).to_csv(OUT_ANALYSIS / "dropped_cols.csv", index=False, encoding="utf-8-sig")


# ─────────────────────────────────────────────────────────────────────────────
# Save fused.csv
# ─────────────────────────────────────────────────────────────────────────────
fused.to_csv(OUT_ANALYSIS / "fused.csv", index=False, encoding="utf-8-sig")
print(f"\n[Save] fused.csv  →  {len(fused):,} rows × {len(fused.columns)} cols")

# Update MEDICAL_TEXT_COLS to only include cols still present after column drop
MEDICAL_TEXT_COLS = [c for c in MEDICAL_TEXT_COLS if c in fused.columns]


# ─────────────────────────────────────────────────────────────────────────────
# Step 5 — Text summaries for B超 / X片 / 心电图
# ─────────────────────────────────────────────────────────────────────────────
print("\n[Text] Summarising medical text columns…")

# Split on delimiters AND numbered-list markers (1. 2. etc.) AND * ( ) :
SPLIT_RE = re.compile(
    r'[;；,，、。\|\n\r\t\*\(\)（）:：]+'  # punctuation/symbol delimiters
    r'|(?<!\d)\d+[\.。]\s*'               # numbered markers: 1. 2. 3.
)
# Remove measurements: digits+unit combos like 44mm, 3.5cm, 33*19mm, 1.2×0.8cm
MEAS_RE  = re.compile(
    r'\d+(?:[.,]\d+)?(?:\s*[×xX\*]\s*\d+(?:[.,]\d+)?)*\s*'
    r'(?:mm|cm|mm²|cm²|μm|ml|mL|dB|Hz|bpm|次|岁|年|月|天|%|‰)',
    re.IGNORECASE,
)
# Remove bare bracket-enclosed numbers/codes: (44), （19）, [2]
BRACKET_NUM_RE = re.compile(r'[（(【\[]\s*[\d\.]+\s*[）)】\]]')
STRIP_PREFIX_RE = re.compile(r'^(?:异常|B超|超声|X片|心电图)[:：\s]*')
IGNORE_TERMS = {
    "", "nan", "正常", "异常", "未见异常", "未见明显异常", "未检", "无",
    "肝", "胆", "脾", "胰", "肾", "心", "肺",
}

if not MEDICAL_TEXT_COLS:
    MEDICAL_KEYWORDS = ["心电图", "B超", "超声", "胸片", "X片", "CT", "磁共振", "MRI", "眼底"]
    MEDICAL_TEXT_COLS = [
        c for c in fused.columns
        if any(kw in c for kw in MEDICAL_KEYWORDS) and c != "patient_id"
    ]

print(f"  Text columns: {MEDICAL_TEXT_COLS}")
text_summary_dir = OUT_ANALYSIS / "text_summaries"
text_summary_dir.mkdir(exist_ok=True)

for col in MEDICAL_TEXT_COLS:
    if col not in fused.columns:
        continue

    counter: Counter = Counter()
    for raw in fused[col].dropna():
        s = str(raw).strip()
        if not s or s.lower() == "nan":
            continue
        s = STRIP_PREFIX_RE.sub("", s)
        # Split into fragments
        frags = SPLIT_RE.split(s)
        for frag in frags:
            frag = frag.strip(" \t;；,，()（）[]【】")
            # Remove measurements and bracket-numbers
            frag = MEAS_RE.sub("", frag)
            frag = BRACKET_NUM_RE.sub("", frag)
            frag = re.sub(r'\s+', ' ', frag).strip()
            if not frag or frag.lower() in IGNORE_TERMS:
                continue
            if len(frag) < 2:   # skip single-char noise
                continue
            counter[frag] += 1

    total_frags = sum(counter.values())
    n_unique    = len(counter)
    sorted_items = counter.most_common()

    # One file per column, in text_summaries/
    out_path = text_summary_dir / f"{col}.txt"
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(f"Column  : {col}\n")
        f.write(f"Unique terms: {n_unique:,}  |  Total fragments: {total_frags:,}\n")
        f.write(f"{'─'*55}\n")
        f.write(f"{'Term':<40}  {'Count':>8}  {'Freq%':>7}\n")
        f.write(f"{'─'*55}\n")
        for term, cnt in sorted_items:
            pct = cnt / total_frags * 100 if total_frags else 0
            f.write(f"{term:<40}  {cnt:>8,}  {pct:>6.2f}%\n")

    print(f"  {col:<25} {n_unique:>6,} unique terms  ({total_frags:,} frags)  → {out_path.name}")


# ─────────────────────────────────────────────────────────────────────────────
# Step 6 — Outlier report
# ─────────────────────────────────────────────────────────────────────────────
print("\n[Outliers] Generating outlier report…")

lines: list[str] = []
def add(s: str = ""):
    lines.append(s)

add("=" * 65)
add("  OUTLIER / ANALYSIS REPORT")
add("=" * 65)

# ── 6a. Patient-entry distribution ────────────────────────────────────────
add()
add("── PATIENT ENTRY DISTRIBUTION ──")
add("  (how many patients have exactly N exam records)")
add()
entry_counts = fused.groupby("patient_id").size()
dist = entry_counts.value_counts().sort_index()
add(f"  {'Entries/patient':>18}  {'# patients':>12}  {'% patients':>11}")
add(f"  {'-'*18}  {'-'*12}  {'-'*11}")
total_patients = dist.sum()
for n_entries, n_patients in dist.items():
    pct = n_patients / total_patients * 100
    add(f"  {n_entries:>18,}  {n_patients:>12,}  {pct:>10.1f}%")
add(f"  {'─'*44}")
add(f"  {'Total patients':>18}  {total_patients:>12,}")
add(f"  {'Total records':>18}  {len(fused):>12,}")

# List patients with > 10 entries
heavy = entry_counts[entry_counts > 10].sort_values(ascending=False)
add()
add(f"── PATIENTS WITH > 10 ENTRIES  ({len(heavy):,} patients) ──")
if len(heavy) == 0:
    add("  None.")
else:
    add(f"  {'patient_id':<40}  {'entries':>7}")
    add(f"  {'-'*40}  {'-'*7}")
    for pid, cnt in heavy.items():
        add(f"  {str(pid):<40}  {cnt:>7,}")

# ── 6b. Per-file dropped columns ──────────────────────────────────────────
add()
add("── PER-FILE DROPPED COLUMNS ──")
for fname, dropped in file_drop_summary:
    add()
    add(f"  {fname}  —  dropped {len(dropped)} cols")
    if dropped:
        for i in range(0, len(dropped), 5):
            add("    " + ", ".join(dropped[i:i+5]))
for fname, n_shared in skipped_files:
    add(f"  {fname}  —  SKIPPED (only {n_shared} shared cols, threshold {MIN_SHARED_COLS})")

# ── 6c. Per-column outliers ────────────────────────────────────────────────
add()
add("── COLUMN OUTLIERS ──")
add(f"  Numeric: IQR × {OUTLIER_IQR_MULT}  |  Categorical: freq < {RARE_FREQ_FRAC:.0%} or count < {RARE_FREQ_ABS*3}")

numeric_count = categ_count = 0
id_col = "patient_id" if "patient_id" in fused.columns else fused.columns[0]

# Columns to skip in outlier analysis: identifiers, dates, free-text medical cols
_skip_outlier = {"patient_id", "体检日期"} | set(MEDICAL_TEXT_COLS)

for col in fused.columns:
    if col in _skip_outlier:
        continue
    series = fused[col].dropna()
    if len(series) == 0:
        continue

    num_series = pd.to_numeric(series, errors="coerce")
    valid_num  = num_series.dropna()
    num_frac   = len(valid_num) / len(series)

    if num_frac >= 0.8 and len(valid_num) >= 10:
        q1, q3 = valid_num.quantile(0.25), valid_num.quantile(0.75)
        iqr = q3 - q1
        lo  = q1 - OUTLIER_IQR_MULT * iqr
        hi  = q3 + OUTLIER_IQR_MULT * iqr
        outliers = valid_num[(valid_num < lo) | (valid_num > hi)]
        if len(outliers) > 0:
            numeric_count += 1
            add()
            add(f"[NUMERIC] {col}")
            add(f"  n={len(valid_num):,}  mean={valid_num.mean():.3g}  std={valid_num.std():.3g}"
                f"  range=[{valid_num.min():.4g}, {valid_num.max():.4g}]")
            add(f"  Normal band (IQR×{OUTLIER_IQR_MULT}): [{lo:.4g}, {hi:.4g}]")
            add(f"  Outliers: {len(outliers):,}  ({len(outliers)/len(valid_num):.1%})")
            sorted_out = outliers.sort_values()
            examples = (list(sorted_out.head(3))
                        + (["…"] if len(sorted_out) > 6 else [])
                        + list(sorted_out.tail(3)))
            add(f"  Extreme values: {examples}")
            outlier_idx = valid_num[(valid_num < lo) | (valid_num > hi)].index
            sample = fused.loc[outlier_idx[:5], [id_col, "年度", col]].to_string(index=False)
            for line in sample.split("\n"):
                add("    " + line)
    else:
        counts = series.value_counts()
        total  = len(series)
        rare   = counts[(counts / total < RARE_FREQ_FRAC) & (counts < RARE_FREQ_ABS * 3)]
        median_len = series.str.len().median() or 0
        if len(rare) > 0 and median_len <= 20:
            categ_count += 1
            add()
            add(f"[CATEGORICAL] {col}  (n={total:,})")
            for val, cnt in rare.items():
                add(f"    {str(val):<30}  count={cnt:>5}  ({cnt/total:.2%})")

add()
add("=" * 65)
add(f"  {numeric_count} numeric cols with outliers, {categ_count} categorical with rare values")
add("=" * 65)

report_path = OUT_ANALYSIS / "outlier_report.txt"
with open(report_path, "w", encoding="utf-8") as f:
    f.write("\n".join(lines) + "\n")

for line in lines[:60]:
    print(line)
if len(lines) > 60:
    print(f"  … ({len(lines)-60} more lines — see outlier_report.txt)")
print(f"\n  Saved → outlier_report.txt")


# ─────────────────────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────────────────────
total_time = time.perf_counter() - t_start
print(f"""
{'=' * 65}
  Done in {fmt(total_time)}

  Output files:
    fused.csv                 {len(fused):>8,} rows × {len(fused.columns)} cols
    dropped_cols.csv          {len(drop_cols):>8,} cols dropped  (null > {VACANT_THRESHOLD:.0%})
    text_summaries/*.txt      {len(MEDICAL_TEXT_COLS)} column(s) summarised
    outlier_report.txt        {numeric_count} numeric + {categ_count} categorical cols flagged

  Config:
    VACANT_THRESHOLD = {VACANT_THRESHOLD:.0%}
    OUTLIER_IQR_MULT = {OUTLIER_IQR_MULT}
    RARE_FREQ_FRAC   = {RARE_FREQ_FRAC:.0%}
{'=' * 65}""")
