#!/usr/bin/env python3
"""
step4_fill_missing.py — Fill missing values and encode categorical columns in fused.csv.

Run AFTER step3_risk_classify.py.

Fill rules
──────────
Lab _值 columns (normalised numeric lab results)
    NaN → -1   (sentinel for "test not performed")

Risk columns (心电图_风险, 胸片_风险)
    If the paired text column has content  → 0  (exam done; unclassified → treated as normal)
    If the paired text column is also blank → -1 (exam not performed)

Exam flag columns (心电图, 胸部X片, 腹部B超)
    If the paired 异常 text column has content → "异常"   (text proves it was done & abnormal)
    Otherwise                                  → "未检查"  (not performed or unrecorded)

Exam description columns (心电图异常, 胸部X片异常, 腹部B超异常)
    Left as-is — free text, blank means "no recorded abnormality".

足背动脉搏动
    NaN → "未检查"  (then encoded; see below)

Physical measurements (体温, 脉率, 呼吸频率, 血压, 身高, 体重, 腰围, BMI, 视力, 心脏心率)
    NaN → -1

Categorical encoding (applied after fill)
──────────────────────────────────────────
性别              男→1  女→0
心电图             异常→1  未检/未检查→-1
胸部X片            异常→1  未检/未检查→-1
腹部B超            异常→1  未检/未检查→-1
足背动脉搏动         ordinal severity:
                    0  = 触及双侧对称 (normal)
                    1  = unilateral weakened (触及左/右侧弱或消失, 左/右侧搏动减弱)
                    2  = unilateral absent OR bilateral weakened
                         (左侧搏动消失, 双侧搏动减弱)
                    3  = 双侧搏动消失 (both absent — most severe)
                   -1  = 未检查 / not recorded

Outputs:
    analysis/fused_filled.csv   — filled + encoded, fused.csv untouched
    analysis/fill_report.txt    — per-column fill/encode summary
"""
import time
from pathlib import Path

import pandas as pd

FUSED_PATH  = Path("/Users/haochen/Desktop/PatientTables/output/analysis/fused.csv")
OUTPUT_PATH = Path("/Users/haochen/Desktop/PatientTables/output/analysis/fused_filled.csv")
OUT_DIR     = FUSED_PATH.parent

t0 = time.perf_counter()

if not FUSED_PATH.exists():
    raise FileNotFoundError(f"fused.csv not found at {FUSED_PATH}. Run the pipeline first.")

print("=" * 60)
print("  PatientTables — step4: Fill missing + encode categoricals")
print("=" * 60)

print(f"\nLoading {FUSED_PATH.name}…", end=" ", flush=True)
df = pd.read_csv(FUSED_PATH, dtype=str, encoding="utf-8-sig", low_memory=False)
total_rows = len(df)
print(f"{total_rows:,} rows × {len(df.columns)} columns")

report_lines: list[str] = []

def _log(col: str, n: int, rule: str):
    msg = f"  {col:<30}  {n:>8,}  ({rule})"
    print(msg)
    report_lines.append(msg)


# ═══════════════════════════════════════════════════════════
# PHASE 1 — Fill missing values
# ═══════════════════════════════════════════════════════════

# ── 1. Lab _值 columns → -1 ───────────────────────────────
print("\n[Fill] Lab _值 columns: NaN → -1")
val_cols = [c for c in df.columns if c.endswith("_值")]
for col in val_cols:
    n = int(df[col].isna().sum())
    if n:
        df[col] = df[col].fillna("-1")
        _log(col, n, "not tested → -1")


# ── 2. Risk columns — context-aware ───────────────────────
print("\n[Fill] 心电图_风险 / 胸片_风险")
RISK_PAIRS = [
    ("心电图_风险",  "心电图异常"),
    ("胸片_风险",   "胸部X片异常"),
]
for risk_col, text_col in RISK_PAIRS:
    if risk_col not in df.columns:
        continue
    mask_nan   = df[risk_col].isna()
    has_text   = df[text_col].notna() & (df[text_col].str.strip() != "")
    m_done     = mask_nan &  has_text
    m_not_done = mask_nan & ~has_text
    df.loc[m_done,     risk_col] = "0"
    df.loc[m_not_done, risk_col] = "-1"
    if m_done.sum():
        _log(risk_col, int(m_done.sum()),     "text present, unclassified → 0")
    if m_not_done.sum():
        _log(risk_col, int(m_not_done.sum()), "no exam text → -1")


# ── 3. Exam flag columns ──────────────────────────────────
print("\n[Fill] 心电图 / 胸部X片 / 腹部B超 flags")
FLAG_PAIRS = [
    ("心电图",  "心电图异常"),
    ("胸部X片", "胸部X片异常"),
    ("腹部B超", "腹部B超异常"),
]
for flag_col, text_col in FLAG_PAIRS:
    if flag_col not in df.columns:
        continue
    mask_blank = df[flag_col].isna()
    if not mask_blank.any():
        continue
    has_text   = df[text_col].notna() & (df[text_col].str.strip() != "")
    m_abnormal = mask_blank &  has_text
    m_unknown  = mask_blank & ~has_text
    df.loc[m_abnormal, flag_col] = "异常"
    df.loc[m_unknown,  flag_col] = "未检查"
    if m_abnormal.sum():
        _log(flag_col, int(m_abnormal.sum()), "text present → 异常")
    if m_unknown.sum():
        _log(flag_col, int(m_unknown.sum()),  "no text → 未检查")


# ── 4. 足背动脉搏动 blank → 未检查 ────────────────────────
print("\n[Fill] 足背动脉搏动: NaN → 未检查")
col = "足背动脉搏动"
if col in df.columns:
    n = int(df[col].isna().sum())
    if n:
        df[col] = df[col].fillna("未检查")
        _log(col, n, "未检查")


# ── 5. Physical measurements → -1 ────────────────────────
print("\n[Fill] Physical measurements: NaN → -1")
PHYS_COLS = [
    "体温", "脉率", "呼吸频率",
    "左侧血压_高压", "左侧血压_低压", "右侧血压_高压", "右侧血压_低压",
    "身高", "体重", "腰围", "BMI",
    "左眼视力", "右眼视力", "心脏心率",
]
for col in PHYS_COLS:
    if col not in df.columns:
        continue
    n = int(df[col].isna().sum())
    if n:
        df[col] = df[col].fillna("-1")
        _log(col, n, "missing → -1")


# ═══════════════════════════════════════════════════════════
# PHASE 2 — Encode categorical text → numeric
# ═══════════════════════════════════════════════════════════

print("\n[Encode] Categorical text → numeric")

# ── 性别: 男→1, 女→0 ──────────────────────────────────────
col = "性别"
if col in df.columns:
    before = df[col].copy()
    df[col] = df[col].map({"男": "1", "女": "0"})
    n = int(before.notna().sum())
    _log(col, n, "男→1  女→0")


# ── Exam flag columns: 异常→1, 未检*/未检查→-1 ─────────────
EXAM_FLAG_MAP = {"异常": "1", "未检": "-1", "未检查": "-1"}
for col in ["心电图", "胸部X片", "腹部B超"]:
    if col not in df.columns:
        continue
    before = df[col].copy()
    df[col] = df[col].map(EXAM_FLAG_MAP)
    n = int(before.notna().sum())
    _log(col, n, "异常→1  未检/未检查→-1")


# ── 足背动脉搏动: ordinal severity ────────────────────────
col = "足背动脉搏动"
if col in df.columns:
    DORSAL_MAP = {
        "触及双侧对称":   "0",   # both present and equal — normal
        "左侧搏动减弱":   "1",   # unilateral weakened
        "右侧搏动减弱":   "1",
        "触及左侧弱或消失": "1",
        "触及右侧弱或消失": "1",
        "左侧搏动消失":   "2",   # unilateral absent
        "双侧搏动减弱":   "2",   # bilateral weakened
        "双侧搏动消失":   "3",   # bilateral absent — most severe
        "未检查":        "-1",  # not examined
    }
    before = df[col].copy()
    df[col] = df[col].map(DORSAL_MAP)
    # Any value not in the map (unexpected text) → -1
    unmapped = df[col].isna() & before.notna()
    if unmapped.any():
        print(f"    WARNING: {unmapped.sum()} unexpected values mapped to -1:")
        for v in before[unmapped].unique():
            print(f"      {v!r}")
        df.loc[unmapped, col] = "-1"
    n = int(before.notna().sum())
    _log(col, n, "0=正常  1=单侧减弱  2=单侧消失/双侧减弱  3=双侧消失  -1=未检查")


# ═══════════════════════════════════════════════════════════
# Save
# ═══════════════════════════════════════════════════════════
print(f"\nSaving {OUTPUT_PATH.name}…", end=" ", flush=True)
df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")
elapsed = time.perf_counter() - t0
print(f"done  ({elapsed:.1f}s)")

# Remaining blanks summary
remaining = df.isna().sum()
remaining = remaining[remaining > 0]
if remaining.empty:
    print("\n  No remaining NaN values.")
else:
    print(f"\n  Remaining NaN (free-text columns, intentionally left blank):")
    for col, n in remaining.items():
        print(f"    {col}: {n:,}")

# Save report
report_path = OUT_DIR / "fill_report.txt"
header = [
    "=" * 60,
    "  PatientTables — step4 fill + encode report",
    f"  Rows: {total_rows:,}",
    "=" * 60,
    "",
    "  col                             cells     rule",
    "  " + "-" * 56,
]
(report_path).write_text("\n".join(header + report_lines) + "\n", encoding="utf-8")

print(f"\n  Input:  {FUSED_PATH}  (unchanged)")
print(f"  Output: {OUTPUT_PATH}")
print(f"  Report: {report_path}")
print(f"\n{'=' * 60}")
print(f"  Done in {elapsed:.1f}s")
print(f"{'=' * 60}")
