#!/usr/bin/env python3
"""
main2.py — Lab triplet processing + all transforms (requires ANTHROPIC_API_KEY)

Run order:  main.py  →  main_2018.py  →  main2.py  →  step2_fuse_clean_files.py

What it does:
  1. Loads all intermediate files from output/intermediate/
     (2022, 2023, 2024, stroke, 2018 — person_id assigned, raw triplet cols intact)
  2. Detects lab triplet column groups in each file
  3. Collects all unique normal-range strings across ALL files
  4. Parses ranges: Python regex first, Claude API for ambiguous strings (one batch)
  5. Applies transforms to each file:
       - apply_triplets  → base_值 columns (scaled 0–1 for numeric, ordinal for qualitative)
       - expand_multitag → one-hot binary columns for 人员类型
       - merge_dental_cols → single 牙齿情况 text column
       - split_blood_pressure → 高压/低压 columns
       - clean_medical_text → extract abnormal findings from ECG/ultrasound text
       - strip_normals → blank out normal default values
  6. Saves to output/clean/

Requires:  export ANTHROPIC_API_KEY=...
"""
import anthropic
import json
import re
import time
from pathlib import Path

import pandas as pd

# ─────────────────────────────────────────────────────────────────────────────
# Paths
# ─────────────────────────────────────────────────────────────────────────────
PATH      = Path("/Users/haochen/Desktop/PatientTables")
OUT       = PATH / "output"
OUT_INTER = OUT / "intermediate"
OUT_CLEAN = OUT / "clean"
OUT_CLEAN.mkdir(parents=True, exist_ok=True)

t_start = time.perf_counter()


def fmt(s: float) -> str:
    return f"{s:.0f}s" if s < 60 else f"{int(s//60)}m {int(s%60)}s"


# ─────────────────────────────────────────────────────────────────────────────
# Normal/default values (blanked out after transforms)
# ─────────────────────────────────────────────────────────────────────────────
NORMAL_VALUES = {
    "无症状", "无症状;", "正常", "无", "否", "红润", "无充血",
    "听见", "可顺利完成", "未触及", "触及正常", "齐", "无异常",
    "阴性", "未见异常", "无殊", "无特殊", "正常范围", "未见明显异常",
    "未见", "无压痛", "软", "清", "无杂音",
}


def strip_normals(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].apply(
            lambda v: None if (isinstance(v, str) and v.strip() in NORMAL_VALUES) else v
        )
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Lab triplet processing
# ─────────────────────────────────────────────────────────────────────────────
RANGE_SUFFIX = "-正常范围"
FLAG_SUFFIX  = "-异常结果"
QUAL_BASES   = {"尿蛋白", "尿糖", "尿酮体", "尿潜血"}
QUAL_ORDINAL = {
    "-": 0, "阴性": 0, "（-）": 0, "(-)": 0,
    "弱阳性": 1, "（弱阳性）": 1,
    "阳性(+)": 2,  "阳性（+）": 2,  "(+)": 2,  "（+）": 2,
    "阳性(++)": 3, "阳性（++）": 3, "(++)": 3, "（++）": 3,
    "阳性(+++)": 4, "阳性（+++）": 4, "(+++)": 4, "（+++）": 4,
    "阳性(++++)": 5, "阳性（++++）": 5, "(++++)": 5, "（++++）": 5,
}

# Gender-neutral defaults
DEFAULT_RANGES: dict = {
    "白细胞":         {"lower":  4.0,  "upper":  10.0,  "qual": False},
    "血小板":         {"lower": 100.0, "upper": 300.0,  "qual": False},
    "中性粒细胞":     {"lower":  2.0,  "upper":   7.0,  "qual": False},
    "淋巴细胞":       {"lower":  0.8,  "upper":   4.0,  "qual": False},
    "单核细胞":       {"lower":  0.12, "upper":   1.2,  "qual": False},
    "嗜酸性粒细胞":   {"lower":  0.02, "upper":   0.5,  "qual": False},
    "嗜碱性粒细胞":   {"lower":  0.0,  "upper":   0.1,  "qual": False},
    "空腹血糖":       {"lower":  3.9,  "upper":   6.1,  "qual": False},
    "糖化血红蛋白":   {"lower":  4.0,  "upper":   6.0,  "qual": False},
    "血清谷丙转氨酶": {"lower":  7.0,  "upper":  40.0,  "qual": False},
    "血清谷草转氨酶": {"lower": 10.0,  "upper":  40.0,  "qual": False},
    "白蛋白":         {"lower": 35.0,  "upper":  55.0,  "qual": False},
    "总胆红素":       {"lower":  5.0,  "upper":  21.0,  "qual": False},
    "结合胆红素":     {"lower":  0.0,  "upper":   7.0,  "qual": False},
    "血尿素":         {"lower":  2.9,  "upper":   8.2,  "qual": False},
    "血钾浓度":       {"lower":  3.5,  "upper":   5.5,  "qual": False},
    "血钠浓度":       {"lower": 136.0, "upper": 145.0,  "qual": False},
    "总胆固醇":       {"lower": None,  "upper":   5.2,  "qual": False},
    "甘油三酯":       {"lower": None,  "upper":   1.7,  "qual": False},
    "低密度脂蛋白":   {"lower": None,  "upper":   3.4,  "qual": False},
    "甲胎蛋白":       {"lower":  0.0,  "upper":   8.78, "qual": False},
    "癌胚抗原":       {"lower":  0.0,  "upper":   9.7,  "qual": False},
}

# Gender-specific defaults
GENDER_RANGES: dict = {
    "血红蛋白": {
        "男":       {"lower": 130.0, "upper": 175.0, "qual": False},
        "女":       {"lower": 110.0, "upper": 150.0, "qual": False},
        "_default": {"lower": 110.0, "upper": 175.0, "qual": False},
    },
    "红细胞": {
        "男":       {"lower": 4.0,   "upper": 5.5,   "qual": False},
        "女":       {"lower": 3.5,   "upper": 5.0,   "qual": False},
        "_default": {"lower": 3.5,   "upper": 5.5,   "qual": False},
    },
    "血清肌酐": {
        "男":       {"lower": 62.0,  "upper": 115.0, "qual": False},
        "女":       {"lower": 53.0,  "upper":  97.0, "qual": False},
        "_default": {"lower": 53.0,  "upper": 115.0, "qual": False},
    },
    "血尿酸": {
        "男":       {"lower": 208.0, "upper": 428.0, "qual": False},
        "女":       {"lower": 155.0, "upper": 357.0, "qual": False},
        "_default": {"lower": 155.0, "upper": 428.0, "qual": False},
    },
    "高密度脂蛋白": {
        "男":       {"lower": 1.0,   "upper": None,  "qual": False},
        "女":       {"lower": 1.3,   "upper": None,  "qual": False},
        "_default": {"lower": 1.0,   "upper": None,  "qual": False},
    },
}

# Unit stripping (for 2018 data which embeds units in cell values)
_UNIT_STRIP_RE = re.compile(
    r'[\s　]*'
    r'(?:\d+\s*[Cc]ell\s*/\s*[μuµ][Ll]'
    r'|\d+\s*[Cc]ell\s*/\s*[uU][Ll]'
    r'|[Cc]ell\s*/\s*[μuµ][Ll]'
    r'|[Cc]ell\s*/\s*[uU][Ll]'
    r'|×\s*10\^?\d+\s*/\s*[Ll]'
    r'|\*+\s*10\^?\d+\s*/\s*[Ll]'
    r'|10\^?\d+\s*/\s*[Ll]'
    r'|\*+\s*[0-9]+\s*/\s*[lL]'
    r'|[μuµ]mol\s*/\s*[Ll]'
    r'|[Mm][Mm]ol\s*/\s*[Ll]'
    r'|mol\s*/\s*[Ll]'
    r'|g\s*/\s*[Ld][Ll]?'
    r'|[Uu]\s*/\s*[Ll]'
    r'|IU\s*/\s*[Ll]'
    r'|ng\s*/\s*m[Ll]'
    r'|pg\s*/\s*m[Ll]'
    r'|/[μuµ][Ll]'
    r'|/[Ll]'
    r'|%'
    r')\s*$',
    re.IGNORECASE,
)
_LEADING_NUM_RE = re.compile(r'^([+-]?\d+(?:\.\d*)?)')


def _strip_unit(val) -> str:
    if pd.isna(val):
        return ""
    s = str(val).strip()
    prev = None
    while prev != s:
        prev = s
        s = _UNIT_STRIP_RE.sub("", s).strip()
    return s


def _to_float(val) -> float | None:
    if pd.isna(val):
        return None
    s = str(val).strip()
    try:
        return float(s)
    except ValueError:
        pass
    s2 = _strip_unit(s)
    try:
        return float(s2)
    except ValueError:
        pass
    m = _LEADING_NUM_RE.match(s2 or s)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            pass
    return None


def _normalize_qual(val) -> object:
    if pd.isna(val):
        return None
    raw = str(val).strip()
    if not raw or raw.lower() == "nan":
        return None
    s = re.sub(r"\s+", "", _strip_unit(raw))
    if s in QUAL_ORDINAL:
        return QUAL_ORDINAL[s]
    if re.match(r'^[\*\?！!]+$', s):
        return None
    if re.match(r'^[（(]?\+[\-–—/][）)]?', s) or s.startswith("±"):
        return 1
    m_plus = re.match(r'^[（(]?(\++)', s)
    if m_plus:
        n = len(m_plus.group(1))
        return {1: 2, 2: 3, 3: 4, 4: 5}.get(min(n, 4))
    if re.search(r'[（(][-–][）)]', raw) or \
       any(x in raw for x in ["阴性", "(-)", "（-）", "(-）", "（-)"]) and "阳性" not in raw:
        return 0
    m_inner = re.search(r'[（(][^）)]*阳性([+]*)[^）)]*[）)]', raw)
    if m_inner:
        n = len(m_inner.group(1)) or 1
        return {1: 2, 2: 3, 3: 4, 4: 5}.get(min(n, 4))
    if "弱阳" in raw:
        return 1
    return None


def _scale_value(val_raw, rng: dict) -> object:
    if not rng or rng.get("qual"):
        return None
    v = _to_float(val_raw)
    if v is None:
        return None
    lo = rng.get("lower")
    hi = rng.get("upper")
    if lo is not None and hi is not None:
        span = hi - lo
        if span <= 0:
            return None
        return round((v - lo) / span, 4)
    if hi is not None:
        return round((v - hi) / hi, 4) if hi != 0 else round(v - hi, 4)
    if lo is not None:
        return round((v - lo) / lo, 4) if lo != 0 else round(v - lo, 4)
    return None


def detect_triplets(df: pd.DataFrame) -> list:
    cols = set(df.columns)
    triplets, seen = [], set()
    for col in df.columns:
        if col.endswith(RANGE_SUFFIX):
            base = col[:-len(RANGE_SUFFIX)]
            flag = base + FLAG_SUFFIX
            if base in cols and flag in cols and base not in seen:
                triplets.append((base, flag, col))
                seen.add(base)
    return triplets


def collect_all_ranges(frames_list: list, triplets_list: list) -> set:
    unique: set = set()
    for df, triplets in zip(frames_list, triplets_list):
        for _, _, rc in triplets:
            if rc in df.columns:
                for v in df[rc].dropna().unique():
                    s = str(v).strip()
                    if s:
                        unique.add(s)
    return unique


_NUM_RE = re.compile(r'\d+(?:\.\d*)?')
_QUAL_KEYWORDS = {"阴性", "阳性", "弱阳性"}


def _to_f(s: str) -> float:
    return float(s) if s[-1] != "." else float(s + "0")


def _parse_range_py(s: str) -> dict | None:
    s = s.strip()
    if not s:
        return {"lower": None, "upper": None, "qual": False}
    if any(kw in s for kw in _QUAL_KEYWORDS):
        return {"lower": None, "upper": None, "qual": True}
    m = re.match(r'^[>≥]\s*(\d+(?:\.\d*)?)$', s)
    if m:
        return {"lower": _to_f(m.group(1)), "upper": None, "qual": False}
    m = re.match(r'^[<≤]\s*(\d+(?:\.\d*)?)$', s)
    if m:
        return {"lower": None, "upper": _to_f(m.group(1)), "qual": False}
    nums = _NUM_RE.findall(s)
    if len(nums) == 2:
        a, b = _to_f(nums[0]), _to_f(nums[1])
        return {"lower": min(a, b), "upper": max(a, b), "qual": False}
    if len(nums) == 4:
        a, b, c, d = (_to_f(x) for x in nums)
        if abs(a - c) < 1e-9 and abs(b - d) < 1e-9:
            return {"lower": min(a, b), "upper": max(a, b), "qual": False}
        return None
    return None


def _parse_ranges_llm_batch(ranges_list: list) -> dict:
    prompt = (
        "Parse these Chinese medical lab normal-range strings into JSON.\n\n"
        "For EACH string return: {\"lower\": number_or_null, \"upper\": number_or_null, \"qual\": true_or_false}\n\n"
        "Rules:\n"
        "- Two numbers: lower=min, upper=max\n"
        "- One-sided \">4.0\" → lower=4.0, upper=null\n"
        "- One-sided \"<10.0\" → lower=null, upper=10.0\n"
        "- Qualitative text (阴性 etc.) → qual=true, lower/upper=null\n"
        "- Truly unparseable → lower=null, upper=null, qual=false\n\n"
        "Return ONLY a JSON object, no explanation:\n"
        "{\"string1\": {\"lower\": ..., \"upper\": ..., \"qual\": ...}, ...}\n\n"
        f"Strings ({len(ranges_list)} total):\n"
        + json.dumps(ranges_list, ensure_ascii=False)
    )
    with anthropic.Anthropic().messages.stream(
        model="claude-opus-4-6",
        max_tokens=16384,
        messages=[{"role": "user", "content": prompt}],
    ) as stream:
        response = stream.get_final_message()
    text = next((b.text for b in response.content if b.type == "text"), "")
    start, end = text.find("{"), text.rfind("}") + 1
    if start >= 0 and end > start:
        try:
            return json.loads(text[start:end])
        except json.JSONDecodeError:
            pass
    print(f"  WARNING: LLM returned unparseable JSON for {len(ranges_list)} ranges")
    return {}


def parse_ranges(unique_ranges: set) -> dict:
    result: dict = {}
    need_llm: list = []
    for s in unique_ranges:
        parsed = _parse_range_py(s)
        if parsed is not None:
            result[s] = parsed
        else:
            need_llm.append(s)
    print(f"  Python parsed: {len(result):,}  |  sending to Claude: {len(need_llm):,}")
    if need_llm:
        llm_result = _parse_ranges_llm_batch(sorted(need_llm))
        result.update(llm_result)
    return result


def apply_triplets(df: pd.DataFrame, triplets: list, range_map: dict,
                   gender_col: str | None = None) -> pd.DataFrame:
    df = df.copy()
    for base, flag_col, range_col in triplets:
        if base not in df.columns:
            extras = [c for c in [flag_col, range_col] if c in df.columns]
            if extras:
                df.drop(columns=extras, inplace=True)
            continue
        new_name = base + "_值"
        pos = df.columns.get_loc(base)
        if base in QUAL_BASES:
            new_vals = df[base].apply(_normalize_qual)
        else:
            rng_series = (
                df[range_col].fillna("").astype(str).str.strip()
                if range_col in df.columns
                else pd.Series("", index=df.index)
            )
            def _usable(rng: dict) -> bool:
                """Return True if rng has at least one numeric bound."""
                return bool(rng) and (rng.get("lower") is not None or rng.get("upper") is not None)

            if base in GENDER_RANGES and gender_col and gender_col in df.columns:
                gr = GENDER_RANGES[base]
                def _rng_for_row(val_raw, rng_str, gender_raw):
                    if rng_str and rng_str in range_map:
                        rng = range_map[rng_str]
                        if _usable(rng):
                            return rng
                    g = str(gender_raw).strip() if pd.notna(gender_raw) else ""
                    return gr.get(g, gr["_default"])
                new_vals = pd.Series(
                    [_scale_value(v, _rng_for_row(v, r, g))
                     for v, r, g in zip(df[base], rng_series, df[gender_col])],
                    index=df.index,
                )
            else:
                fallback = DEFAULT_RANGES.get(base, {})
                def _pick_rng(r):
                    if not r:
                        return fallback
                    rng = range_map.get(r, fallback)
                    return rng if _usable(rng) else fallback
                new_vals = pd.Series(
                    [_scale_value(v, _pick_rng(r)) for v, r in zip(df[base], rng_series)],
                    index=df.index,
                )
        df.insert(pos, new_name, new_vals)
        df.drop(columns=[c for c in [base, flag_col, range_col] if c in df.columns],
                inplace=True)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Other transforms
# ─────────────────────────────────────────────────────────────────────────────
MULTITAG_COLS = {
    "人员类型": ["老年人", "高血压", "糖尿病"],
}


def expand_multitag(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col, tags in MULTITAG_COLS.items():
        if col not in df.columns:
            continue
        pos = df.columns.get_loc(col)
        for i, tag in enumerate(tags):
            df.insert(
                pos + i,
                f"{col}_{tag}",
                df[col].apply(
                    lambda v: 1 if isinstance(v, str) and tag in v else 0
                ),
            )
        df.drop(columns=col, inplace=True)
    return df


DENTAL_COLS = ["齿列", "缺牙位置", "缺牙第几颗", "龋齿位置", "龋齿第几颗", "义齿位置", "义齿第几颗"]
_DENTAL_MAP = [
    ("缺齿", "缺牙", "缺牙位置", "缺牙第几颗"),
    ("龋齿", "龋齿", "龋齿位置", "龋齿第几颗"),
    ("义齿", "义齿", "义齿位置", "义齿第几颗"),
]


def _fmt_dental_count(s) -> str:
    if pd.isna(s):
        return ""
    s = str(s).strip()
    if s in ("@@@", "0@0@0@0", ""):
        return ""
    if re.match(r"^\d+$", s):
        return f"{s}颗"
    return ""


def _merge_dental_row(row) -> object:
    zl = str(row.get("齿列", "") or "").strip()
    if not zl or zl == "正常":
        return None
    parts = []
    for tag, label, pos_col, cnt_col in _DENTAL_MAP:
        if tag not in zl:
            continue
        pos = row.get(pos_col)
        cnt = _fmt_dental_count(row.get(cnt_col))
        detail = []
        if not pd.isna(pos) and str(pos).strip():
            detail.append(str(pos).strip())
        if cnt:
            detail.append(cnt)
        parts.append(f"{label}（{'、'.join(detail)}）" if detail else label)
    return "；".join(parts) if parts else None


def merge_dental_cols(df: pd.DataFrame) -> pd.DataFrame:
    present = [c for c in DENTAL_COLS if c in df.columns]
    if "齿列" not in present:
        return df
    df = df.copy()
    pos = df.columns.get_loc("齿列")
    df.insert(pos, "牙齿情况", df[present].apply(_merge_dental_row, axis=1))
    df.drop(columns=present, inplace=True)
    return df


def split_blood_pressure(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in ["左侧血压", "右侧血压"]:
        if col not in df.columns:
            continue
        pos = df.columns.get_loc(col)
        split = df[col].str.extract(r"^(\d+)/(\d+)$")
        df.insert(pos,     f"{col}_高压", pd.to_numeric(split[0], errors="coerce"))
        df.insert(pos + 1, f"{col}_低压", pd.to_numeric(split[1], errors="coerce"))
        df.drop(columns=col, inplace=True)
    return df


MEDICAL_TEXT_COLS = ["心电图异常", "腹部B超异常", "B超其他异常"]


def _clean_medical_findings(text) -> object:
    if pd.isna(text) or not str(text).strip():
        return None
    s = str(text).strip()
    s = re.sub(r'^(?:异常|B超)[:：]\s*', '', s)
    s = re.sub(r'\d+[\.。](?!\d)', ';', s)
    s = re.sub(r'(\d(?:cm|mm))([^\s;；,，（(）)\d×*])', r'\1;\2', s)
    s = re.sub(r'((?:轻度|中度|重度)?脂肪肝)([肝胆脾胰肾心肺])', r'\1;\2', s)
    s = re.sub(r'[（(][^）)]{0,40}(?:未见|未检|显示不清)[^）)]{0,40}[）)]', '', s)
    s = re.sub(r'[,，][^,，;；\n]*(?:未见|未检)[^;；\n]*', '', s)
    segments = re.split(r'[;；\n]+', s)
    kept = []
    for seg in segments:
        seg = seg.strip().strip(';；,，')
        if seg.endswith('）') and seg.count('（') < seg.count('）'):
            seg = seg.rstrip('）').rstrip()
        if not seg:
            continue
        if '未见' in seg or '未检' in seg:
            continue
        if re.search(r'显示不清|肠气|^因.{1,15}太多', seg):
            continue
        if re.match(r'^正常(心电图)?$', seg):
            continue
        if re.match(r'^[肝胆脾胰肾心肺脏、，\s]+$', seg):
            continue
        kept.append(seg)
    result = '；'.join(kept)
    return result if result else None


def clean_medical_text(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in MEDICAL_TEXT_COLS:
        if col in df.columns:
            df[col] = df[col].apply(_clean_medical_findings)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
print("=" * 60)
print("  PatientTables — Lab Triplet Processing (main2.py)")
print("=" * 60)

# ── Step 1: Load all intermediate files ───────────────────────────────────────
print("\n[Load] Reading intermediate files…")
FILES = {
    "2022":   "2022_intermediate.csv",
    "2023":   "2023_intermediate.csv",
    "2024":   "2024_intermediate.csv",
    "stroke": "stroke_intermediate.csv",
    "2018":   "2018_intermediate.csv",
}

frames: dict[str, pd.DataFrame] = {}
for key, fname in FILES.items():
    fpath = OUT_INTER / fname
    if not fpath.exists():
        print(f"  WARNING: {fname} not found — skipping")
        continue
    t0 = time.perf_counter()
    df = pd.read_csv(fpath, dtype=str, low_memory=False)
    frames[key] = df
    print(f"  {fname:<34} {len(df):>8,} rows | {len(df.columns):>4} cols  ({fmt(time.perf_counter()-t0)})")

if not frames:
    raise RuntimeError("No intermediate files found. Run main.py and main_2018.py first.")

# ── Step 2: Detect triplet groups ─────────────────────────────────────────────
print("\n[Triplets] Detecting lab triplet column groups…")
frame_triplets: dict[str, list] = {}
for key, df in frames.items():
    trips = detect_triplets(df)
    frame_triplets[key] = trips
    all_b = {b for b, _, _ in trips}
    print(f"  {key:<8}  {len(trips):>3} triplet groups  "
          f"({sum(b in QUAL_BASES for b in all_b)} qual, {len(all_b)-sum(b in QUAL_BASES for b in all_b)} num)")

# ── Step 3: Collect all unique range strings ───────────────────────────────────
print("\n[Ranges] Collecting unique normal-range strings across all files…")
unique_ranges = collect_all_ranges(list(frames.values()), list(frame_triplets.values()))
print(f"  {len(unique_ranges):,} unique range strings")

# ── Step 4: Parse ranges (Python + Claude) ────────────────────────────────────
if unique_ranges:
    print("  Parsing ranges (Python first, Claude for ambiguous)…")
    t_llm = time.perf_counter()
    range_map = parse_ranges(unique_ranges)
    parsed_qual = sum(1 for v in range_map.values() if v.get("qual"))
    parsed_num  = sum(1 for v in range_map.values()
                      if not v.get("qual") and (v.get("lower") is not None or v.get("upper") is not None))
    print(f"  → {parsed_num} numeric, {parsed_qual} qualitative, "
          f"{len(unique_ranges)-parsed_num-parsed_qual} unknown  ({fmt(time.perf_counter()-t_llm)})")
else:
    range_map = {}
    print("  No range strings found — skipping LLM call")

# ── Step 5: Apply transforms and save ─────────────────────────────────────────
print("\n[Transform] Applying transforms and saving to output/clean/…")

for key, df in frames.items():
    t0 = time.perf_counter()

    # Detect gender column (needed for gender-specific ranges in 2018)
    gender_col = next((c for c in df.columns if "性别" in c), None)

    # Apply lab triplets
    df = apply_triplets(df, frame_triplets[key], range_map, gender_col=gender_col)

    # Expand multi-tag, merge dental, split blood pressure
    df = expand_multitag(df)
    df = merge_dental_cols(df)
    df = split_blood_pressure(df)

    # Clean medical free-text
    df = clean_medical_text(df)

    # Blank out normal default values
    cells_before = df.notna().sum().sum()
    df = strip_normals(df)
    cells_after = df.notna().sum().sum()

    # Drop fully-empty columns
    empty_cols = [c for c in df.columns if df[c].isna().all()]
    df.drop(columns=empty_cols, inplace=True)

    fname = f"{key}_clean.csv"
    df.to_csv(OUT_CLEAN / fname, index=False, encoding="utf-8-sig")
    print(f"  clean/{fname:<28} {len(df):>8,} rows | {len(df.columns):>4} cols | "
          f"blanked: {cells_before - cells_after:,} | empty cols: {len(empty_cols)}  "
          f"({fmt(time.perf_counter()-t0)})")

total = time.perf_counter() - t_start
print(f"""
{'=' * 60}
  Done in {fmt(total)}

  Output: output/clean/
    2022_clean.csv  2023_clean.csv  2024_clean.csv
    stroke_clean.csv  2018_clean.csv

  Run step2_fuse_clean_files.py next.
{'=' * 60}""")
