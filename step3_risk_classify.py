#!/usr/bin/env python3
"""
step3_risk_classify.py — Classify ECG and chest X-ray text into 3-tier risk columns.

Run AFTER step2_fuse_clean_files.py (reads fused.csv, writes back with new columns).

New columns added:
  心电图_风险    — ECG risk:       2=高危, 1=低危, 0=正常/不纳入, NaN=空/无法判断
  胸片_风险      — X-ray risk:     2=高危, 1=低危, 0=正常/不纳入, NaN=空/无法判断

Classification rule: scan text for ALL matching keywords → take MAX level found.
  Any high-risk keyword → result = 2.
  Only low-risk keywords → result = 1.
  Only normal/irrelevant keywords → result = 0.
  Empty / 未检 / 拒检 / 未查 / no match → NaN.

Keywords derived from actual term-frequency analysis of the dataset.
Text is normalised before matching: dash variants → '-', full-width digits/letters → half-width.
"""
import re
import time
from pathlib import Path

import numpy as np
import pandas as pd

# ── Config ────────────────────────────────────────────────────────────────────
OUT_ANALYSIS = Path("/Users/haochen/Desktop/PatientTables/output/analysis")
FUSED_PATH   = OUT_ANALYSIS / "fused.csv"

t_start = time.perf_counter()

def fmt(s: float) -> str:
    return f"{s:.1f}s" if s < 60 else f"{int(s//60)}m {int(s%60)}s"


# ─────────────────────────────────────────────────────────────────────────────
# Text normalisation
# ─────────────────────────────────────────────────────────────────────────────
# Dash variants seen in data: ST—T改变, ST－T改变, ST-T改变
# All normalised to ASCII hyphen before matching.
_DASH_RE = re.compile(r'[—－–‐‑−]')

# Common leading noise prefixes that don't change the finding
_NOISE_PREFIX_RE = re.compile(r'^(?:提示|建议|考虑|可能|怀疑|★|[\d\s]+)+')

def _norm(text_val) -> str | None:
    """Return normalised lowercase string, or None if empty/junk."""
    if pd.isna(text_val):
        return None
    s = str(text_val).strip()
    if not s or s.lower() in ("nan",):
        return None
    s = _DASH_RE.sub("-", s)       # unify dash variants
    return s


# ─────────────────────────────────────────────────────────────────────────────
# Keyword dictionaries  (all keys are normalised strings)
#
# Matching rule: keyword is a substring of the normalised cell text.
# Longer / more specific keywords are listed first so they are matched
# before a shorter overlapping one — but since we take the MAX level,
# order only affects which keyword is reported in debug, not the final score.
#
# Level 2 = 高危, Level 1 = 低危, Level 0 = 正常/不纳入
# ─────────────────────────────────────────────────────────────────────────────

ECG_KEYWORDS: dict[int, list[str]] = {
    2: [
        # ── 房颤 / 房扑 ──────────────────────────────────────────────────
        "快速型心房颤动",      # 507  (data form; "快速型房颤" also kept)
        "快速型房颤",          # keep for any alternate short form
        "阵发性心房颤动",
        "阵发性房颤",
        "持续性心房颤动",
        "持续性房颤",
        "永久性心房颤动",
        "心房颤动",            # 4843  (covers 提示心房颤动, 异位心律心房颤动, etc.)
        "房颤",                # 241  (short abbreviation)
        "心房扑动",            # 128
        "房扑",

        # ── LVH (左心室肥厚/高电压) ──────────────────────────────────────
        "左心室高电压",        # 16671
        "左室高电压",          # 425
        "左心室肥厚",          # 2666
        "左心室肥大",          # 1642
        "左室肥厚",
        "左室肥大",            # 81
        "LVH",

        # ── 频发房性早搏 ─────────────────────────────────────────────────
        "频发房性早搏",        # 1140
        "频发房性期前收缩",    # 194  (formal medical term)

        # ── 心肌梗塞/梗死 ────────────────────────────────────────────────
        "心肌梗塞",            # 下壁心肌梗塞(90), 前间壁(150), 前壁(105), etc.
        "心肌梗死",            # alternate spelling
        "下壁梗塞",
        "前间壁梗塞",

        # ── 心肌缺血 ─────────────────────────────────────────────────────
        "心肌缺血",            # 可能由心肌缺血引起(101)
        "冠脉供血不足",        # 141

        # ── 异常Q波 ──────────────────────────────────────────────────────
        "异常Q波",             # 1305
        "异常q波",             # 96  (lowercase variant)
        "病理性Q波",
        "病理性q波",
        "下壁异常Q波",         # 80+82 etc
        "下壁导联异常Q波",     # 82

        # ── ST段抬高 ─────────────────────────────────────────────────────
        "显著ST段抬高",        # 69  (explicit: significant elevation)
        "中度ST段抬高",        # 142
        "ST段抬高",            # 232  (catches most; see negation below)

        # ── 完全性左束支传导阻滞 ─────────────────────────────────────────
        "完全性左束支传导阻滞", # 943
        "完全性左束支阻滞",    # 198  (alternate; shorter)

        # ── 起搏心律 / 起搏器 ────────────────────────────────────────────
        "起搏心律",            # 523  (pacemaker rhythm present)
        "起搏器",              # catches all: 起搏器起搏及感知功能良好(128) etc.

        # ── 室性心动过速 / 室颤 ──────────────────────────────────────────
        "室性心动过速",        # 143
        "心室颤动",
        "室颤",

        # ── 预激综合征 ───────────────────────────────────────────────────
        "预激综合征",
        "预激综合症",          # 134  (alternate spelling with 症)
        "WPW",

        # ── 二度以上房室传导阻滞 ─────────────────────────────────────────
        "II度房室传导阻滞",    # 248
        "Ⅱ度房室传导阻滞",
        "二度房室传导阻滞",
        "2度房室传导阻滞",
        "II度I型房室传导阻滞", # 71  Wenckebach
        "II度II型房室传导阻滞",
        "III度房室传导阻滞",   # 77  complete heart block
        "Ⅲ度房室传导阻滞",
        "三度房室传导阻滞",
        "3度房室传导阻滞",
        "完全性房室传导阻滞",

        # ── 广泛ST-T异常 ─────────────────────────────────────────────────
        "广泛ST-T异常",        # 160
    ],

    1: [
        # ── ST-T改变 / ST-T异常 (各类写法) ─────────────────────────────
        "ST-T改变",            # 5216  (normalised dash covers ST—T改变)
        "ST-T异常",            # 2059
        "ST-T轻度改变",        # 1036
        "ST-T波改变",          # 90
        "原发性ST-T改变",      # 237
        "继发性ST-T改变",      # 189
        "继发性ST-T段改变",
        "侧壁ST-T异常",        # 320
        "前壁ST-T异常",
        "前侧壁ST-T异常",      # 93
        "下壁ST-T异常",
        "下/侧壁ST-T异常",     # 103

        # ── T波改变 ───────────────────────────────────────────────────────
        "T波低平",             # 10329
        "T波改变",             # 12793  (also covers T波轻度改变, 部分T波改变)
        "T波异常",             # 155
        "T波倒置",             # 2678
        "T波平坦",             # 674
        "T波双向",             # 361
        "T波高尖",             # 297
        "T波高耸",             # 79
        "侧壁T波异常",         # 187
        "下壁T波异常",         # 151
        "前壁T波异常",         # 108

        # ── ST段改变/压低 ─────────────────────────────────────────────────
        "轻度ST段抬高",        # 1091  (early repolarization-like; lower than frank elevation)
        "ST段轻度改变",        # 3312
        "ST段改变",            # 1210
        "ST段压低",            # 477
        "轻度ST段压低",        # 1947
        "中度ST段压低",        # 247
        "ST段下移",            # 148
        "ST段轻度压低",        # 72
        "部分导联ST段改变",    # 232

        # ── 完全性右束支传导阻滞 (risk level 1, not 2) ───────────────────
        "完全性右束支传导阻滞", # 5662
        "完全性右束支阻滞",    # 2337  (alternate)
        "完全性右束传导阻滞",  # 229  (typo in data: 支 missing)
        "右束支传导阻滞",      # 243
        "右束支阻滞",          # 388
        "局限性右束支传导阻滞", # 104

        # ── 左前/后分支阻滞 ──────────────────────────────────────────────
        "左前分支传导阻滞",    # 836
        "左前分支阻滞",        # 1244
        "左后分支传导阻滞",
        "左后分支阻滞",

        # ── 一度房室传导阻滞 ─────────────────────────────────────────────
        "一度房室传导阻滞",    # 2306
        "I度房室传导阻滞",     # 2973
        "Ⅰ度房室传导阻滞",    # 137
        "1度房室传导阻滞",     # 135
        "1度房室阻滞",
        "窦性心律伴一度房室阻滞", # 119

        # ── 低电压 ────────────────────────────────────────────────────────
        "低电压",              # 200  (also matches 肢导低电压, 肢体导联低电压, etc.)

        # ── 电轴偏移 ──────────────────────────────────────────────────────
        "电轴左偏",            # catches 心电轴左偏(4071), QRS电轴左偏(872), 电轴左偏(499)
        "电轴右偏",            # catches 心电轴右偏(1232), 显著心电轴右偏(128), etc.

        # ── QT间期延长 ────────────────────────────────────────────────────
        "QT间期延长",          # 990
        "QTc间期延长",         # 167  (different from QT间期延长!)
        "Q-Tc",               # catches Q-Tc> fragments
        "QTc延长",
        "边界性QT间期延长",    # 185

        # ── 偶见/偶发房性早搏 ────────────────────────────────────────────
        "偶见房性早搏",        # 2035
        "偶发房性早搏",        # 1316
        "房性早搏",            # 2593  (generic: covers 窦性心律伴房性早搏 etc.)
        "房性期前收缩",        # 2968  (formal term for 房性早搏)

        # ── 室性早搏 (频发争议项, 偶发正常) ─────────────────────────────
        "频发室性早搏",        # 1468
        "频发室性期前收缩",    # 306  (formal)
        "室性期前收缩",        # 2514  (formal term for 室性早搏, covers general)
        "室性早搏",            # 1636  (covers 窦性心律室性早搏 etc.)
        "室性期前收缩二联律",  # 107

        # ── 不完全性左束支 (lower risk than complete) ────────────────────
        "不完全性左束支传导阻滞",
        "不完全性左束支阻滞",
        "不完全左束支",

        # ── 其他异常 ─────────────────────────────────────────────────────
        "P波增宽",             # 2463  (P mitrale)
        "P波高尖",             # 124  (P pulmonale)
        "左心房肥大",          # 271
        "左心房增大",          # 75
        "左心房负荷过重",      # 82
        "右心室肥厚",          # 336
        "右心室肥大",          # 309
        "右心房肥大",          # 256
        "右心房扩大",          # 67
        "R波递增不良",         # 954  (poor R progression)
        "室内传导阻滞",        # 389
        "室内传导延缓",        # 132
        "短PR间期",            # 873
        "短PR综合症",          # 130
        "心律不齐",            # 241  (generic arrhythmia)
        "U波改变",             # 132
        "T波尖峰",             # 72
    ],

    0: [
        # ── 窦性心律 (各种形式) ──────────────────────────────────────────
        "窦性心律不齐",        # 7883  (benign in young; listed normal here)
        "窦性心动过缓",        # 36115
        "窦性心律过缓",        # 1327  (alternate phrasing)
        "窦性心律过速",        # 147  (alternate for 窦性心动过速)
        "窦性心动过速",        # 4234
        "窦性心律",            # 80619  (must come after more specific entries above)

        # ── 正常心电图 ────────────────────────────────────────────────────
        "正常心电图",          # 6768
        "大致正常心电图",      # 6166
        "正常范围心电图",      # 3328
        "正常范围",            # 12633  (e.g. "心电图正常范围")
        "大致正常范围",        # 1043
        "大致正常",            # 83
        "边界性心电图",        # 1047

        # ── 早期复极 / J点抬高 ────────────────────────────────────────────
        "早期复极",            # 388
        "J点抬高",             # 1307
        "S-T呈凹面向上抬高",   # 162  (concave ST elevation = early repolarisation)

        # ── 转位 ──────────────────────────────────────────────────────────
        "逆钟向转位",          # 800
        "逆时钟转位",          # 107  (alternate)
        "顺钟向转位",          # 355

        # ── 不完全性右束支 (benign) ───────────────────────────────────────
        "不完全性右束支传导阻滞", # 826
        "不完全性右束支阻滞",  # 900
        "不完全右束支传导阻滞", # 213
        "不完全性右束支",      # generic

        # ── 偶发室性早搏 (benign) ─────────────────────────────────────────
        "偶见室性早搏",        # 1623
        "偶发室性早搏",        # 1283
        "偶发室早",            # 81

        # ── 未检 / 拒检 ───────────────────────────────────────────────────
        # (handled as NaN in _classify; listed here as fallback)
        "未检",
        "未查",
        "拒检",
    ],
}


XRAY_KEYWORDS: dict[int, list[str]] = {
    2: [
        # ── 心脏扩大 / 心影增大 ──────────────────────────────────────────
        "心脏扩大",
        "心脏增大",            # 282  (data form; 心脏扩大 is rarer in this dataset)
        "心影增大",            # 4258  (also catches 心影轻度增大, 左心影增大)
        "心影偏大",            # 1417

        # ── 主动脉钙化 / 硬化 ────────────────────────────────────────────
        "主动脉迂曲钙化",      # most specific first
        "主动脉弓弧形钙化",    # 38
        "主动脉弓结钙化",      # 70  (弓结 = arch junction)
        "主动脉弓壁钙化",      # 960  (NOT substring of 主动脉弓钙化!)
        "主动脉弓钙化",        # 3191
        "主动脉壁钙化",        # 109
        "主动脉弓硬化",
        "主动脉硬化",          # 799  (NOT 弓硬化 — covers 主动脉硬化 standalone)
        "主动脉迂曲",          # 102
        "主动脉增宽",
        "主动脉钙化",          # 69  (standalone form)

        # ── 冠脉钙化 ─────────────────────────────────────────────────────
        "冠脉局部管壁钙化",    # 224  (most specific first)
        "冠脉管壁多发钙化",    # 132
        "冠脉管壁钙化",        # 31
        "冠脉多发钙化",        # 28
        "冠脉壁多发钙化",      # 62
        "冠脉壁钙化",          # 79
        "冠脉壁少许钙化",      # 41
        "冠脉少许钙化",        # 18
        "冠脉钙化",            # 81

        # ── 瓣膜钙化 ─────────────────────────────────────────────────────
        "主动脉瓣钙化",        # 26
        "瓣膜钙化",            # 18

        # ── 起搏器 / 术后 ─────────────────────────────────────────────────
        "心脏起搏器术后改变",  # 46
        "心脏起搏器置入",      # 129  (置入 vs 植入 — both covered)
        "心脏起搏器植入",
        "起搏器置入",
        "起搏器植入",
        "启博器置入",          # 24  (alternate spelling: 启博 = 起搏)
        "起搏器",              # generic: catches all pacemaker mentions
        "开胸术后",            # 49  (post-open-heart surgery)
        "心脏术后改变",        # 24
        "心脏术后",            # 21
        "胸骨术后改变",        # 29  (sternotomy = cardiac surgery)
    ],

    1: [
        # ── 肺气肿 ────────────────────────────────────────────────────────
        "慢性支气管炎并肺气肿", # 48  most specific first
        "慢性支气管炎伴肺气肿", # 41
        "支气管炎伴肺气肿",    # 69
        "慢支肺气肿",          # 916  (common abbreviation combination)
        "慢性支气管炎",        # 376
        "提示慢性支气管炎",    # 56  (matches 慢性支气管炎 too)
        "提示慢支",            # 36
        "慢支",                # 155
        "支气管炎",            # 289  (generic bronchitis)
        "提示支气管炎",        # 24
        "支炎改变",            # 150
        "细支气管炎",
        "两上肺轻度间隔旁型肺气肿", # 29  specific
        "两侧肺气肿",          # 94
        "两下肺肺气肿",        # 21
        "两肺气肿",            # 125
        "两侧轻度肺气肿",      # 27
        "轻度肺气肿",          # 67
        "提示肺气肿",          # 31
        "肺气肿",              # 1473  (general, catches all above if specific ones missed)

        # ── 肺纹理 ────────────────────────────────────────────────────────
        "两肺纹理增多增粗",    # 304
        "肺纹理增多增粗",      # 38
        "肺纹理增多",          # 655
        "肺纹理增粗",          # 185
        "肺纹理紊乱",
        "两肺纹理增多",        # 221
        "两肺纹理增粗",        # 185

        # ── 胸腔积液 ─────────────────────────────────────────────────────
        "两侧胸腔少量积液",    # 42
        "两侧胸腔积液",
        "右侧胸腔少量积液",    # 21
        "左侧胸腔少量积液",    # 21
        "右胸腔少量积液",      # 19
        "胸腔少量积液",        # 20
        "肋膈角变钝",          # 18  (blunted = effusion)
        "左肋膈角变钝",        # 48
        "右肋膈角变钝",        # 32
        "两侧肺水肿",          # 19

        # ── 胸膜 ──────────────────────────────────────────────────────────
        "局部胸膜增厚",        # 27
        "右侧胸膜增厚",        # 24
        "右胸膜改变",          # 70
        "左胸膜改变",          # 36
        "右下胸膜改变",        # 19
        "左下胸膜改变",        # 19
        "胸膜增厚",            # 32
        "胸膜钙化",
        "胸膜改变",
        "胸膜粘连",

        # ── 肺门 ──────────────────────────────────────────────────────────
        "右肺门影增宽",        # 29
        "右肺门影增浓",        # 18
        "肺门影增大",
        "肺门影增宽",
        "肺门增大",
        "肺门增宽",
        "右肺门区增大",

        # ── 膈肌 ──────────────────────────────────────────────────────────
        "右膈面抬高",          # 43
        "右膈面膨隆",          # 31
        "右膈抬高",            # 19
        "膈面抬高",
        "膈肌抬高",

        # ── 其他弱相关 ────────────────────────────────────────────────────
        "两肺间质性改变",      # 42
        "肺大泡形成",          # 68
        "肺大泡",
        "心包少量积液",        # 60
        "心包积液",
        "心影饱满",            # 461  (borderline enlarged — level 1, not 2)
        "心影略饱满",          # 54
        "提示肺动脉高压",      # 44
        "肺动脉高压",          # 33
        "肺动脉增宽",          # 32
        "右上纵隔影增宽",      # 39
        "两肺渗出性改变",      # 20
        "尘肺",                # 23  (occupational lung disease — indirect)
        "矽肺",                # 29
        "两肺多发感染",        # 32
    ],

    0: [
        # ── 正常表述 ──────────────────────────────────────────────────────
        "两肺及心肋常膈未见异", # 63  (truncated "正常" phrase)
        "两肺及心肋膈未见异常", # 1412
        "两肺心膈未见异常",    # 19
        "两肺及心隔未见异常",  # 9015
        "两肺及心膈未见异常",  # 717
        "两肺及膈未见明显异常", # 34
        "心肺膈未见异常",      # 20
        "心肺隔未见异常",      # 4896  (original: "心肺隔" variant)
        "心肺膈未见明显异常",
        "肺心膈未见明显异常",  # 36
        "肺心膈未见异常",      # 24
        "心膈未见明显异常",    # 3567
        "心膈未见异常",        # 10848
        "心隔未见明显异常",    # 56
        "心隔未见异常",        # 99
        "肺及心膈未见异常",    # 23
        "两膈未见明显异常",    # 84
        "两隔未见异常",        # 26
        "纵膈未见明显异常",    # 24
        "心肺未见异常",        # 80
        "两肺未见异常",        # 27
        "两肺未见明显异常",    # 207
        "胸部平片未见异常",    # 208
        "胸部平片未见明显异常", # 36
        "未见明显异常",        # general (catches many variants)
        "未见异常",            # general

        # ── 良性病灶 (与脑卒中无关) ──────────────────────────────────────
        # 纤维灶 — many variants: 两肺散在纤维灶, 右上肺纤维灶, etc.
        "陈旧性肺结核",        # 90  (NOT 陈旧性结核 — '肺' sits between them)
        "两肺陈旧性结核",      # 111
        "两上肺陈旧性结核",    # 69
        "右上肺陈旧性结核",    # 82
        "左上肺陈旧性结核",    # 42
        "陈旧性结核",          # catches any remaining 陈旧性结核
        "纤维增殖灶",          # 804  (catches 两肺散在纤维增殖灶 etc.)
        "纤维灶",              # 67+  (catches 右上肺纤维灶, 两肺少许纤维灶, etc.)
        "纤维索影",
        "纤维化灶",            # 两肺散在纤维化灶
        "增殖灶",              # 87  (proliferative lesion)
        "增殖",                # 31  (catches standalone 增殖 fragment)
        "钙化灶",              # 106  (calcified granuloma — benign)

        # ── 结节 (肺结节 — 各类写法) ─────────────────────────────────────
        "可疑结节",
        "结节灶",              # 右肺结节灶, 左肺门区小结节灶 etc.
        "微小结节",            # 右上肺微小结节
        "小结节",              # 右上肺小结节, 右下肺小结节 etc.
        "肺结节",              # 右上肺结节 contains 肺结节 ✓
        "多发结节",
        "多发小结节",

        # ── 脊柱侧弯 (各类写法) ──────────────────────────────────────────
        "胸腰椎向左侧弯",      # 26
        "胸腰椎",              # catches 胸腰椎呈"S"型
        "胸椎向左侧弯",        # 290
        "胸椎向右侧弯",        # 19
        "胸椎轻度侧弯",        # 42
        "胸椎侧弯",            # 137
        "腰椎向右侧弯",        # 53
        "腰椎向左侧弯",        # 22
        "腰椎侧弯",            # 21
        "脊椎侧弯",            # 448  (脊椎 NOT 脊柱)
        "脊柱侧弯",            # 75
        "脊柱侧凸",
        "脊柱畸形",
        "颈椎病",              # 45

        # ── 未检 / 拒检 ───────────────────────────────────────────────────
        "未检",
        "未查",
        "拒检",
    ],
}


# ─────────────────────────────────────────────────────────────────────────────
# Classification function
# ─────────────────────────────────────────────────────────────────────────────
# Negation prefixes: if one of these appears in the 4 characters immediately
# BEFORE a keyword, downgrade from level 2 → level 1.
# Applied only to level-2 keywords to avoid false high-risk tagging.
_NEG_PREFIXES = ("不完全", "轻度", "临界", "可疑")

# Sentinel terms that mean "not examined" → return NaN regardless.
_NOT_EXAMINED = {"未检", "未查", "拒检"}


def _classify(text_val, keywords: dict[int, list[str]]) -> float:
    """
    Return max risk level found in the normalised text, or NaN.
    """
    normed = _norm(text_val)
    if normed is None:
        return np.nan
    # Quick check for "not examined"
    for ne in _NOT_EXAMINED:
        if ne in normed and len(normed) < 6:   # only if it's essentially the whole value
            return np.nan

    max_level = -1

    for level in sorted(keywords.keys(), reverse=True):   # 2 → 1 → 0
        for kw in keywords[level]:
            idx = normed.find(kw)
            if idx == -1:
                continue
            if level == 2:
                # Check negation prefix in the 4 chars before the keyword
                pre = normed[max(0, idx - 4):idx]
                if any(neg in pre for neg in _NEG_PREFIXES):
                    # Negated high-risk → treat as low-risk at most
                    max_level = max(max_level, 1)
                    continue
            max_level = max(max_level, level)
            if max_level == 2:
                return 2.0   # can't go higher — short-circuit

    return float(max_level) if max_level >= 0 else np.nan


# ─────────────────────────────────────────────────────────────────────────────
print("=" * 65)
print("  step3_risk_classify — ECG & Chest X-ray Risk Classification")
print("=" * 65)

# ── Load fused.csv ────────────────────────────────────────────────────────────
if not FUSED_PATH.exists():
    raise FileNotFoundError(
        f"fused.csv not found at {FUSED_PATH}\n"
        "Run step2_fuse_clean_files.py first."
    )

print(f"\n[Load] Reading fused.csv…")
t0 = time.perf_counter()
fused = pd.read_csv(FUSED_PATH, dtype=str, encoding="utf-8-sig", low_memory=False)
print(f"  {len(fused):,} rows × {len(fused.columns)} cols  ({fmt(time.perf_counter()-t0)})")


# ── Detect ECG and X-ray columns ──────────────────────────────────────────────
def _find_col(df: pd.DataFrame, patterns: list[str]) -> str | None:
    for pat in patterns:
        for c in df.columns:
            if pat in c:
                return c
    return None

ecg_col  = _find_col(fused, ["心电图异常", "心电图描述", "心电图结果", "心电图"])
xray_col = _find_col(fused, ["胸部X片异常", "胸部X线片", "胸片异常", "胸部X片", "胸片结果"])

print(f"\n[Columns] ECG column  : {ecg_col!r}")
print(f"          X-ray column: {xray_col!r}")

if ecg_col is None and xray_col is None:
    raise ValueError(
        "Could not find ECG or X-ray columns in fused.csv.\n"
        f"Available columns containing 心电/胸: "
        f"{[c for c in fused.columns if any(k in c for k in ['心电','胸','X片'])]}"
    )


# ── Classify ──────────────────────────────────────────────────────────────────
print("\n[Classify] Applying keyword classification…")
t0 = time.perf_counter()
label = {2: "高危", 1: "低危", 0: "正常/不纳入"}

if ecg_col:
    fused["心电图_风险"] = fused[ecg_col].apply(lambda v: _classify(v, ECG_KEYWORDS))
    n_valid = fused["心电图_风险"].notna().sum()
    dist    = fused["心电图_风险"].value_counts().sort_index()
    print(f"\n  心电图_风险  (from '{ecg_col}', {n_valid:,} classified / {len(fused):,} total)")
    for lvl, cnt in dist.items():
        print(f"    {int(lvl)} {label.get(int(lvl), '?'):<12}  {cnt:>8,}  ({cnt/n_valid*100:.1f}%)")
    print(f"    NaN (空/未分类)  {fused['心电图_风险'].isna().sum():>8,}")

if xray_col:
    fused["胸片_风险"] = fused[xray_col].apply(lambda v: _classify(v, XRAY_KEYWORDS))
    n_valid = fused["胸片_风险"].notna().sum()
    dist    = fused["胸片_风险"].value_counts().sort_index()
    print(f"\n  胸片_风险    (from '{xray_col}', {n_valid:,} classified / {len(fused):,} total)")
    for lvl, cnt in dist.items():
        print(f"    {int(lvl)} {label.get(int(lvl), '?'):<12}  {cnt:>8,}  ({cnt/n_valid*100:.1f}%)")
    print(f"    NaN (空/未分类)  {fused['胸片_风险'].isna().sum():>8,}")

print(f"\n  ({fmt(time.perf_counter()-t0)})")


# ── Save ──────────────────────────────────────────────────────────────────────
print(f"\n[Save] Writing back to fused.csv…")
t0 = time.perf_counter()
fused.to_csv(FUSED_PATH, index=False, encoding="utf-8-sig")
new_cols = [c for c in ["心电图_风险", "胸片_风险"] if c in fused.columns]
print(f"  {len(fused):,} rows × {len(fused.columns)} cols")
print(f"  New columns: {new_cols}")
print(f"  → {FUSED_PATH}  ({fmt(time.perf_counter()-t0)})")

# ─────────────────────────────────────────────────────────────────────────────
# Visit statistics: per-year patient counts + cross-year span distribution
#
# Rules:
#   • Deduplicate by (patient_id, year) — same patient same year = 1 visit.
#   • 年度 column is the year source; NaN years are labelled "未知".
#   • "span" = number of distinct years a patient appears in across ALL data.
# ─────────────────────────────────────────────────────────────────────────────
print("\n[Stats] Computing visit statistics…")
t0 = time.perf_counter()

id_col   = "patient_id" if "patient_id" in fused.columns else fused.columns[0]
yr_col   = "年度"       if "年度"       in fused.columns else None

if yr_col:
    # Normalise year: keep 4-digit values; blank/NaN → "未知"
    def _safe_yr(v):
        import re as _re
        if pd.isna(v): return "未知"
        m = _re.search(r'((?:19|20)\d{2})', str(v).strip())
        return m.group(1) if m else "未知"

    fused["_yr_norm"] = fused[yr_col].apply(_safe_yr)

    # ── 1. Deduplicated (patient_id, year) pairs ─────────────────────────────
    pairs = fused[[id_col, "_yr_norm"]].drop_duplicates()
    pairs = pairs[pairs[id_col].notna() & (pairs[id_col] != "")]

    # ── 2. Per-year unique patient count ─────────────────────────────────────
    per_year = (
        pairs.groupby("_yr_norm")[id_col]
        .nunique()
        .rename("unique_patients")
        .reset_index()
        .rename(columns={"_yr_norm": "year"})
        .sort_values("year")
    )
    per_year["pct_of_total"] = (
        per_year["unique_patients"] / pairs[id_col].nunique() * 100
    ).round(1)

    print(f"\n  Per-year unique patients (deduped by patient×year):")
    print(f"  {'Year':<8}  {'Unique patients':>16}  {'% of all patients':>18}")
    print(f"  {'-'*8}  {'-'*16}  {'-'*18}")
    for _, row in per_year.iterrows():
        print(f"  {row['year']:<8}  {int(row['unique_patients']):>16,}  {row['pct_of_total']:>17.1f}%")

    # ── 3. Span distribution ──────────────────────────────────────────────────
    # For each patient: how many distinct years do they appear in?
    known_pairs = pairs[pairs["_yr_norm"] != "未知"]
    span = (
        known_pairs.groupby(id_col)["_yr_norm"]
        .nunique()
        .rename("n_years")
    )
    span_dist = (
        span.value_counts()
        .sort_index()
        .rename("n_patients")
        .reset_index()
        .rename(columns={"index": "years_appeared"})
    )
    # pandas ≥2.0 value_counts() returns the groupby col as index name
    if "n_years" in span_dist.columns:
        span_dist = span_dist.rename(columns={"n_years": "years_appeared"})

    total_with_year = len(span)
    span_dist["pct"] = (span_dist["n_patients"] / total_with_year * 100).round(1)

    print(f"\n  Patient span distribution (distinct years per patient):")
    print(f"  {'Years appeared':<16}  {'Patients':>10}  {'%':>8}")
    print(f"  {'-'*16}  {'-'*10}  {'-'*8}")
    for _, row in span_dist.iterrows():
        print(f"  {int(row['years_appeared']):<16}  {int(row['n_patients']):>10,}  {row['pct']:>7.1f}%")
    print(f"  {'─'*38}")
    print(f"  {'Total':<16}  {total_with_year:>10,}")

    # ── 4. Save stats CSVs ────────────────────────────────────────────────────
    per_year.to_csv(OUT_ANALYSIS / "stats_per_year.csv",
                    index=False, encoding="utf-8-sig")
    span_dist.to_csv(OUT_ANALYSIS / "stats_patient_span.csv",
                     index=False, encoding="utf-8-sig")
    print(f"\n  Saved → analysis/stats_per_year.csv")
    print(f"  Saved → analysis/stats_patient_span.csv")

    fused.drop(columns=["_yr_norm"], inplace=True)
else:
    print("  WARNING: '年度' column not found — skipping visit statistics.")

print(f"  ({fmt(time.perf_counter()-t0)})")


total = time.perf_counter() - t_start
print(f"""
{'=' * 65}
  Done in {fmt(total)}

  Encoding:
    2 = 高危  (strong stroke-risk association)
    1 = 低危  (weak / indirect marker)
    0 = 正常 / 不纳入风险
    NaN = 空白 / 未检 / 无法匹配

  Output files:
    fused.csv              — with 心电图_风险 + 胸片_风险 columns
    stats_per_year.csv     — unique patients per year
    stats_patient_span.csv — distribution of years-per-patient
{'=' * 65}""")
