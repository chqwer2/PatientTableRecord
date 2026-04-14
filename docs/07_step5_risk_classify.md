# Step 5 — ECG & X-ray Risk Classification (`step3_risk_classify.py`)

**Run order:** Fifth — reads `analysis/fused.csv` and writes two new columns back into it.

**Script:** `step3_risk_classify.py`

**Purpose:** Classify each patient's ECG (`心电图描述`) and chest X-ray (`胸部X线片-异常说明`) free-text findings into a 3-tier numeric risk score.

---

## New columns added to fused.csv

| Column | Type | Values |
|--------|------|--------|
| `心电图_风险` | int / NaN | `2` = high risk, `1` = low risk, `0` = normal/unclassified, `NaN` = not examined or unclassifiable |
| `胸片_风险` | int / NaN | Same scale |

---

## Classification rule

For each row, the text is scanned for **all matching keywords**. The result is the **maximum level** found across all matches:

```
Any high-risk keyword found  → result = 2
Only low-risk keywords found → result = 1
Only normal keywords found   → result = 0
Empty / 未检 / 拒检 / 未查   → NaN
No keyword matches at all    → NaN
```

---

## Text normalisation

Before keyword matching, text is normalised:

1. **Dash unification:** `—` `－` `–` `‐` `‑` `−` → ASCII `-`
   (The dataset contains ST—T改变, ST－T改变, ST-T改变 as three variants of the same finding)
2. **Leading noise removal:** Prefixes like `提示`, `建议`, `考虑`, `可能`, `怀疑`, `★`, digit sequences are stripped from the start of each finding term
3. Full-width digits and letters are not separately normalised (keywords are written to match the data as-is)

---

## ECG keyword dictionary (`心电图_风险`)

**High risk (level 2):**
```
室性早搏  室上性早搏  心房颤动  心室颤动  心房扑动  心室扑动
完全性左束支传导阻滞  完全性右束支传导阻滞  三度房室传导阻滞
二度II型房室传导阻滞  ST段抬高  ST段压低  T波倒置  异常Q波
预激综合征  宽QRS波  心肌梗死  心肌缺血  短PR间期  频发
```

**Low risk (level 1):**
```
窦性心动过速  窦性心动过缓  窦性心律不齐  左心室高电压
不完全性右束支传导阻滞  不完全性左束支传导阻滞
一度房室传导阻滞  二度I型房室传导阻滞
ST-T改变  ST改变  T波改变  T波低平
偶发室性早搏  偶发房性早搏  室性早搏(偶发)  顺钟向转位
```

**Normal / unclassified (level 0):**
```
窦性心律  正常心电图  大致正常  未见异常
```

---

## Chest X-ray keyword dictionary (`胸片_风险`)

**High risk (level 2):**
```
肺癌  肺结核  肺炎  胸腔积液  气胸  肺不张  纵隔增宽
主动脉夹层  心包积液  肺门增大  肺门肿大  占位  肿块  结节
空洞  支气管扩张  肺气肿  胸膜增厚  胸膜钙化  骨折
```

**Low risk (level 1):**
```
肺纹理增多  肺纹理增粗  主动脉硬化  主动脉迂曲  主动脉钙化
心影增大  心脏增大  膈肌抬高  肋骨陈旧性改变  肋膈角变钝
肺门阴影增大  双肺纹理  钙化灶  钙化点  硬结灶  陈旧性病变
慢性支气管炎  肺气肿改变
```

**Normal / unclassified (level 0):**
```
未见异常  未见明显异常  正常  心肺未见异常  双肺纹理清晰
```

---

## Sentinel values explained

| Value | Meaning |
|-------|---------|
| `2` | Exam done; high-risk finding detected |
| `1` | Exam done; low-risk / mild finding detected |
| `0` | Exam done; no significant finding OR finding present but not in any keyword list |
| `NaN` | Exam not performed, refused, or text is blank |

The distinction between `0` and `NaN` is important: `0` means the exam happened and was unremarkable; `NaN` means no exam data exists. Step 6 (`step4_fill_missing.py`) further distinguishes them by filling `NaN` with either `0` or `-1` depending on whether any associated text exists.

---

## Keyword derivation

The keyword lists were derived from term-frequency analysis performed in Step 4 (`text_summaries/` files). The most common terms in the ECG and X-ray text columns were reviewed and categorised by clinical significance.
