# Step 3 — Lab Processing & Feature Transforms (`main2.py`)

**Run order:** Third — requires intermediate files from `main_2018.py` and `main.py`.

**Script:** `main2.py`

**Requires:** `ANTHROPIC_API_KEY` environment variable (for parsing ambiguous reference range strings)

**Purpose:** Convert raw lab triplet columns into single normalised values, expand multi-label columns into binary flags, and blank out uninformative "normal" default text values.

---

## What it produces

All files saved to `output/clean/`:

| File | Description |
|------|-------------|
| `2018_clean.csv` | Fully transformed 2018 data |
| `stroke_clean.csv` | Fully transformed stroke data |
| `2022_clean.csv` | Fully transformed 2022 data |
| `2023_clean.csv` | Fully transformed 2023 data |
| `2024_clean.csv` | Fully transformed 2024 data |

---

## Lab triplet processing

Each lab test is stored in the raw data as three columns:

| Column pattern | Example | Meaning |
|----------------|---------|---------|
| `<test>` | `血红蛋白` | Measured numeric value |
| `<test>-异常结果` | `血红蛋白-异常结果` | Abnormality flag (`↑` or `↓`) |
| `<test>-正常范围` | `血红蛋白-正常范围` | Patient's reference range string (e.g., `110~150`) |

All three are collapsed into one column: `<test>_值`.

### Numeric tests — interval normalisation (28 tests)

```
_值 = (measured_value − lower_bound) / (upper_bound − lower_bound)
```

| `_值` range | Meaning |
|------------|---------|
| `0.0 – 1.0` | Within normal range |
| `< 0` | Below lower bound (e.g., `-0.18` = 18% below) |
| `> 1` | Above upper bound (e.g., `1.10` = 10% above) |
| `NaN` | Value missing, or reference range unparseable |

### Reference range parsing

Python regex handles all common formats without any API calls:

| Raw string | Parsed as |
|-----------|-----------|
| `4.0~10.0` | lower=4.0, upper=10.0 |
| `4.0~10.` (truncated decimal point) | lower=4.0, upper=10.0 |
| `4.0-10.0~4.0-10.0` (duplicated) | lower=4.0, upper=10.0 |
| `160~110` (reversed order) | lower=110, upper=160 |
| `>4.0` (one-sided) | Cannot normalise → NaN |
| `阴性`, `>阴性` | Qualitative → handled separately |

Only strings that match none of the above patterns are sent to the Claude API for parsing. This keeps API usage minimal — typically only a small batch of edge cases per run.

### Qualitative tests — ordinal encoding (4 tests)

`尿蛋白`, `尿糖`, `尿酮体`, `尿潜血` are encoded as 0–5 integers:

| Raw value | `_值` |
|-----------|-------|
| `-` / `阴性` | 0 |
| `弱阳性` | 1 |
| `阳性(+)` | 2 |
| `阳性(++)` | 3 |
| `阳性(+++)` | 4 |
| `阳性(++++)` | 5 |

Full-width variants (e.g., `（+）`) are normalised before lookup.

---

## Multi-label column expansion

### 人员类型 → 3 binary columns

The raw `人员类型` column contains comma-separated labels, e.g., `"高血压，老年人"`. Split into:

| New column | Value | Meaning |
|-----------|-------|---------|
| `人员类型_老年人` | 1 / 0 | Is elderly care registrant |
| `人员类型_高血压` | 1 / 0 | Has hypertension |
| `人员类型_糖尿病` | 1 / 0 | Has diabetes |

Original `人员类型` column is deleted.

### 齿列 → 4 binary columns

The raw `齿列` column contains comma-separated dental status, e.g., `"缺齿，龋齿"`. Split into:

| New column | Value | Meaning |
|-----------|-------|---------|
| `齿列_正常` | 1 / 0 | Teeth normal |
| `齿列_缺齿` | 1 / 0 | Missing teeth |
| `齿列_龋齿` | 1 / 0 | Cavities present |
| `齿列_义齿(假牙)` | 1 / 0 | Has dentures |

Original `齿列` column is deleted.

---

## Other transforms

### Blood pressure splitting

If a single `血压` column contains both systolic and diastolic (e.g., `"120/80"`), it is split into `高压` (systolic) and `低压` (diastolic) numeric columns.

### Dental columns merge

Multiple dental status columns that may exist across years are merged into a single `牙齿情况` free-text column.

### Medical text cleaning (`clean_medical_text`)

ECG, X-ray, and ultrasound text columns are scanned. Terms that are purely measurements (e.g., `44mm`, `33*19mm`) or contain only numeric content are stripped. The result is a cleaned abnormal-finding text suitable for keyword classification in Step 5.

---

## Normal value blanking (`strip_normals`)

After all transforms, the following exact string values in any text column are replaced with NaN (blank). They represent "no finding" and carry no analytical signal:

```
无症状  无症状;  正常  无  否  红润  无充血
听见  可顺利完成  未触及  触及正常  齐  无异常
阴性  未见异常  无殊  无特殊  正常范围  未见明显异常
未见  无压痛  软  清  无杂音
```

Numeric `_值` columns are not affected.

---

## Claude API usage

A single batch API call is made to parse reference range strings that the Python regex cannot handle. The batch is deduplicated (each unique unparseable string sent only once), minimising token usage.

The `ANTHROPIC_API_KEY` environment variable must be exported before running. The `02_run.sh` script will exit with an error if it is not set.
