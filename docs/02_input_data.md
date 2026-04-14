# Input Data

All input files must be placed in `/Users/haochen/Desktop/PatientTables/`.

---

## 2018.xlsx

Annual health-check records for the 2018 cohort.

- **Encoding:** Excel (`.xlsx`)
- **Key columns used:**
  - `姓名` (or similar — auto-detected by `含'姓名'`) — patient name
  - `性别` — gender (`男` / `女`)
  - `出生日期` — date of birth (preferred for exact DOB)
  - `身份证` / `证件` — ID card number (fallback for DOB extraction)
  - `年龄` — age at exam
  - `体检时间` / `检查时间` / `体检日期` — exam date (used to derive birth year when DOB missing)
  - `年度_x` — exam year (fallback; set to `2018` if absent)
  - All lab and clinical columns — mapped to the same schema as the 2022–2024 CSVs

**Column mapping quirk:** 2018.xlsx uses different column names for some fields. `main_2018.py` auto-maps them:

| 2018.xlsx column | CSV schema column |
|-----------------|------------------|
| `心电图异常` | `心电图描述` |
| `胸部X片异常` | `胸部X线片-异常说明` |
| `腹部B超异常` | `腹部B超-异常说明` |

Fuzzy matching (difflib, cutoff 0.76) handles minor name differences. Columns absent in 2018.xlsx are added as empty columns so the schema matches the CSVs.

**Truncated column names:** Excel sometimes truncates column names at field-width boundaries. These are auto-repaired:
- `总胆红素-正常范` → `总胆红素-正常范围`
- `甘油三酯-正常范` → `甘油三酯-正常范围`
- (any column ending in `-正常范` → append `围`)

---

## 2022-2024.xlsx (stroke registry)

Sheet name: `脑卒中`

- **Encoding:** Excel (`.xlsx`)
- **Key columns:**
  - `姓名` — patient name
  - `性别` — gender (raw: `"1 男"` / `"2 女"` → extracted to `男` / `女`)
  - `出生日期` — exact date of birth
  - `发病年龄` — age at stroke onset
  - `发病日期` — stroke onset date (year extracted for birth-year estimation)
  - `脑卒中诊断` — stroke diagnosis string (preserved in reference)

**Gender normalisation:** The stroke registry stores gender as `"1 男"` / `"2 女"`. The pipeline strips the numeric prefix with `str.extract(r"([男女])")`.

---

## 2022.csv / 2023.csv / 2024.csv

Annual health-check records for 2022, 2023, 2024 cohorts.

- **Encoding:** GBK (Chinese Windows encoding)
- **Size:** ~300 MB each; contain ~1,048,575 rows because Excel exports pad files to maximum row count
- **Actual data rows:** ~73,000–80,000 per file; ~975,000 rows per file are 100%-blank padding

**Key columns:**
- `姓名_x` — patient name
- `性别_x` — gender
- `年龄` — age at exam
- `年度_x` — exam year
- `体检时间` / `检查时间` / `体检日期` — exam date
- Lab columns — each test stored as a triplet (value / flag / reference range); see [step3 docs](05_step3_lab_processing.md)
- `人员类型` — comma-separated type labels (e.g., `"高血压，老年人"`)
- `齿列` — comma-separated dental status
- `心电图描述` — ECG free text
- `胸部X线片-异常说明` — chest X-ray free text
- `腹部B超-异常说明` — abdominal ultrasound free text

**Lab triplet pattern:** For each lab test (e.g., `血红蛋白`), three columns exist:

| Column | Meaning |
|--------|---------|
| `血红蛋白` | Measured value (numeric string) |
| `血红蛋白-异常结果` | Abnormality flag (`↑` or `↓`) |
| `血红蛋白-正常范围` | Patient-specific reference range (e.g., `110~150`) |

These are collapsed to a single `血红蛋白_值` column by `main2.py`.

---

## Encoding notes

| File | Encoding | How to open in Python |
|------|----------|-----------------------|
| `2018.xlsx` | Excel binary | `pd.read_excel(..., dtype=str)` |
| `2022-2024.xlsx` | Excel binary | `pd.read_excel(..., dtype=str)` |
| `2022/23/24.csv` | GBK | `pd.read_csv(..., encoding="gbk", dtype=str)` |
| All output files | UTF-8 with BOM | `pd.read_csv(..., encoding="utf-8-sig")` — BOM allows direct Excel open |
