# Output Files Reference

All output is written to `/Users/haochen/Desktop/PatientTables/output/`.  
All CSV files use UTF-8 with BOM encoding (open directly in Excel without encoding issues).

---

## `reference/patient_reference.csv`

Master patient index. One row per unique patient across all data sources.

| Column | Type | Description |
|--------|------|-------------|
| `person_id` | string (6 chars) | Unique patient identifier, e.g., `A3K9PZ`. Always starts with a letter. |
| `name` | string | Patient name (retained for reference; not in intermediate/clean files) |
| `gender` | string | `男` or `女` |
| `birth_year` | int or blank | Canonical birth year (mean of cluster, rounded) |
| `dob` | string | Best available full DOB, `YYYY-MM-DD` format, or blank |
| `source` | string | Which files contributed: `2018`, `stroke`, `2018+stroke`, or `csv` (CSV-only) |
| `in_2018` | `Y`/`N` | Patient appears in 2018.xlsx |
| `in_stroke` | `Y`/`N` | Patient appears in stroke registry |
| `in_2022` | `Y`/`N` | Patient appears in 2022.csv |
| `in_2023` | `Y`/`N` | Patient appears in 2023.csv |
| `in_2024` | `Y`/`N` | Patient appears in 2024.csv |
| `stroke_diagnosis` | string | Stroke diagnosis text, or blank |

---

## `origin/*_origin.csv`

Raw data with only blank rows and blank columns removed. All original column names preserved. No `person_id`. Name columns retained.

Files: `2018_origin.csv`, `stroke_origin.csv`, `2022_origin.csv`, `2023_origin.csv`, `2024_origin.csv`

---

## `intermediate/*_intermediate.csv`

Same as origin but with `person_id` prepended as the first column and name columns dropped (privacy).

Lab triplet columns are still present in raw form (not yet processed into `_值` columns).

Files: `2018_intermediate.csv`, `stroke_intermediate.csv`, `2022_intermediate.csv`, `2023_intermediate.csv`, `2024_intermediate.csv`

---

## `clean/*_clean.csv`

Fully processed individual-year files. These are the inputs to the fusion step.

Key differences from intermediate:
- Lab triplet columns (`<test>`, `<test>-异常结果`, `<test>-正常范围`) replaced by `<test>_值` (single normalised float)
- `人员类型` replaced by `人员类型_老年人`, `人员类型_高血压`, `人员类型_糖尿病` (0/1)
- `齿列` replaced by `齿列_正常`, `齿列_缺齿`, `齿列_龋齿`, `齿列_义齿(假牙)` (0/1)
- Normal default text values blanked (NaN)
- All-blank columns removed

Files: `2018_clean.csv`, `stroke_clean.csv`, `2022_clean.csv`, `2023_clean.csv`, `2024_clean.csv`

---

## `analysis/fused.csv`

The primary analysis dataset. All five clean files fused into one table.

Key properties:
- `patient_id` column identifies patients (same values as `person_id` in reference)
- NaN values preserved as-is (blank = not measured or not recorded)
- Includes `心电图_风险` and `胸片_风险` columns (added by Step 5)
- Sorted by `体检日期` then `patient_id`
- Exact duplicate rows removed
- Rows with >70% blank fields removed

---

## `analysis/fused_filled.csv`

ML-ready version of `fused.csv`. All NaN replaced with numeric sentinels. Categorical columns encoded to integers.

| Sentinel | Meaning |
|----------|---------|
| `-1` | Not performed / not recorded |
| `0` | Normal / female / exam done, no finding |
| `1` | Abnormal / male / low risk |
| `2` | High risk |

See [step 6 docs](08_step6_fill_missing.md) for full encoding table.

---

## `analysis/dropped_rows.csv`

Rows excluded from `fused.csv` because more than 70% of their fields were blank. Contains the same columns as `fused.csv`. Useful for auditing data quality.

---

## `analysis/outlier_report.txt`

Plain-text report containing:
1. Patient visit distribution (how many patients appear 1/2/3/4+ times)
2. Per-file dropped column counts
3. Numeric outlier rows per column (IQR × 2.5)
4. Rare categorical values per column (<1% frequency or <3 occurrences)

---

## `analysis/text_summaries/<column>.txt`

One file per detected medical free-text column (ECG, X-ray, ultrasound). Each file lists all unique terms found in that column sorted by frequency (descending):

```
ST-T改变    4521
窦性心律    3891
左心室高电压  1204
...
```

Used during development to build the risk classification keyword dictionaries.

---

## `analysis/stats_per_year.csv`

| Column | Description |
|--------|-------------|
| `year` | Exam year |
| `unique_patients` | Distinct patients with exams in this year (deduped by patient × year) |
| `stroke_patients` | Stroke registry patients among the above |
| `disease_rate` | `stroke_patients / unique_patients × 100` (%) |

---

## `analysis/stats_patient_span.csv`

| Column | Description |
|--------|-------------|
| `years_appeared` | Number of distinct exam years this patient has records in |
| `n_patients` | Count of patients with this span |
| `stroke_patients` | Stroke registry patients in this group |
| `disease_rate` | Stroke rate (%) |
| `pct_of_total` | % of all patients |
| `cumulative_pct` | Running cumulative % |

---

## `analysis/fill_report.txt`

Per-column summary from Step 6:
- How many NaN cells were filled with each sentinel value
- How many cells were encoded from categorical to numeric
- Final value distribution per column
