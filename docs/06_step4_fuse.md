# Step 4 — Fuse & Analyse (`step2_fuse_clean_files.py`)

**Run order:** Fourth — requires all `*_clean.csv` files from `main2.py`.

**Script:** `step2_fuse_clean_files.py`

**Purpose:** Combine all five clean CSV files (2018, stroke, 2022, 2023, 2024) into a single fused dataset, remove low-quality rows, and produce text-frequency and outlier reports.

---

## What it produces

| Output file | Description |
|-------------|-------------|
| `analysis/fused.csv` | All years fused, sorted, deduplicated |
| `analysis/dropped_rows.csv` | Rows excluded because >70% fields were blank |
| `analysis/outlier_report.txt` | Visit distribution, per-column outlier diagnosis |
| `analysis/text_summaries/<col>.txt` | Term-frequency list for each medical text column |

---

## Processing steps

### 1. Load all clean files

All files matching `output/clean/*_clean.csv` are loaded. A `_source` column is added to track which file each row came from (e.g., `"2022"`, `"stroke"`).

Column name normalisation before fusing:
- `姓名_x` → `姓名`
- `性别_x` → `性别`
- `年度_x` → `年度`
- `体检日期_x` → `体检日期`

### 2. Schema intersection — which columns get fused

Only columns that appear in **at least 2 files** and have fewer than `MIN_SHARED_COLS` (20) absent files are included in the fused table. Files whose schema shares fewer than 20 columns with the union are skipped entirely with a warning.

This ensures the fused table contains meaningful data for most columns, rather than being mostly NaN for columns that only exist in one source.

### 3. Sort and deduplicate

Rows are sorted by `体检日期` (exam date) then `patient_id`. Exact duplicate rows (all column values identical) are dropped.

### 4. Drop sparse rows

Rows where the fraction of blank/NaN fields exceeds `VACANT_THRESHOLD` (70%) are removed and saved to `dropped_rows.csv`. These rows typically represent data-entry errors or patients who showed up but had no tests recorded.

### 5. Text summaries

For each medical free-text column (ECG, X-ray, ultrasound — auto-detected by column name patterns), the script:
1. Splits cell contents on all common delimiters (`;` `，` `、` `。` newline etc.)
2. Strips measurement strings (e.g., `44mm`, `3.5×2.1cm`)
3. Deduplicates terms within each cell
4. Counts term frequency across all rows
5. Saves to `analysis/text_summaries/<column_name>.txt`

These frequency lists were used to build the keyword dictionaries in Step 5 (risk classification).

### 6. Outlier report

The outlier report (`analysis/outlier_report.txt`) contains three sections:

**a. Patient visit distribution**
How many patients have 1, 2, 3, or 4 records in the fused table.

**b. Per-file dropped column counts**
How many columns from each source file were excluded from the fusion (schema mismatch).

**c. Per-column outlier diagnosis**
- **Numeric columns:** values outside `[Q1 − 2.5×IQR, Q3 + 2.5×IQR]` are flagged
- **Categorical columns:** values appearing in fewer than 1% of rows *and* fewer than 3 times absolute are flagged as rare categories

---

## Configuration constants

| Constant | Default | Meaning |
|----------|---------|---------|
| `VACANT_THRESHOLD` | 0.70 | Drop rows with >70% blank fields |
| `OUTLIER_IQR_MULT` | 2.5 | IQR multiplier for numeric outliers |
| `RARE_FREQ_FRAC` | 0.01 | Categorical values below 1% of rows are "rare" |
| `RARE_FREQ_ABS` | 3 | Categorical values below 3 absolute occurrences are "rare" |
| `MIN_SHARED_COLS` | 20 | Minimum shared columns for a file to be included |

---

## Column naming in fused.csv

After fusion, the `patient_id` column is the patient identifier (renamed from `person_id` / `_person_key`). The `_source` column is dropped. All other columns retain their clean-file names.

The fused table preserves blanks as NaN — it is the authoritative combined dataset before any imputation or encoding.
