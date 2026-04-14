# Step 1 — Golden Patient Reference (`main_2018.py`)

**Run order:** First — must complete before any other step.

**Script:** `main_2018.py`

**Purpose:** Build the canonical patient identity table (`patient_reference.csv`) from the two sources that have exact dates of birth: 2018 health-check records (DOB from ID card / birth date field) and the stroke registry (explicit 出生日期 column).

---

## What it produces

| Output file | Description |
|-------------|-------------|
| `output/origin/2018_origin.csv` | 2018 data with blank rows/cols removed; names dropped |
| `output/origin/stroke_origin.csv` | Stroke data with blank rows/cols removed |
| `output/intermediate/2018_intermediate.csv` | 2018 data with `person_id` prepended; names dropped |
| `output/intermediate/stroke_intermediate.csv` | Stroke data with `person_id` prepended; names dropped |
| `output/reference/patient_reference.csv` | **Golden patient index** — one row per unique patient |

---

## Processing steps

### 1. Load and clean input files

- **Stroke registry:** Read from `2022-2024.xlsx` sheet `脑卒中`. Strip leading digit from gender values (`"1 男"` → `"男"`). Remove 100%-blank rows.
- **2018.xlsx:** Read all rows. Remove 100%-blank rows. Fix truncated column names ending in `-正常范` → `-正常范围`.

### 2. Map 2018.xlsx columns to the CSV schema

The 2018.xlsx uses different column names than the 2022–2024 CSVs. This step maps every CSV-schema column to its 2018.xlsx equivalent so the two sources share a unified schema downstream.

Matching strategy (applied in order, first match wins):

1. **Exact match** — same column name
2. **Strip `_x` suffix** — `年度_x` → `年度`
3. **Normalise + exact** — strip trailing unit (`/L`), strip leading prefix (`体检`/`开始`), compare
4. **Substring** — CSV column name is a substring of a 2018 column name (or vice versa)
5. **Fuzzy match** — `difflib.get_close_matches` with cutoff 0.76

Manual overrides always take priority:

| CSV column | 2018 column |
|-----------|-------------|
| `心电图异常` | `心电图描述` |
| `胸部X片异常` | `胸部X线片-异常说明` |
| `腹部B超异常` | `腹部B超-异常说明` |

Columns absent in 2018.xlsx are added as all-empty columns to keep the schema identical.

### 3. Extract date of birth for each 2018 row

Priority order:
1. `出生日期` column — used directly if it contains a 4-digit year
2. `身份证` / `证件` column — ID card number; birth date extracted:
   - 18-digit ID: characters 7–14 = `YYYYMMDD`
   - 15-digit ID: characters 7–12 = `YYMMDD` (century assumed 1900)

### 4. Build the golden patient pool

For each (name, gender) pair across both sources, collect all records with their birth-year information:

```
Record tuple: (birth_year, is_exact, exam_year, age, full_dob_string, source)
```

- `is_exact = True` when DOB was derived from an exact date (not estimated from age)
- `source` is `"2018"` or `"stroke"` — used to prevent merging same-name same-DOB patients within the same file

### 5. Patient clustering algorithm

Within each (name, gender) group, records that belong to the same physical person are grouped together using a **greedy compatibility algorithm**.

Two records are **compatible** (= same person) if **all three** of these checks pass:

**Check 1 — Birth year proximity**

| Situation | Rule |
|-----------|------|
| Both exact DOBs, same source file | Full DOB strings must be identical |
| Both exact DOBs, different sources (cross: 2018 vs stroke) | Birth years must match exactly |
| At least one estimated | `\|birth_year_A − birth_year_B\| ≤ 1` |

**Check 2 — Same-year age consistency**
If both records are from the same exam year and both have age values:
- At least one exact DOB: `\|age_A − age_B\| ≤ 1`
- Both estimated: ages must be equal (no tolerance)

**Check 3 — Cross-year age monotonicity**
If records are from different years and both have age values, the older record must have a smaller age, and the increase must not exceed the year gap:
- At least one exact DOB: `age_increase ≤ year_gap + 1`
- Both estimated: `age_increase ≤ year_gap`

The algorithm processes records sorted by (birth_year, exam_year, age) and assigns each to the first existing cluster that is compatible with all its current members. Records that fit no existing cluster start a new cluster.

### 6. Assign person_ids

Each cluster gets one unique 6-character alphanumeric ID (e.g., `A3K9PZ`):
- First character is always a letter — prevents Excel from misinterpreting IDs like `6E0024` as scientific notation (`6E+24`)
- IDs are globally unique within the run (checked against a pool set)

### 7. Build patient_reference.csv

One row per cluster (= one unique patient):

| Column | Description |
|--------|-------------|
| `person_id` | Unique 6-char alphanumeric ID |
| `name` | Patient name |
| `gender` | `男` / `女` |
| `birth_year` | Canonical birth year (mean of cluster, rounded) |
| `dob` | Best available full DOB string (`YYYY-MM-DD`) |
| `source` | Which files contributed: `"2018"`, `"stroke"`, or `"2018+stroke"` |
| `in_2018` | `Y` / `N` |
| `in_stroke` | `Y` / `N` |
| `in_2022` | `N` (updated by `main.py`) |
| `in_2023` | `N` (updated by `main.py`) |
| `in_2024` | `N` (updated by `main.py`) |
| `stroke_diagnosis` | Stroke diagnosis text (from stroke registry, blank if none) |

### 8. Save intermediate files

Names are dropped from intermediate files (privacy). Each row gets `person_id` prepended as the first column.

---

## Known limitations

- **Name collision:** Two different people with the same name and gender and similar birth years may be merged into one `person_id`. This is unavoidable without a patient number. The birth-year clustering minimises this risk.
- **2018 exam year:** Some 2018.xlsx rows have an actual exam date in 2019–2022 (follow-up visits stored in the same file). The script uses the actual exam date from `体检时间` for birth-year estimation when available, falling back to `年度`.
