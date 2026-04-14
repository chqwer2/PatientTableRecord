# Step 2 — Assign IDs to 2022/23/24 Records (`main.py`)

**Run order:** Second — requires `patient_reference.csv` from `main_2018.py`.

**Script:** `main.py`

**Purpose:** Assign `person_id` to every row in the 2022, 2023, and 2024 annual health-check CSVs by matching them against the golden patient reference. New patients not in the golden reference get fresh IDs.

---

## What it produces

| Output file | Description |
|-------------|-------------|
| `output/origin/2022_origin.csv` | Raw 2022 data, blank rows/cols removed |
| `output/origin/2023_origin.csv` | Raw 2023 data, blank rows/cols removed |
| `output/origin/2024_origin.csv` | Raw 2024 data, blank rows/cols removed |
| `output/intermediate/2022_intermediate.csv` | `person_id` added, name column dropped |
| `output/intermediate/2023_intermediate.csv` | Same for 2023 |
| `output/intermediate/2024_intermediate.csv` | Same for 2024 |
| `output/reference/patient_reference.csv` | Updated: `in_2022/23/24` flags set; new CSV-only patients appended |

---

## Processing steps

### 1. Load CSVs (GBK, chunked)

Each CSV is read in 50,000-row chunks to handle the ~300 MB file size. 100%-blank rows (Excel padding) are dropped immediately. The truncated column name fix (`-正常范` → `-正常范围`) is applied.

Result after loading and blank-row removal: ~73,000–80,000 rows per file.

### 2. Save origin versions

For each year, save a copy with all-blank columns removed. This is the archival "raw" reference — no other transformations applied.

### 3. Load golden reference

`patient_reference.csv` is loaded. Two lookup structures are built:

- `golden_lookup`: `(name, gender)` → `[(canonical_birth_year, person_id), ...]` — one entry per golden patient with that name+gender
- `golden_dobs`: `person_id` → full DOB string — used for exact expected-age computation during matching

### 4. Match 2022/23/24 records against golden patients

For each record in the CSVs, the estimated birth year is computed as:
```
estimated_birth_year = exam_year − age
```

The record is matched to a golden patient if:
1. `|estimated_birth_year − golden_birth_year| ≤ 1`, **and**
2. When the golden patient has a full DOB string, the record's actual age must fall within the expected age range for that exam date/year (±1 tolerance for rounding)

**Expected age check (when full DOB is available):**
- If both full DOB and full exam date are known → compute exact age → check `|actual_age − exact_age| ≤ 1`
- If only birth year and exam year are known → expected age is `[exam_yr − birth_yr − 1, exam_yr − birth_yr]` → check actual age falls in range ±1

Records with `|birth_year_diff| > 1` are treated as different people, even if name+gender match.

### 5. Handle records with no golden match

Records whose estimated birth year does not match any golden patient are clustered among themselves using the same greedy compatibility algorithm as `main_2018.py` (see [Step 1 docs](03_step1_golden_reference.md#5-patient-clustering-algorithm)). Each resulting cluster gets a new unique `person_id` and is added to `patient_lookup`.

These new patients are marked `in_2018=N`, `in_stroke=N` in the reference.

### 6. Assign person_ids and save intermediate files

For each row in each year's CSV:
- Look up `(name, gender)` in `patient_lookup`
- Find the best-matching `(birth_year, person_id)` pair using the same age-consistency check
- If no match passes — assign empty string (indicates unresolved match)

The name column (`姓名_x`) is dropped. `person_id` is inserted as column 0. Saved to `output/intermediate/`.

### 7. Update patient_reference.csv

- For each golden patient, set `in_2022=Y` / `in_2023=Y` / `in_2024=Y` if their `person_id` appears in the respective year's intermediate file
- New CSV-only patients are appended to the reference

### 8. Cross-year age monotonicity validation

After all IDs are assigned, the script validates that each patient's age is non-decreasing across years and does not jump unrealistically fast:

- `age_2023 < age_2022` → warning (age decreased — suggests wrong ID assignment or data error)
- `age_increase > 2 × year_gap` → warning (age jumped too fast)

Violations are printed to console. They indicate either a clustering error or bad source data, but do not block the pipeline.

---

## Why estimates (not exact DOBs) for 2022–2024

The 2022–2024 CSVs do not contain ID card numbers or explicit birth date fields — only `年龄` (age at exam). The birth year must therefore be estimated as `exam_year − age`. This introduces ±1 year uncertainty (depending on whether the birthday has passed by the exam date).

The ±1 tolerance in the matching rule accounts for this: a patient aged 73 in 2022 (est. birth year 1949) and aged 74 in 2023 (est. birth year 1949) will still match correctly.
