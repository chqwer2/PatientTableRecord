# Visit Statistics (`visit_stats.py`)

**Run order:** After the full pipeline completes (also called automatically at the end of `02_run.sh`).

**Script:** `visit_stats.py`

**Inputs:**
- `output/analysis/fused.csv`
- `output/reference/patient_reference.csv`

**Purpose:** Produce statistical tables showing patient visit distribution and stroke disease rates by year and by number of years of participation. Also shows the distribution of ECG and X-ray risk scores.

---

## What it produces

| Output | Description |
|--------|-------------|
| `analysis/stats_per_year.csv` | Per-year patient counts and stroke rates |
| `analysis/stats_patient_span.csv` | Distribution of years-per-patient with stroke rates |
| `analysis/stats_summary.txt` | Full printed output (all four tables) |
| Console output | Same tables printed during the run |

---

## How patients and years are counted

**Year extraction:**
1. Parse `体检日期` column with regex `(19|20)\d{2}` — extracts exam year from date string
2. If `体检日期` is blank for a row, fall back to the `年度` column

**Deduplication:**
One row per `(patient_id, year)` pair. If a patient has multiple visits in the same year, they count as one patient for that year's statistics.

**Stroke flag:**
A patient is counted as a stroke case if their `person_id` appears in `patient_reference.csv` with `in_stroke = Y`. This is a prevalent case definition — the flag applies regardless of which year the patient appears in the health-check data.

---

## Table 0 — Stroke patient source overlap

Shows how many of the stroke registry patients appear in each health-check data source (2018, 2022, 2023, 2024):

```
  Source file    Stroke pts present    % of stroke pts    % of source pts
  ────────────   ──────────────────    ───────────────    ───────────────
  2018                          XXX             XX.X%             XX.X%
  2022                          XXX             XX.X%             XX.X%
  ...
  Stroke-only (not in any health-check file): XXX (XX.X%)
```

This reveals how many stroke patients were ever captured in a routine health-check exam.

---

## Table 1 — Per exam-year statistics

For each distinct exam year in the fused dataset:

| Column | Description |
|--------|-------------|
| Year | Exam year (from 体检日期 or 年度) |
| Patients | Unique patients with exams in this year |
| Stroke cases | Patients also in stroke registry |
| Disease rate | `stroke_cases / patients × 100%` |

An "Overall" row shows totals counted across all years (each patient counted once even if they appear in multiple years).

---

## Table 2 — Patient span distribution

Groups patients by how many distinct exam years they participated in:

| Column | Description |
|--------|-------------|
| Years | Number of distinct exam years (1, 2, 3, or 4) |
| Patients | Count of patients with this many years |
| % of total | Fraction of all patients |
| Cumul% | Cumulative percentage |
| Stroke cases | Stroke registry patients in this group |
| Disease rate | Stroke rate for this group |

Patients who participated in more years tend to have different disease rates — this table allows comparison.

---

## Table 3 — Risk column distribution

For each of the two risk columns (`心电图_风险`, `胸片_风险`), shows the count and percentage of rows at each risk level across all visit rows (not patient-deduplicated):

| Level | Label | Count | % |
|-------|-------|-------|---|
| -1 | not examined | … | … |
| 0 | normal | … | … |
| 1 | low risk | … | … |
| 2 | high risk | … | … |
| NaN | (blank — pre-Step 6 run) | … | … |

This table uses all rows in `fused.csv` (not deduped by patient), so it reflects the per-visit distribution of exam outcomes.

---

## Running manually

```bash
python visit_stats.py
```

The script reads hardcoded paths (`/Users/haochen/Desktop/PatientTables/output/...`). It will exit with a clear error message if either input file is missing.

Runtime is typically 5–15 seconds depending on fused.csv size.
