# PatientTables

Annual health-check × stroke-registry data pipeline.

Reads raw Excel/CSV files, assigns unique patient IDs across all years, processes lab results, classifies ECG/X-ray risk, fills missing values, and produces analysis-ready fused datasets.

---

## Quick start

```bash
bash 01_install.sh          # install Python dependencies (first time only)
export ANTHROPIC_API_KEY=sk-ant-...
bash 02_run.sh              # run the full 6-step pipeline
python visit_stats.py       # generate visit statistics (also called by 02_run.sh)
```

Input files must sit in `/Users/haochen/Desktop/PatientTables/`:

| File | Contents |
|------|----------|
| `2018.xlsx` | Annual health-check data, 2018 cohort |
| `2022-2024.xlsx` | Stroke registry (sheet: 脑卒中) |
| `2022.csv` / `2023.csv` / `2024.csv` | Annual health-check data, 2022–2024 cohorts (GBK encoded) |

All output lands in `/Users/haochen/Desktop/PatientTables/output/`.

---

## Pipeline steps

| Step | Script | What it does |
|------|--------|--------------|
| 1 | `main_2018.py` | Build golden patient reference from 2018.xlsx + stroke registry |
| 2 | `main.py` | Match 2022/2023/2024 records against golden IDs |
| 3 | `main2.py` | Lab triplet processing (Claude API) + all feature transforms |
| 4 | `step2_fuse_clean_files.py` | Fuse all clean files into one dataset |
| 5 | `step3_risk_classify.py` | Classify ECG & chest X-ray text into risk scores |
| 6 | `step4_fill_missing.py` | Fill NaN sentinels + encode categorical columns |
| — | `visit_stats.py` | Compute visit/disease statistics on the fused dataset |

---

## Documentation

Detailed documentation for each step lives in [`docs/`](docs/):

- [`docs/01_overview.md`](docs/01_overview.md) — Data flow diagram and directory layout
- [`docs/02_input_data.md`](docs/02_input_data.md) — Input file schemas and encoding quirks
- [`docs/03_step1_golden_reference.md`](docs/03_step1_golden_reference.md) — `main_2018.py`: building the golden patient reference
- [`docs/04_step2_id_assignment.md`](docs/04_step2_id_assignment.md) — `main.py`: matching 2022/23/24 against golden IDs
- [`docs/05_step3_lab_processing.md`](docs/05_step3_lab_processing.md) — `main2.py`: lab triplet processing and feature transforms
- [`docs/06_step4_fuse.md`](docs/06_step4_fuse.md) — `step2_fuse_clean_files.py`: fusing and analysing
- [`docs/07_step5_risk_classify.md`](docs/07_step5_risk_classify.md) — `step3_risk_classify.py`: ECG/X-ray risk classification
- [`docs/08_step6_fill_missing.md`](docs/08_step6_fill_missing.md) — `step4_fill_missing.py`: filling missing values
- [`docs/09_output_files.md`](docs/09_output_files.md) — All output files, columns, and formats
- [`docs/10_visit_stats.md`](docs/10_visit_stats.md) — `visit_stats.py`: visit statistics tables
