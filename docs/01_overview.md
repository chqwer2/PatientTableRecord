# Pipeline Overview

## Data flow

```
INPUT FILES
  2018.xlsx              ─┐
  2022-2024.xlsx           ├─ main_2018.py ──► patient_reference.csv (golden IDs)
  (stroke registry)      ─┘                    2018_origin.csv
                                                2018_intermediate.csv
                                                stroke_intermediate.csv

  2022.csv  ─┐
  2023.csv   ├─ main.py ──► 2022/23/24_origin.csv
  2024.csv  ─┘               2022/23/24_intermediate.csv
                              patient_reference.csv (updated with in_2022/23/24 flags)

  output/intermediate/ ──► main2.py ──► output/clean/*_clean.csv
   (raw triplet cols)       (Claude API)  (lab _值 cols, one-hot tags, normalised)

  output/clean/ ──► step2_fuse_clean_files.py ──► analysis/fused.csv
                                                    analysis/dropped_rows.csv
                                                    analysis/text_summaries/
                                                    analysis/outlier_report.txt

  analysis/fused.csv ──► step3_risk_classify.py ──► fused.csv (+心电图_风险 +胸片_风险)

  analysis/fused.csv ──► step4_fill_missing.py ──► analysis/fused_filled.csv
                                                    analysis/fill_report.txt

  analysis/fused.csv ──► visit_stats.py ──► analysis/stats_per_year.csv
  reference/patient_reference.csv             analysis/stats_patient_span.csv
                                              analysis/stats_summary.txt
```

---

## Output directory layout

```
/Users/haochen/Desktop/PatientTables/output/
│
├── origin/                      Raw data — blank rows/cols removed, no other changes
│   ├── 2018_origin.csv
│   ├── stroke_origin.csv
│   ├── 2022_origin.csv
│   ├── 2023_origin.csv
│   └── 2024_origin.csv
│
├── intermediate/                person_id added; names dropped; raw triplet cols intact
│   ├── 2018_intermediate.csv
│   ├── stroke_intermediate.csv
│   ├── 2022_intermediate.csv
│   ├── 2023_intermediate.csv
│   └── 2024_intermediate.csv
│
├── clean/                       Fully processed — lab _值 features, one-hot tags, normals blanked
│   ├── 2018_clean.csv
│   ├── stroke_clean.csv
│   ├── 2022_clean.csv
│   ├── 2023_clean.csv
│   └── 2024_clean.csv
│
├── reference/
│   └── patient_reference.csv    Master patient index (person_id ↔ name/gender/DOB/flags)
│
└── analysis/
    ├── fused.csv                 All years fused (blanks preserved; + risk columns)
    ├── fused_filled.csv          fused.csv with NaN → sentinels + categorical encoding
    ├── dropped_rows.csv          Rows dropped because >70% fields were vacant
    ├── outlier_report.txt        Visit distribution, column outliers
    ├── fill_report.txt           Per-column fill/encode summary
    ├── stats_per_year.csv        Unique patients + stroke cases per exam year
    ├── stats_patient_span.csv    How many patients appear in 1/2/3/4 years
    ├── stats_summary.txt         Full printed visit statistics
    └── text_summaries/
        ├── 心电图描述.txt
        ├── 胸部X线片-异常说明.txt
        └── 腹部B超-异常说明.txt
```

---

## Key design decisions

| Decision | Reason |
|----------|--------|
| Golden reference built from 2018 + stroke first | These two sources have exact DOBs (from ID card / birth certificate), giving the most reliable patient identity anchors |
| Match key = name + gender + birth year (±1) | No patient number exists across all files; name collision is handled by birth-year tolerance |
| Lab triplet → single normalised value | Three columns (value / flag / reference range) are redundant; interval normalisation makes values comparable across patients with different reference ranges |
| Blanks preserved in `fused.csv` | Downstream tasks may need to distinguish "exam not performed" from "exam performed, result normal" |
| Separate `fused_filled.csv` | ML pipelines need numeric sentinels (-1); analysis scripts may prefer NaN — both are served |
