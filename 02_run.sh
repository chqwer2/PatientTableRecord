
export ANTHROPIC_API_KEY=sxxx

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "============================================================"
echo "  PatientTables Pipeline"
echo "============================================================"

# ── Step 1: Build golden reference from 2018.xlsx + stroke ───────────────────
echo ""
echo "[1/6] main_2018.py — Building golden patient reference (2018 + stroke)…"
python3 main_2018.py

# ── Step 2: Match 2022/2023/2024 against golden reference ────────────────────
echo ""
echo "[2/6] main.py — Matching 2022/2023/2024 CSVs against golden patient IDs…"
python3 main.py

# ── Step 3: Lab triplet processing (API required) ────────────────────────────
echo ""
echo "[3/6] main2.py — Lab triplet processing (Claude API)…"
if [ -z "$ANTHROPIC_API_KEY" ]; then
    echo "  ERROR: ANTHROPIC_API_KEY is not set."
    echo "  Run:  export ANTHROPIC_API_KEY=sk-ant-..."
    exit 1
fi
python3 main2.py

# ── Step 4: Fuse + analyse ────────────────────────────────────────────────────
echo ""
echo "[4/6] step2_fuse_clean_files.py — Fusing and analysing clean files…"
python3 step2_fuse_clean_files.py

# ── Step 5: Risk classification (ECG + chest X-ray) ──────────────────────────
echo ""
echo "[5/6] step3_risk_classify.py — ECG & chest X-ray risk classification…"
python3 step3_risk_classify.py

# ── Step 6: Fill missing values ───────────────────────────────────────────────
echo ""
echo "[6/6] step4_fill_missing.py — Filling missing values in fused.csv…"
python3 step4_fill_missing.py

echo ""
echo "============================================================"
echo "  All steps complete."
echo "  Outputs in: /Users/haochen/Desktop/PatientTables/output/"
echo ""
echo "    origin/                    raw source files (blank rows/cols removed)"
echo "    intermediate/              person_id added, raw triplet columns intact"
echo "    clean/                     fully processed, _值 features"
echo "    reference/                 patient_reference.csv (golden patient IDs)"
echo "    analysis/fused.csv         fused dataset (original, blanks preserved)"
echo "    analysis/fused_filled.csv  fused dataset with blanks filled"
echo "    analysis/fused.csv         + 心电图_风险 column (-1/0/1/2)"
echo "    analysis/fused.csv         + 胸片_风险 column   (-1/0/1/2)"
echo "    analysis/stats_per_year.csv       unique patients per year"
echo "    analysis/stats_patient_span.csv   years-per-patient distribution"
echo "    analysis/outlier_report.txt       outlier diagnostics"
echo "    analysis/text_summaries/          term frequencies per text column"
echo "    analysis/fill_report.txt          cells filled per column"
echo "============================================================"

python visit_stats.py


