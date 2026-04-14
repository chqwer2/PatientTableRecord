# Step 6 — Fill Missing Values (`step4_fill_missing.py`)

**Run order:** Sixth (last) — reads `analysis/fused.csv`, writes `analysis/fused_filled.csv`.

**Script:** `step4_fill_missing.py`

**Purpose:** Replace NaN sentinels with meaningful numeric values and encode categorical columns so that `fused_filled.csv` can be used directly in machine learning pipelines without further preprocessing.

**Note:** `fused.csv` is left untouched. `fused_filled.csv` is a separate output file.

---

## What it produces

| Output file | Description |
|-------------|-------------|
| `analysis/fused_filled.csv` | fused.csv with NaN filled + categoricals encoded |
| `analysis/fill_report.txt` | Per-column summary of how many cells were filled/encoded |

---

## Fill rules

### Lab `_值` columns (normalised numeric results)

All columns ending in `_值` that contain lab test results:

```
NaN → -1
```

`-1` is the sentinel for "test not performed". Because valid normalised values are in the range `[0, 1]` (or slightly outside for out-of-range results), `-1` is unambiguously distinguishable from real data.

### Risk columns (`心电图_风险`, `胸片_风险`)

These were assigned `2/1/0` or `NaN` by Step 5. The fill rule depends on whether any associated exam text exists:

```
NaN + paired text column has content → 0  (exam done but text unclassified → treated as normal)
NaN + paired text column also blank  → -1  (exam not performed)
```

| Value | Final meaning |
|-------|--------------|
| `2` | High-risk finding |
| `1` | Low-risk finding |
| `0` | Exam done; no significant finding (or unclassified finding) |
| `-1` | Exam not performed |

### Exam flag columns (`心电图`, `胸部X片`, `腹部B超`)

```
Paired 异常说明 text column has content → "异常"   (exam done and abnormal)
Otherwise                               → "未检查"  (not performed or no finding)
```

These string values are then encoded numerically (see Encoding section below).

### `足背动脉搏动`

```
NaN → "未检查"
```

Then encoded as ordinal (see below).

### Physical measurements

Columns: `体温`, `脉率`, `呼吸频率`, `血压`, `身高`, `体重`, `腰围`, `BMI`, `视力`, `心脏心率`

```
NaN → -1
```

---

## Categorical encoding

Applied after all fill rules.

### 性别

| Raw value | Encoded |
|-----------|---------|
| `男` | `1` |
| `女` | `0` |

### Exam flag columns (`心电图`, `胸部X片`, `腹部B超`)

| Raw value | Encoded |
|-----------|---------|
| `异常` | `1` |
| `未检` / `未检查` | `-1` |

### `足背动脉搏动` — ordinal severity

| Raw value | Encoded | Meaning |
|-----------|---------|---------|
| `触及双侧对称` | `0` | Normal |
| Unilateral weakened (e.g., `触及左侧弱`, `右侧搏动减弱`) | `1` | Mild |
| Unilateral absent OR bilateral weakened (`左侧搏动消失`, `双侧搏动减弱`) | `2` | Moderate |
| `双侧搏动消失` | `3` | Severe |
| `未检查` / not recorded | `-1` | Not examined |

---

## fill_report.txt

The report lists, for every column that was modified:
- Number of NaN cells filled with `-1` (or other sentinel)
- Number of cells encoded (categorical → numeric)
- Final value distribution after fill

This allows verification that the fill logic applied correctly and shows how much missingness exists per column.

---

## Choosing between fused.csv and fused_filled.csv

| Use case | Recommended file |
|----------|-----------------|
| Statistical analysis, descriptive tables, manual review | `fused.csv` (NaN = genuine missing) |
| Machine learning, gradient boosted trees, sklearn pipelines | `fused_filled.csv` (no NaN; all numeric) |
| Visit statistics (`visit_stats.py`) | `fused.csv` |
