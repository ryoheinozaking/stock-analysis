# 経営変化ボーナス分解診断 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax.

**Goal:** バリュー funda_score の経営変化ボーナスを成分分解し、各成分の単独 Rank IC と本番設定の leave-one-out α を出力する（測定のみ・funda_score 値は不変）。

**Architecture:** `calc_funda_score`（value 分岐）で各ボーナスを `bonus_*` 列として露出（合算は不変）。`diagnose_value_service` に IC factor 追加と leave-one-out variant（funda_score − bonus_X）を追加し、専用 CSV を出力。

**Tech Stack:** Python 3.9.1 / pandas / numpy / pytest。`.venv\Scripts\python.exe`。

Spec: [docs/superpowers/specs/2026-06-14-bonus-decomposition-diagnostic-design.md](../specs/2026-06-14-bonus-decomposition-diagnostic-design.md)

---

## File Structure

- `services/pipeline_service.py`（修正）— `calc_funda_score` value 分岐で `bonus_*`/`bonus_total`/`payout_in_band` 列を露出。
- `tests/test_bonus_decomposition.py`（新規）— 不変条件 + 成分点数 + variant 算術のテスト。
- `services/diagnose_value_service.py`（修正・untracked WIP）— `FACTORS_BONUS`、`_add_bonus_variants`、leave-one-out sweep、bonus CSV 出力、`run_diagnosis` 戻り辞書拡張。
- `diagnose_value.py`（修正・untracked WIP）— bonus CSV 保存・サマリー表示。

---

## Task 1: calc_funda_score でボーナス成分を列露出（TDD）

**Files:**
- Modify: `services/pipeline_service.py`（`calc_funda_score` value 分岐 357-399 付近）
- Test: `tests/test_bonus_decomposition.py`（新規）

- [ ] **Step 1: 失敗するテストを書く**

`tests/test_bonus_decomposition.py`:

```python
# -*- coding: utf-8 -*-
"""経営変化ボーナス成分の列露出テスト（funda_score 値は不変が最重要）。"""
import numpy as np
import pandas as pd

from services.pipeline_service import calc_funda_score


def _vrow(code_4, pbr, sales_fy, market_cap, **kw):
    row = {
        "code": code_4 + "0", "code_4": code_4,
        "PBR": pbr, "PER": 12.0, "ROE": 8.0, "op_margin": 5.0,
        "sales_fy": sales_fy, "market_cap": market_cap,
        "div_trend": 0, "op_trend": 0, "op_turnaround": False,
        "payout_ratio": np.nan,
    }
    row.update(kw)
    return row


def _score(rows):
    return calc_funda_score(pd.DataFrame(rows), mode="value")


def test_bonus_component_columns_exist():
    df = _score([_vrow("1000", 0.8, 1e11, 2e11, div_trend=2)])
    for col in ["bonus_activist", "bonus_div", "bonus_op",
                "bonus_turnaround", "bonus_payout", "bonus_total",
                "payout_in_band", "activist"]:
        assert col in df.columns


def test_bonus_total_is_sum_of_components():
    df = _score([_vrow("1000", 0.8, 1e11, 2e11,
                        div_trend=2, op_trend=1, op_turnaround=True,
                        payout_ratio=50.0)])
    r = df.iloc[0]
    assert r["bonus_total"] == (r["bonus_activist"] + r["bonus_div"]
                                + r["bonus_op"] + r["bonus_turnaround"]
                                + r["bonus_payout"])


def test_component_points_match_definition():
    df = _score([
        _vrow("1000", 0.8, 1e11, 2e11, div_trend=2),            # +10 div
        _vrow("1001", 0.8, 1e11, 2e11, op_turnaround=True),     # +15 turnaround
        _vrow("1002", 0.8, 1e11, 2e11, payout_ratio=50.0),      # +10 payout(40-70)
        _vrow("1003", 0.8, 1e11, 2e11, payout_ratio=30.0),      # +5 payout(25-40)
        _vrow("1004", 0.8, 1e11, 2e11, payout_ratio=80.0),      # 0 payout(>70)
    ]).set_index("code_4")
    assert df.loc["1000", "bonus_div"] == 10.0
    assert df.loc["1001", "bonus_turnaround"] == 15.0
    assert df.loc["1002", "bonus_payout"] == 10.0
    assert df.loc["1002", "payout_in_band"] == 1.0
    assert df.loc["1003", "bonus_payout"] == 5.0
    assert df.loc["1004", "bonus_payout"] == 0.0
    assert df.loc["1004", "payout_in_band"] == 0.0


def test_funda_score_unchanged_equals_core_plus_bonus_total():
    # funda_score == (コア percentile 部分) + bonus_total。
    # bonus_total を引いた値が、ボーナス無し行の funda_score と一致することで不変を確認。
    rows = [
        _vrow("1000", 0.8, 1e11, 2e11, div_trend=2, payout_ratio=50.0),
        _vrow("1001", 0.8, 1e11, 2e11),  # 同条件でボーナス無し
    ]
    df = _score(rows).set_index("code_4")
    # PBR/PER/ROE/op_margin/sales/market_cap が同一 → コア percentile は同一
    core_with    = df.loc["1000", "funda_score"] - df.loc["1000", "bonus_total"]
    core_without = df.loc["1001", "funda_score"] - df.loc["1001", "bonus_total"]
    assert core_with == core_without
```

- [ ] **Step 2: 失敗確認**

Run: `.venv\Scripts\python.exe -m pytest tests/test_bonus_decomposition.py -v`
Expected: FAIL（`bonus_activist` 等の列が無い → KeyError/AssertionError）

- [ ] **Step 3: calc_funda_score の value 分岐を改修**

`services/pipeline_service.py` の `if mode == "value":` ブロック（現状 357-399 付近）を次に置き換える。**合算結果（funda_score 値）は不変**、各成分を列に保存する点だけが変更:

```python
    if mode == "value":
        # ── 経営変化シグナル群（バリュー戦略の真の α 源）
        # 「PBR 改善 = 利益改善 × 経営の意志」の後者を捕捉。
        # 各ボーナスを列として保存してから合算する（funda_score 値は不変）。

        # 全ボーナス列を 0 初期化（code_4 や各信号列が無くても bonus_total が成立）
        df["bonus_activist"]   = 0.0
        df["activist"]         = False
        df["bonus_div"]        = 0.0
        df["bonus_op"]         = 0.0
        df["bonus_turnaround"] = 0.0
        df["bonus_payout"]     = 0.0
        df["payout_in_band"]   = 0.0

        # アクティビスト保有（+10pt / look-ahead: 現在スナップショット）
        if "code_4" in df.columns:
            governance = calc_governance_score_for_df(df, code_4_col="code_4")
            df["activist"]       = governance > 0
            df["bonus_activist"] = governance.astype(float)

        # 増配トレンド（+5pt/1期, +10pt/2期連続）
        if "div_trend" in df.columns:
            df["bonus_div"] = df["div_trend"].fillna(0).clip(0, 2) * 5.0

        # 営業益トレンド（+5pt/1期増, +10pt/2期連続増）
        if "op_trend" in df.columns:
            df["bonus_op"] = df["op_trend"].fillna(0).clip(0, 2) * 5.0

        # V字転換（前期減益→今期回復: +15pt）
        if "op_turnaround" in df.columns:
            df["bonus_turnaround"] = df["op_turnaround"].eq(True).astype(float) * 15.0

        # 配当性向（段階: 40-70%→+10 / 25-40%→+5 / それ以外→0）
        if "payout_ratio" in df.columns:
            pr = df["payout_ratio"].fillna(-1)
            df["bonus_payout"] = (((pr >= 40) & (pr <= 70)).astype(float) * 10.0
                                  + ((pr >= 25) & (pr < 40)).astype(float) * 5.0)
            df["payout_in_band"] = ((pr >= 25) & (pr <= 70)).astype(float)

        df["bonus_total"] = (df["bonus_activist"] + df["bonus_div"] + df["bonus_op"]
                             + df["bonus_turnaround"] + df["bonus_payout"])
        score = score + df["bonus_total"]

    df["funda_score"] = score.round(2)
    return df
```

- [ ] **Step 4: テスト緑を確認**

Run: `.venv\Scripts\python.exe -m pytest tests/test_bonus_decomposition.py -v`
Expected: PASS（4 件）

- [ ] **Step 5: 既存テストの非退行を確認**

Run: `.venv\Scripts\python.exe -m pytest tests/ -q`
Expected: PASS（既存 38 + 新規 4 = 42 件）

- [ ] **Step 6: コミット（クリーンな追跡ファイルのみ）**

```bash
git add services/pipeline_service.py tests/test_bonus_decomposition.py
git commit -m "feat(pipeline): バリュー経営変化ボーナスを bonus_* 列として露出（funda_score 値は不変）"
```

---

## Task 2: IC 分解 factor の追加（diagnose_value_service）

**Files:**
- Modify: `services/diagnose_value_service.py`（FACTORS 定義 44-53 / calc_rank_ic factor リスト / run_diagnosis 戻り）

- [ ] **Step 1: FACTORS_BONUS を定義し FACTORS_ALL に含める**

`services/diagnose_value_service.py` の FACTORS 定義（44-53 付近）の直後に追加:

```python
# 経営変化ボーナス成分（#1 分解診断）。
# div_trend(0/1/2) 等は適用点(bonus_*)と単調 → raw 信号で代表。
# payout は非単調なので連続値とバンド指標の両方。activist は look-ahead。
FACTORS_BONUS = [
    "div_trend", "op_trend", "op_turnaround",
    "payout_ratio", "payout_in_band",
    "bonus_total",
    "activist",          # look-ahead（出力で明記）
]
```

そして `FACTORS_ALL` の定義を変更:

```python
FACTORS_ALL   = FACTORS_FUNDA + FACTORS_SCORE + FACTORS_BONUS
```

`calc_rank_ic` は `factors is None → FACTORS_ALL` を使い、`scored` に無い factor は NaN スキップするため、これだけでボーナス成分の IC が ic_by_factor に乗る。

- [ ] **Step 2: bonus_ic 用の look-ahead フラグ付き出力を run_diagnosis に追加**

`services/diagnose_value_service.py` の `run_diagnosis` 内、`ic_df = calc_rank_ic(snapshot_results)`（736 付近）の直後に追加:

```python
    # ボーナス成分のみ抽出した IC 表（look-ahead フラグ付き）
    bonus_ic_df = ic_df[ic_df["factor"].isin(FACTORS_BONUS)].copy()
    bonus_ic_df["look_ahead"] = bonus_ic_df["factor"].eq("activist")
```

- [ ] **Step 3: 構文チェック**

Run: `.venv\Scripts\python.exe -c "import services.diagnose_value_service; print('OK')"`
Expected: `OK`

---

## Task 3: leave-one-out α と CSV 出力（diagnose_value_service + diagnose_value.py）

**Files:**
- Modify: `services/diagnose_value_service.py`（`_add_bonus_variants`、LOO sweep、戻り辞書、CSV 保存）
- Modify: `diagnose_value.py`（サマリー表示）
- Test: `tests/test_bonus_decomposition.py`（variant 算術）

- [ ] **Step 1: variant 算術のテストを追加**

`tests/test_bonus_decomposition.py` に追加:

```python
def test_bonus_variant_subtraction():
    from services.diagnose_value_service import _add_bonus_variants
    scored = pd.DataFrame([{
        "funda_score": 100.0, "bonus_activist": 10.0, "bonus_div": 5.0,
        "bonus_op": 0.0, "bonus_turnaround": 15.0, "bonus_payout": 10.0,
        "bonus_total": 40.0,
    }])
    _add_bonus_variants([{"scored": scored, "as_of": "2024-01-31"}])
    r = scored.iloc[0]
    assert r["funda_no_activist"]   == 90.0
    assert r["funda_no_turnaround"] == 85.0
    assert r["funda_no_bonus"]      == 60.0
```

- [ ] **Step 2: 失敗確認**

Run: `.venv\Scripts\python.exe -m pytest tests/test_bonus_decomposition.py::test_bonus_variant_subtraction -v`
Expected: FAIL（`_add_bonus_variants` 未定義）

- [ ] **Step 3: `_add_bonus_variants` を実装**

`services/diagnose_value_service.py` の `_add_value_funda_v2`（535 付近）の直後に追加:

```python
LOO_VARIANTS = [
    "funda_score", "funda_no_activist", "funda_no_div", "funda_no_op",
    "funda_no_turnaround", "funda_no_payout", "funda_no_bonus",
]


def _add_bonus_variants(snapshot_results: List[Dict]) -> None:
    """各スナップショットの scored に leave-one-out variant 列を追加（破壊的）。
    funda_no_X = funda_score - bonus_X。bonus 列が無いスナップショットはスキップ。"""
    for r in snapshot_results:
        if "scored" not in r:
            continue
        df = r["scored"]
        if "bonus_total" not in df.columns or "funda_score" not in df.columns:
            continue
        f = df["funda_score"]
        df["funda_no_activist"]   = f - df["bonus_activist"]
        df["funda_no_div"]        = f - df["bonus_div"]
        df["funda_no_op"]         = f - df["bonus_op"]
        df["funda_no_turnaround"] = f - df["bonus_turnaround"]
        df["funda_no_payout"]     = f - df["bonus_payout"]
        df["funda_no_bonus"]      = f - df["bonus_total"]
```

- [ ] **Step 4: テスト緑を確認**

Run: `.venv\Scripts\python.exe -m pytest tests/test_bonus_decomposition.py -v`
Expected: PASS（5 件）

- [ ] **Step 5: run_diagnosis に LOO sweep と CSV 保存を追加**

`services/diagnose_value_service.py` の `run_diagnosis`、`_add_value_funda_v2(snapshot_results)`（733 付近）の直後に追加:

```python
    _cb("経営変化ボーナス variant 計算中...")
    _add_bonus_variants(snapshot_results)
```

`variant_sweep_df = sweep_funda_variants(...)`（748 付近）の直後に追加:

```python
    _cb("leave-one-out α 計算中...")
    loo_df = sweep_funda_variants(
        snapshot_results, prices_df, forward_days,
        variants=LOO_VARIANTS, weights=[(0.6, 0.4)], top_ns=[20],
    )
    # full との α 差分（= 各ボーナスの限界寄与）
    full_alpha = loo_df.loc[loo_df["variant"] == "funda_score", "alpha"]
    full_a = float(full_alpha.iloc[0]) if len(full_alpha) else np.nan
    loo_df["alpha_drop_vs_full"] = full_a - loo_df["alpha"]
    loo_df["look_ahead"] = loo_df["variant"].eq("funda_no_activist")
```

そして CSV 保存箇所（`run_diagnosis` 末尾で各 df を `to_csv` している箇所）に 2 本追加。保存箇所を特定するため、既存の `.to_csv` 行の並びに合わせて以下を追記:

```python
    bonus_ic_df.to_csv(os.path.join(_OUT_DIR, "bonus_ic.csv"), index=False)
    loo_df.to_csv(os.path.join(_OUT_DIR, "bonus_leave_one_out.csv"), index=False)
```

戻り辞書（`return {...}`）に追加:

```python
        "bonus_ic":            bonus_ic_df,
        "bonus_leave_one_out": loo_df,
```

- [ ] **Step 6: diagnose_value.py にサマリー表示を追加**

`diagnose_value.py` の `print_summary` 末尾（CSV 保存先表示の前）に追加:

```python
    bonus_ic = result.get("bonus_ic")
    if bonus_ic is not None and not bonus_ic.empty:
        print("\n" + "-" * 78)
        print("  [経営変化ボーナス 成分別 Rank IC]  (activist は look-ahead)")
        print("-" * 78)
        for _, r in bonus_ic.iterrows():
            la = "  <- look-ahead" if r.get("look_ahead") else ""
            print(f"  {r['factor']:<16} IC={r['mean_ic']:+.4f}  "
                  f"t={r['t_stat']}  pos%={r['pos_pct']}{la}")

    loo = result.get("bonus_leave_one_out")
    if loo is not None and not loo.empty:
        print("\n" + "-" * 78)
        print("  [leave-one-out α (funda0.6/tech0.4 × Top20)]  α低下=ボーナスの限界寄与")
        print("-" * 78)
        for _, r in loo.iterrows():
            la = "  <- look-ahead" if r.get("look_ahead") else ""
            drop = r.get("alpha_drop_vs_full")
            drop_s = f"  Δ={drop:+.2f}" if drop is not None and pd.notna(drop) else ""
            print(f"  {r['variant']:<20} α={r['alpha']}  univ_α={r['universe_alpha']}{drop_s}{la}")
```

- [ ] **Step 7: 既存テスト非退行 + 構文チェック**

Run: `.venv\Scripts\python.exe -m pytest tests/ -q`
Expected: PASS（42 件）
Run: `.venv\Scripts\python.exe -c "import diagnose_value, services.diagnose_value_service; print('OK')"`
Expected: `OK`

- [ ] **Step 8: 実データで診断実行（fwd250）→ 結果確認**

Run: `.venv\Scripts\python.exe diagnose_value.py --fwd 250 --top 20`
Expected: 正常終了。`data/diagnose_value/bonus_ic.csv` と `bonus_leave_one_out.csv` が生成され、サマリーに成分別 IC と leave-one-out α が表示される。

---

## Self-Review

**1. Spec coverage:**
- bonus_* 列露出（funda_score 不変）→ Task 1。✓
- IC 分解（div_trend/op_trend/op_turnaround/payout_ratio/payout_in_band/bonus_total/activist、fwd250+fwd60）→ Task 2（factor 追加。fwd は実行時引数）。✓
- activist look-ahead 明記 → Task 2 Step 2（bonus_ic の look_ahead 列）+ Task 3（loo の look_ahead 列）+ サマリー表示。✓
- leave-one-out α（本番 0.6/0.4×Top20、full との差分）→ Task 3 Step 5。✓
- 専用 CSV（bonus_ic.csv / bonus_leave_one_out.csv）→ Task 3 Step 5。✓
- テスト（不変条件・成分点数・variant 算術）→ Task 1 + Task 3 Step 1。✓

**2. Placeholder scan:** TBD/TODO 無し。全コードステップに実コード。CSV 保存箇所のみ「既存の to_csv の並びに合わせて追記」と指示（実装時に既存行を確認して挿入）。

**3. Type consistency:**
- `bonus_activist/div/op/turnaround/payout`・`bonus_total`・`payout_in_band`・`activist` の列名は Task 1 で定義し Task 2/3 で参照 — 一致。✓
- `_add_bonus_variants` が作る `funda_no_*` 列名と `LOO_VARIANTS` リストが一致。✓
- `sweep_funda_variants(variants=, weights=, top_ns=)` は既存シグネチャ準拠。✓
- `loo_df` の列 `variant`/`alpha`/`universe_alpha` は `sweep_funda_variants` の出力列に存在。✓
