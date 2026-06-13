# バリュー深層分析動線「経営変化 × 出遅れ 発掘」Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** バリューモードの tab2 を、常にゼロ件になる「SEPA2絞り込み」から「経営変化（活動家/増配/配当性向）× RSI≤60」の深層分析発掘網に作り替える。

**Architecture:** 候補抽出を `services/pipeline_service.py` の純関数 `select_value_discovery_candidates(scored)` に切り出してユニットテストし、`pages/7_pipeline_report.py` の tab2 を mode で分岐させる（value=新動線 / growth=既存SEPA2を関数化して挙動不変）。配点・ハードフィルタ・Top20選定・バックテストは一切変更しない。

**Tech Stack:** Python 3.9.1 / pandas / numpy / pytest / Streamlit。`.venv\Scripts\python.exe` を使用。

設計スペック: [docs/superpowers/specs/2026-06-14-value-governance-discovery-funnel-design.md](../specs/2026-06-14-value-governance-discovery-funnel-design.md)

---

## File Structure

- `services/pipeline_service.py`（修正）
  - `calc_funda_score` の value 分岐に `df["activist"] = governance > 0` を1行追加（テスト可能な列の露出）。
  - `select_value_discovery_candidates(scored, ...)` 純関数と `_extract_rsi` ヘルパーを新設（`select_top_candidates` の直後）。責務 = 経営変化×出遅れ候補の抽出のみ。
- `tests/test_value_discovery.py`（新規）
  - 抽出ロジックの回帰テスト。合成 DataFrame で経営変化OR・RSIゲート・参考枠・順序を検証。
- `pages/7_pipeline_report.py`（修正）
  - tab2 を mode 分岐。既存 SEPA2 ロジックを `_render_sepa2_funnel(...)` に切り出し（挙動不変）、`_render_value_discovery(...)` を新設。
  - UI 描画はユニットテスト対象外。抽出ロジックは Task 1 の純関数に委譲する。

---

## Task 1: 抽出ロジック純関数 + activist 列の露出（TDD）

**Files:**
- Modify: `services/pipeline_service.py`（`calc_funda_score` value 分岐 / `select_top_candidates` 直後に新関数）
- Test: `tests/test_value_discovery.py`（新規）

- [ ] **Step 1: 失敗するテストを書く**

`tests/test_value_discovery.py` を新規作成:

```python
# -*- coding: utf-8 -*-
"""
バリュー深層分析動線「経営変化 × 出遅れ 発掘」の候補抽出ロジック回帰テスト。

select_value_discovery_candidates(scored) は
- 経営変化シグナル（activist OR div_trend>=1 OR payout_ratio in [25,70]）あり
- かつ RSI<=60（欠損は含める）
を total_score 降順で main、RSI>60 を reference として返す。
利益モメンタムのみ（op_trend）は候補に入らない。
"""
import numpy as np
import pandas as pd

from services.pipeline_service import select_value_discovery_candidates


def _row(code_4, total_score, *, activist=False, div_trend=0,
         payout_ratio=np.nan, rsi=40.0, op_trend=0):
    return {
        "code_4": code_4,
        "company_name": f"会社{code_4}",
        "total_score": total_score,
        "activist": activist,
        "div_trend": div_trend,
        "payout_ratio": payout_ratio,
        "rsi": rsi,
        "op_trend": op_trend,
    }


def _scored(rows):
    return pd.DataFrame(rows)


def test_activist_row_is_main_candidate():
    main, ref = select_value_discovery_candidates(
        _scored([_row("1000", 80, activist=True, rsi=40)]))
    assert list(main["code_4"]) == ["1000"]
    assert ref.empty


def test_div_trend_row_is_candidate():
    main, _ = select_value_discovery_candidates(
        _scored([_row("1001", 70, div_trend=1, rsi=45)]))
    assert list(main["code_4"]) == ["1001"]


def test_payout_in_band_is_candidate_out_of_band_is_not():
    main, ref = select_value_discovery_candidates(_scored([
        _row("1002", 70, payout_ratio=50, rsi=40),   # 50% → 候補
        _row("1003", 65, payout_ratio=80, rsi=40),   # 80% → 還元姿勢とみなさない
    ]))
    assert list(main["code_4"]) == ["1002"]
    assert ref.empty


def test_no_signal_row_excluded():
    main, ref = select_value_discovery_candidates(
        _scored([_row("1004", 90, rsi=40)]))   # シグナル無し
    assert main.empty and ref.empty


def test_profit_momentum_only_excluded():
    # op_trend のみ（経営の意志シグナル無し）は候補に入らない（思想の確認）
    main, ref = select_value_discovery_candidates(
        _scored([_row("1005", 90, op_trend=2, rsi=40)]))
    assert main.empty and ref.empty


def test_overheated_signal_goes_to_reference():
    main, ref = select_value_discovery_candidates(
        _scored([_row("1006", 80, activist=True, rsi=72)]))
    assert main.empty
    assert list(ref["code_4"]) == ["1006"]


def test_missing_rsi_is_included_in_main():
    main, _ = select_value_discovery_candidates(
        _scored([_row("1007", 80, activist=True, rsi=np.nan)]))
    assert list(main["code_4"]) == ["1007"]


def test_main_sorted_by_total_score_desc():
    main, _ = select_value_discovery_candidates(_scored([
        _row("1008", 70, activist=True, rsi=40),
        _row("1009", 85, activist=True, rsi=40),
        _row("1010", 78, div_trend=2, rsi=40),
    ]))
    assert list(main["code_4"]) == ["1009", "1010", "1008"]


def test_empty_input_returns_empty():
    main, ref = select_value_discovery_candidates(pd.DataFrame())
    assert main.empty and ref.empty
```

- [ ] **Step 2: テストが失敗することを確認**

Run: `.venv\Scripts\python.exe -m pytest tests/test_value_discovery.py -v`
Expected: FAIL（`ImportError: cannot import name 'select_value_discovery_candidates'`）

- [ ] **Step 3: 純関数を実装**

`services/pipeline_service.py` の `select_top_candidates`（677-678行付近で終わる関数）の**直後**に追加:

```python
def _extract_rsi(detail) -> float:
    """tech_detail dict から RSI を取り出す（無ければ NaN）。"""
    if isinstance(detail, dict):
        v = detail.get("rsi")
        return float(v) if v is not None else np.nan
    return np.nan


def select_value_discovery_candidates(
    scored: pd.DataFrame,
    top_n: int = 10,
    ref_n: int = 3,
    rsi_max: float = 60.0,
):
    """バリュー深層分析動線の「経営変化 × 出遅れ」候補を返す。

    経営変化シグナル（OR）= activist 保有 / div_trend>=1（増配）/
    payout_ratio in [25,70]（健全な還元姿勢）。
    過熱フィルタ = RSI<=rsi_max（RSI 欠損は除外しない＝main に含める）。

    Returns:
        (main_df, reference_df)
        main      : 経営変化あり AND (RSI<=rsi_max OR RSI欠損)、total_score 降順 top_n
        reference : 経営変化あり AND RSI>rsi_max（過熱気味で待ち）、total_score 降順 ref_n
    """
    if scored is None or scored.empty:
        empty = scored.iloc[0:0] if scored is not None else pd.DataFrame()
        return empty, empty

    df = scored.copy()
    idx = df.index

    activist = (df["activist"].fillna(False).astype(bool)
                if "activist" in df.columns else pd.Series(False, index=idx))
    div_up = (pd.to_numeric(df["div_trend"], errors="coerce").fillna(0) >= 1
              if "div_trend" in df.columns else pd.Series(False, index=idx))
    if "payout_ratio" in df.columns:
        payout = pd.to_numeric(df["payout_ratio"], errors="coerce")
        payout_ok = payout.between(25, 70).fillna(False)
    else:
        payout_ok = pd.Series(False, index=idx)

    gov = activist | div_up | payout_ok

    # RSI: 専用列があれば優先、無ければ tech_detail から取り出す
    if "rsi" in df.columns:
        rsi = pd.to_numeric(df["rsi"], errors="coerce")
    elif "tech_detail" in df.columns:
        rsi = pd.to_numeric(df["tech_detail"].apply(_extract_rsi), errors="coerce")
    else:
        rsi = pd.Series(np.nan, index=idx)

    not_overheated = rsi.isna() | (rsi <= rsi_max)

    cand = df[gov]
    main = (cand[not_overheated[gov]]
            .sort_values("total_score", ascending=False)
            .head(top_n).reset_index(drop=True))
    ref = (cand[~not_overheated[gov]]
           .sort_values("total_score", ascending=False)
           .head(ref_n).reset_index(drop=True))
    return main, ref
```

- [ ] **Step 4: テストが通ることを確認**

Run: `.venv\Scripts\python.exe -m pytest tests/test_value_discovery.py -v`
Expected: PASS（9 件）

- [ ] **Step 5: activist 列を calc_funda_score で露出**

`services/pipeline_service.py` の `calc_funda_score` value 分岐、現状:

```python
        if "code_4" in df.columns:
            governance = calc_governance_score_for_df(df, code_4_col="code_4")
            score = score + governance
```

を次に変更（`df["activist"]` の1行を追加）:

```python
        if "code_4" in df.columns:
            governance = calc_governance_score_for_df(df, code_4_col="code_4")
            df["activist"] = governance > 0          # 発掘動線フィルタ用の列（テスト可能化）
            score = score + governance
```

- [ ] **Step 6: 既存テストが壊れていないことを確認**

Run: `.venv\Scripts\python.exe -m pytest tests/ -v`
Expected: PASS（既存 21 件 + 新規 9 件 = 30 件）

- [ ] **Step 7: コミット**

```bash
git add services/pipeline_service.py tests/test_value_discovery.py
git commit -m "feat(pipeline): バリュー発掘の純関数 select_value_discovery_candidates と activist 列を追加"
```

---

## Task 2: tab2 を mode 分岐（value=新動線 / growth=現状維持）

**Files:**
- Modify: `pages/7_pipeline_report.py`（786-837行の tab2 ブロック）

- [ ] **Step 1: 既存 SEPA2 ロジックを関数に切り出す（挙動不変）**

`pages/7_pipeline_report.py` の `tab1, tab2 = st.tabs(...)`（786行付近）の**直前**に、既存ロジックを移植した関数を追加する。**現状の tab2 内（793-837行）のコードと同一挙動**にすること:

```python
def _render_sepa2_funnel(scored, ai_stocks, cached_mode):
    """成長株モード: SEPA Stage2 絞り込み + 飛躍/深層 動線（従来挙動）。"""
    if not scored.empty and "sepa_stage" in scored.columns:
        sepa2_df = (
            scored[scored["sepa_stage"] == 2]
            .sort_values("total_score", ascending=False)
            .head(10)
            .reset_index(drop=True)
        )
    else:
        sepa2_df = pd.DataFrame()

    if sepa2_df.empty:
        st.info("SEPA Stage2条件を満たす銘柄がありませんでした。")
        return

    sepa2_count = int((scored["sepa_stage"] == 2).sum()) if not scored.empty else 0
    st.subheader(f"SEPA2絞り込み TOP10（全{sepa2_count}件中）")
    st.caption("フィルタ通過銘柄のうち SEPA Stage2 を満たす銘柄をスコア順に表示。")
    _skill = "飛躍分析" if cached_mode == "growth" else "深層分析"
    _priority = (sepa2_df[sepa2_df["signal"] == "BUY"]
                 if "signal" in sepa2_df.columns else sepa2_df.head(0))
    if len(_priority) > 0:
        _lines = [
            f"### → 次にかけるべき分析（{_skill}推奨）",
            "パイプラインは**発掘**、最終判定は**二刀流**で。過熱していない優先精査（Stage2）を最優先候補として提示:",
        ]
        for _, _r in _priority.head(5).iterrows():
            _lines.append(
                f"- **`{_skill} {_r['code_4']}`** — {_r['company_name']}"
                f"（総合 {_r['total_score']:.0f} / Stage2 / 優先精査）"
            )
        _ref = sepa2_df[sepa2_df["signal"] != "BUY"].head(3)
        if len(_ref) > 0:
            _lines.append(
                "\n参考（Stage2 だが過熱等で監視中）: "
                + " / ".join(
                    f"{_r['code_4']} {_r['company_name']}"
                    for _, _r in _ref.iterrows()
                )
            )
        st.success("\n".join(_lines))
    else:
        st.info(f"現在、優先精査（過熱していない × Stage2）の {_skill} 推奨候補はありません。")
    for rank, row in enumerate(sepa2_df.itertuples(), 1):
        _render_scorecard(rank, row, ai_stocks, key_prefix="t2", mode=cached_mode)
```

- [ ] **Step 2: バリュー発掘の描画関数を追加**

同じく `tab1, tab2 = st.tabs(...)` の直前に、`select_value_discovery_candidates` を使う新関数を追加:

```python
def _render_value_discovery(scored, ai_stocks):
    """バリューモード: 経営変化（活動家/増配/配当性向）× RSI<=60 の深層分析発掘網。"""
    from services.pipeline_service import select_value_discovery_candidates

    main_df, ref_df = select_value_discovery_candidates(scored)

    if main_df.empty and ref_df.empty:
        st.info("経営変化シグナル（活動家保有/増配/配当性向25-70%）を持つ"
                "出遅れ（RSI≤60）銘柄が見つかりませんでした。")
        return

    st.subheader(f"経営変化×出遅れ 発掘（{len(main_df)}件）")
    st.caption("フィルタ通過全銘柄から「経営変化シグナルあり × 過熱していない（RSI≤60）」を"
               "スコア順に抽出。バリュートラップ判定を深層分析に委ねる発掘網。")

    def _sig_labels(r):
        labels = []
        if bool(getattr(r, "activist", False)):
            labels.append("アクティビスト保有")
        if int(getattr(r, "div_trend", 0) or 0) >= 2:
            labels.append("2期連続増配")
        elif int(getattr(r, "div_trend", 0) or 0) == 1:
            labels.append("増配")
        pr = getattr(r, "payout_ratio", None)
        if pr is not None and pd.notna(pr) and 25 <= pr <= 70:
            labels.append(f"配当性向{pr:.0f}%")
        return "、".join(labels) if labels else "経営変化"

    if not main_df.empty:
        _lines = [
            "### → 次にかけるべき分析（深層分析推奨）",
            "パイプラインは**発掘**、最終判定は**二刀流**で。割安×経営の意志を持つ出遅れ銘柄を"
            "バリュートラップ判定の最優先候補として提示:",
        ]
        for _r in main_df.head(5).itertuples():
            _lines.append(
                f"- **`深層分析 {_r.code_4}`** — {_r.company_name}"
                f"（総合 {_r.total_score:.0f} / {_sig_labels(_r)}）"
            )
        if not ref_df.empty:
            _lines.append(
                "\n参考（経営変化ありだが過熱気味・待ち）: "
                + " / ".join(
                    f"{_r.code_4} {_r.company_name}"
                    for _r in ref_df.itertuples()
                )
            )
        st.success("\n".join(_lines))

    for rank, row in enumerate(main_df.itertuples(), 1):
        _render_scorecard(rank, row, ai_stocks, key_prefix="t2v", mode="value")
```

- [ ] **Step 3: tab2 ラベルと中身を mode 分岐に置き換える**

現状の tab2 ブロック（786-837行）:

```python
tab1, tab2 = st.tabs(["スコア TOP10", "SEPA2絞り込み TOP10"])

with tab1:
    st.subheader(f"スコアランキング TOP{len(top10)}")
    for rank, row in enumerate(top10.itertuples(), 1):
        _render_scorecard(rank, row, ai_stocks, mode=cached_mode)

with tab2:
    # ...（793-837行の SEPA2 ロジック）...
```

を次に置き換える（tab1 は不変、tab2 を分岐に）:

```python
_tab2_label = "経営変化×出遅れ 発掘" if cached_mode == "value" else "SEPA2絞り込み TOP10"
tab1, tab2 = st.tabs(["スコア TOP10", _tab2_label])

with tab1:
    st.subheader(f"スコアランキング TOP{len(top10)}")
    for rank, row in enumerate(top10.itertuples(), 1):
        _render_scorecard(rank, row, ai_stocks, mode=cached_mode)

with tab2:
    if cached_mode == "value":
        _render_value_discovery(scored, ai_stocks)
    else:
        _render_sepa2_funnel(scored, ai_stocks, cached_mode)
```

- [ ] **Step 4: 構文チェック（import 可能か）**

Run: `.venv\Scripts\python.exe -c "import ast; ast.parse(open('pages/7_pipeline_report.py', encoding='utf-8').read()); print('OK')"`
Expected: `OK`

- [ ] **Step 5: 既存テストが壊れていないことを確認**

Run: `.venv\Scripts\python.exe -m pytest tests/ -q`
Expected: PASS（30 件）

- [ ] **Step 6: 手動確認（ユーザ操作・任意）**

`.venv\Scripts\streamlit.exe run app.py --server.port 8502` でパイプラインレポートを開き:
- バリューモード: tab2 ラベルが「経営変化×出遅れ 発掘」になり、`深層分析 <code>` 動線が1件以上出る。
- 成長株モード: tab2 が従来の「SEPA2絞り込み TOP10」と同一挙動。

- [ ] **Step 7: コミット**

```bash
git add pages/7_pipeline_report.py
git commit -m "feat(report): バリュー tab2 を経営変化×出遅れ発掘動線に作り替え（成長株は現状維持）"
```

---

## Self-Review

**1. Spec coverage:**
- スコープ（表示のみ・配点不変）→ Task 2 は tab2 のみ、Task 1 の activist 追加は calc_funda_score への1行のみ。✓
- 候補条件（activist OR 増配 OR 配当性向25-70、RSI≤60、欠損含む）→ Task 1 Step 3 + テスト。✓
- 利益モメンタム除外 → `test_profit_momentum_only_excluded`。✓
- 参考枠（RSI>60）→ `test_overheated_signal_goes_to_reference` + `_render_value_discovery`。✓
- total_score 降順 → `test_main_sorted_by_total_score_desc`。✓
- 成長株 tab2 現状維持 → `_render_sepa2_funnel` は既存コードの移植、`test` ではなく Step 6 手動確認 + 既存テスト不変で担保。✓
- activist 列の露出 → Task 1 Step 5。✓
- 受け入れ基準4（バックテスト差分なし）→ activist は value 分岐内の列追加のみ、`calc_total_score`/選定/バックテストは未変更。✓

**2. Placeholder scan:** TBD/TODO 無し。全ステップに実コード・実コマンド・期待出力あり。✓

**3. Type consistency:**
- `select_value_discovery_candidates(scored, top_n, ref_n, rsi_max)` の呼び出しは Task 2 で引数デフォルトのまま `select_value_discovery_candidates(scored)`。✓
- 返り値 `(main_df, ref_df)` を Task 2 で `main_df, ref_df = ...` で受ける。✓
- `_render_scorecard(... key_prefix=, mode=)` は既存シグネチャに準拠（既存呼び出しと同じ引数名）。✓
- `_sig_labels` は itertuples の row 属性（activist/div_trend/payout_ratio）を参照。これらは scored の列として存在（div_trend/payout_ratio は _build_fins_metrics 由来、activist は Task 1 Step 5 で追加）。✓
