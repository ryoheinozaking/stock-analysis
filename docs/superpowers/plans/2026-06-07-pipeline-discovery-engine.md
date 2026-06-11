# パイプライン発掘エンジン化 Phase 1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 成長株モードのテクニカルスコアに SEPA ステージを組み込んで Stage4 をランキング上位から降格し、シグナルを「分析優先度」に再定義して二刀流（飛躍/深層分析）への動線を出力に追加する。

**Architecture:** `services/pipeline_service.py` の `_tech_score_single`（配点再設計）・`calc_tech_scores`（sepa_stage 配線）・`calc_trade_signals`（Stage条件＋ラベル）を改修し、`pages/7_pipeline_report.py` に表示ラベルと動線ブロックを追加する。SEPA stage は `batch_service._calc_sepa` が算出して `stock_cache.parquet` の `sepa_stage` 列に保存済みのものを流用（新規のステージ判定は書かない）。バリュー株モード（mode='value'）は一切変更しない。

**Tech Stack:** Python 3.9.1 / pandas / numpy / Streamlit。テスト体制が無いため検証は `python -c` の直接呼び出し + `diagnose_growth.py` の実データ比較。

---

## 重要な前提（実装者は必ず読む）

- **ブランチ**: これは stock_analysis の実コード変更。`main` で直接実装せず、`git switch -c feature/pipeline-discovery-engine` で feature ブランチを切ってから着手する。
- **Python 3.9.1**: `X | Y` 型ヒント不可。`Optional[X]` を使う。
- **cp932**: `print` 等の出力に cp932 非互換文字（α・ギリシャ文字・特殊記号）を混ぜない。`α` は `alpha` と書く（diagnose の既存制約）。
- **バリューモード不変**: `_tech_score_single` / `calc_trade_signals` の `mode == "value"` 分岐は触らない。成長株モード（growth）のみ改修する。回帰確認は Task 5。
- **設計 spec**: `docs/superpowers/specs/2026-06-07-pipeline-discovery-engine-design.md`（git main、`c261efd`）。
- **検証の `python -c` は** `cd C:\Users\ryohei\stock_analysis` 後に `.venv/Scripts/python.exe -c "..."` で実行する。

## File Structure

- **Modify** `services/pipeline_service.py`:
  - `_sepa_stage_score`（新規ヘルパー）— SEPA stage を 0-30点に変換
  - `_tech_score_single` — growth モードの配点再設計、`sepa_stage` 引数追加
  - `calc_tech_scores` — filtered_df の `sepa_stage` を `_tech_score_single` に渡す配線
  - `calc_trade_signals` — growth の BUY 条件に Stage2/3 必須を追加
  - `SIGNAL_LABELS`（新規定数）— signal 値→分析優先度ラベルのマッピング
- **Modify** `pages/7_pipeline_report.py` — シグナル表示を「分析優先度」ラベルに、冒頭に「次にかけるべき分析」動線ブロックを追加

---

## Task 1: SEPA stage → スコア変換と _tech_score_single の配点再設計

**Files:**
- Modify: `services/pipeline_service.py`（`_tech_score_single` 周辺 387-578 行）

- [ ] **Step 1: SEPA stage→score ヘルパーを追加**

`_calc_macd` の直後（おおよそ 418 行の後）に追加:

```python
def _sepa_stage_score(sepa_stage) -> int:
    """SEPA ステージを成長株テクニカルスコアの SEPA 項（0-30点）に変換。
    Stage2(上昇)=30 / Stage3(天井)=15 / Stage1(基盤形成)=10 / Stage4(下降)=0 / 不明=0。"""
    if sepa_stage is None or (isinstance(sepa_stage, float) and pd.isna(sepa_stage)):
        return 0
    return {2: 30, 3: 15, 1: 10, 4: 0}.get(int(sepa_stage), 0)
```

- [ ] **Step 2: `_tech_score_single` のシグネチャに sepa_stage を追加**

`def _tech_score_single(cp: pd.DataFrame, mode: str = "growth") -> dict:` を以下に変更:

```python
def _tech_score_single(cp: pd.DataFrame, mode: str = "growth", sepa_stage=None) -> dict:
```

- [ ] **Step 3: growth モードの MA スコアを 30→20点に圧縮**

`_tech_score_single` の growth 分岐（488-497 行の `else:` ブロック）を以下に置換。**value 分岐（467-485 行）は変更しない**:

```python
    else:
        # 成長株: 短中期トレンド継続を重視（SEPA 項と重複するため 30→20 に圧縮）
        if pd.notna(ma25) and pd.notna(ma60):
            if latest_close > ma25 and ma25 > ma60:
                ma_score = 20
            elif latest_close > ma25 and pd.notna(ma25_prev5) and ma25 > ma25_prev5:
                ma_score = 13
            elif latest_close > ma60:
                ma_score = 7
        elif pd.notna(ma25):
            if latest_close > ma25:
                ma_score = 10
```

- [ ] **Step 4: growth モードの RSI/MACD/出来高/高値ブレイクを圧縮**

各 growth 分岐の配点を圧縮（**value 分岐は不変**）:

RSI growth（512-518 行の `else:` 内）:
```python
        else:
            # 成長株: トレンド継続（RSI 50-65 が最適）。20→15 に圧縮
            if 50 <= rsi <= 65:
                rsi_score = 15
            elif 40 <= rsi < 50:
                rsi_score = 8
            elif 65 < rsi <= 75:
                rsi_score = 4
```

MACD（523-531 行、mode 共通だが growth/value 両方に効く。配点を 20→15 に圧縮。**value も MACD は同配点だったため、value 回帰を避けるため mode 分岐を導入**）:
```python
    macd_score = 0
    if pd.notna(macd) and pd.notna(sig):
        golden_cross = (pd.notna(macd_p) and pd.notna(sig_p)
                        and macd_p < sig_p and macd >= sig)
        if mode == "value":
            if macd > 0 and macd > sig:
                macd_score = 20
            elif golden_cross:
                macd_score = 15
            elif macd > sig:
                macd_score = 10
        else:
            if macd > 0 and macd > sig:
                macd_score = 15
            elif golden_cross:
                macd_score = 11
            elif macd > sig:
                macd_score = 7
    score += macd_score
```

出来高 growth（547-553 行の `else:` 内、15→10 に圧縮）:
```python
    else:
        # 成長株: 当日出来高 ÷ 25日平均（ブレイク日の急増を評価）。15→10 に圧縮
        if vol_avg25 > 0:
            ratio = latest_vol / vol_avg25
            if ratio >= 1.5:
                vol_score = 10
            elif ratio >= 1.0:
                vol_score = 7
```

高値ブレイク（556-562 行、mode 共通。value 回帰を避けるため mode 分岐を導入し growth のみ 15→10 に圧縮）:
```python
    break_score = 0
    if mode == "value":
        if pd.notna(high60) and latest_close >= high60:
            break_score = 15
        elif pd.notna(high20) and latest_close >= high20:
            break_score = 10
    else:
        if pd.notna(high60) and latest_close >= high60:
            break_score = 10
        elif pd.notna(high20) and latest_close >= high20:
            break_score = 7
    score += break_score
```

- [ ] **Step 5: SEPA 項を growth モードのみ加点**

高値ブレイクスコア加算（`score += break_score`）の直後に追加:

```python
    # ── SEPA ステージスコア（30点・成長株モードのみ）
    sepa_score = 0
    if mode != "value":
        sepa_score = _sepa_stage_score(sepa_stage)
    score += sepa_score
```

- [ ] **Step 6: detail に sepa を記録**

`detail = { ... }` の `"break_score": break_score,` の後に追加:

```python
        "sepa_stage": int(sepa_stage) if (sepa_stage is not None and not (isinstance(sepa_stage, float) and pd.isna(sepa_stage))) else None,
        "sepa_score": sepa_score,
```

- [ ] **Step 7: 検証（growth 配点が 100 上限・value 不変）**

```bash
cd C:\Users\ryohei\stock_analysis
.venv/Scripts/python.exe -c "
from services.pipeline_service import _sepa_stage_score
assert _sepa_stage_score(2)==30 and _sepa_stage_score(3)==15 and _sepa_stage_score(1)==10 and _sepa_stage_score(4)==0
assert _sepa_stage_score(None)==0 and _sepa_stage_score(0)==0
print('sepa_stage_score OK')
"
```
Expected: `sepa_stage_score OK`

- [ ] **Step 8: Commit**

```bash
git add services/pipeline_service.py
git commit -m "feat(pipeline): 成長株テクニカルにSEPA30点を組み込み配点再設計（value不変）"
```

---

## Task 2: calc_tech_scores が sepa_stage を渡す配線

**Files:**
- Modify: `services/pipeline_service.py`（`calc_tech_scores` 581-596 行）

- [ ] **Step 1: filtered_df に sepa_stage 列があることを確認**

```bash
cd C:\Users\ryohei\stock_analysis
.venv/Scripts/python.exe -c "
import pandas as pd
df = pd.read_parquet('data/stock_cache.parquet')
print('sepa_stage' in df.columns, df['sepa_stage'].value_counts(dropna=False).to_dict() if 'sepa_stage' in df.columns else 'MISSING')
"
```
Expected: `True` と Stage 分布（1/2/3/4）。`False/MISSING` の場合は `_load_stock_cache`（114 行）が列を落としていないか確認し、落としていれば保持する。

- [ ] **Step 2: calc_tech_scores で sepa_stage を渡す**

`calc_tech_scores`（588-593 行のループ）を以下に置換:

```python
    results = []
    for _, row in filtered_df.iterrows():
        code = row["code"]   # 5桁コード
        cp   = prices_df[prices_df["Code"] == code]
        stage = row.get("sepa_stage")
        res  = (_tech_score_single(cp, mode=mode, sepa_stage=stage)
                if len(cp) >= 26 else {"tech_score": np.nan, "tech_detail": {}})
        res["code"] = code
        results.append(res)
```

- [ ] **Step 3: 検証（実データで growth テクニカルが動く）**

```bash
cd C:\Users\ryohei\stock_analysis
.venv/Scripts/python.exe -c "
import pandas as pd
from services import pipeline_service as ps
fdf = ps._load_stock_cache()
pdf = ps._load_prices()
fil = ps.apply_hard_filter(fdf, mode='growth')
out = ps.calc_tech_scores(fil.head(20), pdf, mode='growth')
import json
row = out.iloc[0]
print('tech_score', row['tech_score'], 'sepa in detail:', 'sepa_score' in (row['tech_detail'] or {}))
print('max tech_score in sample:', out['tech_score'].max())
"
```
Expected: `tech_score` が出力され、`sepa_score` が detail に含まれ、max が 100 以下。

- [ ] **Step 4: Commit**

```bash
git add services/pipeline_service.py
git commit -m "feat(pipeline): calc_tech_scoresがstock_cacheのsepa_stageをテクニカルに配線"
```

---

## Task 3: シグナルの Stage 条件と「分析優先度」ラベル

**Files:**
- Modify: `services/pipeline_service.py`（signal 定数 616-630 行 / `calc_trade_signals` 633-723 行）

- [ ] **Step 1: SIGNAL_LABELS 定数を追加**

signal 定数群（630 行付近、`_TARGET2_PCT_VALUE` の後）に追加:

```python
# signal 値 → 分析優先度ラベル（発掘エンジン再定義: 売買シグナルではなく分析の優先度）
SIGNAL_LABELS = {"BUY": "優先精査", "WATCH": "監視", "AVOID": "除外"}
```

- [ ] **Step 2: growth の BUY 条件に Stage2/3 必須を追加**

`calc_trade_signals` のループ内、`detail = row.get("tech_detail", {}) or {}` の後に sepa 取得を追加し、BUY 判定に AND する。658-684 行のブロックを以下に置換:

```python
    for _, row in df.iterrows():
        close      = float(row["close"])
        total      = float(row.get("total_score", 0) or 0)
        tech       = float(row.get("tech_score",  0) or 0)
        detail     = row.get("tech_detail", {}) or {}
        rsi        = detail.get("rsi")
        ma25       = detail.get("ma25")
        sepa_stage = detail.get("sepa_stage")

        # ── シグナル判定 ──────────────────────────────
        above_ma25 = (ma25 is not None) and (close > ma25)
        rsi_ok     = (rsi  is not None) and (rsi_min <= rsi <= rsi_max)
        # 成長株モードのみ: 優先精査(BUY)は SEPA Stage2/3 に限定（Stage4 を上位にしない）
        stage_ok   = True if mode == "value" else (sepa_stage in (2, 3))

        if (total >= _BUY_TOTAL_MIN and tech >= tech_min
                and above_ma25 and rsi_ok and stage_ok):
            sig = "BUY"
            reason = "全条件クリア"
        elif total >= _WATCH_TOTAL_MIN:
            sig = "WATCH"
            parts = []
            if total < _BUY_TOTAL_MIN:  parts.append(f"総合{total:.0f}<60")
            if tech  < tech_min:        parts.append(f"テクニカル{tech:.0f}<{tech_min:.0f}")
            if not above_ma25:          parts.append("MA25下")
            if not rsi_ok:              parts.append(f"RSI={rsi:.0f}(対象:{rsi_min}-{rsi_max})" if rsi else "RSI範囲外")
            if mode != "value" and not stage_ok:
                parts.append(f"SEPA Stage{sepa_stage}(優先精査はStage2/3)")
            reason = " / ".join(parts) if parts else "スコア基準は満たすが条件不足"
        else:
            sig    = "AVOID"
            reason = f"総合スコア{total:.0f}<50"
```

- [ ] **Step 3: 検証（growth で Stage4 が BUY にならない）**

```bash
cd C:\Users\ryohei\stock_analysis
.venv/Scripts/python.exe -c "
from services import pipeline_service as ps
fdf = ps._load_stock_cache(); pdf = ps._load_prices()
fil = ps.apply_hard_filter(fdf, mode='growth')
out = ps.calc_tech_scores(fil, pdf, mode='growth')
out = ps.calc_funda_score(out, mode='growth')
out = ps.calc_total_score(out)
out = ps.calc_trade_signals(out, mode='growth')
buys = out[out['signal']=='BUY']
stages = [ (r['tech_detail'] or {}).get('sepa_stage') for _,r in buys.iterrows() ]
print('BUY数', len(buys), 'BUYのstage集合', set(stages))
assert all(s in (2,3) for s in stages if s is not None), 'Stage4/1 が BUY に混入'
print('Stage条件 OK: BUYは Stage2/3 のみ')
"
```
Expected: `Stage条件 OK`（BUY の sepa_stage が 2/3 のみ）。
（注: `calc_funda_score` の正確な呼び出し名・順序は 301 行で確認。実データ列名が違う場合は合わせる。）

- [ ] **Step 4: Commit**

```bash
git add services/pipeline_service.py
git commit -m "feat(pipeline): 成長株の優先精査をSEPA Stage2/3限定にしSIGNAL_LABELS追加"
```

---

## Task 4: 表示ラベルと二刀流動線ブロック

**Files:**
- Modify: `pages/7_pipeline_report.py`

- [ ] **Step 1: 現状の signal 表示箇所を把握**

```bash
cd C:\Users\ryohei\stock_analysis
.venv/Scripts/python.exe -c "print(open('pages/7_pipeline_report.py',encoding='utf-8').read())" | grep -n "signal\|BUY\|WATCH\|mode" | head -40
```
（Grep ツールで `pages/7_pipeline_report.py` の `signal`/`BUY`/`mode` を確認してもよい。）

- [ ] **Step 2: signal 表示を分析優先度ラベルに**

`7_pipeline_report.py` で `signal` 列を表示している箇所に、`from services.pipeline_service import SIGNAL_LABELS` を import し、表示時に `SIGNAL_LABELS.get(sig, sig)` でラベル変換する。列見出しは「シグナル」→「分析優先度」に変更。

- [ ] **Step 3: 「次にかけるべき分析」動線ブロックを追加**

結果テーブル描画の**直前**に、優先精査(BUY)かつ Stage2 の銘柄を抽出して動線ブロックを描画:

```python
    # 二刀流への動線: 優先精査 × Stage2 を「次にかけるべき分析」として提示
    skill_name = "飛躍分析" if mode == "growth" else "深層分析"
    priority = df[df["signal"] == "BUY"].copy()
    priority["_stage"] = priority["tech_detail"].apply(
        lambda d: (d or {}).get("sepa_stage"))
    priority = priority[priority["_stage"] == 2]
    if len(priority) > 0:
        st.markdown(f"#### 次にかけるべき分析（{skill_name}推奨）")
        for _, r in priority.head(5).iterrows():
            code4 = str(r["code"])[:4]
            st.markdown(f"- `{skill_name} {code4}` — {r['name']}（総合{r['total_score']:.0f} / Stage2）")
        st.caption("パイプラインは発掘（母集団生成）。最終判定は二刀流（飛躍/深層分析）で。")
```
（`df`・`r['name']`・`r['code']` の正確な変数名は当該ページの実装に合わせる。`st` は streamlit。）

- [ ] **Step 4: 検証（Streamlit 起動・目視）**

```bash
cd C:\Users\ryohei\stock_analysis
.venv/Scripts/streamlit.exe run app.py --server.port 8502
```
7_pipeline_report ページで成長株モードを実行し、(a) 列見出しが「分析優先度」で値が「優先精査/監視/除外」、(b) 冒頭に「次にかけるべき分析（飛躍分析推奨）」ブロックが出ることを確認。

- [ ] **Step 5: Commit**

```bash
git add pages/7_pipeline_report.py
git commit -m "feat(report): 分析優先度ラベルと二刀流への動線ブロックを追加"
```

---

## Task 5: 検証（改修前後の比較 + バリュー回帰）

**Files:** なし（検証のみ）

- [ ] **Step 1: 成長株モード Top10 から Stage4 が消えたことを確認**

```bash
cd C:\Users\ryohei\stock_analysis
.venv/Scripts/python.exe -c "
from services import pipeline_service as ps
fdf = ps._load_stock_cache(); pdf = ps._load_prices()
fil = ps.apply_hard_filter(fdf, mode='growth')
out = ps.calc_tech_scores(fil, pdf, mode='growth')
out = ps.calc_funda_score(out, mode='growth')
out = ps.calc_total_score(out)
out = ps.calc_trade_signals(out, mode='growth')
top10 = out.head(10)
for _,r in top10.iterrows():
    d = r['tech_detail'] or {}
    print(str(r['code'])[:4], r['signal'], 'stage', d.get('sepa_stage'), 'tech', r['tech_score'])
"
```
Expected: Top10 の上位（特に BUY=優先精査）が Stage2/3 中心。Stage4 が BUY 上位に居ない。

- [ ] **Step 2: バリュー株モードが回帰していないことを確認**

```bash
cd C:\Users\ryohei\stock_analysis
.venv/Scripts/python.exe -c "
from services import pipeline_service as ps
fdf = ps._load_stock_cache(); pdf = ps._load_prices()
fil = ps.apply_hard_filter(fdf, mode='value')
out = ps.calc_tech_scores(fil, pdf, mode='value')
print('value tech_score サンプル:', out['tech_score'].dropna().head(5).tolist())
print('value tech 最大:', out['tech_score'].max(), '(<=100 のはず)')
"
```
Expected: value の tech_score が改修前と同じ（SEPA 項が入らず、配点は元のまま）。max <= 100。手元に改修前の値があれば一致を確認。

- [ ] **Step 3: diagnose_growth で α 非悪化を確認**

```bash
cd C:\Users\ryohei\stock_analysis
.venv/Scripts/python.exe diagnose_growth.py --fwd 60 --top 10 --snapshots 1
```
Expected: エラーなく完走。Top10 alpha 表が出る。可能なら改修前の同コマンド結果と universe_alpha を比較し、悪化していないことを確認（改善が理想だが、Phase 1 の主目的は Stage4 降格＝質の改善で、α は非悪化が許容ライン）。

- [ ] **Step 4: 完了報告**

Top10 の Stage 分布・value 回帰なし・diagnose 完走を報告。改修前後で Top10 がどう入れ替わったか（Stage4 が抜けたか）を要約。

---

## Self-Review（このプランを書いた後の自己点検結果）

**1. Spec coverage:** spec の全要素をマップ済み —
- A（SEPA配点再設計）→ Task 1+2 / B-1（signal分析優先度化＋Stage2/3限定）→ Task 3 / B-2（二刀流動線）→ Task 4 / value不変 → Task 1 の mode分岐 + Task 5 Step2回帰確認 / 検証（diagnose改修前後）→ Task 5。Phase 2（直近YoY）は spec でスコープ外明記のためプラン対象外（正しい）。

**2. Placeholder scan:** 各 Step に実コード・実コマンド・期待結果を記載。7_pipeline_report.py の変数名（`df`/`name`/`code`）は当該ページ実装に合わせる旨を明示（該当ページを未読のため。Task 4 Step1 で把握してから適用する正当な指示）。

**3. Type consistency:** 用語・関数名を全 Task で統一 — `_sepa_stage_score` / `sepa_stage` 引数 / `SIGNAL_LABELS` / detail の `sepa_stage`・`sepa_score` キー。growth 配点合計が SEPA30+MA20+RSI15+MACD15+出来高10+ブレイク10=100 で整合。value 分岐は全 Task で「不変」と一貫。
