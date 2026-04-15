# 上方修正フィルター バックテスト Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `backtest_pipeline.py` に上方修正（FEPS +20%以上）フィルターを追加し、REVISION / BUY_REVISION / BUY_SEPA2_REVISION 戦略を窓期間30/60/90日で検証する。

**Architecture:** 既存の `run_backtest()` に `revision_events` / `revision_window` パラメータを追加。`_build_revision_events()` で fins_cache から上方修正日を辞書化し、バックテストループ内で `_has_revision()` を判定する。既存の結果①〜④は変更せず、結果⑤として新戦略の比較表を追加する。

**Tech Stack:** pandas, numpy, parquet（fins_cache.parquet / prices.parquet）

---

## ファイル構成

| ファイル | 変更種別 | 内容 |
|---------|---------|------|
| `backtest_pipeline.py` | 修正のみ | ヘルパー関数2件追加、`run_backtest()`拡張、`main()`拡張 |

他ファイルの変更は不要。

---

### Task 1: ヘルパー関数 `_build_revision_events()` と `_has_revision()` を追加

**Files:**
- Modify: `backtest_pipeline.py`（`_calc_market_condition()` の直前に挿入）

- [ ] **Step 1: `_build_revision_events()` を追加**

`backtest_pipeline.py` の `_calc_market_condition()` 関数（100行目付近）の直前に以下を挿入する：

```python
def _build_revision_events(fins_df: pd.DataFrame, threshold_pct: float = 20.0) -> dict:
    """
    fins_cache から上方修正イベントを抽出する。
    同一銘柄の連続するFEPS開示を比較し、(new-prev)/|prev|*100 >= threshold_pct の
    開示日を「上方修正日」として記録する。
    戻り値: {code_5digit_str: [pd.Timestamp, ...]}
    """
    events: dict = {}
    work = fins_df.copy()
    work["DiscDate"] = pd.to_datetime(work["DiscDate"], errors="coerce")
    work["FEPS"]     = pd.to_numeric(work["FEPS"],     errors="coerce")
    valid = work.dropna(subset=["DiscDate", "FEPS"]).query("FEPS > 0")

    for code, grp in valid.groupby("Code"):
        grp = grp.sort_values("DiscDate").reset_index(drop=True)
        dates = []
        for i in range(1, len(grp)):
            prev_eps = float(grp.loc[i - 1, "FEPS"])
            curr_eps = float(grp.loc[i,     "FEPS"])
            if prev_eps > 0:
                rev = (curr_eps - prev_eps) / abs(prev_eps) * 100
                if rev >= threshold_pct:
                    dates.append(grp.loc[i, "DiscDate"])
        if dates:
            events[str(code).zfill(5)] = dates

    return events


def _has_revision(revision_events: dict, code: str, date: str, window_days: int) -> bool:
    """date から window_days 日以内（含む）に上方修正イベントがあれば True を返す。"""
    event_dates = revision_events.get(code, [])
    if not event_dates:
        return False
    ts     = pd.Timestamp(date)
    cutoff = ts - pd.Timedelta(days=window_days)
    return any(cutoff <= e <= ts for e in event_dates)
```

- [ ] **Step 2: 動作確認（関数単体の検証）**

以下のコマンドで fins_cache を読み込み、上方修正イベントの件数を確認する：

```bash
.venv/Scripts/python.exe -c "
import pandas as pd
import sys
sys.path.insert(0, '.')
# backtest_pipeline モジュールとして読み込む
import importlib.util, types
spec = importlib.util.spec_from_file_location('bp', 'backtest_pipeline.py')
bp = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bp)

fins = pd.read_parquet('data/fins_cache.parquet')
events = bp._build_revision_events(fins)
total = sum(len(v) for v in events.values())
print(f'上方修正あり銘柄: {len(events)}件 / イベント合計: {total}件')

# _has_revision の動作確認（最初のイベント日で True になるか）
if events:
    code, dates = next(iter(events.items()))
    test_date = str(dates[0].date())
    print(f'サンプル: code={code}, 修正日={test_date}')
    print(f'  窓30日(当日): {bp._has_revision(events, code, test_date, 30)}')  # True
    print(f'  窓30日(100日後): {bp._has_revision(events, code, str((dates[0] + pd.Timedelta(days=100)).date()), 30)}')  # False
"
```

期待される出力（数値は環境依存）：
```
上方修正あり銘柄: 〇〇件 / イベント合計: 〇〇件
サンプル: code=XXXXX, 修正日=XXXX-XX-XX
  窓30日(当日): True
  窓30日(100日後): False
```

- [ ] **Step 3: コミット**

```bash
git add backtest_pipeline.py
git commit -m "feat: _build_revision_events / _has_revision 追加"
```

---

### Task 2: `run_backtest()` に revision パラメータを追加

**Files:**
- Modify: `backtest_pipeline.py`（`run_backtest()` 関数）

- [ ] **Step 1: シグネチャに2パラメータを追加**

`run_backtest()` のシグネチャを以下に変更する（既存パラメータはそのまま）：

```python
def run_backtest(
    prices_df: pd.DataFrame,
    universe_codes: list,
    profit_growth_map: dict = None,
    sepa2_set: set = None,
    max_hold: int = 20,
    skip_bear: bool = False,
    target_pct: float = 0.25,
    revision_events: dict = None,   # 追加
    revision_window: int = None,    # 追加: 30 / 60 / 90
) -> pd.DataFrame:
```

docstring の末尾に以下を追加：

```python
    """
    ...（既存）...
    revision_events: {code: [Timestamp, ...]} の辞書（_build_revision_events の戻り値）
    revision_window: 上方修正イベントを有効とみなす遡及日数（None = 上方修正戦略を使わない）
    """
```

- [ ] **Step 2: STRATEGIES リストを動的に構築するよう変更**

現在の固定リスト：
```python
    STRATEGIES = ["ALL", "BUY", "BUY_G50", "ALL_G50", "SEPA2", "BUY_SEPA2"]
```

これを以下に置き換える：

```python
    use_revision = (revision_events is not None) and (revision_window is not None)
    STRATEGIES = ["ALL", "BUY", "BUY_G50", "ALL_G50", "SEPA2", "BUY_SEPA2"]
    if use_revision:
        STRATEGIES += ["REVISION", "BUY_REVISION", "BUY_SEPA2_REVISION"]
```

- [ ] **Step 3: 銘柄ループ内に `rev_ok` フラグを追加**

`is_sepa2` の定義の直後（`for stop_pct in STOP_LIST:` の前）に追加：

```python
        # revision フラグは日付ごとに変わるため、ループ内で評価する（後述）
        # ここでは use_revision フラグだけ確認
```

`for i in range(25, len(closes) - 1):` ループ内の `above_ma25 = ...` の直後に追加：

```python
                    # 上方修正フラグ（窓期間内にイベントがあるか）
                    rev_ok = (
                        use_revision
                        and _has_revision(revision_events, code, dates[i], revision_window)
                    )
```

- [ ] **Step 4: 戦略フィルターに revision 条件を追加（2か所）**

**【外側フィルター】** `for stop_pct` ループ内、`for i in range(...)` の外にある既存ブロック：

```python
                if strategy in ("BUY_G50", "ALL_G50") and not g50:
                    continue
                if strategy in ("SEPA2", "BUY_SEPA2") and not is_sepa2:
                    continue
```

を以下に置き換える（`BUY_SEPA2_REVISION` の `is_sepa2` チェックを追加）：

```python
                if strategy in ("BUY_G50", "ALL_G50") and not g50:
                    continue
                if strategy in ("SEPA2", "BUY_SEPA2", "BUY_SEPA2_REVISION") and not is_sepa2:
                    continue
```

**【内側フィルター】** `for i in range(25, len(closes) - 1):` ループ内、`rev_ok = ...` の直後に追加：

```python
                    # 上方修正条件（日付ごとに変化するため内側ループで評価）
                    if strategy in ("REVISION", "BUY_REVISION", "BUY_SEPA2_REVISION") and not rev_ok:
                        continue
```

さらに、同じ内側ループ内の BUY 条件ブロック：

```python
                    if strategy in ("BUY", "BUY_G50", "BUY_SEPA2"):
                        if not (above_ma25 and rsi_ok):
                            continue
```

を以下に置き換える：

```python
                    if strategy in ("BUY", "BUY_G50", "BUY_SEPA2",
                                    "BUY_REVISION", "BUY_SEPA2_REVISION"):
                        if not (above_ma25 and rsi_ok):
                            continue
```

- [ ] **Step 5: records.append に `revision_window` 列を追加**

`records.append({...})` の辞書に以下を追加する：

```python
                    records.append({
                        "code":             code,
                        "entry_date":       dates[i],
                        "strategy":         strategy,
                        "stop_pct":         stop_pct,
                        "max_hold":         max_hold,
                        "target_pct":       target_pct,
                        "skip_bear":        skip_bear,
                        "revision_window":  revision_window,   # 追加
                        "return_pct":       round(trade["return_pct"] * 100, 3),
                        "win":              trade["return_pct"] > 0,
                        "exit_reason":      trade["exit_reason"],
                        "market":           mkt,
                    })
```

- [ ] **Step 6: 動作確認（revision_window=None でも壊れないことを確認）**

```bash
.venv/Scripts/python.exe -c "
import pandas as pd, numpy as np
import sys; sys.path.insert(0, '.')
import importlib.util
spec = importlib.util.spec_from_file_location('bp', 'backtest_pipeline.py')
bp = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bp)

prices = pd.read_parquet('data/prices.parquet')
from services.pipeline_service import run_pipeline
result = run_pipeline(use_claude=False)
filtered = result['filtered']
universe = filtered['code'].astype(str).tolist()[:5]  # 最初の5銘柄で確認
pg_map = dict(zip(filtered['code'].astype(str), filtered['profit_growth']))
sepa2 = set()

df = bp.run_backtest(prices, universe, pg_map, sepa2, max_hold=20)
print('既存モード OK:', len(df), '件')
print('列:', list(df.columns))
assert 'revision_window' in df.columns, 'revision_window 列が存在しない'
print('revision_window 列: OK (値はNone)')
"
```

期待される出力：
```
既存モード OK: ○○件
列: [..., 'revision_window', ...]
revision_window 列: OK (値はNone)
```

- [ ] **Step 7: コミット**

```bash
git add backtest_pipeline.py
git commit -m "feat: run_backtest に revision パラメータ追加（REVISION/BUY_REVISION/BUY_SEPA2_REVISION）"
```

---

### Task 3: `main()` に上方修正バックテストループと結果⑤を追加

**Files:**
- Modify: `backtest_pipeline.py`（`main()` 関数）

- [ ] **Step 1: バージョン表記を更新**

`main()` の先頭の print 文：

```python
    print("  パイプライン バックテスト v3（弱気フィルター + 利確ターゲット検証）")
```

を以下に変更する：

```python
    print("  パイプライン バックテスト v4（上方修正フィルター追加）")
```

- [ ] **Step 2: fins_cache 読み込みと revision_events 構築をデータ読み込みセクションに追加**

既存の `[1] データ読み込み中...` セクション末尾（`print(f"    ユニバース: ...")` の直後）に追加する：

```python
    # fins_cache 読み込み（上方修正イベント構築用）
    fins_df = pd.read_parquet("data/fins_cache.parquet")
    revision_events = _build_revision_events(fins_df)
    rev_total = sum(len(v) for v in revision_events.values())
    print(f"    上方修正イベント: {len(revision_events)} 銘柄 / {rev_total} 件（閾値+20%）")
```

- [ ] **Step 3: 上方修正バックテストループを `[2]` の後ろに追加**

`df_all = pd.concat(all_records, ...)` の直後、かつ「結果①」の print の前に以下を追加する：

```python
    # ── 上方修正バックテスト（窓期間 30 / 60 / 90 日）─────────────────
    REVISION_WINDOWS = [30, 60, 90]
    rev_records = []
    print(f"\n[3] 上方修正バックテスト実行中（{len(REVISION_WINDOWS)}窓期間 × 保有30日）...")
    for window in REVISION_WINDOWS:
        print(f"  窓期間 {window}日...")
        df_r = run_backtest(
            prices_df, universe, profit_growth_map, sepa2_set,
            max_hold=30, skip_bear=False, target_pct=0.25,
            revision_events=revision_events, revision_window=window,
        )
        rev_records.append(df_r)

    df_rev = pd.concat(rev_records, ignore_index=True) if rev_records else pd.DataFrame()
    print(f"    上方修正系トレード記録: {len(df_rev)} 件")
```

- [ ] **Step 4: 結果⑤ の出力セクションを追加**

「結果④」の `_print_comparison("", rows)` の直後に追加する：

```python
    # ── 結果⑤ 上方修正フィルター戦略 × 窓期間比較 ───────────────────
    print("\n" + "=" * 70)
    print("  結果⑤ 上方修正フィルター戦略 × 窓期間比較（保有30日・損切り-15%・利確25%）")
    print("=" * 70)
    if df_rev.empty:
        print("  ⚠ トレード記録なし（fins_cache にデータがない可能性）")
    else:
        rows = []
        for strategy in ["REVISION", "BUY_REVISION", "BUY_SEPA2_REVISION"]:
            for window in REVISION_WINDOWS:
                grp = df_rev[
                    (df_rev["strategy"]        == strategy) &
                    (df_rev["revision_window"] == window)   &
                    (df_rev["stop_pct"]        == 0.15)     &
                    (df_rev["max_hold"]        == 30)       &
                    (df_rev["target_pct"]      == 0.25)
                ]
                if grp.empty:
                    continue
                note = "⚠ 少" if len(grp) < 100 else ""
                rows.append({
                    "戦略":        strategy,
                    "窓期間(日)":  window,
                    "件数":        len(grp),
                    "勝率(%)":    round(grp["win"].mean() * 100, 1),
                    "平均R(%)":   round(grp["return_pct"].mean(), 2),
                    "中央値R(%)": round(grp["return_pct"].median(), 2),
                    "※":          note,
                })
        if rows:
            _print_comparison("", rows)
        else:
            print("  ⚠ 該当データなし")

    # ── 参考: BUY_SEPA2（既存）と並べて比較 ─────────────────────────
    print("\n--- 参考: BUY_SEPA2（既存・保有30日・損切り-15%・利確25%・弱気含む）---")
    ref = df_all[
        (df_all["strategy"]   == "BUY_SEPA2") &
        (df_all["max_hold"]   == 30) &
        (df_all["stop_pct"]   == 0.15) &
        (df_all["target_pct"] == 0.25) &
        (df_all["skip_bear"]  == False)
    ]
    if not ref.empty:
        print(f"  件数: {len(ref)} / 勝率: {ref['win'].mean()*100:.1f}% / 平均R: {ref['return_pct'].mean():.2f}% / 中央値R: {ref['return_pct'].median():.2f}%")
```

- [ ] **Step 5: CSV 保存に df_rev を追記**

既存の CSV 保存セクション（`df_all.to_csv(...)` の直後）に追加する：

```python
    if not df_rev.empty:
        df_rev.to_csv("data/backtest_revision_records.csv", index=False, encoding="utf-8-sig")
        print("上方修正詳細CSV: data/backtest_revision_records.csv")
```

- [ ] **Step 6: 全体実行して結果確認**

```bash
cd C:/Users/ryohei/stock_analysis && .venv/Scripts/python.exe backtest_pipeline.py 2>&1 | tail -60
```

確認ポイント：
1. `[3] 上方修正バックテスト実行中...` が表示される
2. 「結果⑤」が表示される
3. 各戦略のサンプル数が表示される（100件未満の場合 `⚠ 少` が付く）
4. エラーなく完了する

- [ ] **Step 7: コミット**

```bash
git add backtest_pipeline.py
git commit -m "feat: 上方修正フィルターバックテスト追加（REVISION/BUY_REVISION/BUY_SEPA2_REVISION × 窓30/60/90日）"
```
