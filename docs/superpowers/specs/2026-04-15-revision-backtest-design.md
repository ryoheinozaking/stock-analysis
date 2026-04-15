# 上方修正フィルター バックテスト追加設計

**日付**: 2026-04-15  
**対象ファイル**: `backtest_pipeline.py`  
**目的**: 上方修正（FEPS +20%以上）フィルターを既存パイプラインバックテストに追加し、PEAD効果を検証する

---

## 背景・動機

旧バックテスト（backtest.py）で「上方修正+20% × モメンタム複合」が勝率84.5%（71件）を記録。  
ただし以下の懸念がある：
- サンプル数71件は統計的に小さい
- 現在のハードフィルター通過銘柄と組み合わせた場合の効果が未検証

本設計では `backtest_pipeline.py`（ハードフィルター通過銘柄・約46銘柄）に上方修正フィルターを追加し、サンプル数と勝率を検証する。

---

## データソース

- `data/fins_cache.parquet`: 2021-07〜2026-04（約4.5年・3,806銘柄・19,079行）
- `data/prices.parquet`: 2023-01〜現在（バックテスト本体）
- ユニバース: `pipeline_service.run_pipeline()` のハードフィルター通過銘柄

---

## 設計詳細

### 1. 上方修正イベント辞書の構築

```python
def _build_revision_events(fins_df: pd.DataFrame, threshold_pct: float = 20.0) -> dict:
    """
    fins_cache から上方修正イベントを抽出。
    戻り値: {code_5digit: [Timestamp, ...]}
    """
```

**ロジック**:
- 銘柄ごとに FEPS > 0 のレコードを DiscDate 昇順でソート
- 連続する2レコードを比較: `(new_FEPS - prev_FEPS) / |prev_FEPS| * 100 >= threshold_pct`
- 条件を満たした場合、新レコードの DiscDate を「上方修正日」として記録

### 2. 有効判定関数

```python
def _has_revision(revision_events: dict, code: str, date: str, window_days: int) -> bool:
    """date から window_days 日以内に上方修正イベントがあれば True"""
```

### 3. 追加する戦略（STRATEGIES リスト拡張）

| 戦略名 | 条件 |
|--------|------|
| `REVISION` | `has_revision` のみ（テクニカル条件なし） |
| `BUY_REVISION` | BUY条件（MA25上 + RSI50-65）+ `has_revision` |
| `BUY_SEPA2_REVISION` | BUY + SEPA2 + `has_revision` |

### 4. `run_backtest()` シグネチャ変更

```python
def run_backtest(
    prices_df, universe_codes,
    profit_growth_map=None,
    sepa2_set=None,
    max_hold=20,
    skip_bear=False,
    target_pct=0.25,
    revision_events=None,   # 追加
    revision_window=None,   # 追加: 30 / 60 / 90
) -> pd.DataFrame:
```

- `revision_events` と `revision_window` が両方 None でない場合のみ、3つの新戦略をアクティブ化
- レコードに `revision_window` 列を追加（既存戦略は `None`）

### 5. `main()` の変更

既存のシナリオループ後に、窓期間ループを追加：

```python
REVISION_WINDOWS = [30, 60, 90]

revision_events = _build_revision_events(fins_df)

for window in REVISION_WINDOWS:
    df = run_backtest(
        prices_df, universe,
        profit_growth_map, sepa2_set,
        max_hold=30,        # 保有30日固定（最優秀と判明済み）
        skip_bear=False,
        target_pct=0.25,
        revision_events=revision_events,
        revision_window=window,
    )
    revision_records.append(df)
```

### 6. 出力セクション追加（結果⑤）

```
結果⑤: 上方修正フィルター戦略 × 窓期間比較
（保有30日・損切り-15%・利確+25%）
```

| 戦略 | 窓期間 | 件数 | 勝率(%) | 平均R(%) | 中央値R(%) |
|------|--------|------|---------|---------|-----------|
| REVISION | 30日 | ? | ? | ? | ? |
| REVISION | 60日 | ? | ? | ? | ? |
| REVISION | 90日 | ? | ? | ? | ? |
| BUY_REVISION | 30日 | ? | ? | ? | ? |
| ... | | | | | |
| BUY_SEPA2_REVISION | 90日 | ? | ? | ? | ? |

---

## 過学習リスクへの対処

- **サンプル数チェック**: 100件未満の戦略は参考値として扱う（結果に注記）
- **理論的根拠**: PEAD（Post Earnings Announcement Drift）は学術的に確立済みのため、純粋なデータマイニングではない
- **窓期間の比較**: 最良窓期間だけ採用するのではなく、3つの傾向を比較して判断する

---

## 変更範囲

- `backtest_pipeline.py` のみ変更（他ファイル不要）
- 既存の結果①〜④は変更なし（後方互換）
- CSV出力に `revision_window` 列が追加される
