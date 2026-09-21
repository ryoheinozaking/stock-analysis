# ブレイクアウト順張り バックテスト 実装計画（第1段階）

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 設計書 `docs/superpowers/specs/2026-09-22-breakout-paper-trading-design.md` の売買ルールを、資金 300 万円のポートフォリオとして 2022-02〜2026-09 で再現し、合格基準を判定する。

**Architecture:** 指標と判定は副作用なしの関数（`breakout_strategy`）、口座は `PaperBroker`、1 営業日の処理は `breakout_engine.run_day`。バックテストはこの処理を日付順に回すだけにして、第2段階のペーパー運用でも同じコードを使う。

**Tech Stack:** Python 3.9.1 / pandas / pytest。データは `data/prices.parquet`・`valuation.parquet`・`earnings_dates.parquet`・`fins_cache.parquet`（ローカル parquet のみ、API 不要）。

**範囲:** この計画は第1段階（バックテスト）まで。ペーパー運用の画面（`pages/10_paper_trading.py`）・Streamlit の「データ更新」との連携・TradingView への描き込みは、バックテスト合格後に別計画で作る。

---

## 株価スケールの扱い（全タスク共通）

- 指標（移動平均・55日高値・出来高倍率・ATR）は `split_adjust` で末尾日スケールに正規化した系列で計算する
- 売買（約定値・損切りライン・株数・損益）は**その日の生の株価（分割前後そのまま）**で行う。
  ATR は `atr_raw = atr_正規化 ÷ cum_factor` でその日の生スケールに戻す
- 保有中の銘柄に分割（その日の `AdjFactor != 1`）が来たら、株数 ÷ f、買値・損切りライン・最高値 × f。
  翌日約定予定の注文も株数 ÷ f、ATR × f

## Task 1: 指標と判定（`services/breakout_strategy.py`）

**Files:** Create `services/breakout_strategy.py`, Test `tests/test_breakout.py`

公開するもの:
- `BreakoutParams`（dataclass。設計書の初期値。`breakout_days=55, trail_atr=3.0, init_stop_atr=2.0` など）
- `compute_indicators(cp: DataFrame, p) -> DataFrame` … 1 銘柄分。列 `Date, Code, O, H, L, C, AdjFactor, atr_raw, vol_ratio, turnover_avg, tech_signal`
- `market_filter(cp_1306, p) -> Series[Date -> bool]`
- `earnings_block_set(earnings_df, trading_dates, buffer_days) -> set[(code, Timestamp)]` … その日に知られていた最新の予定（公表日 ≦ D の最後の公表）で、予定日まで 3 営業日以内の (銘柄, 日) の集合
- `build_candidates(ind, mktcap, company_codes, market_ok, earn_block, p) -> DataFrame` … 買い条件を全部満たした (Date, Code, vol_ratio, atr_raw, C)

- [ ] テストを書く:

```python
def test_tech_signal_fires_on_volume_breakout():
    cp = _uptrend_then_breakout()           # 260 日の緩やかな上昇 → 最終日に高値更新 + 出来高 3 倍
    ind = compute_indicators(cp, BreakoutParams())
    assert bool(ind["tech_signal"].iloc[-1]) is True
    assert not ind["tech_signal"].iloc[-30:-1].any()   # 出来高を伴わない日は出ない

def test_tech_signal_needs_volume():
    cp = _uptrend_then_breakout(vol_mult=1.2)
    assert bool(compute_indicators(cp, BreakoutParams())["tech_signal"].iloc[-1]) is False

def test_atr_raw_is_on_that_days_scale():
    # 1:2 分割を挟む。分割前の日の atr_raw は分割前の値幅（正規化値の 2 倍）
    ...

def test_earnings_block_uses_latest_published_schedule():
    # 予定 10/10 を 9/1 に公表 → 9/20 に 10/30 へ変更。10/7 は変更後なので止めない、9/15 時点で 9/18 の予定なら止める
    ...
```

- [ ] 失敗を確認 → 実装 → `.venv/Scripts/python.exe -m pytest tests/test_breakout.py -q` が通る → コミット

## Task 2: ペーパー口座（`services/paper_broker.py`）

`PaperBroker(cash)`: `positions`（銘柄 → 株数・買値・買付日・損切り・最高値）、`orders`（翌日の買い注文）、`trades`（決済済み）、`equity_curve`、`last_processed`。
`buy / sell / apply_split / equity(prices) / to_dict / from_dict / save / load`。
実運用ではこのクラスと同じ窓口の「証券会社版」に差し替える。

- [ ] テスト: 損切りの窓開け（始値 850・損切り 900 → 850×0.999 で約定）、ザラ場の損切り（始値 950・安値 890 → 900×0.999）、分割で株数 2 倍・損切り半分、保存して読み込むと同じ状態
- [ ] 実装 → テスト通過 → コミット

## Task 3: 1 営業日の処理（`services/breakout_engine.py`）

`run_day(broker, date, bars, candidates, p, allow_new=True)` の順序:
1. 分割の反映 → 2. 前日注文を始値で約定（バーが無ければ取り消し、現金・20% 上限で株数を削る、100 株未満は見送り）
→ 3. 損切り判定（当日約定分も対象）→ 4. トレーリング引き上げ（上げるのみ）→ 5. 終値で資産評価
→ 6. `allow_new` なら出来高倍率の高い順に翌日の注文（株数 = floor(資産×0.5% ÷ (ATR×2) ÷ 100)×100、20% 上限）
`date <= broker.last_processed` なら何もしない（二重処理防止）。

- [ ] テスト: 株数計算（資産 300 万・ATR 50 → 100 株）、20% 上限で削られる、トレーリングが下がらない、同じ日を 2 回回しても状態が変わらない、始値が無い日は注文取り消し
- [ ] 実装 → テスト通過 → コミット

## Task 4: バックテスト（`scripts/breakout_backtest.py`）

- データ読み込み → 全銘柄の指標 → 候補 → 期間ごとに新しい口座（300 万円）で `run_day` を回す
- 期間: 前半 2022-02-01〜2024-12-30、後半 2025-01-06〜2026-09-18、参考に全期間
- 周辺の値: 高値期間 {40, 55, 80} × トレーリング倍率 {2, 3, 4}（初期損切りは ATR×2 で固定）
- 指標: 年率リターン、TOPIX(1306) 買い持ちの年率、プロフィットファクター（期末の保有は期末終値で評価して含める）、
  最大ドローダウン、取引数、勝率、平均保有日数、月あたり取引数、平均の投下資金比率
- 出力: `data/breakout_backtest/summary.csv`、`trades_<期間>.csv`、`equity_<期間>.csv`、合否の判定を標準出力

- [ ] 実行 → 結果を利用者に報告（後半を見た後でルールを変えない）→ コミット
