# -*- coding: utf-8 -*-
"""
パイプライン バックテスト

検証内容:
  1. シグナル別パフォーマンス比較（BUY / WATCH / ALL）
  2. 損切りライン最適化（-8% vs -15%）
  3. 地合いフィルターの効果
  4. 旧モメンタム戦略との比較

データ: prices.parquet (2025-11-25〜2026-04-03 / 分割調整済み)
ユニバース: 現在のハードフィルター通過銘柄 (~99銘柄)
"""

import os, sys
import numpy as np
import pandas as pd
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ── 設定 ─────────────────────────────────────────────────────────────────
COST_PCT     = 0.002   # 往復取引コスト 0.2%
STOP_LIST    = [0.15]         # 損切りシナリオ（-15%が最優秀と判明済み）
HOLD_LIST    = [20, 30]       # 保有期間（20日・30日を中心に検証）
TARGET_LIST  = [0.25, 0.40, 0.50]  # 利確ターゲット検証

# BUYシグナル条件
BUY_RSI_MIN  = 50
BUY_RSI_MAX  = 65

# 地合いフィルター用ETFコード
ETF_TOPIX    = "13060"
ETF_GROWTH   = "25160"
# ─────────────────────────────────────────────────────────────────────────


def _calc_adj_close(cp: pd.DataFrame) -> pd.Series:
    """AdjFactorで株式分割を正規化した終値を返す。"""
    cp = cp.sort_values("Date").reset_index(drop=True)
    raw_close  = pd.to_numeric(cp["AdjC"],      errors="coerce")
    adj_factor = pd.to_numeric(cp["AdjFactor"], errors="coerce").fillna(1.0)
    rev_cum    = adj_factor.iloc[::-1].cumprod().iloc[::-1]
    cum_factor = rev_cum.shift(-1).fillna(1.0)
    return (raw_close * cum_factor).values


def _calc_rsi(close: np.ndarray, period: int = 14) -> np.ndarray:
    """RSI を計算する。"""
    rsi = np.full(len(close), np.nan)
    if len(close) < period + 1:
        return rsi
    delta = np.diff(close.astype(float))
    gain  = np.where(delta > 0, delta, 0.0)
    loss  = np.where(delta < 0, -delta, 0.0)
    avg_g = np.mean(gain[:period])
    avg_l = np.mean(loss[:period])
    for i in range(period, len(delta)):
        avg_g = (avg_g * (period - 1) + gain[i]) / period
        avg_l = (avg_l * (period - 1) + loss[i]) / period
        if avg_l == 0:
            rsi[i + 1] = 100.0
        else:
            rsi[i + 1] = 100.0 - 100.0 / (1.0 + avg_g / avg_l)
    return rsi


def _simulate_trade(closes: np.ndarray, entry_idx: int, stop_pct: float, max_hold: int,
                    target_pct: float = 0.25) -> dict:
    """
    entry_idx の翌日始値（≒ close×1.005 をエントリー価格）で買い、
    stop/target/maxhold のいずれかで決済するトレードをシミュレート。
    """
    entry_price = closes[entry_idx] * 1.005  # ブレイクアウトエントリー
    exit_idx    = None
    exit_reason = "maxhold"
    ret         = np.nan

    for j in range(entry_idx + 1, min(entry_idx + max_hold + 1, len(closes))):
        price = closes[j]
        if np.isnan(price):
            continue
        if price <= entry_price * (1 - stop_pct):
            exit_idx    = j
            exit_reason = "stop"
            break
        if price >= entry_price * (1 + target_pct):
            exit_idx    = j
            exit_reason = "target"
            break
        exit_idx = j  # maxhold候補を更新

    if exit_idx is not None and not np.isnan(closes[exit_idx]):
        gross = (closes[exit_idx] - entry_price) / entry_price
        ret   = gross - COST_PCT  # コスト差引

    return {"exit_idx": exit_idx, "exit_reason": exit_reason, "return_pct": ret}


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
    # 通期（FY）開示のみを対象とする（四半期間比較による誤検知を防ぐ）
    if "CurPerType" in work.columns:
        work = work[work["CurPerType"] == "FY"]
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
    if revision_events is None:
        return False
    event_dates = revision_events.get(code, [])
    if not event_dates:
        return False
    ts     = pd.Timestamp(date)
    cutoff = ts - pd.Timedelta(days=window_days)
    return any(cutoff <= e <= ts for e in event_dates)


def _calc_market_condition(prices_df: pd.DataFrame, date: str) -> str:
    """指定日時点の地合いを返す: 強気 / 中立 / 弱気 / 不明"""
    results = {}
    for key, code in [("topix", ETF_TOPIX), ("growth", ETF_GROWTH)]:
        cp = prices_df[prices_df["Code"] == code].copy()
        cp = cp[cp["Date"] <= date].sort_values("Date").reset_index(drop=True)
        if len(cp) < 25:
            return "不明"
        raw   = pd.to_numeric(cp["C"], errors="coerce")
        adjf  = pd.to_numeric(cp["AdjFactor"], errors="coerce").fillna(1.0)
        revc  = adjf.iloc[::-1].cumprod().iloc[::-1]
        cumf  = revc.shift(-1).fillna(1.0)
        close = (raw * cumf).dropna()
        if len(close) < 25:
            return "不明"
        latest = float(close.iloc[-1])
        ma25   = float(close.iloc[-25:].mean())
        results[key] = latest > ma25

    both_above = results.get("topix") and results.get("growth")
    both_below = (not results.get("topix")) and (not results.get("growth"))
    if both_above:
        return "強気"
    elif both_below:
        return "弱気"
    else:
        return "中立"


def run_backtest(
    prices_df: pd.DataFrame,
    universe_codes: list,
    profit_growth_map: dict = None,
    sepa2_set: set = None,
    max_hold: int = 20,
    skip_bear: bool = False,
    target_pct: float = 0.25,
    revision_events: dict = None,
    revision_window: int = None,
) -> pd.DataFrame:
    """
    メインのバックテストループ。
    各銘柄×各シグナル×各損切りシナリオでトレード記録を生成する。

    profit_growth_map: {code: profit_growth_pct} の辞書（利益成長率フィルター用）
    sepa2_set:         SEPA Stage2 銘柄コードのセット
    max_hold:          最大保有日数
    skip_bear:         True の場合、地合い「弱気」の日はエントリーしない
    target_pct:        利確ターゲット（デフォルト 0.25 = +25%）
    revision_events:   {code: [Timestamp, ...]} の辞書（_build_revision_events の戻り値）
    revision_window:   上方修正イベントを有効とみなす遡及日数（None = 上方修正戦略を使わない）
    """
    # 地合いキャッシュ（日付→状態）
    all_dates = sorted(prices_df["Date"].unique())
    market_cache = {}
    for d in all_dates:
        market_cache[d] = _calc_market_condition(prices_df, d)

    use_revision = (revision_events is not None) and (revision_window is not None)
    STRATEGIES = ["ALL", "BUY", "BUY_G50", "ALL_G50", "SEPA2", "BUY_SEPA2"]
    if use_revision:
        STRATEGIES += ["REVISION", "BUY_REVISION", "BUY_SEPA2_REVISION"]

    records = []
    n = len(universe_codes)
    for idx, code in enumerate(universe_codes, 1):
        if idx % 10 == 0:
            print(f"  {idx}/{n} 銘柄処理中...")

        cp = prices_df[prices_df["Code"] == code].copy()
        if len(cp) < 30:
            continue

        cp = cp.sort_values("Date").reset_index(drop=True)
        dates  = cp["Date"].values
        closes = _calc_adj_close(cp)
        rsi    = _calc_rsi(closes)
        ma25   = pd.Series(closes).rolling(25).mean().values

        pg   = (profit_growth_map or {}).get(code, None)
        g50  = (pg is not None) and (not np.isnan(float(pg))) and (float(pg) > 50)
        is_sepa2 = (sepa2_set is not None) and (code in sepa2_set)

        for stop_pct in STOP_LIST:
            for strategy in STRATEGIES:
                if strategy in ("BUY_G50", "ALL_G50") and not g50:
                    continue
                if strategy in ("SEPA2", "BUY_SEPA2", "BUY_SEPA2_REVISION") and not is_sepa2:
                    continue

                last_entry = -max_hold - 1

                for i in range(25, len(closes) - 1):
                    if i - last_entry < max_hold:
                        continue

                    c   = closes[i]
                    r   = rsi[i]
                    m25 = ma25[i]

                    if np.isnan(c) or np.isnan(m25):
                        continue

                    mkt = market_cache.get(dates[i], "不明")

                    # 弱気フィルター
                    if skip_bear and mkt == "弱気":
                        continue

                    above_ma25 = c > m25
                    rsi_ok     = (not np.isnan(r)) and (BUY_RSI_MIN <= r <= BUY_RSI_MAX)

                    # 上方修正フラグ（日付ごとに変化するため内側ループで評価）
                    rev_ok = (
                        use_revision
                        and _has_revision(revision_events, code, dates[i], revision_window)
                    )

                    # 上方修正条件チェック
                    if strategy in ("REVISION", "BUY_REVISION", "BUY_SEPA2_REVISION") and not rev_ok:
                        continue

                    if strategy in ("BUY", "BUY_G50", "BUY_SEPA2",
                                    "BUY_REVISION", "BUY_SEPA2_REVISION"):
                        if not (above_ma25 and rsi_ok):
                            continue
                    # ALL / ALL_G50 / SEPA2 / REVISION: 価格条件なし

                    trade = _simulate_trade(closes, i, stop_pct, max_hold, target_pct)
                    if np.isnan(trade["return_pct"]):
                        continue

                    records.append({
                        "code":             code,
                        "entry_date":       dates[i],
                        "strategy":         strategy,
                        "stop_pct":         stop_pct,
                        "max_hold":         max_hold,
                        "target_pct":       target_pct,
                        "skip_bear":        skip_bear,
                        "revision_window":  revision_window,
                        "return_pct":       round(trade["return_pct"] * 100, 3),
                        "win":              trade["return_pct"] > 0,
                        "exit_reason":      trade["exit_reason"],
                        "market":           mkt,
                    })
                    last_entry = i

    return pd.DataFrame(records)


def summarize(df: pd.DataFrame, label: str = "", group_hold: bool = False) -> pd.DataFrame:
    """戦略×（保有期間）別の集計サマリーを返す。"""
    group_keys = ["strategy", "max_hold"] if group_hold else ["strategy"]
    rows = []
    for keys, grp in df.groupby(group_keys):
        strategy = keys[0] if group_hold else keys
        hold     = keys[1] if group_hold else df["max_hold"].iloc[0] if "max_hold" in df.columns else "-"
        rows.append({
            "戦略":         strategy,
            "保有日数":     hold,
            "トレード数":   len(grp),
            "勝率(%)":     round(grp["win"].mean() * 100, 1),
            "平均R(%)":    round(grp["return_pct"].mean(), 2),
            "中央値R(%)":  round(grp["return_pct"].median(), 2),
            "最大利益(%)": round(grp["return_pct"].max(), 2),
            "最大損失(%)": round(grp["return_pct"].min(), 2),
        })
    result = pd.DataFrame(rows).sort_values(["戦略", "保有日数"])
    if label:
        print(f"\n{label}")
    return result


def _print_comparison(label: str, rows: list):
    if rows:
        print(f"\n{label}")
        print(pd.DataFrame(rows).to_string(index=False))


def main():
    print("=" * 70)
    print("  パイプライン バックテスト v4（上方修正フィルター追加）")
    print("=" * 70)

    # ── データ読み込み ─────────────────────────────────────────
    print("\n[1] データ読み込み中...")
    prices_df = pd.read_parquet("data/prices.parquet")
    print(f"    prices.parquet: {prices_df['Date'].min()} ~ {prices_df['Date'].max()}")

    from services.pipeline_service import run_pipeline
    result   = run_pipeline(use_claude=False)
    filtered = result["filtered"]
    scored   = result.get("scored", filtered)

    universe = filtered["code"].astype(str).tolist()
    profit_growth_map = dict(zip(
        filtered["code"].astype(str),
        filtered["profit_growth"]
    ))

    sepa2_set = set()
    if "sepa_stage" in scored.columns:
        sepa2_set = set(scored[scored["sepa_stage"] == 2]["code"].astype(str).tolist())
    g50_count = sum(1 for v in profit_growth_map.values() if v and not np.isnan(float(v)) and float(v) > 50)
    print(f"    ユニバース: {len(universe)} 銘柄 / SEPA2: {len(sepa2_set)} 銘柄 / 利益成長>50%: {g50_count} 銘柄")

    # fins_cache 読み込み（上方修正イベント構築用）
    fins_df = pd.read_parquet("data/fins_cache.parquet")
    revision_events = _build_revision_events(fins_df)
    rev_total = sum(len(v) for v in revision_events.values())
    print(f"    上方修正イベント: {len(revision_events)} 銘柄 / {rev_total} 件（閾値+20%）")

    # ── バックテスト実行 ──────────────────────────────────────
    # 検証軸: 弱気フィルター(on/off) × 利確ターゲット × 保有期間
    scenarios = [
        {"skip_bear": False, "target_pct": t} for t in TARGET_LIST
    ] + [
        {"skip_bear": True,  "target_pct": t} for t in TARGET_LIST
    ]

    all_records = []
    total = len(scenarios) * len(HOLD_LIST)
    done  = 0
    print(f"\n[2] バックテスト実行中（{total}シナリオ）...")
    for sc in scenarios:
        for hold in HOLD_LIST:
            done += 1
            bear_label   = "弱気除外" if sc["skip_bear"] else "弱気含む"
            target_label = f'利確{int(sc["target_pct"]*100)}%'
            print(f"  [{done}/{total}] {bear_label} / {target_label} / 保有{hold}日")
            df = run_backtest(
                prices_df, universe, profit_growth_map, sepa2_set,
                max_hold=hold, skip_bear=sc["skip_bear"], target_pct=sc["target_pct"]
            )
            all_records.append(df)

    df_all = pd.concat(all_records, ignore_index=True)
    print(f"\n    総トレード記録: {len(df_all)} 件")

    if df_all.empty:
        print("トレード記録なし。終了。")
        return

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

    # ── 結果① BUY_SEPA2 × 弱気フィルター比較（保有30日・利確25%） ──
    print("\n" + "=" * 70)
    print("  結果① BUY_SEPA2：弱気フィルター比較（保有30日・損切り-15%・利確25%）")
    print("=" * 70)
    rows = []
    for skip in [False, True]:
        grp = df_all[
            (df_all["strategy"]   == "BUY_SEPA2") &
            (df_all["max_hold"]   == 30) &
            (df_all["stop_pct"]   == 0.15) &
            (df_all["target_pct"] == 0.25) &
            (df_all["skip_bear"]  == skip)
        ]
        if grp.empty:
            continue
        rows.append({
            "条件":       "弱気除外" if skip else "弱気含む（ベースライン）",
            "件数":       len(grp),
            "勝率(%)":   round(grp["win"].mean() * 100, 1),
            "平均R(%)":  round(grp["return_pct"].mean(), 2),
            "中央値R(%)": round(grp["return_pct"].median(), 2),
        })
    _print_comparison("", rows)

    # ── 結果② BUY_SEPA2 × 利確ターゲット比較（保有30日・弱気含む） ──
    print("\n" + "=" * 70)
    print("  結果② BUY_SEPA2：利確ターゲット比較（保有30日・損切り-15%・弱気含む）")
    print("=" * 70)
    rows = []
    for tgt in TARGET_LIST:
        grp = df_all[
            (df_all["strategy"]   == "BUY_SEPA2") &
            (df_all["max_hold"]   == 30) &
            (df_all["stop_pct"]   == 0.15) &
            (df_all["target_pct"] == tgt) &
            (df_all["skip_bear"]  == False)
        ]
        if grp.empty:
            continue
        exit_counts = grp["exit_reason"].value_counts().to_dict()
        rows.append({
            "利確":        f"+{int(tgt*100)}%",
            "件数":        len(grp),
            "勝率(%)":    round(grp["win"].mean() * 100, 1),
            "平均R(%)":   round(grp["return_pct"].mean(), 2),
            "中央値R(%)":  round(grp["return_pct"].median(), 2),
            "利確発動(件)": exit_counts.get("target", 0),
            "損切発動(件)": exit_counts.get("stop", 0),
        })
    _print_comparison("", rows)

    # ── 結果③ 最良組み合わせ（弱気除外 × 利確ターゲット）──────────
    print("\n" + "=" * 70)
    print("  結果③ BUY_SEPA2：弱気除外 × 利確ターゲット 全組み合わせ（保有30日）")
    print("=" * 70)
    rows = []
    for tgt in TARGET_LIST:
        for skip in [False, True]:
            grp = df_all[
                (df_all["strategy"]   == "BUY_SEPA2") &
                (df_all["max_hold"]   == 30) &
                (df_all["stop_pct"]   == 0.15) &
                (df_all["target_pct"] == tgt) &
                (df_all["skip_bear"]  == skip)
            ]
            if grp.empty:
                continue
            rows.append({
                "弱気":       "除外" if skip else "含む",
                "利確":       f"+{int(tgt*100)}%",
                "件数":       len(grp),
                "勝率(%)":   round(grp["win"].mean() * 100, 1),
                "平均R(%)":  round(grp["return_pct"].mean(), 2),
                "中央値R(%)": round(grp["return_pct"].median(), 2),
            })
    _print_comparison("", rows)

    # ── 結果④ SEPA2単体でも同様に確認（保有30日） ──────────────
    print("\n" + "=" * 70)
    print("  結果④ SEPA2単体：弱気除外 × 利確ターゲット（保有30日）")
    print("=" * 70)
    rows = []
    for tgt in TARGET_LIST:
        for skip in [False, True]:
            grp = df_all[
                (df_all["strategy"]   == "SEPA2") &
                (df_all["max_hold"]   == 30) &
                (df_all["stop_pct"]   == 0.15) &
                (df_all["target_pct"] == tgt) &
                (df_all["skip_bear"]  == skip)
            ]
            if grp.empty:
                continue
            rows.append({
                "弱気":       "除外" if skip else "含む",
                "利確":       f"+{int(tgt*100)}%",
                "件数":       len(grp),
                "勝率(%)":   round(grp["win"].mean() * 100, 1),
                "平均R(%)":  round(grp["return_pct"].mean(), 2),
                "中央値R(%)": round(grp["return_pct"].median(), 2),
            })
    _print_comparison("", rows)

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

    # ── CSV保存 ──────────────────────────────────────────────
    df_all.to_csv("data/backtest_pipeline_records.csv", index=False, encoding="utf-8-sig")
    if not df_rev.empty:
        df_rev.to_csv("data/backtest_revision_records.csv", index=False, encoding="utf-8-sig")
        print("上方修正詳細CSV: data/backtest_revision_records.csv")
    summary_rows = []
    for keys, grp in df_all.groupby(["strategy", "max_hold", "stop_pct", "target_pct", "skip_bear"]):
        summary_rows.append({
            "戦略": keys[0], "保有日数": keys[1], "損切り": keys[2],
            "利確": keys[3], "弱気除外": keys[4],
            "件数": len(grp),
            "勝率(%)": round(grp["win"].mean() * 100, 1),
            "平均R(%)": round(grp["return_pct"].mean(), 2),
            "中央値R(%)": round(grp["return_pct"].median(), 2),
        })
    pd.DataFrame(summary_rows).to_csv("data/backtest_pipeline_summary.csv", index=False, encoding="utf-8-sig")
    print("\n詳細CSV: data/backtest_pipeline_records.csv")
    print("サマリー: data/backtest_pipeline_summary.csv")
    print("\n完了")


if __name__ == "__main__":
    main()
