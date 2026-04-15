# -*- coding: utf-8 -*-
"""
モメンタム系シグナル バックテスト (Phase 2)
シグナル: GC / 52週高値ブレイク / 出来高急増 / MACD / SEPA Stage2 / Granville G1/G2
損切り比較: -8% / -15% / なし
"""

import bisect
import os
import sys
import time
import logging
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from screener import JQuantsClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ── 設定 ─────────────────────────────────────────────
UNIVERSE_SIZE  = 200
FROM_DATE      = "2021-01-01"   # MA200計算のために十分な過去データを確保
BACKTEST_FROM  = "2022-07-01"   # バックテスト開始日（MA200が安定してから）
TO_DATE        = datetime.today().strftime("%Y-%m-%d")
HOLDING_DAYS   = [5, 10, 20]
STOP_LOSSES    = [-8.0, -15.0, None]   # None = 損切りなし
COST_PCT       = 0.2
PRICE_DIR      = Path("data/backtest_prices")
MIN_HIST_ROWS  = 400   # MA200+バックテスト期間として最低必要な行数
# ─────────────────────────────────────────────────────


def _build_revision_events(fins_df: pd.DataFrame, threshold_pct: float = 20.0) -> dict:
    """fins_cache から上方修正イベントを抽出。{code_5digit: [Timestamp, ...]}"""
    events: dict = {}
    work = fins_df.copy()
    work["DiscDate"] = pd.to_datetime(work["DiscDate"], errors="coerce")
    work["FEPS"]     = pd.to_numeric(work["FEPS"],     errors="coerce")
    if "CurPerType" in work.columns:
        work = work[work["CurPerType"] == "FY"]
    valid = work.dropna(subset=["DiscDate", "FEPS"]).query("FEPS > 0")
    for code, grp in valid.groupby("Code"):
        grp = grp.sort_values("DiscDate").reset_index(drop=True)
        dates = []
        for i in range(1, len(grp)):
            prev = float(grp.loc[i - 1, "FEPS"])
            curr = float(grp.loc[i,     "FEPS"])
            if prev > 0 and (curr - prev) / abs(prev) * 100 >= threshold_pct:
                dates.append(grp.loc[i, "DiscDate"])
        if dates:
            events[str(code).zfill(5)] = dates
    return events


def _has_revision(revision_events: dict, code: str, entry_date, window_days: int) -> bool:
    """entry_date から window_days 日以内に上方修正イベントがあれば True。"""
    if revision_events is None:
        return False
    event_dates = revision_events.get(code, [])
    if not event_dates:
        return False
    ts     = pd.Timestamp(entry_date)
    cutoff = ts - pd.Timedelta(days=window_days)
    lo = bisect.bisect_left(event_dates, cutoff)
    hi = bisect.bisect_right(event_dates, ts)
    return lo < hi


def load_universe():
    df = pd.read_parquet("data/stock_cache.parquet")
    top = df.sort_values("score", ascending=False).head(UNIVERSE_SIZE)
    codes = top["code"].astype(str).str.zfill(5).tolist()
    logger.info(f"Universe: {len(codes)} stocks")
    return codes


def fetch_price(code, client):
    path = PRICE_DIR / f"{code}.csv"
    df   = pd.DataFrame()
    needs_fetch = True

    if path.exists():
        try:
            cached = pd.read_csv(path, parse_dates=["Date"])
            if len(cached) >= MIN_HIST_ROWS:
                return cached
        except Exception:
            pass

    try:
        df = client.get_daily_quotes(code, FROM_DATE, TO_DATE)
        if not df.empty:
            df.to_csv(path, index=False)
        time.sleep(0.35)
    except Exception as e:
        logger.debug(f"Skip {code}: {e}")

    return df


def calc_signals(df, revision_events=None):
    close  = pd.to_numeric(df["Close"],  errors="coerce")
    volume = pd.to_numeric(df["Volume"], errors="coerce").fillna(0)

    # ── MA系 ──────────────────────────────────────
    ma5   = close.rolling(5,   min_periods=3).mean()
    ma25  = close.rolling(25,  min_periods=15).mean()
    ma50  = close.rolling(50,  min_periods=30).mean()
    ma150 = close.rolling(150, min_periods=80).mean()
    ma200 = close.rolling(200, min_periods=120).mean()

    # ── 既存シグナル ──────────────────────────────
    gc = (ma5 > ma25) & (ma5.shift(1) <= ma25.shift(1))

    high_252 = close.shift(1).rolling(252, min_periods=60).max()
    new_high  = close > high_252

    avg_vol   = volume.shift(1).rolling(20, min_periods=10).mean()
    vol_surge = volume > avg_vol * 2

    ema12      = close.ewm(span=12, adjust=False).mean()
    ema26      = close.ewm(span=26, adjust=False).mean()
    macd       = ema12 - ema26
    macd_sig   = macd.ewm(span=9, adjust=False).mean()
    macd_cross = (macd > macd_sig) & (macd.shift(1) <= macd_sig.shift(1))

    combo = (gc.astype(int) + new_high.astype(int)
             + vol_surge.astype(int) + macd_cross.astype(int)) >= 2

    # ── SEPA Stage2 ───────────────────────────────
    ma200_up   = ma200 > ma200.shift(20)
    ma_align   = (ma50 > ma150) & (ma150 > ma200)
    above_ma50 = close > ma50
    low_252    = close.shift(1).rolling(252, min_periods=100).min()
    high_252b  = close.shift(1).rolling(252, min_periods=100).max()
    from_low   = (close - low_252)   / low_252   * 100
    from_high  = (close - high_252b) / high_252b * 100
    sepa2 = (ma_align & ma200_up & above_ma50
             & (from_low >= 25) & (from_high >= -25))

    # ── Granville G1/G2 ──────────────────────────
    gran_g1 = ((close > ma200) & (close.shift(1) <= ma200.shift(1))
               & ma200.notna())
    near_ma200 = (close >= ma200 * 0.95) & (close <= ma200 * 1.05)
    gran_g2    = near_ma200 & (close > close.shift(1)) & ma200.notna()

    sig_df = pd.DataFrame({
        "Close"     : close.values,
        "GC"        : gc.values,
        "NewHigh52W": new_high.values,
        "VolSurge"  : vol_surge.values,
        "MACDCross" : macd_cross.values,
        "Combo2+"   : combo.values,
        "SEPA2"     : sepa2.values,
        "GranG1"    : gran_g1.values,
        "GranG2"    : gran_g2.values,
    }, index=df["Date"].values)

    # 上方修正シグナル（fins_cache がある場合のみ追加）
    if revision_events is not None:
        code = str(df["Code"].iloc[0]).zfill(5) if "Code" in df.columns else ""
        for w in [30, 60, 90]:
            col = f"REVISION_{w}"
            sig_df[col] = [
                _has_revision(revision_events, code, d, w)
                for d in sig_df.index
            ]

    # バックテスト開始日以降のみ対象
    bt_from = pd.Timestamp(BACKTEST_FROM)
    return sig_df[sig_df.index >= bt_from]


def run_backtest_signal(sig_df, signal_col, stop_pct=None):
    closes  = sig_df["Close"].values
    signals = sig_df[signal_col].values
    dates   = sig_df.index.tolist()
    records = []

    for i, (date, close, signal) in enumerate(zip(dates, closes, signals)):
        if not signal or pd.isna(close) or close == 0:
            continue
        for hold in HOLDING_DAYS:
            j = i + hold
            if j >= len(closes):
                continue

            exit_idx = j
            stop_hit = False

            # 損切りチェック: 保有期間中の各終値を確認
            if stop_pct is not None:
                stop_price = close * (1 + stop_pct / 100)
                for k in range(i + 1, j + 1):
                    if not np.isnan(closes[k]) and closes[k] <= stop_price:
                        exit_idx = k
                        stop_hit = True
                        break

            exit_price = closes[exit_idx] if not stop_hit else close * (1 + stop_pct / 100)
            if pd.isna(exit_price):
                continue

            gross = (exit_price - close) / close * 100
            net   = gross - COST_PCT
            records.append({
                "date"      : date,
                "signal"    : signal_col,
                "hold_days" : hold,
                "stop_pct"  : f"{stop_pct:.0f}%" if stop_pct is not None else "none",
                "return_pct": round(net, 3),
                "win"       : net > 0,
                "stop_hit"  : stop_hit,
            })
    return records


def summarize(df):
    rows = []
    for (sig, hold, stop), grp in df.groupby(["signal", "hold_days", "stop_pct"]):
        stop_hit_rate = grp["stop_hit"].mean() * 100 if "stop_hit" in grp.columns else 0
        rows.append({
            "シグナル"         : sig,
            "保有日数"         : int(hold),
            "損切り"           : stop,
            "トレード数"       : len(grp),
            "勝率(%)"         : round(grp["win"].mean() * 100, 1),
            "平均リターン(%)": round(grp["return_pct"].mean(), 2),
            "中央値(%)"       : round(grp["return_pct"].median(), 2),
            "最大利益(%)"     : round(grp["return_pct"].max(), 2),
            "最大損失(%)"     : round(grp["return_pct"].min(), 2),
            "損切り発動率(%)": round(stop_hit_rate, 1),
        })
    return pd.DataFrame(rows).sort_values(["シグナル", "保有日数", "損切り"])


def main():
    PRICE_DIR.mkdir(parents=True, exist_ok=True)
    client   = JQuantsClient()
    universe = load_universe()

    # fins_cache 読み込み（上方修正シグナル用）
    fins_path = Path("data/fins_cache.parquet")
    revision_events = None
    if fins_path.exists():
        fins_df = pd.read_parquet(fins_path)
        revision_events = _build_revision_events(fins_df)
        rev_total = sum(len(v) for v in revision_events.values())
        logger.info(f"上方修正イベント: {len(revision_events)} 銘柄 / {rev_total} 件")

    base_signals = ["GC", "NewHigh52W", "VolSurge", "MACDCross", "Combo2+", "SEPA2", "GranG1", "GranG2"]
    rev_signals  = ["REVISION_30", "REVISION_60", "REVISION_90"] if revision_events else []
    signals      = base_signals + rev_signals
    all_records  = []
    fetch_count  = 0

    for i, code in enumerate(universe, 1):
        df = fetch_price(code, client)
        if df.empty or len(df) < 60:
            logger.debug(f"Skip {code}: insufficient data")
            continue
        if fetch_count > 0 and fetch_count % 50 == 0:
            logger.info(f"Fetched {fetch_count} stocks from API so far")

        # Code列を付与（revision_events の照合用）
        if "Code" not in df.columns:
            df = df.copy()
            df["Code"] = code

        try:
            sig_df = calc_signals(df, revision_events)
        except Exception as e:
            logger.warning(f"Signal error {code}: {e}")
            continue

        if sig_df.empty:
            continue

        for col in signals:
            for stop in STOP_LOSSES:
                all_records.extend(run_backtest_signal(sig_df, col, stop))

        if i % 20 == 0:
            logger.info(f"Progress: {i}/{len(universe)}")

    if not all_records:
        logger.error("トレード記録なし")
        return

    df_records = pd.DataFrame(all_records)
    df_records.to_csv("data/backtest_records.csv", index=False, encoding="utf-8-sig")

    summary = summarize(df_records)
    summary.to_csv("data/backtest_summary.csv", index=False, encoding="utf-8-sig")

    print("\n" + "=" * 100)
    print("  バックテスト結果（コスト0.2%差引済み）")
    print(f"  データ期間: {FROM_DATE} ～ {TO_DATE}")
    print(f"  バックテスト期間: {BACKTEST_FROM} ～ {TO_DATE}")
    print(f"  ユニバース: スコア上位{UNIVERSE_SIZE}銘柄")
    print("=" * 100)
    print(summary.to_string(index=False))
    print("\n詳細CSV: data/backtest_records.csv")
    print("サマリー: data/backtest_summary.csv")


if __name__ == "__main__":
    main()
