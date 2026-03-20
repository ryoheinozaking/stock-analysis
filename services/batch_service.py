# -*- coding: utf-8 -*-
"""
全銘柄メトリクス一括取得バッチ
"""

import os
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

from screener import JQuantsClient, calc_rsi, calc_moving_average, calc_avg_volume, calc_signal_score
from services.jquants_service import get_listed_info

CACHE_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "stock_cache.csv")


def load_cache():
    """キャッシュCSVを読み込む。なければNoneを返す。"""
    if os.path.exists(CACHE_PATH):
        df = pd.read_csv(CACHE_PATH, dtype={"code_4": str, "code": str})
        return df
    return None


def get_cache_updated_at():
    """キャッシュの最終更新日時を返す。なければNone。"""
    if os.path.exists(CACHE_PATH):
        return datetime.fromtimestamp(os.path.getmtime(CACHE_PATH))
    return None


def _fetch_stock_metrics(code, client, from_date, to_date):
    """フィルタなしで1銘柄のメトリクスを計算して返す。データ不足はNone。"""
    try:
        price_df = client.get_daily_quotes(code, from_date, to_date)
        if price_df.empty or len(price_df) < 20:
            return None

        close = price_df["Close"]
        volume = price_df["Volume"]
        latest_close = float(close.iloc[-1])
        latest_volume = int(volume.iloc[-1])
        avg_vol = calc_avg_volume(volume)
        rsi = calc_rsi(close)
        ma25 = calc_moving_average(close, 25)

        fin_df = client.get_financials(code)
        if fin_df.empty:
            return None
        fin_df = fin_df.sort_values("DiscDate", ascending=False)
        latest = fin_df.iloc[0]

        eps = pd.to_numeric(latest.get("EPS"), errors="coerce")
        per = latest_close / eps if (not np.isnan(eps) and eps > 0) else np.nan

        eq = pd.to_numeric(latest.get("Eq"), errors="coerce")
        sh_out = pd.to_numeric(latest.get("ShOutFY"), errors="coerce")
        bps = eq / sh_out if (not np.isnan(eq) and not np.isnan(sh_out) and sh_out > 0) else np.nan
        pbr = latest_close / bps if (not np.isnan(bps) and bps > 0) else np.nan

        np_val = pd.to_numeric(latest.get("NP"), errors="coerce")
        roe = np_val / eq * 100 if (not np.isnan(np_val) and not np.isnan(eq) and eq > 0) else np.nan

        div_ann = pd.to_numeric(latest.get("FDivAnn"), errors="coerce")
        if np.isnan(div_ann):
            div_ann = pd.to_numeric(latest.get("DivAnn"), errors="coerce")
        div_yield = div_ann / latest_close * 100 if (not np.isnan(div_ann) and latest_close > 0) else np.nan

        revenue_growth = np.nan
        profit_growth = np.nan
        if len(fin_df) >= 2:
            curr = fin_df.iloc[0]
            prev = fin_df.iloc[1]
            rev_c = pd.to_numeric(curr.get("Sales"), errors="coerce")
            rev_p = pd.to_numeric(prev.get("Sales"), errors="coerce")
            prf_c = pd.to_numeric(curr.get("NP"), errors="coerce")
            prf_p = pd.to_numeric(prev.get("NP"), errors="coerce")
            if not np.isnan(rev_p) and rev_p != 0:
                revenue_growth = (rev_c - rev_p) / abs(rev_p) * 100
            if not np.isnan(prf_p) and prf_p != 0:
                profit_growth = (prf_c - prf_p) / abs(prf_p) * 100

        # スコア計算（NaN安全）
        score = 0.0
        if not np.isnan(per) and per > 0:
            score += max(0, (20 - per) / 20 * 25)
        if not np.isnan(pbr):
            score += max(0, (1.5 - pbr) / 1.5 * 15)
        if not np.isnan(roe):
            score += min(roe / 20 * 20, 20)
        if not np.isnan(revenue_growth):
            score += min(revenue_growth / 20 * 20, 20)
        if not np.isnan(rsi):
            score += max(0, 10 - abs(rsi - 50) / 5)

        sig_score, sig_labels = calc_signal_score(close)

        return {
            "code": code,
            "code_4": code[:4],
            "close": round(latest_close, 1),
            "score": round(score, 1),
            "signal_score": sig_score,
            "signals": ", ".join(sig_labels) if sig_labels else "−",
            "PER": round(per, 2) if not np.isnan(per) else np.nan,
            "PBR": round(pbr, 2) if not np.isnan(pbr) else np.nan,
            "ROE": round(roe, 2) if not np.isnan(roe) else np.nan,
            "div_yield": round(div_yield, 2) if not np.isnan(div_yield) else np.nan,
            "rev_growth": round(revenue_growth, 1) if not np.isnan(revenue_growth) else np.nan,
            "profit_growth": round(profit_growth, 1) if not np.isnan(profit_growth) else np.nan,
            "RSI": round(rsi, 1) if not np.isnan(rsi) else np.nan,
            "MA25": round(ma25, 1) if not np.isnan(ma25) else np.nan,
            "avg_volume": int(avg_vol),
            "latest_volume": latest_volume,
        }
    except Exception:
        return None


def fetch_all_stocks(market_codes=None, progress_callback=None):
    """
    全銘柄（または指定市場）のメトリクスを取得してCSVに保存する。

    Args:
        market_codes: 対象市場コードリスト（Noneなら全市場）
        progress_callback: (current, total, code) を受け取る関数

    Returns:
        pd.DataFrame
    """
    client = JQuantsClient()
    listed_df = get_listed_info()

    if market_codes:
        listed_df = listed_df[listed_df["Mkt"].isin(market_codes)]

    codes = listed_df["Code"].dropna().unique().tolist()
    to_date = datetime.today().strftime("%Y-%m-%d")
    from_date = (datetime.today() - timedelta(days=120)).strftime("%Y-%m-%d")

    results = []
    total = len(codes)

    for i, code in enumerate(codes):
        if progress_callback:
            progress_callback(i, total, code)

        result = _fetch_stock_metrics(code, client, from_date, to_date)
        if result:
            info = listed_df[listed_df["Code"] == code]
            if not info.empty:
                result["company_name"] = info.iloc[0].get("CoName", "")
                result["market"] = info.iloc[0].get("MktNm", "")
                result["sector"] = info.iloc[0].get("S33Nm", info.iloc[0].get("S17Nm", ""))
            results.append(result)

    df = pd.DataFrame(results) if results else pd.DataFrame()
    os.makedirs(os.path.dirname(CACHE_PATH), exist_ok=True)
    df.to_csv(CACHE_PATH, index=False, encoding="utf-8-sig")
    return df
