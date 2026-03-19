# -*- coding: utf-8 -*-
"""
Japan Stock Screener
Indicators: Value / Technical / Fundamental / Liquidity
Data source: J-Quants API v2
"""

import os
import time
import logging
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


@dataclass
class ScreeningCriteria:
    per_max: float = 20.0
    pbr_max: float = 1.5
    pbr_min: float = 0.5
    dividend_yield_min: float = 2.0
    revenue_growth_min: float = 5.0
    profit_growth_min: float = 5.0
    roe_min: float = 8.0
    rsi_min: float = 40.0
    rsi_max: float = 70.0
    above_ma25: bool = True
    volume_avg_min: int = 100000


class JQuantsClient:
    BASE_URL = "https://api.jquants.com/v2"

    def __init__(self):
        api_key = os.getenv("JQUANTS_API_KEY")
        if not api_key:
            raise EnvironmentError(".env に JQUANTS_API_KEY が設定されていません")
        self.session = requests.Session()
        self.session.headers.update({"x-api-key": api_key})

    def _get(self, endpoint, params=None, _retry=3):
        for attempt in range(_retry):
            r = self.session.get(
                f"{self.BASE_URL}{endpoint}",
                params=params or {},
            )
            if r.status_code == 429:
                wait = 60
                logger.warning(f"Rate limit hit. Waiting {wait}s...")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        r.raise_for_status()

    def get_listed_info(self):
        data = self._get("/equities/master")
        return pd.DataFrame(data.get("data", []))

    def get_daily_quotes(self, code, from_date, to_date):
        data = self._get("/equities/bars/daily", {"code": code, "from": from_date, "to": to_date})
        df = pd.DataFrame(data.get("data", []))
        if df.empty:
            return df
        df["Date"] = pd.to_datetime(df["Date"])
        # V2 uses abbreviated column names; use split-adjusted values
        df = df.rename(columns={"AdjC": "Close", "AdjVo": "Volume"})
        for col in ["Close", "Volume"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        return df.sort_values("Date").reset_index(drop=True)

    def get_financials(self, code):
        data = self._get("/fins/summary", {"code": code})
        df = pd.DataFrame(data.get("data", []))
        if df.empty:
            return df
        # Prefer annual (FY) reports for growth/ratio calculations
        if "CurPerType" in df.columns:
            annual = df[df["CurPerType"] == "FY"]
            if not annual.empty:
                return annual.reset_index(drop=True)
        return df


def calc_rsi(series, period=14):
    delta = series.diff().dropna()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))
    return float(rsi.iloc[-1]) if len(rsi) > 0 else np.nan


def calc_moving_average(series, window):
    ma = series.rolling(window).mean()
    return float(ma.iloc[-1]) if len(ma) >= window else np.nan


def calc_avg_volume(series, days=20):
    return float(series.tail(days).mean())


def calc_signal_score(close, lookback=5):
    """テクニカルシグナルスコアを計算する（最大45点）"""
    if len(close) < 14:
        return 0.0, []
    score = 0.0
    labels = []

    ma5 = close.rolling(5, min_periods=1).mean()
    ma25 = close.rolling(25, min_periods=1).mean()
    gc = (ma5 > ma25) & (ma5.shift(1) <= ma25.shift(1))
    if gc.iloc[-lookback:].any():
        score += 15
        labels.append("GC")

    if len(close) >= 75:
        ma75 = close.rolling(75, min_periods=1).mean()
        gc75 = (ma25 > ma75) & (ma25.shift(1) <= ma75.shift(1))
        if gc75.iloc[-lookback:].any():
            score += 10
            labels.append("GC(中期)")

    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    sig = macd.ewm(span=9, adjust=False).mean()
    if ((macd > sig) & (macd.shift(1) <= sig.shift(1))).iloc[-lookback:].any():
        score += 10
        labels.append("MACD買転換")

    delta = close.diff()
    gain = delta.clip(lower=0).rolling(14, min_periods=1).mean()
    loss = (-delta.clip(upper=0)).rolling(14, min_periods=1).mean()
    rsi_s = 100 - (100 / (1 + gain / loss.replace(0, float("nan"))))
    if ((rsi_s > 30) & (rsi_s.shift(1) <= 30)).iloc[-lookback:].any():
        score += 10
        labels.append("RSI反転")

    return round(score, 1), labels


def evaluate_stock(code, client, criteria, from_date, to_date):
    try:
        price_df = client.get_daily_quotes(code, from_date, to_date)
        if price_df.empty or len(price_df) < 20:
            return None

        close = price_df["Close"]
        volume = price_df["Volume"]
        latest_close = float(close.iloc[-1])

        avg_vol = calc_avg_volume(volume)
        if avg_vol < criteria.volume_avg_min:
            return None

        rsi = calc_rsi(close)
        ma25 = calc_moving_average(close, 25)

        if np.isnan(rsi) or rsi < criteria.rsi_min or rsi > criteria.rsi_max:
            return None
        if criteria.above_ma25 and (np.isnan(ma25) or latest_close < ma25):
            return None

        fin_df = client.get_financials(code)
        if fin_df.empty:
            return None
        fin_df = fin_df.sort_values("DiscDate", ascending=False)
        latest = fin_df.iloc[0]

        # PER = 株価 / EPS
        eps = pd.to_numeric(latest.get("EPS"), errors="coerce")
        per = latest_close / eps if (not np.isnan(eps) and eps > 0) else np.nan

        # PBR = 株価 / BPS (BPS = 純資産 / 発行済株式数)
        eq = pd.to_numeric(latest.get("Eq"), errors="coerce")
        sh_out = pd.to_numeric(latest.get("ShOutFY"), errors="coerce")
        bps = eq / sh_out if (not np.isnan(eq) and not np.isnan(sh_out) and sh_out > 0) else np.nan
        pbr = latest_close / bps if (not np.isnan(bps) and bps > 0) else np.nan

        # ROE = 当期純利益 / 純資産 * 100
        np_val = pd.to_numeric(latest.get("NP"), errors="coerce")
        roe = np_val / eq * 100 if (not np.isnan(np_val) and not np.isnan(eq) and eq > 0) else np.nan

        # 配当利回り = 予想年間配当 / 株価 * 100
        div_ann = pd.to_numeric(latest.get("FDivAnn"), errors="coerce")
        if np.isnan(div_ann):
            div_ann = pd.to_numeric(latest.get("DivAnn"), errors="coerce")
        div_yield = div_ann / latest_close * 100 if (not np.isnan(div_ann) and latest_close > 0) else np.nan

        if np.isnan(per) or per <= 0 or per > criteria.per_max:
            return None
        if np.isnan(pbr) or pbr > criteria.pbr_max or pbr < criteria.pbr_min:
            return None
        if np.isnan(div_yield) or div_yield < criteria.dividend_yield_min:
            return None
        if np.isnan(roe) or roe < criteria.roe_min:
            return None

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

        if np.isnan(revenue_growth) or revenue_growth < criteria.revenue_growth_min:
            return None
        if np.isnan(profit_growth) or profit_growth < criteria.profit_growth_min:
            return None

        score = 0.0
        score += max(0, (20 - per) / 20 * 25)
        score += max(0, (1.5 - pbr) / 1.5 * 15)
        score += min(roe / 20 * 20, 20)
        score += min(revenue_growth / 20 * 20, 20)
        score += max(0, 10 - abs(rsi - 50) / 5)

        sig_score, sig_labels = calc_signal_score(close)

        return {
            "code": code,
            "close": latest_close,
            "score": round(score, 1),
            "signal_score": sig_score,
            "signals": ", ".join(sig_labels) if sig_labels else "−",
            "PER": round(per, 2),
            "PBR": round(pbr, 2),
            "ROE": round(roe, 2),
            "div_yield": round(div_yield, 2),
            "rev_growth": round(revenue_growth, 1),
            "profit_growth": round(profit_growth, 1),
            "RSI": round(rsi, 1),
            "MA25": round(ma25, 1),
            "avg_volume": int(avg_vol),
        }

    except Exception as e:
        logger.debug(f"{code} error: {e}")
        return None


class StockScreener:
    def __init__(self, criteria=None):
        self.criteria = criteria or ScreeningCriteria()
        self.client = JQuantsClient()

    def run(self, market_codes=None, max_stocks=200, delay_sec=0.3, to_date=None, from_date=None):
        to_date = to_date or datetime.today().strftime("%Y-%m-%d")
        from_date = from_date or (datetime.strptime(to_date, "%Y-%m-%d") - timedelta(days=120)).strftime("%Y-%m-%d")

        logger.info("Getting listed stocks...")
        listed = self.client.get_listed_info()

        if market_codes:
            listed = listed[listed["Mkt"].isin(market_codes)]

        codes = listed["Code"].dropna().unique().tolist()[:max_stocks]
        logger.info(f"Screening {len(codes)} stocks...")

        results = []
        for i, code in enumerate(codes, 1):
            result = evaluate_stock(code, self.client, self.criteria, from_date, to_date)
            if result:
                results.append(result)
                logger.info(f"[{i}/{len(codes)}] HIT: {code}  score={result['score']}")
            else:
                logger.debug(f"[{i}/{len(codes)}] skip: {code}")
            time.sleep(delay_sec)

        if not results:
            logger.warning("No stocks matched the criteria.")
            return pd.DataFrame()

        df = pd.DataFrame(results).sort_values("score", ascending=False).reset_index(drop=True)
        logger.info(f"Done: {len(df)} stocks matched")
        return df


if __name__ == "__main__":
    criteria = ScreeningCriteria(
        per_max=20.0,
        pbr_max=1.5,
        pbr_min=0.5,
        dividend_yield_min=2.0,
        revenue_growth_min=5.0,
        profit_growth_min=5.0,
        roe_min=8.0,
        rsi_min=40.0,
        rsi_max=70.0,
        above_ma25=True,
        volume_avg_min=100000,
    )

    screener = StockScreener(criteria)
    result_df = screener.run(market_codes=["0111"], max_stocks=10)

    if not result_df.empty:
        print("\n" + "=" * 60)
        print("  Screening Results (Top 20)")
        print("=" * 60)
        print(result_df.head(20).to_string(index=False))

        out_path = f"screening_result_{datetime.today().strftime('%Y%m%d')}.csv"
        result_df.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"\nSaved: {out_path}")
