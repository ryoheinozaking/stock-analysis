#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
深層分析用 J-Quants データ取得ヘルパー

Usage:
    .venv/Scripts/python.exe scripts/deep_analysis_helper.py <ticker> [--source local|api] [--save]

ticker は 4桁数字（例: 7974）、5桁（例: 79740）、英数字混合（例: 290A）いずれも可。
J-Quants v2 の 5桁コード（末尾0付与）に自動変換する。

データソース:
  --source local (デフォルト): data/prices.parquet + data/fins_cache.parquet を直読み
    Streamlit パイプラインで取得済みのキャッシュを再利用。Claude Code Bash の
    SSL 制限を回避できる。Streamlit 起動で「データ更新」ボタンを定期実行している
    前提（現状 2026-05-18 までカバー、銘柄数 5,012）。
  --source api: J-Quants v2 を直接呼び出す。Claude Code Bash では SSL エラーで
    動作しないが、ユーザのターミナル直接実行では動く。--refresh 用途。

Output: JSON to stdout（深層分析レポートで利用）

含まれる情報:
- 最新終値・日付・5/20/60 日リターン
- MA5/25/60/200 と乖離率（split_adjust 正規化済み）
- RSI(14)・MACD(12/26/9)・MA25/200 上下判定
- 52週高値・安値・現価との乖離
- 5日/20日平均出来高・倍率
- 過去 2 年の分割イベント一覧
- 直近 5-7 期の決算（売上・OP・NP・EPS・BPS・年間配当・通期予想）

【重要】CLAUDE.md の警告に従い、AdjC をそのまま使わず生 C × AdjFactor で
末尾日スケールに正規化（services/split_adjust.normalize_close）。
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

from services.split_adjust import (  # noqa: E402
    normalize_close,
    normalize_high,
    normalize_low,
    normalize_volume,
    forecast_per_share_multiplier,
    split_factor_between,
)
from services.fins_utils import filter_fy_statements  # noqa: E402


class RawJQuantsClient:
    """生 OHLC + AdjFactor を保持するクライアント。

    screener.JQuantsClient は AdjC を Close に rename して
    生 C を捨てているため、長期テクニカル指標の計算には使えない。
    こちらは AdjC ではなく C + AdjFactor を保持し、split_adjust で正規化する。
    """

    BASE_URL = "https://api.jquants.com/v2"

    def __init__(self):
        api_key = os.getenv("JQUANTS_API_KEY")
        if not api_key:
            raise SystemExit("JQUANTS_API_KEY not set in .env")
        self.s = requests.Session()
        self.s.headers.update({"x-api-key": api_key})

    def _get(self, endpoint, params=None, retries=3):
        for _ in range(retries):
            r = self.s.get(f"{self.BASE_URL}{endpoint}", params=params or {})
            if r.status_code == 429:
                time.sleep(60)
                continue
            r.raise_for_status()
            return r.json()
        r.raise_for_status()

    def get_daily_raw(self, code, from_date, to_date):
        data = self._get(
            "/equities/bars/daily",
            {"code": code, "from": from_date, "to": to_date},
        )
        df = pd.DataFrame(data.get("data", []))
        if df.empty:
            return df
        df["Date"] = pd.to_datetime(df["Date"])
        for c in ["O", "H", "L", "C", "Vo", "Va", "AdjFactor"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        return df.sort_values("Date").reset_index(drop=True)

    def get_financials(self, code):
        data = self._get("/fins/summary", {"code": code})
        return pd.DataFrame(data.get("data", []))

    def get_listed_info(self, code):
        data = self._get("/equities/master", {"code": code})
        df = pd.DataFrame(data.get("data", []))
        return df


def to_code5(ticker):
    """4桁→5桁変換（末尾0付与）。英数字混合（290A 等）にも対応。"""
    t = str(ticker).strip().upper()
    if len(t) == 4:
        return t + "0"
    if len(t) == 5:
        return t
    raise ValueError(f"Unknown ticker format: {ticker}")


class LocalParquetClient:
    """data/prices.parquet + data/fins_cache.parquet を直読みする一次情報クライアント。

    Streamlit パイプラインがメンテしているローカルキャッシュを再利用するため、
    Claude Code Bash の SSL 制限を回避できる。
    """

    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self._prices = None
        self._fins = None
        self._stock_cache = None

    def _load_stock_cache(self):
        if self._stock_cache is None:
            p = self.data_dir / "stock_cache.parquet"
            if not p.exists():
                # 致命的ではない（株価・財務だけでも分析は走る）
                return None
            self._stock_cache = pd.read_parquet(p)
        return self._stock_cache

    def get_stock_cache_row(self, code5):
        """指定銘柄の stock_cache 行を dict で返す（PER/PBR/ROE/RSI/Altman Z/SEPA stage 等）"""
        df = self._load_stock_cache()
        if df is None:
            return None
        sub = df[df["code"] == code5]
        if len(sub) == 0:
            # code (5桁) でヒットしない場合 code_4 でも試す
            code4 = code5.rstrip("0") if code5[-1] == "0" else code5
            sub = df[df["code_4"] == code4]
        if len(sub) == 0:
            return None
        return sub.iloc[0].to_dict()

    def get_sector_peers(self, sector, market=None):
        """同セクター・同市場の peer DataFrame を返す（自銘柄も含む）"""
        df = self._load_stock_cache()
        if df is None:
            return None
        peers = df[df["sector"] == sector]
        if market is not None:
            peers = peers[peers["market"] == market]
        return peers.reset_index(drop=True)

    def _load_prices(self):
        if self._prices is None:
            p = self.data_dir / "prices.parquet"
            if not p.exists():
                raise FileNotFoundError(
                    f"{p} not found. Streamlit でスクリーナーを更新してください。"
                )
            self._prices = pd.read_parquet(p)
            self._prices["Date"] = pd.to_datetime(self._prices["Date"])
        return self._prices

    def _load_fins(self):
        if self._fins is None:
            p = self.data_dir / "fins_cache.parquet"
            if not p.exists():
                raise FileNotFoundError(
                    f"{p} not found. Streamlit でスクリーナーを更新してください。"
                )
            self._fins = pd.read_parquet(p)
        return self._fins

    def get_daily_raw(self, code, from_date=None, to_date=None):
        df = self._load_prices()
        sub = df[df["Code"] == code].copy()
        if from_date:
            sub = sub[sub["Date"] >= pd.to_datetime(from_date)]
        if to_date:
            sub = sub[sub["Date"] <= pd.to_datetime(to_date)]
        for c in ["O", "H", "L", "C", "Vo", "Va", "AdjFactor"]:
            if c in sub.columns:
                sub[c] = pd.to_numeric(sub[c], errors="coerce")
        return sub.sort_values("Date").reset_index(drop=True)

    def get_financials(self, code):
        df = self._load_fins()
        sub = df[df["Code"] == code].copy()
        if "DiscDate" in sub.columns:
            sub = sub.sort_values("DiscDate", ascending=False)
        return sub.reset_index(drop=True)

    def get_cache_freshness(self):
        """parquet 最終更新日時を返す。古ければユーザに更新依頼を出すための情報。"""
        import datetime

        info = {}
        for f in ["prices.parquet", "fins_cache.parquet", "stock_cache.parquet"]:
            p = self.data_dir / f
            if p.exists():
                mt = datetime.datetime.fromtimestamp(p.stat().st_mtime)
                info[f] = mt.isoformat()
            else:
                info[f] = None
        return info


def calc_rsi(series, period=14):
    delta = series.diff().dropna()
    gain = delta.clip(lower=0).rolling(period).mean()
    loss = (-delta.clip(upper=0)).rolling(period).mean()
    rs = gain / loss.replace(0, float("nan"))
    rsi = 100 - (100 / (1 + rs))
    if len(rsi) == 0 or pd.isna(rsi.iloc[-1]):
        return None
    return float(rsi.iloc[-1])


def calc_macd(series, fast=12, slow=26, signal=9):
    if len(series) < slow + signal:
        return None
    ema_fast = series.ewm(span=fast, adjust=False).mean()
    ema_slow = series.ewm(span=slow, adjust=False).mean()
    macd = ema_fast - ema_slow
    sig = macd.ewm(span=signal, adjust=False).mean()
    hist = macd - sig
    return {
        "macd": round(float(macd.iloc[-1]), 4),
        "signal": round(float(sig.iloc[-1]), 4),
        "histogram": round(float(hist.iloc[-1]), 4),
        "bullish": bool(macd.iloc[-1] > sig.iloc[-1]),
    }


def dev_pct(v, base):
    if v is None or base is None or base == 0:
        return None
    return round((v - base) / base * 100, 2)


def safe_float(v):
    try:
        if v is None or pd.isna(v):
            return None
        return float(v)
    except (ValueError, TypeError):
        return None


def compute_valuation_estimate(fins, last_close, cp):
    """予想 PER と予想配当利回りを「最新開示レコード」ベースで算出する。

    batch_service._compute_metrics と同じ優先順位（FEPS→NxFEPS→実績EPS）と
    分割スケール判定を用い、stock_cache の PER/利回りと挙動を一致させる。

    最新の通期予想は四半期短信側に載るため、FY フィルタ前の全レコードから
    最新（DiscDate 最大）を採用する。予想 per-share 値が分割前/後どちらの
    ベースで開示されたかは FNP/ShOutFY との突合で判定（forecast_per_share_multiplier）。

    引数:
      fins: 銘柄の財務 df（DiscDate 列必須）
      last_close: 最新終値（最新日スケール）
      cp: 株価 df（Date / AdjFactor 列必須、split_factor 計算用）
    戻り値: (per_forecast, div_yield_pct)（算出不能は None）
    """
    if fins is None or len(fins) == 0 or not last_close or last_close <= 0:
        return None, None

    f = fins.copy()
    f["_disc"] = pd.to_datetime(f.get("DiscDate"), errors="coerce")
    f = f.sort_values("_disc", ascending=False).reset_index(drop=True)
    latest = f.iloc[0]

    last_date = (pd.to_datetime(cp["Date"], errors="coerce").max()
                 if cp is not None and len(cp) else None)
    disc = latest.get("_disc")
    split_factor = (split_factor_between(cp, disc, last_date)
                    if (cp is not None and pd.notna(disc) and last_date is not None) else 1.0)

    feps   = safe_float(latest.get("FEPS"))
    nxfeps = safe_float(latest.get("NxFEPS"))
    fnp    = safe_float(latest.get("FNP"))
    shares = safe_float(latest.get("ShOutFY"))
    # 予想 EPS の開示スケール判定（FNP/ShOutFY と突合）。日付ルールだと
    # 分割発表後・効力前に分割後ベースで開示した会社（例: 6227）を誤判定する。
    fwd_mult = forecast_per_share_multiplier(feps, fnp, shares, split_factor)

    per = None
    if feps and feps > 0:
        per = round(last_close / (feps * fwd_mult), 2)
    elif nxfeps and nxfeps > 0:
        per = round(last_close / nxfeps, 2)   # 来期予想は開示時点で分割後ベース
    else:
        # 実績 EPS フォールバック: 最新の FY 決算短信から（開示日起点で分割調整）
        fy_stmt = filter_fy_statements(f)
        if not fy_stmt.empty:
            fy_latest = fy_stmt.sort_values("_disc", ascending=False).iloc[0]
            eps_fb = safe_float(fy_latest.get("EPS"))
            sf_fb = (split_factor_between(cp, fy_latest.get("_disc"), last_date)
                     if (cp is not None and pd.notna(fy_latest.get("_disc")) and last_date is not None)
                     else 1.0)
            if eps_fb and eps_fb > 0:
                per = round(last_close / (eps_fb * sf_fb), 2)

    # 予想配当利回り（予想 → 来期予想 → 実績）。予想配当は予想 EPS と同一レコード=
    # 同一ベースのため fwd_mult を流用。実績配当は開示時点（分割前）ベース。
    fdivann   = safe_float(latest.get("FDivAnn"))
    nxfdivann = safe_float(latest.get("NxFDivAnn"))
    divann    = safe_float(latest.get("DivAnn"))
    div_ann = None
    if fdivann and fdivann > 0:
        div_ann = fdivann * fwd_mult
    elif nxfdivann and nxfdivann > 0:
        div_ann = nxfdivann
    elif divann and divann > 0:
        div_ann = divann * split_factor
    div_yield = round(div_ann / last_close * 100, 2) if div_ann else None

    return per, div_yield


def analyze(ticker, lookback_days=730, source="local"):
    code5 = to_code5(ticker)

    today = datetime.today()
    from_date = (today - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    to_date = today.strftime("%Y-%m-%d")

    if source == "local":
        cli = LocalParquetClient(ROOT / "data")
        cache_info = cli.get_cache_freshness()
    elif source == "api":
        cli = RawJQuantsClient()
        cache_info = None
    else:
        raise ValueError(f"Unknown source: {source} (use 'local' or 'api')")

    cp = cli.get_daily_raw(code5, from_date, to_date)
    if cp.empty:
        return {
            "error": f"No price data for {code5}. J-Quants v2 may not cover this ticker (e.g., very recent IPO or delisted).",
            "ticker_input": ticker,
            "ticker_5digit": code5,
        }

    # 末尾日スケール正規化
    close = normalize_close(cp, dropna=False).dropna()
    high = normalize_high(cp, dropna=False).dropna()
    low = normalize_low(cp, dropna=False).dropna()
    volume = normalize_volume(cp, fillna=False).dropna()

    if len(close) == 0:
        return {"error": f"Empty close series after normalization for {code5}"}

    last_date = cp["Date"].iloc[-1].strftime("%Y-%m-%d")
    last_close = float(close.iloc[-1])

    def ma(window):
        if len(close) < window:
            return None
        return float(close.rolling(window).mean().iloc[-1])

    ma5, ma25, ma60, ma200 = ma(5), ma(25), ma(60), ma(200)

    if len(close) >= 250:
        h52 = float(high.tail(250).max())
        l52 = float(low.tail(250).min())
    else:
        h52 = float(high.max())
        l52 = float(low.min())

    rsi14 = calc_rsi(close, 14)
    macd_state = calc_macd(close)

    avg_vol5 = float(volume.tail(5).mean()) if len(volume) >= 5 else None
    avg_vol20 = float(volume.tail(20).mean()) if len(volume) >= 20 else None
    avg_vol60 = float(volume.tail(60).mean()) if len(volume) >= 60 else None

    split_events = []
    for _, row in cp.iterrows():
        af = row.get("AdjFactor")
        if pd.notna(af) and abs(af - 1.0) > 1e-9:
            af_f = float(af)
            inv = 1.0 / af_f if af_f != 0 else None
            if inv and abs(round(inv) - inv) < 0.05:
                interp = f"1:{int(round(inv))} 分割"
            elif af_f > 1.05:
                interp = f"株式併合 {af_f}:1"
            else:
                interp = f"AdjFactor {af_f}"
            split_events.append(
                {
                    "date": row["Date"].strftime("%Y-%m-%d"),
                    "factor": af_f,
                    "interpretation": interp,
                }
            )

    fins = cli.get_financials(code5)
    fins_summary = []
    fins_quarters_recent = []
    if not fins.empty:
        # FY のみで時系列（最大 7 期）
        if "CurPerType" in fins.columns:
            fy_only = fins[fins["CurPerType"] == "FY"]
        else:
            fy_only = fins
        if "DiscDate" in fy_only.columns:
            fy_only = fy_only.sort_values("DiscDate", ascending=False).head(7)
        for _, row in fy_only.iterrows():
            # 売上は単体（Sales）優先、連結（NCSales）フォールバック
            sales = safe_float(row.get("Sales")) or safe_float(row.get("NCSales"))
            op_ = safe_float(row.get("OP")) or safe_float(row.get("NCOP"))
            np_ = safe_float(row.get("NP")) or safe_float(row.get("NCNP"))
            eps_ = safe_float(row.get("EPS")) or safe_float(row.get("NCEPS"))
            fins_summary.append(
                {
                    "disc_date": row.get("DiscDate"),
                    "period_type": row.get("CurPerType"),
                    "fy_start": row.get("CurFYSt"),
                    "fy_end": row.get("CurFYEn"),
                    "sales_mil": sales,
                    "op_mil": op_,
                    "np_mil": np_,
                    "eps": eps_,
                    "bps": safe_float(row.get("BPS")) or safe_float(row.get("NCBPS")),
                    "div_ann": safe_float(row.get("DivAnn")),
                    "payout_ratio": safe_float(row.get("PayoutRatioAnn")),
                    "forecast_div_ann": safe_float(row.get("FDivAnn")),
                    "forecast_sales": safe_float(row.get("FSales")) or safe_float(row.get("FNCSales")),
                    "forecast_op": safe_float(row.get("FOP")) or safe_float(row.get("FNCOP")),
                    "forecast_np": safe_float(row.get("FNP")) or safe_float(row.get("FNCNP")),
                    "forecast_eps": safe_float(row.get("FEPS")) or safe_float(row.get("FNCEPS")),
                    "cfo": safe_float(row.get("CFO")),
                    "cfi": safe_float(row.get("CFI")),
                    "cff": safe_float(row.get("CFF")),
                    "cash_eq": safe_float(row.get("CashEq")),
                    "equity_ratio": safe_float(row.get("EqAR")) or safe_float(row.get("NCEqAR")),
                    "shares_outstanding": safe_float(row.get("ShOutFY")),
                }
            )

        # 直近四半期（1Q/2Q/3Q）を時系列で 8 件まで
        if "CurPerType" in fins.columns:
            q_only = fins[fins["CurPerType"].isin(["1Q", "2Q", "3Q"])]
        else:
            q_only = fins.iloc[0:0]
        if "DiscDate" in q_only.columns:
            q_only = q_only.sort_values("DiscDate", ascending=False).head(8)
        for _, row in q_only.iterrows():
            fins_quarters_recent.append(
                {
                    "disc_date": row.get("DiscDate"),
                    "period_type": row.get("CurPerType"),
                    "fy_end": row.get("CurFYEn"),
                    "sales_mil": safe_float(row.get("Sales")) or safe_float(row.get("NCSales")),
                    "op_mil": safe_float(row.get("OP")) or safe_float(row.get("NCOP")),
                    "np_mil": safe_float(row.get("NP")) or safe_float(row.get("NCNP")),
                    "eps": safe_float(row.get("EPS")) or safe_float(row.get("NCEPS")),
                    "forecast_revenue": safe_float(row.get("FSales")) or safe_float(row.get("FNCSales")),
                    "forecast_op": safe_float(row.get("FOP")) or safe_float(row.get("FNCOP")),
                    "forecast_np": safe_float(row.get("FNP")) or safe_float(row.get("FNCNP")),
                    "forecast_eps": safe_float(row.get("FEPS")) or safe_float(row.get("FNCEPS")),
                }
            )

    def lookback_ret(n):
        if len(close) < n + 1:
            return None
        return round((last_close / float(close.iloc[-n - 1]) - 1) * 100, 2)

    # PER 推定・予想配当利回り（最新開示レコードベース。
    # 予想 per-share 値の分割スケールは FNP/ShOutFY との突合で判定する）
    per_estimate, div_yield = compute_valuation_estimate(fins, last_close, cp)

    return {
        "ticker_input": ticker,
        "ticker_5digit": code5,
        "as_of": last_date,
        "price": {
            "close": last_close,
            "ret_1d_pct": lookback_ret(1),
            "ret_5d_pct": lookback_ret(5),
            "ret_20d_pct": lookback_ret(20),
            "ret_60d_pct": lookback_ret(60),
        },
        "moving_averages": {
            "ma5": ma5,
            "ma25": ma25,
            "ma60": ma60,
            "ma200": ma200,
            "dev_ma25_pct": dev_pct(last_close, ma25),
            "dev_ma60_pct": dev_pct(last_close, ma60),
            "dev_ma200_pct": dev_pct(last_close, ma200),
        },
        "technical": {
            "rsi_14": round(rsi14, 2) if rsi14 is not None else None,
            "macd": macd_state,
            "above_ma25": bool(ma25 and last_close > ma25) if ma25 else None,
            "above_ma60": bool(ma60 and last_close > ma60) if ma60 else None,
            "above_ma200": bool(ma200 and last_close > ma200) if ma200 else None,
        },
        "range_52w": {
            "high": round(h52, 2),
            "low": round(l52, 2),
            "from_high_pct": round((last_close - h52) / h52 * 100, 2) if h52 else None,
            "from_low_pct": round((last_close - l52) / l52 * 100, 2) if l52 else None,
            "is_52w_high": bool(last_close >= h52 * 0.999),
        },
        "volume": {
            "avg_5d": int(avg_vol5) if avg_vol5 else None,
            "avg_20d": int(avg_vol20) if avg_vol20 else None,
            "avg_60d": int(avg_vol60) if avg_vol60 else None,
            "vol_ratio_5_20": round(avg_vol5 / avg_vol20, 2)
            if (avg_vol5 and avg_vol20)
            else None,
        },
        "valuation_estimate": {
            "per_forecast": per_estimate,
            "div_yield_pct": div_yield,
            "note": "EPS は J-Quants 最新財務（予想優先、実績フォールバック）。EDINET DB の値と差異あれば EDINET 優先",
        },
        "split_events_2y": split_events,
        "financials_annual": fins_summary,
        "financials_quarters_recent": fins_quarters_recent,
        "data_points": len(close),
        "source": source,
        "cache_info": cache_info,
        "sector_comparison": _build_sector_comparison(cli, code5) if source == "local" else None,
    }


def _build_sector_comparison(cli, code5):
    """stock_cache.parquet からセクター内 percentile 比較を構築。

    local モード（LocalParquetClient）でのみ呼ばれる。stock_cache.parquet が
    存在しない場合は None を返す（致命的ではない）。
    """
    if not hasattr(cli, "get_stock_cache_row"):
        return None

    self_row = cli.get_stock_cache_row(code5)
    if self_row is None:
        return None

    sector = self_row.get("sector")
    market = self_row.get("market")
    if not sector:
        return {"error": "sector unknown for this ticker", "self_row": _clean_row(self_row)}

    # 同セクター peer 集合（市場で絞らない = sector 全体）
    peers_all = cli.get_sector_peers(sector)
    # 同セクター + 同市場
    peers_same_market = cli.get_sector_peers(sector, market=market) if market else peers_all

    def pct_rank(series, value, lower_is_better=True):
        """value が series の何 percentile か（0-100）。lower_is_better=True なら低い方が上位"""
        if value is None or pd.isna(value):
            return None
        s = series.dropna()
        if len(s) < 5:  # peer 少なすぎ
            return None
        if lower_is_better:
            rank = (s <= value).sum() / len(s) * 100
        else:
            rank = (s >= value).sum() / len(s) * 100
        return round(float(rank), 1)

    def safe_median(series):
        s = series.dropna()
        if len(s) == 0:
            return None
        return round(float(s.median()), 2)

    def safe_count(series):
        return int(series.dropna().shape[0])

    # 各指標の percentile（自銘柄の値 と セクター中央値 と percentile）
    metrics = {}
    for col, lower_better in [
        ("PER", True),       # 低い方が割安
        ("PBR", True),       # 低い方が割安
        ("ROE", False),      # 高い方が良い
        ("div_yield", False),# 高い方が良い
        ("RSI", False),      # 中立だが高い = モメンタム
        ("altman_z", False), # 高い方が安全
        ("signal_score", False),  # 高い方が良い
        ("rev_growth", False),    # 高い方が成長
        ("profit_growth", False), # 高い方が成長
    ]:
        if col not in peers_all.columns:
            continue
        v = safe_float(self_row.get(col))
        metrics[col] = {
            "self": v,
            "sector_median": safe_median(peers_all[col]),
            "sector_count": safe_count(peers_all[col]),
            "percentile_in_sector": pct_rank(peers_all[col], v, lower_is_better=lower_better),
            "lower_is_better": lower_better,
        }

    # signal_score 上位 5 銘柄（同セクター内）
    top5_signal = []
    if "signal_score" in peers_all.columns:
        sorted_df = peers_all.dropna(subset=["signal_score"]).sort_values("signal_score", ascending=False).head(5)
        for _, row in sorted_df.iterrows():
            top5_signal.append({
                "code": row.get("code_4"),
                "name": row.get("company_name"),
                "signal_score": safe_float(row.get("signal_score")),
                "PER": safe_float(row.get("PER")),
                "PBR": safe_float(row.get("PBR")),
                "ROE": safe_float(row.get("ROE")),
            })

    # SEPA stage 分布（同セクター内）
    sepa_dist = {}
    if "sepa_stage" in peers_all.columns:
        dist = peers_all["sepa_stage"].value_counts().to_dict()
        sepa_dist = {str(int(k) if pd.notna(k) else "NA"): int(v) for k, v in dist.items()}

    # モメンタムシグナル分布
    momentum_signals = {}
    for col in ["mom_signal", "mom_new_high", "mom_gc", "mom_above_ma200", "dow_uptrend"]:
        if col in peers_all.columns:
            true_count = int(peers_all[col].fillna(False).astype(bool).sum())
            momentum_signals[col] = {
                "count_true": true_count,
                "pct_true": round(true_count / len(peers_all) * 100, 1),
            }

    # 自銘柄のシグナル要約
    self_signals = {
        "sepa_stage": int(self_row["sepa_stage"]) if pd.notna(self_row.get("sepa_stage")) else None,
        "sepa_ma_align": bool(self_row.get("sepa_ma_align")) if pd.notna(self_row.get("sepa_ma_align")) else None,
        "sepa_ma200_trend": bool(self_row.get("sepa_ma200_trend")) if pd.notna(self_row.get("sepa_ma200_trend")) else None,
        "sepa_from_high_pct": safe_float(self_row.get("sepa_from_high")),
        "sepa_from_low_pct": safe_float(self_row.get("sepa_from_low")),
        "sepa_rs": safe_float(self_row.get("sepa_rs")),
        "mom_signal": bool(self_row.get("mom_signal")) if pd.notna(self_row.get("mom_signal")) else None,
        "mom_new_high": bool(self_row.get("mom_new_high")) if pd.notna(self_row.get("mom_new_high")) else None,
        "mom_above_ma200": bool(self_row.get("mom_above_ma200")) if pd.notna(self_row.get("mom_above_ma200")) else None,
        "dow_uptrend": bool(self_row.get("dow_uptrend")) if pd.notna(self_row.get("dow_uptrend")) else None,
    }

    return {
        "sector": sector,
        "market": market,
        "peer_count_sector": len(peers_all),
        "peer_count_same_market": len(peers_same_market),
        "self_row_summary": {
            "company_name": self_row.get("company_name"),
            "close": safe_float(self_row.get("close")),
            "score": safe_float(self_row.get("score")),
        },
        "metrics_vs_sector": metrics,
        "self_signals": self_signals,
        "sepa_stage_distribution_sector": sepa_dist,
        "momentum_signals_in_sector": momentum_signals,
        "top5_by_signal_score_in_sector": top5_signal,
    }


def _clean_row(row_dict):
    """row dict から NaN を None に変換して JSON serialize 可能にする"""
    return {k: (None if pd.isna(v) else (v.item() if hasattr(v, "item") else v)) for k, v in row_dict.items()}


def main():
    import argparse

    p = argparse.ArgumentParser(
        description="深層分析用 J-Quants データ取得ヘルパー",
        epilog="例: .venv/Scripts/python.exe scripts/deep_analysis_helper.py 290A --save",
    )
    p.add_argument("ticker", help="銘柄コード（4桁/5桁/英数字混合 例: 7974 / 79740 / 290A）")
    p.add_argument(
        "--save",
        action="store_true",
        help="出力を data/da_cache/<ticker>.json に保存（Claude Code の深層分析で読まれる）",
    )
    p.add_argument(
        "--cache-dir",
        default=None,
        help="保存先ディレクトリ（既定: stock_analysis/data/da_cache/）",
    )
    p.add_argument(
        "--lookback-days",
        type=int,
        default=730,
        help="株価取得期間（既定: 730 日 = 約 2 年、MA200 計算に必要）",
    )
    p.add_argument(
        "--source",
        choices=["local", "api"],
        default="local",
        help="local: data/*.parquet を直読み（Claude Code 用、推奨）/ api: J-Quants API 直接取得（ユーザのターミナルでのみ動作）",
    )
    args = p.parse_args()

    try:
        result = analyze(args.ticker, lookback_days=args.lookback_days, source=args.source)
    except Exception as e:
        result = {"error": str(e), "ticker_input": args.ticker}

    output = json.dumps(result, ensure_ascii=False, indent=2, default=str)
    print(output)

    if args.save:
        cache_dir = Path(args.cache_dir) if args.cache_dir else ROOT / "data" / "da_cache"
        cache_dir.mkdir(parents=True, exist_ok=True)
        save_path = cache_dir / f"{args.ticker.upper()}.json"
        save_path.write_text(output, encoding="utf-8")
        print(f"\n[saved] {save_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
