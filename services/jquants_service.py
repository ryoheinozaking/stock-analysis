# -*- coding: utf-8 -*-
"""
J-Quants API v2 service layer with Streamlit caching
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))

import requests
import pandas as pd
import streamlit as st
from typing import Optional

BASE_URL = "https://api.jquants.com/v2"


def _api_key() -> str:
    key = os.getenv("JQUANTS_API_KEY")
    if not key:
        raise EnvironmentError(".env に JQUANTS_API_KEY が設定されていません")
    return key


def _get(endpoint: str, params: dict = None) -> dict:
    headers = {"x-api-key": _api_key()}
    r = requests.get(
        f"{BASE_URL}{endpoint}",
        params=params or {},
        headers=headers,
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


@st.cache_data(ttl=3600)
def get_listed_info() -> pd.DataFrame:
    """上場銘柄マスタを取得する"""
    try:
        data = _get("/equities/master")
        df = pd.DataFrame(data.get("data", []))
        if df.empty:
            return df
        cols = [c for c in ["Code", "CoName", "CoNameEn", "Mkt", "MktNm", "S17Nm", "S33Nm"] if c in df.columns]
        return df[cols].copy()
    except Exception as e:
        st.error(f"銘柄マスタの取得に失敗しました: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=1800)
def get_ohlcv(code: str, from_date: str, to_date: str) -> pd.DataFrame:
    """日次OHLCV（権利修正済み）を取得する"""
    try:
        data = _get("/equities/bars/daily", {"code": code, "from": from_date, "to": to_date})
        df = pd.DataFrame(data.get("data", []))
        if df.empty:
            return df
        df["Date"] = pd.to_datetime(df["Date"])
        num_cols = ["AdjO", "AdjH", "AdjL", "AdjC", "AdjVo", "Vo"]
        for col in num_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df.sort_values("Date").reset_index(drop=True)
    except Exception as e:
        st.error(f"株価データの取得に失敗しました ({code}): {e}")
        return pd.DataFrame()


@st.cache_data(ttl=3600)
def get_financials(code: str) -> pd.DataFrame:
    """財務サマリーを取得する（FYのみ、DiscDate降順）"""
    try:
        data = _get("/fins/summary", {"code": code})
        df = pd.DataFrame(data.get("data", []))
        if df.empty:
            return df
        if "CurPerType" in df.columns:
            annual = df[df["CurPerType"] == "FY"]
            if not annual.empty:
                df = annual
        df = df.sort_values("DiscDate", ascending=False).reset_index(drop=True)
        return df
    except Exception as e:
        st.error(f"財務データの取得に失敗しました ({code}): {e}")
        return pd.DataFrame()


def resample_ohlcv(daily_df: pd.DataFrame, freq: str) -> pd.DataFrame:
    """
    日足DataFrameを週足・月足に集計する。
    freq: "D"=日足（そのまま）, "W"=週足, "ME"=月足
    """
    if daily_df.empty or freq == "D":
        return daily_df

    df = daily_df.copy()
    df = df.set_index("Date")

    agg = {
        "AdjO": "first",
        "AdjH": "max",
        "AdjL": "min",
        "AdjC": "last",
    }
    vol_col = "AdjVo" if "AdjVo" in df.columns else ("Vo" if "Vo" in df.columns else None)
    if vol_col:
        agg[vol_col] = "sum"

    resampled = df.resample(freq).agg(agg).dropna(subset=["AdjO", "AdjC"])
    resampled = resampled.reset_index()
    return resampled


@st.cache_data(ttl=3600)
def get_company_info(code: str) -> Optional[dict]:
    """銘柄マスタから1件取得する"""
    try:
        df = get_listed_info()
        if df.empty:
            return None
        row = df[df["Code"] == code]
        if row.empty:
            return None
        return row.iloc[0].to_dict()
    except Exception as e:
        st.error(f"銘柄情報の取得に失敗しました ({code}): {e}")
        return None
