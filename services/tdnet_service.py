# -*- coding: utf-8 -*-
"""
Yanoshin TDnet API client with Streamlit caching
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))

import requests
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta

BASE_URL = "https://webapi.yanoshin.jp/webapi/tdnet/list"


def _fetch(date_str: str, params: dict = None) -> dict:
    """Yanoshin TDnet APIからデータを取得する"""
    url = f"{BASE_URL}/{date_str}.json"
    r = requests.get(url, params=params or {}, timeout=15)
    r.raise_for_status()
    return r.json()


def _parse(items: list) -> pd.DataFrame:
    """Tdnetキーを展開してDataFrameに変換する"""
    rows = []
    for item in items:
        tdnet = item.get("Tdnet", {})
        rows.append({
            "id": tdnet.get("id", ""),
            "pubdate": tdnet.get("pubdate", ""),
            "company_code": tdnet.get("company_code", ""),
            "company_name": tdnet.get("company_name", ""),
            "title": tdnet.get("title", ""),
            "document_url": tdnet.get("document_url", ""),
            "markets_string": tdnet.get("markets_string", ""),
        })
    if not rows:
        return pd.DataFrame(columns=["id", "pubdate", "company_code", "company_name", "title", "document_url", "markets_string"])
    df = pd.DataFrame(rows)
    return df


@st.cache_data(ttl=300)
def get_latest(limit: int = 50) -> pd.DataFrame:
    """最新の適時開示一覧を取得する"""
    try:
        today = datetime.today().strftime("%Y%m%d")
        data = _fetch(today, {"limit": limit})
        items = data.get("items", [])
        if not items:
            # 今日データがない場合は昨日を試す
            yesterday = (datetime.today() - timedelta(days=1)).strftime("%Y%m%d")
            data = _fetch(yesterday, {"limit": limit})
            items = data.get("items", [])
        return _parse(items)
    except Exception as e:
        st.error(f"最新開示データの取得に失敗しました: {e}")
        return pd.DataFrame(columns=["id", "pubdate", "company_code", "company_name", "title", "document_url", "markets_string"])


@st.cache_data(ttl=1800)
def get_by_date(date_str: str, limit: int = 100) -> pd.DataFrame:
    """指定日の適時開示一覧を取得する (date_str: YYYYMMDD)"""
    try:
        data = _fetch(date_str, {"limit": limit})
        items = data.get("items", [])
        return _parse(items)
    except Exception as e:
        st.error(f"指定日 ({date_str}) の開示データ取得に失敗しました: {e}")
        return pd.DataFrame(columns=["id", "pubdate", "company_code", "company_name", "title", "document_url", "markets_string"])


@st.cache_data(ttl=600)
def get_by_company(code_4digit: str, days: int = 30, limit: int = 50) -> pd.DataFrame:
    """指定銘柄の適時開示を過去days日分取得する"""
    try:
        all_items = []
        today = datetime.today()
        for d in range(days):
            date = today - timedelta(days=d)
            date_str = date.strftime("%Y%m%d")
            try:
                data = _fetch(date_str, {"limit": limit, "company_code": code_4digit})
                items = data.get("items", [])
                all_items.extend(items)
                if len(all_items) >= limit:
                    break
            except Exception:
                continue
        return _parse(all_items[:limit])
    except Exception as e:
        st.error(f"銘柄 ({code_4digit}) の開示データ取得に失敗しました: {e}")
        return pd.DataFrame(columns=["id", "pubdate", "company_code", "company_name", "title", "document_url", "markets_string"])
