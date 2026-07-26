# -*- coding: utf-8 -*-
"""セクター回転検知ダッシュボード（KabuTrend /trend 参考・自前データ）。"""
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from services import batch_service, rotation_service, margin_service

st.set_page_config(page_title="セクター回転", layout="wide")
st.title("セクター回転検知")


@st.cache_data(ttl=3600)
def _load_data():
    prices = pd.read_parquet(batch_service.PRICES_PATH)
    sc = batch_service.load_cache()
    return prices, sc


@st.cache_data(ttl=3600)
def _sector_daily(prices, sector_map):
    return rotation_service.load_sector_daily(prices, sector_map)


prices, sc = _load_data()
if sc is None or prices.empty:
    st.warning("stock_cache または prices が空です。先に『データ更新』を実行してください。")
    st.stop()

sector_map = dict(zip(sc["code"], sc["sector"]))
data_date = str(pd.to_datetime(prices["Date"]).max().date())
st.caption(f"データ基準日: {data_date}")

sd = _sector_daily(prices, sector_map)

# サマリー用: 各業種の期間リターン(月)と最新breadth
summary = rotation_service.compute_freshness(sd)[["sector", "month_return"]].copy()
summary = summary.rename(columns={"month_return": "period_return"})
latest_breadth = sd.sort_values("Date").groupby("sector")["up_ratio"].last()
summary["breadth"] = summary["sector"].map(latest_breadth)
temp = rotation_service.compute_theme_temperature(summary)

col1, col2, col3 = st.columns(3)
with col1:
    fig = go.Figure(go.Indicator(
        mode="gauge+number", value=temp,
        title={"text": "テーマ温度"},
        gauge={"axis": {"range": [0, 100]}},
    ))
    fig.update_layout(height=220, margin=dict(t=40, b=0))
    st.plotly_chart(fig, use_container_width=True)
with col2:
    adv = float((summary["period_return"] > 0).mean()) * 100.0
    st.metric("上昇業種比率", f"{adv:.1f}%")
with col3:
    surge = rotation_service.compute_volume_surge(sd)
    st.metric("出来高急増業種数", int((surge["turnover_ratio"] >= 1.5).sum()))
