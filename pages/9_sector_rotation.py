# -*- coding: utf-8 -*-
"""セクター回転検知ダッシュボード（KabuTrend /trend 参考・自前データ）。"""
import os

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from services import batch_service, rotation_service, margin_service

st.set_page_config(page_title="セクター回転", layout="wide")
st.title("セクター回転検知")


def _data_signature():
    """prices/stock_cache の更新時刻をキャッシュキーにする。

    データ更新後にファイルが変わればキーが変わり、@st.cache_data が
    自動で読み直す（古いキャッシュを最大 ttl 秒返し続ける問題を防ぐ）。
    """
    def _mtime(path):
        return os.path.getmtime(path) if os.path.exists(path) else 0.0
    return (_mtime(batch_service.PRICES_PATH), _mtime(batch_service.CACHE_PATH))


@st.cache_data(ttl=3600)
def _load_data(_signature):
    prices = pd.read_parquet(batch_service.PRICES_PATH)
    sc = batch_service.load_cache()
    return prices, sc


@st.cache_data(ttl=3600)
def _sector_daily(prices, sector_map):
    return rotation_service.load_sector_daily(prices, sector_map)


prices, sc = _load_data(_data_signature())
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

st.divider()

# ── 出来高急増 ──
st.subheader("出来高急増")
st.dataframe(surge.head(15), use_container_width=True)

# ── 鮮度 ──
st.subheader("鮮度（資金の新旧）")
fresh = rotation_service.compute_freshness(sd)
c_rise, c_win, c_fall = st.columns(3)
with c_rise:
    st.caption("上昇中（新しく来た）")
    st.dataframe(fresh[fresh["category"] == "rising"][["sector", "week_return", "rank_delta"]], use_container_width=True)
with c_win:
    st.caption("勝ち続け")
    st.dataframe(fresh[fresh["category"] == "winning"][["sector", "week_return", "month_return"]], use_container_width=True)
with c_fall:
    st.caption("失速")
    st.dataframe(fresh[fresh["category"] == "falling"][["sector", "week_return", "rank_delta"]], use_container_width=True)

# ── 資金流入 ──
st.subheader("資金流入スコア")
flow = rotation_service.compute_fund_flow(sd)
st.dataframe(flow.head(15), use_container_width=True)

# ── 信用需給（段階2・アーカイブがあれば） ──
margin_df = margin_service.load_latest_margin()
if margin_df is not None:
    st.subheader("信用需給（JPX週次）")
    st.dataframe(
        margin_df.sort_values("margin_ratio", ascending=False).head(15),
        use_container_width=True,
    )

# ── ランキング ──
st.subheader("ランキング")
rankings = rotation_service.compute_rankings(sc)
tabs = st.tabs(["モメンタム", "出来高急増(銘柄)"])
with tabs[0]:
    st.dataframe(
        rankings["momentum"].head(30)[["code", "company_name", "sector", "score", "self_rank", "self_rank_total"]],
        use_container_width=True,
    )
with tabs[1]:
    st.dataframe(
        rankings["volume_surge"].head(30)[["code", "company_name", "sector", "vol_ratio", "self_rank"]],
        use_container_width=True,
    )

# ── ドリルダウン ──
st.divider()
st.subheader("業種ドリルダウン")
signals = rotation_service.compute_stock_signals(sc)
sectors = sorted(signals["sector"].dropna().unique())
sel = st.selectbox("業種を選択", sectors)
members = signals[signals["sector"] == sel].sort_values("ma25_dev_pct", ascending=False)
st.dataframe(
    members[["code", "company_name", "close", "RSI", "ma25_dev_pct", "from_52w_high_pct", "new_high"]].head(30),
    use_container_width=True,
)

code_to_open = st.text_input("詳細を開く銘柄コード（5桁）", "")
if st.button("銘柄詳細へ") and code_to_open:
    st.session_state["selected_code"] = code_to_open.strip()
    st.switch_page("pages/2_stock_detail.py")
