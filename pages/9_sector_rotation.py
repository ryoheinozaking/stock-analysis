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


# 表示専用の日本語ラベル（内部の計算列名は英語のまま保つ）
# percent 書式で表示する小数比率列は、ラベルに (%) を付けない（書式側が % を付ける）
_LABELS = {
    "sector": "業種", "turnover_ratio": "売買代金倍率", "va": "売買代金(円)",
    "daily_return_pct": "前日比(%)", "week_return": "週間騰落",
    "month_return": "月間騰落", "rank_delta": "順位変化", "week_rank": "週間順位",
    "month_rank": "月間順位", "category": "区分", "score": "スコア",
    "turnover_pace": "代金ペース", "persistence": "継続日数",
    "flow_return": "直近リターン", "breadth": "上昇銘柄比率",
    "up_turnover_share": "上昇代金シェア", "code": "コード",
    "company_name": "銘柄名", "self_rank": "業種内順位",
    "self_rank_total": "業種内銘柄数", "vol_ratio": "出来高倍率", "close": "株価",
    "RSI": "RSI", "ma25_dev_pct": "25日線乖離(%)",
    "from_52w_high_pct": "52週高値差(%)", "from_52w_low_pct": "52週安値差(%)",
    "new_high": "新高値", "sell_balance": "売残", "buy_balance": "買残",
    "sell_wow": "売残前週比", "buy_wow": "買残前週比", "margin_ratio": "信用倍率",
}

# 表示書式（日本語ラベルをキーにする。df に無いキーは Streamlit が無視する）
_N = st.column_config.NumberColumn
_COLCFG = {
    "売買代金(円)": _N(format="localized"),
    "売残": _N(format="localized"), "買残": _N(format="localized"),
    "売残前週比": _N(format="localized"), "買残前週比": _N(format="localized"),
    "株価": _N(format="localized"),
    "売買代金倍率": _N(format="%.2f"), "出来高倍率": _N(format="%.2f"),
    "代金ペース": _N(format="%.2f"), "信用倍率": _N(format="%.2f"),
    "RSI": _N(format="%.1f"),
    "前日比(%)": _N(format="%.2f"), "25日線乖離(%)": _N(format="%.2f"),
    "52週高値差(%)": _N(format="%.2f"), "52週安値差(%)": _N(format="%.2f"),
    "週間騰落": _N(format="percent"), "月間騰落": _N(format="percent"),
    "直近リターン": _N(format="percent"), "上昇銘柄比率": _N(format="percent"),
    "上昇代金シェア": _N(format="percent"),
    "継続日数": _N(format="%d"), "順位変化": _N(format="%d"),
    "業種内順位": _N(format="%d"), "業種内銘柄数": _N(format="%d"),
    "週間順位": _N(format="%d"), "月間順位": _N(format="%d"),
    "スコア": st.column_config.ProgressColumn(min_value=0, max_value=100, format="%.0f"),
    "新高値": st.column_config.CheckboxColumn(),
}

_ROW_PX = 35  # 概算の1行高さ。行数に応じて表の高さを固定し間延びを防ぐ


def _jp(df):
    """表示用に列名を日本語化する（元データは非破壊）。"""
    return df.rename(columns=_LABELS)


def _table(df, max_rows=15):
    """日本語ラベル + 書式 + 行数ぶんの高さ固定 + インデックス非表示で表示する。"""
    shown = df.head(max_rows)
    height = int(_ROW_PX * (len(shown) + 1)) + 3
    st.dataframe(
        _jp(shown), hide_index=True, use_container_width=True,
        height=height, column_config=_COLCFG,
    )


def _hbar(df, cat_col, val_col, title, color):
    """横棒グラフ（上位ほど上に来るよう反転）。"""
    d = df.head(10).iloc[::-1]
    fig = go.Figure(go.Bar(
        x=d[val_col], y=d[cat_col], orientation="h",
        marker_color=color,
        text=[f"{v:,.2f}" for v in d[val_col]], textposition="auto",
    ))
    fig.update_layout(
        title=title, height=340, margin=dict(l=8, r=8, t=40, b=8),
        xaxis_title=None, yaxis_title=None,
    )
    st.plotly_chart(fig, use_container_width=True)


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
st.caption("売買代金が直近中央値の何倍か（＝資金の集まり具合）。上位ほど注目。")
cg, ct = st.columns([3, 2])
with cg:
    _hbar(surge, "sector", "turnover_ratio", "売買代金倍率 上位", "#4c78a8")
with ct:
    _table(surge, max_rows=10)

# ── 鮮度 ──
st.subheader("鮮度（資金の新旧）")
st.caption("週順位が月順位より上＝最近来た（上昇中）／両方上位＝勝ち続け／週が落ちた＝失速。")
fresh = rotation_service.compute_freshness(sd)
c_rise, c_win, c_fall = st.columns(3)
with c_rise:
    st.markdown("**🟢 上昇中（新しく来た）**")
    _table(fresh[fresh["category"] == "rising"][["sector", "week_return", "rank_delta"]], max_rows=8)
with c_win:
    st.markdown("**🔵 勝ち続け**")
    _table(fresh[fresh["category"] == "winning"][["sector", "week_return", "month_return"]], max_rows=8)
with c_fall:
    st.markdown("**🔴 失速**")
    _table(fresh[fresh["category"] == "falling"][["sector", "week_return", "rank_delta"]], max_rows=8)

# ── 資金流入 ──
st.subheader("資金流入スコア")
st.caption("代金ペース・継続日数・直近リターン・上昇銘柄比率の合成（0-100）。")
flow = rotation_service.compute_fund_flow(sd)
fg, ft = st.columns([3, 2])
with fg:
    _hbar(flow, "sector", "score", "資金流入スコア 上位", "#54a24b")
with ft:
    _table(flow[["sector", "score", "turnover_pace", "persistence", "breadth"]], max_rows=10)

# ── 信用需給（段階2・アーカイブがあれば） ──
margin_df = margin_service.load_latest_margin()
if margin_df is not None:
    st.subheader("信用需給（JPX週次）")
    st.caption("信用倍率 = 買残 ÷ 売残。高いほど買い長。")
    _table(margin_df.sort_values("margin_ratio", ascending=False), max_rows=15)

# ── ランキング ──
st.subheader("ランキング")
rankings = rotation_service.compute_rankings(sc)
tabs = st.tabs(["モメンタム", "出来高急増(銘柄)"])
with tabs[0]:
    _table(
        rankings["momentum"][["code", "company_name", "sector", "score", "self_rank", "self_rank_total"]],
        max_rows=30,
    )
with tabs[1]:
    _table(
        rankings["volume_surge"][["code", "company_name", "sector", "vol_ratio", "self_rank"]],
        max_rows=30,
    )

# ── ドリルダウン ──
st.divider()
st.subheader("業種ドリルダウン")
signals = rotation_service.compute_stock_signals(sc)
sectors = sorted(signals["sector"].dropna().unique())
sel = st.selectbox("業種を選択", sectors)
members = signals[signals["sector"] == sel].sort_values("ma25_dev_pct", ascending=False)
_table(
    members[["code", "company_name", "close", "RSI", "ma25_dev_pct", "from_52w_high_pct", "new_high"]],
    max_rows=30,
)

code_to_open = st.text_input("詳細を開く銘柄コード（5桁）", "")
if st.button("銘柄詳細へ") and code_to_open:
    st.session_state["selected_code"] = code_to_open.strip()
    st.switch_page("pages/2_stock_detail.py")
