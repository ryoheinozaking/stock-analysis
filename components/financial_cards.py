# -*- coding: utf-8 -*-
"""
Financial metric card components
"""

import pandas as pd
import streamlit as st
from typing import Optional


def render_score_badge(score: float) -> str:
    """スコアに応じた色付きバッジHTMLを返す"""
    if score >= 60:
        css_class = "score-high"
        bg_color = "#27ae60"
    elif score >= 40:
        css_class = "score-mid"
        bg_color = "#f39c12"
    else:
        css_class = "score-low"
        bg_color = "#c0392b"
    return (
        f'<span class="{css_class}" style="background:{bg_color};color:white;'
        f'padding:2px 8px;border-radius:12px;font-weight:bold;">{score:.1f}</span>'
    )


def render_metric_row(data: dict, listed_info: Optional[dict] = None):
    """
    evaluate_stock の返り値を st.metric で4カラム×2行で表示する
    行1: 株価, PER, PBR, ROE
    行2: 配当利回り, 売上成長, 利益成長, RSI
    """
    def _fmt(val, fmt=".2f", suffix=""):
        if val is None:
            return "N/A"
        try:
            return f"{val:{fmt}}{suffix}"
        except Exception:
            return "N/A"

    # スコアバッジ
    score = data.get("score", 0)
    st.markdown(
        f"**スコア:** {render_score_badge(score)}",
        unsafe_allow_html=True,
    )
    st.markdown("")

    # 行1: 株価, PER, PBR, ROE
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("株価 (円)", _fmt(data.get("close"), ",.0f"))
    with col2:
        st.metric("PER (倍)", _fmt(data.get("PER")))
    with col3:
        st.metric("PBR (倍)", _fmt(data.get("PBR")))
    with col4:
        st.metric("ROE (%)", _fmt(data.get("ROE"), suffix="%"))

    # 行2: 配当利回り, 売上成長, 利益成長, RSI
    col5, col6, col7, col8 = st.columns(4)
    with col5:
        st.metric("配当利回り", _fmt(data.get("div_yield"), suffix="%"))
    with col6:
        st.metric("売上成長率", _fmt(data.get("rev_growth"), suffix="%"))
    with col7:
        st.metric("利益成長率", _fmt(data.get("profit_growth"), suffix="%"))
    with col8:
        st.metric("RSI", _fmt(data.get("RSI"), ".1f"))


def render_financials_table(fin_df: pd.DataFrame):
    """
    直近5期の財務推移テーブルをst.dataframeで表示する
    表示列: DiscDate(期末), Sales(売上), OP(営業利益), NP(純利益), EPS, BPS, DivAnn
    単位: Sales/OP/NP は百万円、EPS/BPS/DivAnn は円
    """
    if fin_df is None or fin_df.empty:
        st.info("財務データがありません")
        return

    display_cols = ["DiscDate", "Sales", "OP", "NP", "EPS", "BPS", "DivAnn"]
    available_cols = [c for c in display_cols if c in fin_df.columns]

    df_show = fin_df[available_cols].head(5).copy()

    # 列名の日本語化
    col_rename = {
        "DiscDate": "期末日",
        "Sales": "売上高(百万円)",
        "OP": "営業利益(百万円)",
        "NP": "純利益(百万円)",
        "EPS": "EPS(円)",
        "BPS": "BPS(円)",
        "DivAnn": "年間配当(円)",
    }
    df_show = df_show.rename(columns={k: v for k, v in col_rename.items() if k in df_show.columns})

    # 数値フォーマット
    for col in df_show.columns:
        if col in ["売上高(百万円)", "営業利益(百万円)", "純利益(百万円)"]:
            df_show[col] = pd.to_numeric(df_show[col], errors="coerce")
        elif col in ["EPS(円)", "BPS(円)", "年間配当(円)"]:
            df_show[col] = pd.to_numeric(df_show[col], errors="coerce")

    st.dataframe(df_show, use_container_width=True, hide_index=True)
