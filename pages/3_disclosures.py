# -*- coding: utf-8 -*-
"""
適時開示ページ
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

from services.tdnet_service import get_latest, get_by_date, get_by_company
from components.disclosure_table import render_disclosure_table

# カスタムCSS読み込み
css_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "styles", "custom.css")
if os.path.exists(css_path):
    with open(css_path, "r", encoding="utf-8") as f:
        css_content = f.read()
    st.markdown(f"<style>{css_content}</style>", unsafe_allow_html=True)

st.title("📰 適時開示")

# session_state 初期化
if "selected_code" not in st.session_state:
    st.session_state["selected_code"] = ""

# ---- サイドバー ----
st.sidebar.header("📰 適時開示検索")

mode = st.sidebar.radio(
    "表示モード",
    options=["最新一覧", "日付指定", "銘柄コード検索"],
    index=0,
)

# モードに応じた入力
limit_val = 50
date_str = None
company_code_input = None

if mode == "最新一覧":
    limit_val = st.sidebar.slider("表示件数", min_value=10, max_value=200, value=50, step=10)

elif mode == "日付指定":
    today = datetime.today().date()
    min_date = today - timedelta(days=30)
    selected_date = st.sidebar.date_input(
        "日付を選択",
        value=today,
        min_value=min_date,
        max_value=today,
    )
    date_str = selected_date.strftime("%Y%m%d")
    limit_val = st.sidebar.slider("表示件数", min_value=10, max_value=200, value=100, step=10)

elif mode == "銘柄コード検索":
    company_code_input = st.sidebar.text_input(
        "4桁の銘柄コードを入力",
        placeholder="例: 7203",
        max_chars=4,
    )
    limit_val = st.sidebar.slider("表示件数上限", min_value=10, max_value=100, value=50, step=10)

st.sidebar.markdown("---")
refresh_button = st.sidebar.button("🔄 更新", type="primary", use_container_width=True)

# ---- メイン画面 ----
df = None

if mode == "最新一覧":
    st.markdown(f"### 最新の適時開示（最大 {limit_val} 件）")
    with st.spinner("最新の適時開示を取得中..."):
        df = get_latest(limit=limit_val)

elif mode == "日付指定":
    if date_str:
        st.markdown(f"### {selected_date.strftime('%Y年%m月%d日')} の適時開示")
        with st.spinner(f"{date_str} の開示情報を取得中..."):
            df = get_by_date(date_str=date_str, limit=limit_val)
    else:
        st.info("日付を選択してください")

elif mode == "銘柄コード検索":
    if company_code_input and len(company_code_input) == 4:
        st.markdown(f"### 銘柄コード `{company_code_input}` の適時開示（過去30日）")
        with st.spinner(f"銘柄 {company_code_input} の開示情報を取得中..."):
            df = get_by_company(code_4digit=company_code_input, days=30, limit=limit_val)
    elif company_code_input and len(company_code_input) != 4:
        st.warning("銘柄コードは4桁で入力してください")
    else:
        st.info("4桁の銘柄コードを左サイドバーに入力してください")

# 結果表示
if df is not None:
    if df.empty:
        st.warning("該当する開示情報がありません。条件を変更して再試行してください。")
    else:
        st.markdown(f"**{len(df)} 件の開示情報が見つかりました**")
        render_disclosure_table(df)

        # CSVダウンロード
        csv_data = df.to_csv(index=False, encoding="utf-8-sig")
        st.download_button(
            label="📥 CSVダウンロード",
            data=csv_data,
            file_name=f"disclosures_{datetime.today().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
        )
elif mode == "最新一覧":
    st.info("「🔄 更新」ボタンをクリックして最新の開示情報を取得してください")
