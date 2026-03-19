# -*- coding: utf-8 -*-
"""
銘柄詳細ページ
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

from services.jquants_service import get_ohlcv, get_financials, get_company_info, resample_ohlcv, get_listed_info
from services.tdnet_service import get_by_company
from components.chart import build_ohlcv_chart
from components.financial_cards import render_metric_row, render_financials_table
from components.disclosure_table import render_disclosure_table
from screener import JQuantsClient, ScreeningCriteria, evaluate_stock

# カスタムCSS読み込み
css_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "styles", "custom.css")
if os.path.exists(css_path):
    with open(css_path, "r", encoding="utf-8") as f:
        css_content = f.read()
    st.markdown(f"<style>{css_content}</style>", unsafe_allow_html=True)

st.title("📈 銘柄詳細")

# session_state 初期化
if "selected_code" not in st.session_state:
    st.session_state["selected_code"] = ""

# ---- 入力 ----
col_input1, col_input2, col_input3 = st.columns([2, 1, 1])

with col_input1:
    listed_df = get_listed_info()
    if not listed_df.empty:
        options = [f"{row['Code'][:4]} {row['CoName']}" for _, row in listed_df.iterrows()]
        stored = st.session_state.get("selected_code", "")
        stored_4 = stored[:4] if len(stored) >= 4 else stored
        default_idx = 0
        if stored_4:
            for i, opt in enumerate(options):
                if opt.startswith(stored_4):
                    default_idx = i
                    break
        selected_opt = st.selectbox("銘柄コード / 銘柄名", options, index=default_idx)
        code_4 = selected_opt[:4].strip()
    else:
        code_4 = st.text_input("銘柄コード (4桁)", placeholder="例: 7203", max_chars=4)

with col_input2:
    freq_options = {"日足": "D", "週足": "W", "月足": "ME"}
    freq_label = st.selectbox("足種", list(freq_options.keys()), index=0)
    freq = freq_options[freq_label]

with col_input3:
    # 足種によって選択できる期間を変える
    if freq == "D":
        period_options = ["1ヶ月", "3ヶ月", "6ヶ月", "1年"]
        default_idx = 1
    elif freq == "W":
        period_options = ["3ヶ月", "6ヶ月", "1年", "3年"]
        default_idx = 2
    else:  # 月足
        period_options = ["1年", "3年", "5年", "10年"]
        default_idx = 1
    period_sel = st.selectbox("期間", period_options, index=default_idx)

# 足種・期間 → 取得日数に変換
period_days_map = {
    "1ヶ月": 35, "3ヶ月": 100, "6ヶ月": 190, "1年": 370,
    "3年": 1100, "5年": 1830, "10年": 3660,
}
# J-Quants Lightプランは過去5年（約1826日）まで
JQUANTS_LIMIT_DAYS = 1826
JQUANTS_MIN_DATE = datetime.today() - timedelta(days=JQUANTS_LIMIT_DAYS)

days_back = period_days_map[period_sel]
to_date = datetime.today().strftime("%Y-%m-%d")
from_date_raw = datetime.today() - timedelta(days=days_back)
# プラン上限を超える場合は自動クリップ
if from_date_raw < JQUANTS_MIN_DATE:
    from_date = JQUANTS_MIN_DATE.strftime("%Y-%m-%d")
    clipped = True
else:
    from_date = from_date_raw.strftime("%Y-%m-%d")
    clipped = False
from_date_120 = (datetime.today() - timedelta(days=120)).strftime("%Y-%m-%d")

# 足種ごとのMA期間
ma_periods_map = {"D": [5, 25, 75, 200], "W": [13, 26], "ME": [6, 12]}

if not code_4:
    st.info("銘柄コード（4桁）を入力してください。例: `7203`（トヨタ自動車）")
    st.stop()

# 4桁 → 内部処理は5桁（末尾に"0"を付加）
code = code_4 + "0" if len(code_4) == 4 else code_4

# session_state に5桁で保存
if code:
    st.session_state["selected_code"] = code

# ---- 銘柄情報ヘッダー ----
with st.spinner("銘柄情報を取得中..."):
    company_info = get_company_info(code)

if company_info:
    company_name = company_info.get("CoName", code)
    market_name = company_info.get("MktNm", "")
    sector = company_info.get("S33Nm", company_info.get("S17Nm", ""))

    # 株価取得（最新）
    ohlcv_df = get_ohlcv(code, from_date_120, to_date)
    current_price = None
    price_change = None
    if not ohlcv_df.empty and "AdjC" in ohlcv_df.columns:
        current_price = float(ohlcv_df["AdjC"].iloc[-1])
        if len(ohlcv_df) >= 2:
            prev_price = float(ohlcv_df["AdjC"].iloc[-2])
            price_change = current_price - prev_price
            price_change_pct = (price_change / prev_price * 100) if prev_price != 0 else 0

    # ヘッダー表示
    st.markdown(f"## {company_name}")
    col_h1, col_h2, col_h3, col_h4 = st.columns(4)
    with col_h1:
        st.markdown(f"**市場:** {market_name}")
    with col_h2:
        st.markdown(f"**セクター:** {sector}")
    with col_h3:
        if current_price is not None:
            st.metric(
                "現在株価 (円)",
                f"{current_price:,.0f}",
                delta=f"{price_change:+.0f} ({price_change_pct:+.2f}%)" if price_change is not None else None,
            )
    with col_h4:
        st.markdown(f"**銘柄コード:** `{code_4}`")
else:
    st.warning(f"銘柄コード `{code}` の情報が見つかりません。コードを確認してください。")
    ohlcv_df = get_ohlcv(code, from_date, to_date)
    company_name = code

if clipped:
    st.info(f"ℹ️ J-Quants Lightプランのデータ取得期間は過去5年までのため、{JQUANTS_MIN_DATE.strftime('%Y/%m/%d')}以降のデータを表示しています。")

st.markdown("---")

# ---- タブ ----
tab1, tab2, tab3 = st.tabs(["📈 株価チャート", "💰 財務指標", "📰 適時開示"])

# タブ1: 株価チャート
with tab1:
    # テクニカル指標トグル
    ind_col1, ind_col2, ind_col3, ind_col4, ind_col5, ind_col6 = st.columns(6)
    with ind_col1:
        show_bb = st.checkbox("ボリンジャーバンド (BB±2σ/3σ)", value=False)
    with ind_col2:
        show_macd = st.checkbox("MACD", value=False)
    with ind_col3:
        show_rsi = st.checkbox("RSI(14)", value=False)
    with ind_col4:
        show_ichimoku = st.checkbox("一目均衡表", value=False)
    with ind_col5:
        show_volume_profile = st.checkbox("価格帯別出来高", value=False)
    with ind_col6:
        show_signals = st.checkbox("シグナル表示", value=False)

    ohlcv_chart_df = get_ohlcv(code, from_date, to_date)
    if ohlcv_chart_df is None or ohlcv_chart_df.empty:
        st.warning("株価データがありません")
    else:
        required_cols = ["Date", "AdjO", "AdjH", "AdjL", "AdjC"]
        if all(c in ohlcv_chart_df.columns for c in required_cols):
            # 足種に応じてリサンプル
            chart_df = resample_ohlcv(ohlcv_chart_df, freq)
            ma_periods = ma_periods_map[freq]
            st.markdown(f"**{company_name} ({code_4}) - {freq_label} {period_sel}**")
            fig = build_ohlcv_chart(
                chart_df,
                title="",
                show_ma=ma_periods,
                show_bb=show_bb,
                show_macd=show_macd,
                show_rsi=show_rsi,
                show_ichimoku=show_ichimoku,
                show_volume_profile=show_volume_profile,
                show_signals=show_signals,
            )
            st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": "hover"})

            # 統計情報
            with st.expander("📊 統計情報"):
                col_s1, col_s2, col_s3, col_s4 = st.columns(4)
                with col_s1:
                    st.metric("期間高値", f"{ohlcv_chart_df['AdjH'].max():,.0f}")
                with col_s2:
                    st.metric("期間安値", f"{ohlcv_chart_df['AdjL'].min():,.0f}")
                with col_s3:
                    vol_col = "AdjVo" if "AdjVo" in ohlcv_chart_df.columns else None
                    if vol_col:
                        st.metric("平均出来高", f"{ohlcv_chart_df[vol_col].mean():,.0f}")
                with col_s4:
                    st.metric("データ件数", f"{len(ohlcv_chart_df)} 日")
        else:
            st.error(f"必要な列が不足しています: {[c for c in required_cols if c not in ohlcv_chart_df.columns]}")

# タブ2: 財務指標
with tab2:
    # evaluate_stock で財務指標を取得
    with st.spinner("財務指標を計算中..."):
        try:
            client = JQuantsClient()
            criteria = ScreeningCriteria()
            stock_data = evaluate_stock(code, client, criteria, from_date_120, to_date)
        except Exception as e:
            st.error(f"財務指標の取得に失敗しました: {e}")
            stock_data = None

    if stock_data:
        render_metric_row(stock_data, company_info)
    else:
        st.info("財務指標データが取得できませんでした（銘柄コードの確認、またはデータが存在しない可能性があります）")

    st.markdown("---")
    st.markdown("#### 財務推移（直近5期）")

    with st.spinner("財務データを取得中..."):
        fin_df = get_financials(code)

    render_financials_table(fin_df)

# タブ3: 適時開示
with tab3:
    code_4digit = code[:4] if len(code) >= 4 else code
    with st.spinner(f"適時開示情報を取得中 ({code_4digit})..."):
        disclosure_df = get_by_company(code_4digit)

    if disclosure_df is not None and not disclosure_df.empty:
        st.markdown(f"**{len(disclosure_df)} 件の開示情報**（過去30日間）")
        render_disclosure_table(disclosure_df)
    else:
        st.info("過去30日間の適時開示情報がありません")
