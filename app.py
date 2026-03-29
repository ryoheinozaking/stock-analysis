# -*- coding: utf-8 -*-
"""
株式スクリーナー - Streamlit アプリ エントリーポイント
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'))

import streamlit as st
import pandas as pd

st.set_page_config(
    page_title="株式スクリーナー",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="auto",
)

# カスタムCSS読み込み
css_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "styles", "custom.css")
if os.path.exists(css_path):
    with open(css_path, "r", encoding="utf-8") as f:
        css_content = f.read()
    st.markdown(f"<style>{css_content}</style>", unsafe_allow_html=True)

# session_state 初期化
if "selected_code" not in st.session_state:
    st.session_state["selected_code"] = ""
if "screening_result" not in st.session_state:
    st.session_state["screening_result"] = None

# ホーム画面
st.title("株式スクリーナー")
st.markdown("J-Quants API v2 を活用した日本株スクリーニング・分析ツールです。")
st.markdown("---")

# 4ページへのリンクカード
col1, col2, col3, col4, col5, col6 = st.columns(6)

CARD_STYLE = (
    "background:#161b22; border-radius:12px; padding:24px;"
    "border:1px solid #30363d; border-top:3px solid {color};"
    "min-height:160px; box-shadow:0 2px 8px rgba(0,0,0,0.4);"
)
ICON_STYLE = (
    "width:42px; height:42px; border-radius:10px;"
    "background:{color}26; display:inline-flex; align-items:center;"
    "justify-content:center; font-size:22px; margin-bottom:14px;"
)
TITLE_STYLE = "color:#e6edf3; margin:0 0 8px 0; font-size:1.05rem; font-weight:600;"
DESC_STYLE  = "color:#8b949e; font-size:0.84rem; line-height:1.6; margin:0;"

cards = [
    ("pages/1_screening.py", "スクリーニングを開始", "#06b6d4", "⚡", "スクリーニング",
     "PER・PBR・ROE・配当利回り・テクニカル指標などを組み合わせて割安・好業績株をスクリーニングします。"),
    ("pages/2_stock_detail.py", "銘柄詳細を見る", "#26a69a", "📈", "銘柄詳細",
     "個別銘柄の株価チャート・財務指標・適時開示情報を一画面で確認できます。"),
    ("pages/3_disclosures.py", "適時開示を確認", "#ff9800", "📰", "適時開示",
     "TDnetの適時開示情報を最新順・日付指定・銘柄コード別で検索・閲覧できます。"),
    ("pages/4_portfolio.py", "ポートフォリオを見る", "#9c27b0", "💹", "ポートフォリオ",
     "SBI証券のCSVをインポートして保有状況・含み損益・セクター分散をグラフで確認できます。"),
    ("pages/5_portfolio_analysis.py", "AI分析を実行", "#e91e63", "🤖", "AI分析",
     "Claude AIが保有銘柄をファンダ・テクニカル両面から分析し、売買提案とアクションを提示します。"),
    ("pages/6_trade_log.py", "トレードを記録", "#2e7d32", "📓", "トレードログ",
     "実トレードを記録・集計し、戦略別勝率・RSI別成績など自己分析データを蓄積します。"),
]

for col, (page, label, color, icon, title, desc) in zip([col1, col2, col3, col4, col5, col6], cards):
    with col:
        st.markdown(f"""
        <div style="{CARD_STYLE.format(color=color)}">
            <div style="{ICON_STYLE.format(color=color)}">{icon}</div>
            <h3 style="{TITLE_STYLE}">{title}</h3>
            <p style="{DESC_STYLE}">{desc}</p>
        </div>
        """, unsafe_allow_html=True)
        st.page_link(page, label=label)

st.markdown("---")
st.markdown("""
### 使い方
1. **スクリーニング**: 左サイドバーで条件を設定し「▶ スクリーニング実行」をクリック
2. **銘柄詳細**: スクリーニング結果から銘柄を選択、または銘柄コードを直接入力
3. **適時開示**: 最新ニュース・特定日付・特定銘柄の開示情報を確認
4. **ポートフォリオ**: SBI証券CSVをインポートして保有状況を可視化
5. **AI分析**: Claude AIによるポートフォリオ分析・売買提案

### データソース
- **株価・財務データ**: [J-Quants API v2](https://jpx-jquants.com/)
- **適時開示**: [TDnet (Yanoshin)](https://webapi.yanoshin.jp/webapi/tdnet/)
""")

# サイドバー情報
st.sidebar.title("📈 株式スクリーナー")
st.sidebar.markdown("---")
st.sidebar.markdown("**ナビゲーション**")
st.sidebar.page_link("app.py", label="ホーム", icon="🏠")
st.sidebar.page_link("pages/1_screening.py", label="スクリーニング", icon="⚡")
st.sidebar.page_link("pages/2_stock_detail.py", label="銘柄詳細", icon="📈")
st.sidebar.page_link("pages/3_disclosures.py", label="適時開示", icon="📰")
st.sidebar.page_link("pages/4_portfolio.py", label="ポートフォリオ", icon="💹")
st.sidebar.page_link("pages/5_portfolio_analysis.py", label="AI分析", icon="🤖")
st.sidebar.page_link("pages/6_trade_log.py", label="トレードログ", icon="📓")

if st.session_state.get("selected_code"):
    st.sidebar.markdown("---")
    st.sidebar.markdown(f"**選択中の銘柄:** `{st.session_state['selected_code']}`")
