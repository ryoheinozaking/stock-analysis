# -*- coding: utf-8 -*-
"""
ポートフォリオ管理ページ
SBI証券のポートフォリオCSVを読み込み、保有状況を可視化する
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))
import streamlit as st

st.set_page_config(layout="wide")
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import glob

from services.portfolio_service import parse_sbi_csv
from services.jquants_service import get_listed_info
from services.tdnet_service import get_by_date
from components.disclosure_table import render_disclosure_table

# カスタムCSS読み込み
css_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "styles", "custom.css")
if os.path.exists(css_path):
    with open(css_path, "r", encoding="utf-8") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

st.title("💹 ポートフォリオ管理")

# session_state 初期化
if "portfolio_df" not in st.session_state:
    st.session_state["portfolio_df"] = None
if "funds_df" not in st.session_state:
    st.session_state["funds_df"] = None
if "portfolio_updated" not in st.session_state:
    st.session_state["portfolio_updated"] = None
# ---- CSVインポート ----
DOWNLOADS_DIR = os.path.expanduser("~/Downloads")
SBI_CSV_NAME = "New_file.csv"  # SBI証券の固定ファイル名
def _find_latest_sbi_csv():
    """Downloadsフォルダから最新のSBI CSVを探す"""
    # New_file*.csv（New_file.csv / New_file (1).csv 等）の中で最新のものを返す
    sbi_candidates = glob.glob(os.path.join(DOWNLOADS_DIR, "New_file*.csv"))
    if sbi_candidates:
        latest = max(sbi_candidates, key=os.path.getmtime)
        return latest, os.path.getmtime(latest)
    # 見つからなければDownloads内の全CSVから最新を返す
    candidates = glob.glob(os.path.join(DOWNLOADS_DIR, "*.csv"))
    if not candidates:
        return None, None
    latest = max(candidates, key=os.path.getmtime)
    return latest, os.path.getmtime(latest)
def _load_csv(file_obj):
    stocks_df, funds_df = parse_sbi_csv(file_obj)
    if not stocks_df.empty:
        st.session_state["portfolio_df"] = stocks_df
        st.session_state["portfolio_updated"] = datetime.now().strftime("%Y/%m/%d %H:%M")
    if not funds_df.empty:
        st.session_state["funds_df"] = funds_df
    if stocks_df.empty and funds_df.empty:
        st.error("株式・投資信託データが見つかりません。SBI証券の保有証券CSVか確認してください。")
        return
    msg = []
    if not stocks_df.empty: msg.append(f"株式 {len(stocks_df)} 銘柄")
    if not funds_df.empty:  msg.append(f"投資信託 {len(funds_df)} 本")
    st.success("、".join(msg) + "を読み込みました")
# ---- ページ表示時に自動読み込み ----
if st.session_state["portfolio_df"] is None:
    latest_path_auto, _ = _find_latest_sbi_csv()
    if latest_path_auto:
        try:
            with open(latest_path_auto, "rb") as f:
                _load_csv(f)
        except Exception:
            pass

with st.sidebar:
    st.header("📂 CSVインポート")

    # 自動検出ボタン
    latest_path, latest_mtime = _find_latest_sbi_csv()
    if latest_path:
        mtime_str = datetime.fromtimestamp(latest_mtime).strftime("%m/%d %H:%M")
        st.info(f"📄 検出: `{os.path.basename(latest_path)}`\n\n更新: {mtime_str}")
        if st.button("⚡ 自動読み込み", type="primary", use_container_width=True,
                     help="Downloadsフォルダの最新CSVを自動で読み込みます"):
            with st.spinner("読み込み中..."):
                try:
                    with open(latest_path, "rb") as f:
                        _load_csv(f)
                except Exception as e:
                    st.error(f"読み込みエラー: {e}")
    else:
        st.warning("Downloadsフォルダに CSV が見つかりません")

    st.markdown("---")

    # 手動アップロード（フォールバック）
    with st.expander("📁 手動でファイルを選択"):
        st.markdown("""
        **SBI証券 CSVダウンロード手順:**
        1. SBI証券にログイン
        2. **口座管理** → **保有証券**
        3. **「CSVダウンロード」** をクリック
        """)
        uploaded_file = st.file_uploader(
            "CSVをアップロード", type=["csv"],
            help="SBI証券の保有証券一覧CSV（CP932エンコード）"
        )
        if uploaded_file:
            if st.button("📊 読み込む", use_container_width=True):
                with st.spinner("解析中..."):
                    try:
                        _load_csv(uploaded_file)
                    except Exception as e:
                        st.error(f"読み込みエラー: {e}")

    if st.session_state["portfolio_updated"]:
        st.caption(f"最終更新: {st.session_state['portfolio_updated']}")

    st.divider()
    st.header("🔧 表示設定")
    show_sector = st.checkbox("セクター情報を表示", value=True)
    sort_by = st.selectbox("並び順", ["評価額（降順）", "損益（降順）", "損益%（降順）", "損益（昇順）"])
# ---- データなし ----
if st.session_state["portfolio_df"] is None:
    st.info("""
    ### 使い方
    1. **左サイドバー**からSBI証券のポートフォリオCSVをアップロードしてください
    2. **「📊 読み込む」** ボタンをクリックすると保有状況が表示されます

    #### CSVのダウンロード方法
    SBI証券 → 口座管理 → 保有証券 → CSVダウンロード
    """)
    st.stop()
# ---- データ取得 ----
df = st.session_state["portfolio_df"].copy()

# セクター情報付与（J-Quants銘柄マスタから）
if show_sector:
    try:
        listed = get_listed_info()
        # 4桁コードでマッチング
        listed["code_4"] = listed["Code"].str[:4]
        sector_map = listed.drop_duplicates("code_4").set_index("code_4")[["S33Nm", "S17Nm", "MktNm"]].to_dict("index")
        df["セクター"] = df["code_4"].map(lambda c: sector_map.get(c, {}).get("S33Nm", "不明"))
        df["市場"] = df["code_4"].map(lambda c: sector_map.get(c, {}).get("MktNm", ""))
    except Exception:
        df["セクター"] = "不明"

# 並び順
sort_map = {
    "評価額（降順）": ("評価額", False),
    "損益（降順）": ("損益", False),
    "損益%（降順）": ("損益(%)", False),
    "損益（昇順）": ("損益", True),
}
sort_col, sort_asc = sort_map[sort_by]
df = df.sort_values(sort_col, ascending=sort_asc).reset_index(drop=True)
# ---- サマリーカード ----
total_market = df["評価額"].sum()
total_cost = df["取得総額"].sum()
total_pnl = df["損益"].sum()
total_pnl_pct = (total_pnl / total_cost * 100) if total_cost > 0 else 0
total_day_change = df["前日比"].mul(df["数量"]).sum()
total_day_change_pct = (total_day_change / total_market * 100) if total_market > 0 else 0

st.markdown("### 📊 ポートフォリオ全体サマリー")
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("総評価額", f"¥{total_market:,.0f}")
with col2:
    st.metric("含み損益", f"¥{total_pnl:+,.0f}", delta=f"{total_pnl_pct:+.2f}%")
with col3:
    st.metric("前日比（概算）", f"¥{total_day_change:+,.0f}", delta=f"{total_day_change_pct:+.2f}%")
with col4:
    st.metric("保有銘柄数", f"{len(df)} 銘柄")
st.divider()
# ---- グラフ ----
col_chart1, col_chart2 = st.columns(2)

with col_chart1:
    st.markdown("#### 保有銘柄別構成割合")
    fig_pie = px.pie(
        df,
        values="評価額",
        names=df["code_4"] + " " + df["会社名"],
        hole=0.4,
        color_discrete_sequence=px.colors.qualitative.Set3,
    )
    fig_pie.update_traces(textposition="inside", textinfo="percent+label", textfont_size=13)
    fig_pie.update_layout(
        height=500,
        showlegend=False,
        margin=dict(l=0, r=0, t=10, b=0),
    )
    st.plotly_chart(fig_pie, use_container_width=True)

with col_chart2:
    if show_sector and "セクター" in df.columns:
        st.markdown("#### セクター別構成割合")
        sector_df = df.groupby("セクター")["評価額"].sum().reset_index().sort_values("評価額", ascending=False)
        fig_sector = px.pie(
            sector_df,
            values="評価額",
            names="セクター",
            hole=0.4,
            color_discrete_sequence=px.colors.sequential.Blues_r,
        )
        fig_sector.update_traces(textposition="inside", textinfo="percent+label", textfont_size=13)
        fig_sector.update_layout(
            height=500,
            showlegend=False,
            margin=dict(l=0, r=0, t=10, b=0),
        )
        st.plotly_chart(fig_sector, use_container_width=True)
    else:
        st.markdown("#### 口座別 評価額")
        acct_df = df.groupby("口座")["評価額"].sum().reset_index()
        fig_acct = px.pie(
            acct_df,
            values="評価額",
            names="口座",
            hole=0.4,
            color_discrete_sequence=["#3498db", "#27ae60", "#e67e22"],
        )
        fig_acct.update_layout(height=380, margin=dict(l=10, r=10, t=30, b=10))
        st.plotly_chart(fig_acct, use_container_width=True)
st.divider()
# ---- 損益バー ----
st.markdown("#### 銘柄別 損益")
labels = df["code_4"] + " " + df["会社名"].str[:10]
pos_mask = df["損益"] >= 0
neg_mask = df["損益"] < 0

fig_bar = make_subplots(
    rows=1, cols=2,
    shared_yaxes=True,
    horizontal_spacing=0.10,
    column_widths=[0.5, 0.5],
)

# 左列：マイナス（絶対値で描画、軸を反転）
fig_bar.add_trace(go.Bar(
    y=labels,
    x=df["損益"].abs().where(neg_mask, 0),
    orientation="h",
    marker_color="#ef5350",
    text=df["損益(%)"].where(neg_mask).apply(lambda x: f"{x:+.1f}%" if pd.notna(x) else ""),
    textposition="outside",
    showlegend=False,
), row=1, col=1)

# 右列：プラス
fig_bar.add_trace(go.Bar(
    y=labels,
    x=df["損益"].where(pos_mask, 0),
    orientation="h",
    marker_color="#26a69a",
    text=df["損益(%)"].where(pos_mask).apply(lambda x: f"{x:+.1f}%" if pd.notna(x) else ""),
    textposition="outside",
    showlegend=False,
), row=1, col=2)

x_abs_max = df["損益"].abs().max() * 1.25
fig_bar.update_xaxes(range=[x_abs_max, 0], title_text="損益（円）", row=1, col=1)
fig_bar.update_xaxes(range=[0, x_abs_max], title_text="損益（円）", row=1, col=2)
fig_bar.update_yaxes(side="right", col=1, tickfont=dict(size=13))
fig_bar.update_layout(
    height=max(320, len(df) * 36),
    template="plotly_dark",
    showlegend=False,
    bargap=0.3,
    margin=dict(l=10, r=80, t=20, b=20),
)
st.plotly_chart(fig_bar, use_container_width=True)

st.markdown("#### ヒートマップ（前日比）")

fig_heatmap = go.Figure(go.Treemap(
    labels=df["code_4"] + "<br>" + df["会社名"].str[:6] + "<br>" + df["前日比(%)"].apply(lambda x: f"{x:+.2f}%"),
    parents=[""] * len(df),
    values=df["評価額"],
    customdata=df[["前日比(%)", "前日比", "評価額", "会社名"]].values,
    hovertemplate="<b>%{customdata[3]}</b><br>評価額: ¥%{customdata[2]:,.0f}<br>前日比: %{customdata[0]:+.2f}%<br>前日比額: ¥%{customdata[1]:+,.0f}<extra></extra>",
    marker=dict(
        colors=df["前日比(%)"].tolist(),
        colorscale=[
            [0.0, "#ef5350"],
            [0.35, "#ffcdd2"],
            [0.5, "#f5f5f5"],
            [0.65, "#c8e6c9"],
            [1.0, "#26a69a"],
        ],
        cmid=0,
        showscale=True,
        colorbar=dict(title="前日比(%)", tickformat="+.2f"),
    ),
    textfont=dict(size=13),
))
fig_heatmap.update_layout(
    height=420,
    margin=dict(l=10, r=10, t=30, b=10),
)
st.plotly_chart(fig_heatmap, use_container_width=True)
st.divider()
# ---- 保有明細テーブル ----
st.markdown("#### 保有明細")

# 表示列の組み立て
display_cols = ["code_4", "会社名", "口座"]
if show_sector and "セクター" in df.columns:
    display_cols.append("セクター")
if "市場" in df.columns:
    display_cols.append("市場")
display_cols += ["数量", "取得単価", "現在値", "前日比", "前日比(%)", "損益", "損益(%)", "評価額"]

display_df = df[display_cols].copy()

_fmt = {
    "数量":      "{:,.0f}株",
    "取得単価":  "{:,.0f}円",
    "現在値":    "{:,.0f}円",
    "前日比":    "{:+,.0f}円",
    "前日比(%)": "{:+.2f}%",
    "損益":      "{:+,.0f}円",
    "損益(%)":   "{:+.2f}%",
    "評価額":    "{:,.0f}円",
}
styled_df = display_df.style.format({k: v for k, v in _fmt.items() if k in display_df.columns})

col_config = {
    "code_4":    st.column_config.TextColumn("コード", width="small"),
    "会社名":    st.column_config.TextColumn("会社名"),
    "口座":      st.column_config.TextColumn("口座", width="small"),
    "セクター":  st.column_config.TextColumn("セクター"),
    "市場":      st.column_config.TextColumn("市場", width="small"),
}

selection = st.dataframe(
    styled_df,
    use_container_width=True,
    hide_index=True,
    column_config=col_config,
    on_select="rerun",
    selection_mode="single-row",
)

# 選択行 → 詳細ページへ
if selection and selection.selection and selection.selection.rows:
    sel_idx = selection.selection.rows[0]
    sel_row = df.iloc[sel_idx]
    col_a, col_b = st.columns([3, 1])
    with col_a:
        pnl_val = sel_row["損益"]
        pnl_pct_val = sel_row["損益(%)"]
        emoji = "📈" if pnl_val >= 0 else "📉"
        st.success(f"{emoji} 選択中: **{sel_row['code_4']} {sel_row['会社名']}**  損益: ¥{pnl_val:+,.0f} ({pnl_pct_val:+.2f}%)")
    with col_b:
        if st.button("📊 詳細チャートを見る", type="primary"):
            st.session_state["selected_code"] = sel_row["code_5"]
            st.switch_page("pages/2_stock_detail.py")
# ---- 口座別サマリー ----
st.divider()
st.markdown("#### 口座別サマリー")
acct_summary = df.groupby("口座").agg(
    銘柄数=("code_4", "count"),
    評価額=("評価額", "sum"),
    損益=("損益", "sum"),
    取得総額=("取得総額", "sum"),
).reset_index()
acct_summary["損益(%)"] = (acct_summary["損益"] / acct_summary["取得総額"] * 100).round(2)

st.dataframe(
    acct_summary[["口座", "銘柄数", "評価額", "損益", "損益(%)"]].style.format({
        "評価額":   "{:,.0f}円",
        "損益":     "{:+,.0f}円",
        "損益(%)":  "{:+.2f}%",
    }),
    use_container_width=True,
    hide_index=True,
)
# ---- 適時開示通知 ----
st.divider()
st.markdown("#### 📰 保有銘柄の最新開示")
portfolio_codes = set(df["code_4"].astype(str).str[:4].unique())
with st.spinner("適時開示情報を取得中..."):
    all_disclosures = []
    today = datetime.today()
    for d in range(30):
        date = today - timedelta(days=d)
        if date.weekday() >= 5:  # 土日スキップ
            continue
        date_str = date.strftime("%Y%m%d")
        try:
            day_df = get_by_date(date_str, limit=300)
            if day_df is not None and not day_df.empty:
                filtered = day_df[day_df["company_code"].str[:4].isin(portfolio_codes)]
                if not filtered.empty:
                    all_disclosures.append(filtered)
        except Exception:
            continue

    if all_disclosures:
        combined = pd.concat(all_disclosures, ignore_index=True)
        if "pubdate" in combined.columns:
            combined = combined.sort_values("pubdate", ascending=False)
        display_limit = 50
        st.markdown(f"過去30日間に **{len(combined)}件** の開示情報があります（最大{display_limit}件表示）")
        render_disclosure_table(combined.head(display_limit))
    else:
        st.info("保有銘柄の開示情報はありません（過去30日間）")

# ---- 投資信託 ----
funds_df = st.session_state.get("funds_df")
if funds_df is not None and not funds_df.empty:
    st.divider()
    st.markdown("#### 投資信託")
    total_fund_val = funds_df["評価額"].sum()
    total_fund_pnl = funds_df["損益"].sum()
    fc1, fc2 = st.columns(2)
    with fc1:
        st.metric("投資信託 評価額合計", f"¥{total_fund_val:,.0f}")
    with fc2:
        st.metric("投資信託 損益合計", f"¥{total_fund_pnl:+,.0f}")

    st.dataframe(
        funds_df[["ファンド名", "口座", "口数", "取得単価", "基準価額", "前日比", "前日比(%)", "損益", "損益(%)", "評価額"]].style.format({
            "口数":      "{:,.0f}口",
            "取得単価":  "{:,.0f}円",
            "基準価額":  "{:,.0f}円",
            "前日比":    "{:+,.0f}円",
            "前日比(%)": "{:+.2f}%",
            "損益":      "{:+,.0f}円",
            "損益(%)":   "{:+.2f}%",
            "評価額":    "{:,.0f}円",
        }),
        use_container_width=True,
        hide_index=True,
    )
