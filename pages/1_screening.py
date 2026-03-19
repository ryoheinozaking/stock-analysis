# -*- coding: utf-8 -*-
"""
スクリーニングページ
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env'))

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

from screener import JQuantsClient, ScreeningCriteria, evaluate_stock
from services.jquants_service import get_listed_info, get_ohlcv
from services.batch_service import load_cache, get_cache_updated_at, fetch_all_stocks

# カスタムCSS読み込み
css_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "styles", "custom.css")
if os.path.exists(css_path):
    with open(css_path, "r", encoding="utf-8") as f:
        css_content = f.read()
    st.markdown(f"<style>{css_content}</style>", unsafe_allow_html=True)

st.title("⚡ スクリーニング")

# session_state 初期化
if "selected_code" not in st.session_state:
    st.session_state["selected_code"] = ""
if "screening_result" not in st.session_state:
    st.session_state["screening_result"] = None

# ---- サイドバー ----
st.sidebar.header("🔍 スクリーニング条件")

market_options = {
    "東証プライム (0111)": "0111",
    "東証スタンダード (0112)": "0112",
    "東証グロース (0113)": "0113",
}
selected_markets = st.sidebar.multiselect(
    "市場選択",
    options=list(market_options.keys()),
    default=["東証プライム (0111)"],
)
market_codes = [market_options[m] for m in selected_markets]

st.sidebar.markdown("---")

per_max = st.sidebar.slider("PER上限", min_value=5, max_value=50, value=20, step=1)
pbr_range = st.sidebar.slider("PBR範囲", min_value=0.0, max_value=5.0, value=(0.5, 1.5), step=0.1)
roe_min = st.sidebar.slider("ROE下限 (%)", min_value=0, max_value=30, value=8)
div_yield_min = st.sidebar.slider("配当利回り下限 (%)", min_value=0.0, max_value=10.0, value=2.0, step=0.1)
rev_growth_min = st.sidebar.slider("売上成長率下限 (%)", min_value=-20, max_value=50, value=5)
profit_growth_min = st.sidebar.slider("利益成長率下限 (%)", min_value=-50, max_value=200, value=5)
rsi_range = st.sidebar.slider("RSI範囲", min_value=0, max_value=100, value=(40, 70))
above_ma25 = st.sidebar.checkbox("25日MA上のみ", value=True)
volume_avg_min = st.sidebar.number_input("平均出来高下限", min_value=0, value=100000, step=10000)

st.sidebar.markdown("---")
st.sidebar.subheader("📊 追加フィルター")
volume_surge = st.sidebar.checkbox("出来高急増（平均の2倍以上）", value=False)
high_roe = st.sidebar.checkbox("高ROE優先（ROE 15%以上）", value=False)
near_52w_high = st.sidebar.checkbox("52週高値圏（直近高値の90%以上）", value=False)

st.sidebar.markdown("---")

# キャッシュ状態表示
cache_df = load_cache()
cache_updated = get_cache_updated_at()

if cache_updated:
    st.sidebar.success(f"📦 キャッシュ: {cache_updated.strftime('%Y/%m/%d %H:%M')} 更新")
else:
    st.sidebar.warning("📦 キャッシュなし（データ更新が必要です）")

update_button = st.sidebar.button("🔄 データ更新（全銘柄取得）", use_container_width=True)
run_button = st.sidebar.button("▶ スクリーニング実行", type="primary", use_container_width=True)

# ---- データ更新処理 ----
if update_button:
    with st.status("全銘柄データを取得中... (数十分かかる場合があります)", expanded=True) as status:
        try:
            progress_bar = st.progress(0)
            log_area = st.empty()

            def progress_callback(i, total, code):
                progress_bar.progress((i + 1) / total)
                if i % 50 == 0:
                    log_area.write(f"処理中... {i+1}/{total} 件")

            cache_df = fetch_all_stocks(
                market_codes=market_codes if market_codes else None,
                progress_callback=progress_callback,
            )
            progress_bar.progress(1.0)
            st.session_state["screening_result"] = None  # 結果リセット
            status.update(label=f"✅ データ更新完了 ({len(cache_df)} 銘柄)", state="complete")
        except Exception as e:
            st.error(f"データ更新中にエラーが発生しました: {e}")
            status.update(label="エラーが発生しました", state="error")

# ---- メイン画面 ----
if not run_button and st.session_state["screening_result"] is None:
    st.info("""
    ### 使い方ガイド

    #### 高速モード（推奨）
    1. サイドバーの **「🔄 データ更新」** を押して全銘柄データを取得（初回・1日1回）
    2. **「▶ スクリーニング実行」** → 瞬時に結果表示

    #### 通常モード
    - データ更新なしでも実行可能（銘柄数上限あり・時間がかかります）

    #### 評価指標の説明
    | 指標 | 説明 |
    |------|------|
    | PER | 株価収益率（低いほど割安） |
    | PBR | 株価純資産倍率（低いほど割安） |
    | ROE | 自己資本利益率（高いほど収益性が高い） |
    | 配当利回り | 年間配当 ÷ 株価（高いほど良い） |
    | 売上成長率 | 前期比売上高の伸び率 |
    | 利益成長率 | 前期比純利益の伸び率 |
    | RSI | テクニカル指標（40〜70が中立圏） |

    #### ファンダスコア算式（最大90点）
    - PER(25点) + PBR(15点) + ROE(20点) + 売上成長(20点) + RSI(10点)

    #### シグナルスコア算式（最大45点）
    | シグナル | 点数 | 条件 |
    |---------|------|------|
    | GC | +15点 | MA5がMA25を直近5本以内で上抜け |
    | GC(中期) | +10点 | MA25がMA75を直近5本以内で上抜け |
    | MACD買転換 | +10点 | MACDがシグナルを直近5本以内で上抜け |
    | RSI反転 | +10点 | RSIが30を直近5本以内で上抜け |
    """)

if run_button:
    cache_df = load_cache()

    if cache_df is not None and not cache_df.empty:
        # ---- キャッシュモード（高速） ----
        df = cache_df.copy()

        # 市場フィルタ
        if market_codes and "market" in df.columns:
            market_name_map = {"0111": "東証プライム", "0112": "東証スタンダード", "0113": "東証グロース"}
            target_names = [market_name_map.get(c, c) for c in market_codes]
            df = df[df["market"].isin(target_names)]

        # 条件フィルタ
        if "PER" in df.columns:
            df = df[df["PER"].notna() & (df["PER"] > 0) & (df["PER"] <= per_max)]
        if "PBR" in df.columns:
            df = df[df["PBR"].notna() & (df["PBR"] >= pbr_range[0]) & (df["PBR"] <= pbr_range[1])]
        if "ROE" in df.columns:
            df = df[df["ROE"].notna() & (df["ROE"] >= roe_min)]
        if "div_yield" in df.columns:
            df = df[df["div_yield"].notna() & (df["div_yield"] >= div_yield_min)]
        if "rev_growth" in df.columns:
            df = df[df["rev_growth"].notna() & (df["rev_growth"] >= rev_growth_min)]
        if "profit_growth" in df.columns:
            df = df[df["profit_growth"].notna() & (df["profit_growth"] >= profit_growth_min)]
        if "RSI" in df.columns:
            df = df[df["RSI"].notna() & (df["RSI"] >= rsi_range[0]) & (df["RSI"] <= rsi_range[1])]
        if above_ma25 and "close" in df.columns and "MA25" in df.columns:
            df = df[df["close"] >= df["MA25"]]
        if "avg_volume" in df.columns:
            df = df[df["avg_volume"] >= volume_avg_min]

        result_df = df.sort_values("score", ascending=False).reset_index(drop=True)
        st.session_state["screening_result"] = result_df
        st.success(f"⚡ キャッシュから高速スクリーニング完了: {len(result_df)} 件マッチ（{cache_updated.strftime('%Y/%m/%d %H:%M')} のデータ）")

    else:
        # ---- 通常モード（リアルタイム・銘柄数制限あり） ----
        max_stocks = 200
        criteria = ScreeningCriteria(
            per_max=float(per_max),
            pbr_max=float(pbr_range[1]),
            pbr_min=float(pbr_range[0]),
            dividend_yield_min=float(div_yield_min),
            revenue_growth_min=float(rev_growth_min),
            profit_growth_min=float(profit_growth_min),
            roe_min=float(roe_min),
            rsi_min=float(rsi_range[0]),
            rsi_max=float(rsi_range[1]),
            above_ma25=above_ma25,
            volume_avg_min=int(volume_avg_min),
        )
        to_date = datetime.today().strftime("%Y-%m-%d")
        from_date = (datetime.today() - timedelta(days=120)).strftime("%Y-%m-%d")

        with st.status("スクリーニング実行中（通常モード）...", expanded=True) as status:
            try:
                st.write("銘柄マスタを取得中...")
                listed_df = get_listed_info()
                if listed_df.empty:
                    st.error("銘柄マスタの取得に失敗しました")
                    status.update(label="エラーが発生しました", state="error")
                    st.stop()

                if market_codes:
                    filtered = listed_df[listed_df["Mkt"].isin(market_codes)]
                else:
                    filtered = listed_df

                codes = filtered["Code"].dropna().unique().tolist()[:max_stocks]
                st.write(f"対象銘柄数: {len(codes)} 件（上限{max_stocks}件）")
                st.info("💡 「🔄 データ更新」を実行すると全銘柄を高速スクリーニングできます")

                client = JQuantsClient()
                progress_bar = st.progress(0)
                log_area = st.empty()
                results = []
                total = len(codes)

                for i, code in enumerate(codes):
                    progress_bar.progress((i + 1) / total)
                    result = evaluate_stock(code, client, criteria, from_date, to_date)
                    if result:
                        code_info = filtered[filtered["Code"] == code]
                        if not code_info.empty and "CoName" in code_info.columns:
                            result["company_name"] = code_info.iloc[0].get("CoName", "")
                        result["code_4"] = code[:4]
                        results.append(result)
                        sig_str = f" | シグナル: {result['signals']}" if result.get("signals", "−") != "−" else ""
                        log_area.write(f"✅ {code[:4]} {result.get('company_name', '')} | スコア: {result['score']:.1f} | 株価: {result['close']:,.0f}円{sig_str}")

                progress_bar.progress(1.0)
                if results:
                    result_df = pd.DataFrame(results).sort_values("score", ascending=False).reset_index(drop=True)
                    st.session_state["screening_result"] = result_df
                    status.update(label=f"完了 {len(results)} 件マッチ", state="complete")
                else:
                    st.session_state["screening_result"] = pd.DataFrame()
                    status.update(label="完了 0件マッチ", state="complete")

            except Exception as e:
                st.error(f"スクリーニング中にエラーが発生しました: {e}")
                status.update(label="エラーが発生しました", state="error")

# ---- 結果表示 ----
if st.session_state["screening_result"] is not None:
    result_df = st.session_state["screening_result"]

    if result_df.empty:
        st.warning("条件に合致する銘柄が見つかりませんでした。条件を緩めて再試行してください。")
    else:
        # 追加フィルター
        if volume_surge and "avg_volume" in result_df.columns:
            result_df = result_df[result_df["avg_volume"] > int(volume_avg_min) * 2]
        if high_roe and "ROE" in result_df.columns:
            result_df = result_df[result_df["ROE"] > 15]

        if near_52w_high and not result_df.empty:
            to_date_52w = datetime.today().strftime("%Y-%m-%d")
            from_date_52w = (datetime.today() - timedelta(days=365)).strftime("%Y-%m-%d")
            keep = []
            for _, row in result_df.iterrows():
                code_5 = str(row.get("code", ""))
                if not code_5:
                    keep.append(False)
                    continue
                try:
                    df_52w = get_ohlcv(code_5, to_date_52w, from_date_52w)
                    if df_52w.empty:
                        keep.append(False)
                        continue
                    high_52w = df_52w["AdjH"].max()
                    current = float(row.get("close", 0))
                    keep.append(current >= high_52w * 0.9)
                except Exception:
                    keep.append(False)
            result_df = result_df[keep]

        st.markdown(f"### スクリーニング結果: {len(result_df)} 件")

        cols_order = ["code_4", "company_name", "close", "score", "signal_score", "signals",
                      "PER", "PBR", "ROE", "div_yield", "rev_growth", "profit_growth", "RSI"]
        display_df = result_df[[c for c in cols_order if c in result_df.columns]]

        col_config = {
            "score": st.column_config.ProgressColumn("ファンダスコア", min_value=0, max_value=90, format="%.1f"),
            "signal_score": st.column_config.ProgressColumn("シグナルスコア", min_value=0, max_value=45, format="%.0f"),
            "signals": st.column_config.TextColumn("発火シグナル"),
            "code_4": st.column_config.TextColumn("銘柄コード"),
            "company_name": st.column_config.TextColumn("会社名"),
            "close": st.column_config.NumberColumn("株価(円)", format="%.0f"),
            "PER": st.column_config.NumberColumn("PER", format="%.2f"),
            "PBR": st.column_config.NumberColumn("PBR", format="%.2f"),
            "ROE": st.column_config.NumberColumn("ROE(%)", format="%.2f"),
            "div_yield": st.column_config.NumberColumn("配当利回り(%)", format="%.2f"),
            "rev_growth": st.column_config.NumberColumn("売上成長(%)", format="%.1f"),
            "profit_growth": st.column_config.NumberColumn("利益成長(%)", format="%.1f"),
            "RSI": st.column_config.NumberColumn("RSI", format="%.1f"),
        }

        selection = st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            column_config=col_config,
            on_select="rerun",
            selection_mode="single-row",
        )

        if selection and selection.selection and selection.selection.rows:
            selected_row_idx = selection.selection.rows[0]
            selected_code = result_df.iloc[selected_row_idx]["code"]
            selected_code_4 = selected_code[:4]

            col_a, col_b = st.columns([2, 1])
            with col_a:
                st.success(f"選択中: {selected_code_4} {result_df.iloc[selected_row_idx].get('company_name', '')}")
            with col_b:
                if st.button("📈 詳細を見る", type="primary"):
                    st.session_state["selected_code"] = selected_code
                    st.switch_page("pages/2_stock_detail.py")

        csv_data = result_df.to_csv(index=False, encoding="utf-8-sig")
        st.download_button(
            label="📥 CSVダウンロード",
            data=csv_data,
            file_name=f"screening_result_{datetime.today().strftime('%Y%m%d')}.csv",
            mime="text/csv",
        )
