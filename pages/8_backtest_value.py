# -*- coding: utf-8 -*-
"""
バリュー株モード バックテスト UI

過去スナップショット時点でパイプラインを再現実行し、
Top N選定群 vs 非選定群 のフォワードリターンを比較表示する。
"""

import os
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px

from services.backtest_value_service import (
    run_backtest, DEFAULT_SNAPSHOTS, DEFAULT_TOP_N, DEFAULT_FORWARD_DAYS,
)
from services.pipeline_service import EXCLUDE_SECTORS_VALUE

st.set_page_config(
    page_title="バリュー株バックテスト",
    page_icon="📊",
    layout="wide",
)

st.title("📊 バリュー株モード バックテスト")
st.caption("過去スナップショット時点でパイプラインを再現実行 → Top N 選定群 vs 非選定群 のリターン比較")


# ── サイドバー ─────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("### 設定")
    snapshots_str = st.text_area(
        "スナップショット日付（1行1日付）",
        value="\n".join(DEFAULT_SNAPSHOTS),
        height=110,
        help="YYYY-MM-DD 形式。フォワード期間が確保できる範囲で選択",
    )
    top_n = st.number_input(
        "Top N（選定銘柄数）", value=DEFAULT_TOP_N, min_value=10, max_value=200, step=10
    )
    forward_days = st.number_input(
        "フォワード日数",
        value=DEFAULT_FORWARD_DAYS, min_value=30, max_value=730, step=30,
        help="保有期間（日数）。365 = 12ヶ月",
    )

    st.markdown("---")
    st.markdown("**追加除外ルール（実験用）**")
    st.caption(f"※ 本番パイプラインは既にシクリカル5業種（{', '.join(EXCLUDE_SECTORS_VALUE)}）を除外済み")

    profit_growth_max = st.number_input(
        "利益成長率の上限 (%)（0 = 無効）",
        value=0.0, min_value=0.0, max_value=500.0, step=10.0,
        help="例: 50 → profit_growth > 50% を除外",
    )
    ma200_dev_max = st.number_input(
        "MA200乖離率の上限 (%)（0 = 無効）",
        value=0.0, min_value=0.0, max_value=100.0, step=5.0,
        help="例: 20 → MA200から+20%超の銘柄を除外",
    )
    extra_excludes = st.text_input(
        "追加除外セクター（カンマ区切り、空欄=なし）",
        value="",
        help="例: 機械,化学  本番除外5業種に追加で除外する業種",
    )

    run_btn = st.button("🚀 バックテスト実行", type="primary", use_container_width=True)

    st.markdown("---")
    st.markdown("**設計**")
    st.markdown("""
- 各スナップショット時点で **その時点の財務・株価データ** を使ってパイプラインを実行
- バリューモードのハードフィルタ + スコアリングを適用
- Top N と それ以外（フィルタ通過したが圏外） に分類
- 各銘柄について `forward_days` 後のリターンを計算
- 全スナップショット統合して集計（自然実験設計）

**注意**
- 上場廃止銘柄は除外されている（生存バイアスは限定的に存在）
- メタ情報（社名・セクター）は現時点の値を使用
""")


# ── セッション・実行 ───────────────────────────────────────────────────
if run_btn:
    snapshots = [s.strip() for s in snapshots_str.split("\n") if s.strip()]
    if not snapshots:
        st.error("スナップショット日付が空です")
        st.stop()

    # 追加除外ルール組み立て
    extra_filters = {}
    if profit_growth_max > 0:
        extra_filters["profit_growth_max"] = float(profit_growth_max)
    if ma200_dev_max > 0:
        extra_filters["ma200_dev_max"] = float(ma200_dev_max)
    if extra_excludes.strip():
        extras = [s.strip() for s in extra_excludes.split(",") if s.strip()]
        if extras:
            extra_filters["exclude_sectors"] = extras

    with st.status("バックテスト実行中...", expanded=True) as status:
        log = st.empty()
        def _cb(msg):
            log.write(f"⏳ {msg}")
        try:
            result = run_backtest(
                snapshots=snapshots, top_n=top_n,
                forward_days=forward_days, progress_cb=_cb,
                extra_filters=extra_filters or None,
            )
            result["extra_filters_used"] = extra_filters
            st.session_state.backtest_value_result = result
            status.update(label="✅ 完了", state="complete")
        except Exception as e:
            status.update(label=f"❌ エラー: {e}", state="error")
            st.exception(e)


# ── 結果表示 ──────────────────────────────────────────────────────────
result = st.session_state.get("backtest_value_result")
if result is None:
    st.info("サイドバーからバックテストを実行してください。")
    st.stop()

agg = result["aggregate"]
data_range = result.get("data_range")
if data_range:
    st.caption(f"価格データ範囲: {data_range[0]} 〜 {data_range[1]}")

# 適用された追加除外ルール
extra_used = result.get("extra_filters_used") or {}
if extra_used:
    parts = []
    if "profit_growth_max" in extra_used:
        parts.append(f"profit_growth ≤ {extra_used['profit_growth_max']}%")
    if "ma200_dev_max" in extra_used:
        parts.append(f"MA200乖離 ≤ {extra_used['ma200_dev_max']}%")
    if "exclude_sectors" in extra_used:
        parts.append(f"追加除外セクター: {', '.join(extra_used['exclude_sectors'])}")
    st.info("🧪 追加除外ルール適用中： " + " / ".join(parts))

if "error" in agg:
    st.error(f"集計エラー: {agg['error']}")
    # エラーでもスナップショットの結果は表示
    for snap_r in result["snapshot_results"]:
        if "error" in snap_r:
            st.warning(f"[{snap_r['as_of']}] {snap_r['error']}")
    st.stop()


# ── サマリー指標 ──────────────────────────────────────────────────────
st.subheader("📊 全スナップショット統合サマリー")

top_stats  = agg["top"]
rest_stats = agg["rest"]
diff_mean  = agg["diff_mean"]

c1, c2, c3, c4 = st.columns(4)
c1.metric(
    f"Top{result['top_n']} 平均リターン",
    f"{top_stats['mean']:.2f}%" if top_stats["mean"] is not None else "N/A",
    delta=(f"{diff_mean:+.2f}%" if diff_mean is not None else None),
)
c2.metric(
    "非選定群 平均リターン",
    f"{rest_stats['mean']:.2f}%" if rest_stats["mean"] is not None else "N/A",
)
c3.metric(
    f"Top{result['top_n']} 勝率",
    f"{top_stats['win_rate']}%" if top_stats["win_rate"] is not None else "N/A",
)
c4.metric(
    "非選定群 勝率",
    f"{rest_stats['win_rate']}%" if rest_stats["win_rate"] is not None else "N/A",
)


# ── 詳細統計テーブル ──────────────────────────────────────────────────
st.subheader("📋 詳細統計")
stat_rows = [
    ("サンプル数 (n)",      top_stats["n"],         rest_stats["n"]),
    ("平均リターン (%)",    top_stats["mean"],      rest_stats["mean"]),
    ("中央値 (%)",          top_stats["median"],    rest_stats["median"]),
    ("勝率 (%)",            top_stats["win_rate"],  rest_stats["win_rate"]),
    ("下位25%分位 (%)",     top_stats["p25"],       rest_stats["p25"]),
    ("上位25%分位 (%)",     top_stats["p75"],       rest_stats["p75"]),
    ("最小 (%)",            top_stats["min"],       rest_stats["min"]),
    ("最大 (%)",            top_stats["max"],       rest_stats["max"]),
    ("バリュートラップ率 (%)", top_stats["trap_rate"], rest_stats["trap_rate"]),
]
stat_df = pd.DataFrame(stat_rows, columns=["指標", f"Top{result['top_n']}", "非選定群"])
st.dataframe(stat_df, use_container_width=True, hide_index=True)
st.caption("※ バリュートラップ率＝-20%以下のリターンになった銘柄の割合")


# ── リターン分布ヒストグラム ─────────────────────────────────────────
st.subheader("📈 リターン分布")
top_df  = agg["top_df"]
rest_df = agg["rest_df"]

if len(top_df) > 0 and len(rest_df) > 0:
    fig = go.Figure()
    fig.add_trace(go.Histogram(
        x=top_df["return_pct"],
        name=f"Top{result['top_n']} (n={len(top_df)})",
        opacity=0.75,
        marker_color="#0891b2",
        histnorm="percent",
        xbins=dict(size=10),
    ))
    fig.add_trace(go.Histogram(
        x=rest_df["return_pct"],
        name=f"非選定群 (n={len(rest_df)})",
        opacity=0.55,
        marker_color="#888",
        histnorm="percent",
        xbins=dict(size=10),
    ))
    fig.add_vline(x=0,                  line_dash="dash", line_color="#ef5350")
    fig.add_vline(x=top_stats["mean"],  line_dash="dot",  line_color="#0891b2",
                  annotation_text=f"Top平均 {top_stats['mean']:.1f}%",
                  annotation_position="top")
    fig.add_vline(x=rest_stats["mean"], line_dash="dot",  line_color="#888",
                  annotation_text=f"非選定平均 {rest_stats['mean']:.1f}%",
                  annotation_position="bottom")
    fig.update_layout(
        barmode="overlay", height=420,
        xaxis_title=f"フォワードリターン {result['forward_days']}日 (%)",
        yaxis_title="頻度 (%)",
        legend=dict(yanchor="top", y=0.99, xanchor="right", x=0.99),
        margin=dict(l=10, r=10, t=10, b=10),
    )
    st.plotly_chart(fig, use_container_width=True)


# ── スナップショット別表示 ────────────────────────────────────────────
st.subheader("📅 スナップショット別結果")

for snap_r in result["snapshot_results"]:
    if "scored" not in snap_r:
        st.warning(f"[{snap_r['as_of']}] {snap_r.get('error','エラー')}")
        continue

    scored = snap_r["scored"]
    valid  = scored[scored["has_fwd_data"]]
    top    = valid[valid["is_top"]]
    rest   = valid[~valid["is_top"]]

    n_filt = snap_r["n_filtered"]
    title  = f"📅 {snap_r['as_of']} → {snap_r['fwd_date']}　（フィルタ通過 {n_filt} 銘柄 / 有効 Top {len(top)}件）"

    with st.expander(title, expanded=False):
        if len(top) == 0:
            st.warning("Top N の有効データがありません（フォワードデータ不足）")
            continue

        c1, c2, c3, c4 = st.columns(4)
        c1.metric(f"Top{result['top_n']} 平均", f"{top['return_pct'].mean():.2f}%")
        c2.metric("非選定群 平均",
                  f"{rest['return_pct'].mean():.2f}%" if len(rest) else "N/A")
        c3.metric(f"Top{result['top_n']} 勝率",
                  f"{(top['return_pct']>0).mean()*100:.1f}%")
        c4.metric(f"Top{result['top_n']} 中央値",
                  f"{top['return_pct'].median():.2f}%")

        # Top N 一覧
        st.markdown(f"**Top{result['top_n']} 銘柄リターン一覧**")
        disp_cols = [
            "rank", "code_4", "company_name", "sector",
            "close", "PER", "PBR", "ROE",
            "funda_score", "tech_score", "total_score",
            "price_fwd", "return_pct",
        ]
        disp = top[[c for c in disp_cols if c in top.columns]].copy()
        disp = disp.sort_values("return_pct", ascending=False)
        for col in ["close", "price_fwd"]:
            if col in disp:
                disp[col] = disp[col].round(0).astype("Int64")
        for col in ["PER", "PBR", "ROE", "funda_score", "tech_score", "total_score", "return_pct"]:
            if col in disp:
                disp[col] = disp[col].round(2)
        st.dataframe(disp, use_container_width=True, hide_index=True)

        # 散布図: total_score vs return_pct
        if len(top) >= 5:
            fig = px.scatter(
                top, x="total_score", y="return_pct",
                hover_name="company_name",
                hover_data={"code_4": True, "PBR": ":.2f", "PER": ":.2f", "ROE": ":.1f"},
                labels={"total_score": "総合スコア", "return_pct": f"リターン {result['forward_days']}日 (%)"},
                title=f"スコア vs リターン（{snap_r['as_of']}）",
            )
            fig.add_hline(y=0, line_dash="dash", line_color="#ef5350")
            fig.update_layout(height=350, margin=dict(l=0, r=0, t=40, b=0))
            st.plotly_chart(fig, use_container_width=True)


# ── 全データダウンロード ──────────────────────────────────────────────
st.subheader("💾 データダウンロード")
if len(top_df) > 0:
    dl_cols = [c for c in [
        "snapshot", "rank", "code_4", "company_name", "sector",
        "close", "PER", "PBR", "ROE",
        "funda_score", "tech_score", "total_score",
        "price_fwd", "return_pct",
    ] if c in top_df.columns]
    csv = top_df[dl_cols].to_csv(index=False, encoding="utf-8-sig")
    st.download_button(
        "Top N 銘柄リターン一覧 CSV",
        csv, f"backtest_value_top{result['top_n']}.csv", "text/csv",
    )
