# -*- coding: utf-8 -*-
"""
Plotly chart builder (pure function, no streamlit import)
TradingView-style: BB, Ichimoku, MACD, RSI, Volume overlay, Volume Profile
"""

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def _detect_signals(df: pd.DataFrame, show_ma: list) -> dict:
    """買い・売りシグナルの発生日インデックスを返す"""
    signals = {"gc": [], "dc": [], "macd_buy": [], "macd_sell": [], "rsi_buy": []}
    if len(df) < 3:
        return signals

    close = df["AdjC"]

    # ゴールデンクロス / デッドクロス
    if len(show_ma) >= 2:
        ma_s = close.rolling(show_ma[0], min_periods=1).mean()
        ma_l = close.rolling(show_ma[1], min_periods=1).mean()
        signals["gc"] = df.index[(ma_s > ma_l) & (ma_s.shift(1) <= ma_l.shift(1))].tolist()
        signals["dc"] = df.index[(ma_s < ma_l) & (ma_s.shift(1) >= ma_l.shift(1))].tolist()

    # MACD クロス
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd  = ema12 - ema26
    sig   = macd.ewm(span=9, adjust=False).mean()
    signals["macd_buy"]  = df.index[(macd > sig) & (macd.shift(1) <= sig.shift(1))].tolist()
    signals["macd_sell"] = df.index[(macd < sig) & (macd.shift(1) >= sig.shift(1))].tolist()

    # RSI 30 上抜け（売られすぎ反転）
    delta = close.diff()
    gain  = delta.clip(lower=0).rolling(14, min_periods=1).mean()
    loss  = (-delta.clip(upper=0)).rolling(14, min_periods=1).mean()
    rsi   = 100 - (100 / (1 + gain / loss.replace(0, float("nan"))))
    signals["rsi_buy"] = df.index[(rsi > 30) & (rsi.shift(1) <= 30)].tolist()

    return signals


def build_ohlcv_chart(
    df: pd.DataFrame,
    title: str = "",
    show_ma: list = [25, 75],
    show_bb: bool = False,
    show_macd: bool = False,
    show_rsi: bool = False,
    show_ichimoku: bool = False,
    show_volume_profile: bool = False,
    show_signals: bool = False,
) -> go.Figure:
    """
    OHLCVチャートを構築して返す
    Row1: ローソク足 + MA + BB + 一目均衡表 + 出来高オーバーレイ（secondary y）
    Row2: MACD（オプション）
    Row2 or 3: RSI（オプション）
    Col2（オプション）: 価格帯別出来高
    """
    n_extra = int(show_macd) + int(show_rsi)
    rows = 1 + n_extra

    if n_extra == 2:
        row_heights = [0.65, 0.175, 0.175]
    elif n_extra == 1:
        row_heights = [0.72, 0.28]
    else:
        row_heights = [1.0]

    cols = 2 if show_volume_profile else 1
    col_widths = [0.85, 0.15] if show_volume_profile else None

    # specs: row 1 col 1 は secondary_y で出来高オーバーレイ
    if show_volume_profile:
        specs_list = [[{"secondary_y": True}, {}]]
        for _ in range(rows - 1):
            specs_list.append([{"secondary_y": False}, {}])
    else:
        specs_list = [[{"secondary_y": True}]]
        for _ in range(rows - 1):
            specs_list.append([{"secondary_y": False}])

    height = 580 + n_extra * 130

    # subplot_titles: 2列の場合は col2 分の空文字を追加
    row_labels = []
    if show_macd:
        row_labels.append("MACD")
    if show_rsi:
        row_labels.append("RSI")
    if show_volume_profile:
        subplot_titles_list = ["", ""] + [t for lbl in row_labels for t in (lbl, "")]
    else:
        subplot_titles_list = [""] + row_labels

    fig = make_subplots(
        rows=rows,
        cols=cols,
        shared_xaxes=True,
        row_heights=row_heights,
        column_widths=col_widths,
        vertical_spacing=0.04,
        horizontal_spacing=0.01 if show_volume_profile else None,
        subplot_titles=subplot_titles_list,
        specs=specs_list,
    )

    x = df["Date"].dt.strftime("%Y/%m/%d").tolist()

    # --- ローソク足 ---
    fig.add_trace(
        go.Candlestick(
            x=x,
            open=df["AdjO"], high=df["AdjH"], low=df["AdjL"], close=df["AdjC"],
            name="株価",
            increasing_line_color="#26a69a", increasing_fillcolor="#26a69a",
            decreasing_line_color="#ef5350", decreasing_fillcolor="#ef5350",
        ),
        row=1, col=1,
    )

    # --- MA線 ---
    ma_colors = {
        5: "#b0bec5",
        25: "#2196f3", 13: "#2196f3", 6: "#2196f3",
        75: "#ff9800", 26: "#ff9800", 12: "#ff9800",
        200: "#ef5350",
    }
    default_colors = ["#9c27b0", "#1abc9c", "#e91e63"]
    for i, period in enumerate(show_ma):
        if "AdjC" in df.columns and len(df) >= 2:
            ma = df["AdjC"].rolling(period, min_periods=1).mean()
            color = ma_colors.get(period, default_colors[i % len(default_colors)])
            fig.add_trace(
                go.Scatter(x=x, y=ma, mode="lines", name=f"MA{period}",
                           line=dict(color=color, width=1.5)),
                row=1, col=1,
            )

    # --- ボリンジャーバンド ---
    if show_bb and len(df) >= 2:
        sma20 = df["AdjC"].rolling(20, min_periods=1).mean()
        std20 = df["AdjC"].rolling(20, min_periods=1).std()
        upper2, lower2 = sma20 + 2 * std20, sma20 - 2 * std20
        upper3, lower3 = sma20 + 3 * std20, sma20 - 3 * std20
        fig.add_trace(go.Scatter(x=x, y=upper3, name="BB+3σ",
            line=dict(color="rgba(255,140,0,0.6)", width=1, dash="dot")), row=1, col=1)
        fig.add_trace(go.Scatter(x=x, y=upper2, name="BB+2σ",
            line=dict(color="rgba(100,149,237,0.7)", width=1, dash="dot"),
            fill="tonexty", fillcolor="rgba(255,140,0,0.06)"), row=1, col=1)
        fig.add_trace(go.Scatter(x=x, y=lower2, name="BB-2σ",
            line=dict(color="rgba(100,149,237,0.7)", width=1, dash="dot"),
            fill="tonexty", fillcolor="rgba(100,149,237,0.08)"), row=1, col=1)
        fig.add_trace(go.Scatter(x=x, y=lower3, name="BB-3σ",
            line=dict(color="rgba(255,140,0,0.6)", width=1, dash="dot"),
            fill="tonexty", fillcolor="rgba(255,140,0,0.06)"), row=1, col=1)

    # --- 一目均衡表 ---
    if show_ichimoku and len(df) >= 2:
        high, low = df["AdjH"], df["AdjL"]
        tenkan = (high.rolling(9, min_periods=1).max() + low.rolling(9, min_periods=1).min()) / 2
        kijun  = (high.rolling(26, min_periods=1).max() + low.rolling(26, min_periods=1).min()) / 2
        span1  = ((tenkan + kijun) / 2).shift(26)
        span2  = ((high.rolling(52, min_periods=1).max() + low.rolling(52, min_periods=1).min()) / 2).shift(26)
        lagging = df["AdjC"].shift(-26)

        fig.add_trace(go.Scatter(x=x, y=tenkan, name="転換線",
            line=dict(color="#e91e63", width=1)), row=1, col=1)
        fig.add_trace(go.Scatter(x=x, y=kijun, name="基準線",
            line=dict(color="#2196f3", width=1.5)), row=1, col=1)
        fig.add_trace(go.Scatter(x=x, y=lagging, name="遅行スパン",
            line=dict(color="#4caf50", width=1, dash="dot")), row=1, col=1)

        # 雲：陽雲（緑）と陰雲（赤）を分けて描画
        bull = span1 >= span2
        # 陽雲（span1 >= span2）
        fig.add_trace(go.Scatter(x=x, y=span1.where(bull), name="先行スパン1",
            line=dict(color="rgba(38,166,154,0.0)", width=0), showlegend=False), row=1, col=1)
        fig.add_trace(go.Scatter(x=x, y=span2.where(bull), name="陽雲",
            line=dict(color="rgba(38,166,154,0.0)", width=0),
            fill="tonexty", fillcolor="rgba(38,166,154,0.2)", showlegend=True), row=1, col=1)
        # 陰雲（span1 < span2）
        fig.add_trace(go.Scatter(x=x, y=span2.where(~bull), name="先行スパン2",
            line=dict(color="rgba(239,83,80,0.0)", width=0), showlegend=False), row=1, col=1)
        fig.add_trace(go.Scatter(x=x, y=span1.where(~bull), name="陰雲",
            line=dict(color="rgba(239,83,80,0.0)", width=0),
            fill="tonexty", fillcolor="rgba(239,83,80,0.2)", showlegend=True), row=1, col=1)
        # スパン線（可視）
        fig.add_trace(go.Scatter(x=x, y=span1, name="先行1",
            line=dict(color="rgba(38,166,154,0.7)", width=1)), row=1, col=1)
        fig.add_trace(go.Scatter(x=x, y=span2, name="先行2",
            line=dict(color="rgba(239,83,80,0.7)", width=1)), row=1, col=1)

    # --- 出来高オーバーレイ（secondary y） ---
    vol_col = "AdjVo" if "AdjVo" in df.columns else ("Vo" if "Vo" in df.columns else None)
    if vol_col:
        vol_colors = [
            "rgba(38,166,154,0.4)" if i == 0 or df["AdjC"].iloc[i] >= df["AdjC"].iloc[i - 1]
            else "rgba(239,83,80,0.4)"
            for i in range(len(df))
        ]
        fig.add_trace(
            go.Bar(x=x, y=df[vol_col], name="出来高", marker_color=vol_colors, showlegend=False),
            row=1, col=1, secondary_y=True,
        )
        max_vol = df[vol_col].dropna().max()
        if max_vol and max_vol > 0:
            fig.update_yaxes(
                range=[0, max_vol * 5],
                showticklabels=False,
                showgrid=False,
                row=1, col=1, secondary_y=True,
            )

    # --- シグナルマーカー ---
    if show_signals and len(df) >= 3:
        sigs = _detect_signals(df, show_ma)
        low_offset  = df["AdjL"].min() * 0.005
        high_offset = df["AdjH"].max() * 0.005

        _sig_defs = [
            ("gc",        "GC（買）",      "triangle-up",   "#00e676", df["AdjL"] - low_offset  * 3),
            ("dc",        "DC（売）",      "triangle-down", "#ff1744", df["AdjH"] + high_offset * 3),
            ("macd_buy",  "MACD買転換",   "triangle-up",   "#2196f3", df["AdjL"] - low_offset  * 6),
            ("macd_sell", "MACD売転換",   "triangle-down", "#ff9800", df["AdjH"] + high_offset * 6),
            ("rsi_buy",   "RSI反転(買）",  "triangle-up",   "#9c27b0", df["AdjL"] - low_offset  * 9),
        ]
        for key, label, symbol, color, y_series in _sig_defs:
            idx_list = sigs[key]
            if not idx_list:
                continue
            sig_x = [x[i] for i in idx_list if i < len(x)]
            sig_y = [y_series.iloc[i] for i in idx_list if i < len(y_series)]
            fig.add_trace(go.Scatter(
                x=sig_x, y=sig_y, mode="markers", name=label,
                marker=dict(symbol=symbol, size=12, color=color,
                            line=dict(color=color, width=1)),
            ), row=1, col=1)

    # --- MACD ---
    if show_macd and len(df) >= 2:
        ema12 = df["AdjC"].ewm(span=12, adjust=False).mean()
        ema26 = df["AdjC"].ewm(span=26, adjust=False).mean()
        macd_line = ema12 - ema26
        signal_line = macd_line.ewm(span=9, adjust=False).mean()
        hist = macd_line - signal_line
        hist_colors = ["#26a69a" if v >= 0 else "#ef5350" for v in hist]
        fig.add_trace(go.Bar(x=x, y=hist, name="MACDヒスト",
            marker_color=hist_colors, showlegend=False), row=2, col=1)
        fig.add_trace(go.Scatter(x=x, y=macd_line, name="MACD",
            line=dict(color="#2196f3", width=1.5)), row=2, col=1)
        fig.add_trace(go.Scatter(x=x, y=signal_line, name="Signal",
            line=dict(color="#ff9800", width=1.5)), row=2, col=1)
        fig.add_hline(y=0, line_color="gray", line_width=0.5, row=2, col=1)

    # --- RSI ---
    if show_rsi and len(df) >= 2:
        delta = df["AdjC"].diff()
        gain = delta.clip(lower=0).rolling(14, min_periods=1).mean()
        loss = (-delta.clip(upper=0)).rolling(14, min_periods=1).mean()
        rs = gain / loss.replace(0, float("nan"))
        rsi = 100 - (100 / (1 + rs))
        rsi_row = 3 if show_macd else 2
        fig.add_trace(go.Scatter(x=x, y=rsi, name="RSI(14)",
            line=dict(color="#9c27b0", width=1.5)), row=rsi_row, col=1)
        fig.add_hline(y=70, line_color="rgba(239,83,80,0.5)", line_width=1,
                      line_dash="dash", row=rsi_row, col=1)
        fig.add_hline(y=30, line_color="rgba(38,166,154,0.5)", line_width=1,
                      line_dash="dash", row=rsi_row, col=1)
        fig.add_hline(y=50, line_color="rgba(150,150,150,0.3)", line_width=0.5,
                      row=rsi_row, col=1)
        fig.update_yaxes(range=[0, 100], row=rsi_row, col=1, secondary_y=False)

    # --- 価格帯別出来高 ---
    if show_volume_profile and vol_col and len(df) >= 2:
        price_min = df["AdjL"].min() * 0.995
        price_max = df["AdjH"].max() * 1.005
        n_bins = 30
        bin_edges = np.linspace(price_min, price_max, n_bins + 1)
        bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2
        bin_vols = np.zeros(n_bins)
        for _, rd in df.iterrows():
            lo, hi, vol = rd["AdjL"], rd["AdjH"], rd.get(vol_col, 0)
            if pd.isna(vol) or pd.isna(lo) or pd.isna(hi) or hi <= lo:
                continue
            for j in range(n_bins):
                overlap = max(0.0, min(hi, bin_edges[j + 1]) - max(lo, bin_edges[j]))
                if overlap > 0:
                    bin_vols[j] += vol * overlap / (hi - lo)
        poc_idx = int(np.argmax(bin_vols))
        bar_colors = [
            "rgba(239,83,80,0.7)" if j == poc_idx else "rgba(100,149,237,0.45)"
            for j in range(n_bins)
        ]
        fig.add_trace(
            go.Bar(x=bin_vols, y=bin_centers, orientation="h",
                   name="価格帯別出来高", marker_color=bar_colors, showlegend=False),
            row=1, col=2,
        )
        # row=1,col=2 のみ価格範囲を設定、rows 2+ は非表示
        fig.update_yaxes(range=[price_min, price_max], showticklabels=False,
                         showgrid=False, row=1, col=2)
        fig.update_xaxes(range=[0, max(bin_vols) * 1.05],
                         showticklabels=False, showgrid=False, row=1, col=2)
        for r in range(2, rows + 1):
            fig.update_yaxes(visible=False, showgrid=False, row=r, col=2)
            fig.update_xaxes(visible=False, showgrid=False, row=r, col=2)

    # --- レイアウト ---
    x_range = [-0.5, len(x) - 0.5]
    fig.update_layout(
        height=height,
        template="plotly_dark",
        xaxis_rangeslider_visible=False,
        margin=dict(l=60, r=80, t=50, b=40),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    fig.update_xaxes(rangeslider_visible=False)

    # col 1 のみカテゴリ軸設定（col 2 の価格帯別出来高は数値軸のまま）
    fig.update_xaxes(type="category", range=x_range, nticks=0, showticklabels=False, col=1)
    fig.update_xaxes(tickangle=-45, nticks=12, showticklabels=True, col=1, row=rows)

    # サブプロットタイトル（MACD・RSI）を左寄せに変更
    for annotation in fig.layout.annotations:
        annotation.x = 0
        annotation.xanchor = "left"

    return fig
