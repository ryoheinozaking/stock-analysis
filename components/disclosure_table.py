# -*- coding: utf-8 -*-
"""
Disclosure table component
"""

import pandas as pd
import streamlit as st


def render_disclosure_table(df: pd.DataFrame):
    """
    st.dataframe で開示一覧を表示する
    - document_url を 📄 アイコンのリンクに設定
    - pubdate を日時フォーマット
    - company_code + company_name を結合表示（4桁コード）
    """
    if df is None or df.empty:
        st.warning("開示データがありません")
        return

    df_show = df.copy()

    # company_code + company_name を結合（4桁表示）
    if "company_code" in df_show.columns and "company_name" in df_show.columns:
        df_show["銘柄"] = df_show["company_code"].astype(str).str[:4] + " " + df_show["company_name"].astype(str)

    # pubdate をフォーマット
    if "pubdate" in df_show.columns:
        df_show["開示日時"] = pd.to_datetime(df_show["pubdate"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M")

    # 表示列の選択
    display_cols = []
    col_config = {}

    if "開示日時" in df_show.columns:
        display_cols.append("開示日時")

    if "銘柄" in df_show.columns:
        display_cols.append("銘柄")

    if "title" in df_show.columns:
        display_cols.append("title")
        col_config["title"] = st.column_config.TextColumn("タイトル", width="large")

    if "document_url" in df_show.columns:
        display_cols.append("document_url")
        col_config["document_url"] = st.column_config.LinkColumn(
            "PDF", width="small", display_text="📄"
        )

    df_display = df_show[display_cols] if display_cols else df_show

    st.dataframe(
        df_display,
        use_container_width=True,
        hide_index=True,
        column_config=col_config,
    )
