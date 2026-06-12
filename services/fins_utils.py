# -*- coding: utf-8 -*-
"""
fins_cache レコード選別ユーティリティ

【背景】（2026-06-12 調査）
fins_cache.parquet の CurPerType=='FY' レコードには決算短信（実績）だけでなく
業績予想修正・配当予想修正（EarnForecastRevision / DividendForecastRevision、
実績列 Sales/OP/EPS/Eq/ShOutFY がすべて空）が混入する。
CurPerType は「どの期間に関する開示か」を示すだけで、文書種別ではない。

実測（2026-06-12 時点の fins_cache）:
  - 最新 FY レコードが予想修正の銘柄: 757 / 3,785（実績が全 NaN になり
    ハードフィルタで無条件除外されていた）
  - 2番目の FY レコードが予想修正の銘柄: 1,047（前期比較・増配判定が破壊）
  - 訂正短信による同一決算期の重複: 229 銘柄（前期比較が同一年度比較になる）

【使い分け】
- filter_fy_statements: 実績値（Sales/OP/EPS/Eq/DivAnn 等）を読む場面で適用。
  予想列（FEPS/FDivAnn 等）は予想修正レコードが最新情報を持つため、
  予想を読む場面ではフィルタしないこと。
- dedupe_same_fy: 同一決算期の訂正重複を最新開示のみに絞る。
  バックテストの point-in-time 性を保つため、as_of でのフィルタ後
  （コンシューマ側）で適用すること。ロード時に dedup すると
  「as_of 時点では未開示の訂正」が原本を消してしまう。
"""
import pandas as pd

_STMT_PREFIX = "FYFinancialStatements"


def filter_fy_statements(df: pd.DataFrame) -> pd.DataFrame:
    """FY の決算短信（実績）レコードのみ返す。予想修正レコードを除外する。

    DocType 列がない場合（古いキャッシュ）は CurPerType のみで絞る。
    """
    if df.empty:
        return df
    mask = df["CurPerType"] == "FY"
    if "DocType" in df.columns:
        mask &= df["DocType"].astype(str).str.startswith(_STMT_PREFIX)
    return df[mask]


def filter_statements(df: pd.DataFrame) -> pd.DataFrame:
    """財務諸表（実績）レコードのみ返す（四半期・FY とも）。

    DocType に 'FinancialStatements' を含むレコードに限定することで、
    EarnForecastRevision / DividendForecastRevision（CurPerType が 1Q-3Q/FY でも
    実績列が空）を除外する。期間で絞りたい場合は呼び出し側で CurPerType を併用する。
    DocType 列がない場合（古いキャッシュ）はそのまま返す。
    """
    if df.empty or "DocType" not in df.columns:
        return df
    return df[df["DocType"].astype(str).str.contains("FinancialStatements", na=False)]


def dedupe_same_fy(df: pd.DataFrame) -> pd.DataFrame:
    """同一 (Code, CurFYEn) の重複（訂正短信の再開示）は DiscDate 最新のみ残す。

    戻り値は Code 昇順・DiscDate 降順（_load_fins_fy の出力と同じ並び）。
    CurFYEn が欠損している行は dedup 対象にせずそのまま残す。
    """
    if df.empty or "CurFYEn" not in df.columns:
        return df
    d = df.copy()
    d["_disc"] = pd.to_datetime(d["DiscDate"], errors="coerce")

    has_fy = d["CurFYEn"].notna() & (d["CurFYEn"].astype(str).str.strip() != "")
    deduped = (d[has_fy]
               .sort_values(["Code", "CurFYEn", "_disc"])
               .drop_duplicates(subset=["Code", "CurFYEn"], keep="last"))
    out = pd.concat([deduped, d[~has_fy]])
    return (out.sort_values(["Code", "_disc"], ascending=[True, False])
               .drop(columns=["_disc"])
               .reset_index(drop=True))
