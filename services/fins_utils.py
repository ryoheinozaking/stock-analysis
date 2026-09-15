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
from typing import Iterable, List, Optional

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


# ════════════════════════════════════════════════════════════════════════
#  変則決算期（決算期変更）の期間長補正
# ════════════════════════════════════════════════════════════════════════
# 【2026-09-15】決算期変更で FY が 7ヶ月などになると、12ヶ月の期との素朴な比較で
# 成長率が大きく歪む（例: 325A TENTIAL は FY2025/8 が 7ヶ月決算）。
# REIT の 6ヶ月決算同士のように「前期と同じ長さ」なら補正しない。
_PERIOD_TOLERANCE = 0.05   # うるう年差(0.3%)は無視、13ヶ月決算(8.5%)は補正する


def fy_period_days(start, end) -> Optional[int]:
    """期間の日数（両端含む）。日付の欠損・逆転は None。"""
    s = pd.to_datetime(start, errors="coerce")
    e = pd.to_datetime(end, errors="coerce")
    if pd.isna(s) or pd.isna(e) or e < s:
        return None
    return int((e - s).days) + 1


def period_length_scale(curr_start, curr_end, prev_start, prev_end) -> float:
    """今期のフロー値（売上・利益・EPS 等）に掛けて、前期と同じ期間長ベースに揃える係数。

    = 前期の日数 / 今期の日数。期間長の差が 5% 以内、または日付が欠損している場合は
    1.0（補正なし・従来挙動）。季節性は考慮しない日数按分なので、補正後も目安値。
    """
    curr_days = fy_period_days(curr_start, curr_end)
    prev_days = fy_period_days(prev_start, prev_end)
    if not curr_days or not prev_days:
        return 1.0
    ratio = prev_days / curr_days
    if abs(ratio - 1.0) <= _PERIOD_TOLERANCE:
        return 1.0
    return ratio


# ════════════════════════════════════════════════════════════════════════
#  開示の鮮度（取りこぼし検知）
# ════════════════════════════════════════════════════════════════════════
# 【2026-09-15】batch_service.update_fins が当日分しか取得していなかったため、
# 2026-04-16 以降の開示が大量に欠落し、7803/5254/4417 などの本決算が未収録のまま
# 成長率・PER が計算されていた。取得ロジックは修正済みだが、再発時にレポートで
# 気付けるよう鮮度を出す。
STATEMENT_GRACE_DAYS = 50   # 東証の決算短信開示期限（期末後45日が原則、50日超は理由の開示が必要）


def to_day_list(values: Iterable) -> List[pd.Timestamp]:
    """日付の集まりを「重複なし・昇順・時刻切り捨て」の Timestamp リストにする。"""
    s = pd.to_datetime(pd.Series(list(values), dtype="object"), errors="coerce").dropna()
    return sorted(pd.Timestamp(v) for v in s.dt.normalize().unique())


def find_disclosure_gaps(disc_dates: Iterable, trading_dates: Iterable,
                         since, until) -> List[pd.Timestamp]:
    """[since, until] の取引日のうち、開示が 1 件もない日を昇順で返す。

    東証の取引日には通常数十件以上の開示（決算短信・予想修正）があるため、
    開示ゼロの取引日は fins_cache の取りこぼしを強く示唆する
    （大納会 12/30 のように実際に開示が無い日もまれにある）。
    """
    since = pd.Timestamp(since).normalize()
    until = pd.Timestamp(until).normalize()
    disc = set(to_day_list(disc_dates))
    return [d for d in to_day_list(trading_dates) if since <= d <= until and d not in disc]


def disclosure_freshness(fins_df: pd.DataFrame, as_of) -> pd.DataFrame:
    """銘柄ごとの最新開示日・最新決算短信日と、次の決算短信が期限を過ぎても未収録かを返す。

    戻り値の列: code / last_disc_date / last_stmt_date / last_stmt_period_end / fins_overdue
    （日付は 'YYYY-MM-DD' 文字列。JSON キャッシュにそのまま保存できるようにする）

    次の決算短信の期限 = 最新決算短信の期末 + 次の期間 + 50日。
    次の期間は通常 3ヶ月（四半期）。6ヶ月以下の FY を開示している銘柄（REIT 等）は 6ヶ月。
    必要な列: Code, DiscDate, DocType, CurPerType, CurPerSt, CurPerEn
    """
    cols = ["code", "last_disc_date", "last_stmt_date", "last_stmt_period_end", "fins_overdue"]
    if fins_df.empty:
        return pd.DataFrame(columns=cols)
    as_of = pd.Timestamp(as_of).normalize()

    d = fins_df.copy()
    d["_disc"] = pd.to_datetime(d["DiscDate"], errors="coerce")
    d = d.dropna(subset=["_disc"])
    last_disc = d.groupby("Code")["_disc"].max()

    stmts = (filter_statements(d).sort_values("_disc")
             .groupby("Code").tail(1).set_index("Code"))
    per_st = pd.to_datetime(stmts["CurPerSt"], errors="coerce")
    per_en = pd.to_datetime(stmts["CurPerEn"], errors="coerce")
    per_days = (per_en - per_st).dt.days + 1
    semiannual = (stmts["CurPerType"] == "FY") & (per_days <= 200)
    deadline = ((per_en + pd.DateOffset(months=3)).where(~semiannual, per_en + pd.DateOffset(months=6))
                + pd.Timedelta(days=STATEMENT_GRACE_DAYS))

    out = pd.DataFrame({
        "code":           last_disc.index,
        "last_disc_date": last_disc.dt.strftime("%Y-%m-%d").values,
    })
    stmt_info = pd.DataFrame({
        "code":                 stmts.index,
        "last_stmt_date":       stmts["_disc"].dt.strftime("%Y-%m-%d").values,
        "last_stmt_period_end": per_en.dt.strftime("%Y-%m-%d").values,
        "fins_overdue":         (deadline < as_of).values,
    })
    out = out.merge(stmt_info, on="code", how="left")
    out["fins_overdue"] = out["fins_overdue"].fillna(False).astype(bool)
    return out[cols]
