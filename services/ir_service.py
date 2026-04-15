# -*- coding: utf-8 -*-
"""
IR自動要約サービス
- 3層フィルタで開示情報を絞り込む
- PDFテキスト抽出
"""

import os
import sys
import glob
import io
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
import pandas as pd
from typing import List, Tuple

# ---- キーワード定義 ----

# 層2（スクリーニング上位銘柄）で通過させるキーワード
_LAYER2_KEYWORDS = [
    "決算短信", "業績予想", "業績修正",
    "上方修正", "下方修正",
    "通期業績", "四半期決算", "増配",
]

# 層3（その他）で通過させるキーワード（ポジティブサプライズのみ）
_LAYER3_KEYWORDS = [
    "上方修正",
    "増配",
    "黒字転換",
]

# 層3で除外するキーワード（ノイズ）
_LAYER3_EXCLUDE_KEYWORDS = [
    "行使価額修正条項付",
]

# 重要度スコア（表示順ソート用）
_TITLE_SCORE_MAP = {
    "上方修正": 10,
    "黒字転換": 9,
    "決算短信": 8,
    "下方修正": 7,
    "業績予想": 5,
    "業績修正": 5,
    "増配": 5,
    "通期業績": 4,
    "四半期決算": 4,
}


def score_title(title: str) -> int:
    """タイトルキーワードから重要度スコアを返す（高いほど重要）"""
    score = 0
    for kw, pt in _TITLE_SCORE_MAP.items():
        if kw in title:
            score = max(score, pt)
    return score


def _matches_any(title: str, keywords: List[str]) -> bool:
    return any(kw in title for kw in keywords)


# ---- ポートフォリオ・スクリーニング銘柄取得 ----

_DOWNLOADS_DIR = os.path.expanduser("~/Downloads")


def get_portfolio_codes() -> List[str]:
    """SBI証券CSVから保有銘柄の4桁コードを返す。取得失敗時は空リスト。"""
    try:
        from services.portfolio_service import parse_sbi_csv
        candidates = glob.glob(os.path.join(_DOWNLOADS_DIR, "New_file*.csv"))
        if not candidates:
            candidates = glob.glob(os.path.join(_DOWNLOADS_DIR, "*.csv"))
        if not candidates:
            return []
        path = max(candidates, key=os.path.getmtime)
        with open(path, "rb") as f:
            stocks_df, _ = parse_sbi_csv(f)
        if stocks_df.empty or "code_4" not in stocks_df.columns:
            return []
        return stocks_df["code_4"].dropna().astype(str).tolist()
    except Exception:
        return []


def get_screening_top_codes(n: int = 50) -> List[str]:
    """stock_cache.parquetからスコア上位n銘柄の4桁コードを返す。"""
    try:
        cache_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "data", "stock_cache.parquet"
        )
        if not os.path.exists(cache_path):
            return []
        df = pd.read_parquet(cache_path)
        if "score" not in df.columns or "code_4" not in df.columns:
            return []
        top = df.nlargest(n, "score")
        return top["code_4"].dropna().astype(str).tolist()
    except Exception:
        return []


def get_prime_codes() -> set:
    """stock_cache.parquetからプライム市場銘柄の4桁コードをsetで返す。"""
    try:
        cache_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "data", "stock_cache.parquet"
        )
        if not os.path.exists(cache_path):
            return set()
        df = pd.read_parquet(cache_path)
        if "market" not in df.columns or "code_4" not in df.columns:
            return set()
        prime = df[df["market"] == "プライム"]
        return set(prime["code_4"].dropna().astype(str).tolist())
    except Exception:
        return set()


# ---- 3層フィルタリング ----

def classify_disclosures(
    df: pd.DataFrame,
    portfolio_codes: List[str],
    screening_codes: List[str],
) -> pd.DataFrame:
    """
    開示DataFrameに layer列・title_score列を付与して返す。

    layer:
        1 = 必読（保有銘柄）
        2 = 推奨（スクリーニング上位 × 決算関連キーワード）
        3 = 参考（その他 × 上方/下方修正 × プライム）
        0 = 対象外

    返すのは layer >= 1 のみ。
    """
    if df.empty:
        return df

    # TDnet company_code は5桁（4桁コード+"0"）なので合わせる
    port_set = set(str(c) + "0" for c in portfolio_codes)
    screen_set = set(str(c) + "0" for c in screening_codes)
    # Layer 3用: プライム市場銘柄（TDnetは5桁コード）
    prime_set = set(c + "0" for c in get_prime_codes())

    layers = []
    scores = []

    for _, row in df.iterrows():
        code = str(row.get("company_code", ""))
        title = str(row.get("title", ""))

        ts = score_title(title)

        if code in port_set:
            layers.append(1)
        elif code in screen_set and _matches_any(title, _LAYER2_KEYWORDS):
            layers.append(2)
        elif (
            _matches_any(title, _LAYER3_KEYWORDS)
            and not _matches_any(title, _LAYER3_EXCLUDE_KEYWORDS)
        ):
            layers.append(3)
        else:
            layers.append(0)

        scores.append(ts)

    df = df.copy()
    df["layer"] = layers
    df["title_score"] = scores

    result = df[df["layer"] >= 1].copy()
    # 層→タイトルスコアの順でソート
    result = result.sort_values(["layer", "title_score"], ascending=[True, False])
    return result.reset_index(drop=True)


# ---- 日付範囲取得 ----

def get_disclosures_by_date_range(
    start_date, end_date, limit_per_day: int = 150
) -> pd.DataFrame:
    """
    start_date〜end_dateの適時開示を取得して結合する。
    weekendや祝日でデータがない日はスキップ。
    """
    from services.tdnet_service import get_by_date
    from datetime import timedelta

    all_dfs = []
    current = start_date
    while current <= end_date:
        date_str = current.strftime("%Y%m%d")
        df = get_by_date(date_str=date_str, limit=limit_per_day)
        if not df.empty:
            all_dfs.append(df)
        current += timedelta(days=1)

    if not all_dfs:
        return pd.DataFrame()

    combined = pd.concat(all_dfs, ignore_index=True)
    if "id" in combined.columns:
        combined = combined.drop_duplicates(subset=["id"])
    return combined.reset_index(drop=True)


# ---- PDF本文抽出 ----

def fetch_pdf_text(url: str, max_chars: int = 8000) -> str:
    """
    URLからPDFをダウンロードしてテキストを抽出する。
    失敗時は空文字を返す。max_charsで文字数を制限。
    """
    try:
        import pypdf
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        content_type = resp.headers.get("Content-Type", "")
        if "pdf" not in content_type.lower() and not url.lower().endswith(".pdf"):
            return ""
        pdf_bytes = io.BytesIO(resp.content)
        reader = pypdf.PdfReader(pdf_bytes)
        parts = []
        for page in reader.pages:
            parts.append(page.extract_text() or "")
        text = "\n".join(parts)
        # 余分な空行を圧縮
        lines = [l.strip() for l in text.splitlines() if l.strip()]
        text = "\n".join(lines)
        return text[:max_chars]
    except Exception:
        return ""
