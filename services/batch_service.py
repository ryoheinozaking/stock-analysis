# -*- coding: utf-8 -*-
"""
全銘柄メトリクス一括取得バッチ（バルク取得・差分更新版）

処理フロー:
  1. update_prices()     : /equities/bars/daily?date= で日付ループ → prices.parquet に差分追記
  2. update_fins()       : 初回=全銘柄ループ、以降=前回確認済み日〜今日の取引日を日付ループ → fins_cache.parquet に差分追記
  3. build_stock_cache() : parquet読み込み→スコア計算→stock_cache.csv（APIコールなし）
  4. fetch_all_stocks()  : 上記3ステップをまとめた後方互換ラッパー
"""

import os
import json
import time
import requests
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import List

from screener import calc_rsi, calc_moving_average, calc_avg_volume, calc_signal_score
from services.split_adjust import normalize_close, normalize_volume, forecast_per_share_multiplier
from services.fins_utils import (
    filter_fy_statements, dedupe_same_fy, find_disclosure_gaps, period_length_scale, to_day_list,
)
from services.valuation_source import apply_live_valuation

_ROOT       = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE_PATH  = os.path.join(_ROOT, "data", "stock_cache.parquet")
PRICES_PATH = os.path.join(_ROOT, "data", "prices.parquet")
FINS_PATH   = os.path.join(_ROOT, "data", "fins_cache.parquet")
FINS_STATE_PATH = os.path.join(_ROOT, "data", "fins_fetch_state.json")
VALUATION_PATH  = os.path.join(_ROOT, "data", "valuation.parquet")
EARNINGS_DATES_PATH = os.path.join(_ROOT, "data", "earnings_dates.parquet")
BASE_URL    = "https://api.jquants.com/v2"

# バリュエーション指標の初回取得期間（暦日）。長期の過去分は J-Quants の CSV 一括ダウンロードで入れる
VALUATION_INITIAL_DAYS = 45

# 財務データ差分更新のパラメータ（update_fins / _plan_fins_fetch_dates）
FINS_REFETCH_OVERLAP         = 2    # 確認済み日を含めて遡って取り直す取引日数（遅れて登録される開示の対策）
FINS_BOOTSTRAP_LOOKBACK_DAYS = 400  # 状態ファイルが無いとき、開示ゼロの取引日を探す期間（暦日）
FINS_BOOTSTRAP_MARGIN        = 5    # 欠落の密集区間から手前に遡る取引日数（当日分だけ取得済みの日の修復）
FINS_GAP_CLUSTER_WINDOW      = 10   # 次の欠落日がこの取引日数以内なら「密集区間の始まり」とみなす


# ─── 共通ユーティリティ ──────────────────────────────────────────

def _api_key():
    key = os.getenv("JQUANTS_API_KEY")
    if not key:
        raise EnvironmentError(".env に JQUANTS_API_KEY が設定されていません")
    return key


def _get(endpoint, params=None, retry=3):
    headers = {"x-api-key": _api_key()}
    for attempt in range(retry):
        r = requests.get(
            f"{BASE_URL}{endpoint}",
            params=params or {},
            headers=headers,
            timeout=30,
        )
        if r.status_code == 429:
            time.sleep(60)
            continue
        r.raise_for_status()
        return r.json()
    r.raise_for_status()


def _get_all(endpoint, params=None):
    """pagination_key を辿って data を全件取得する（開示の多い日は複数ページに分かれうる）。"""
    params = dict(params or {})
    rows = []
    while True:
        data = _get(endpoint, params)
        rows.extend(data.get("data", []))
        key = data.get("pagination_key")
        if not key:
            return rows
        params["pagination_key"] = key


# ─── 既存インターフェース（変更なし） ────────────────────────────

def load_cache():
    """stock_cache.parquet を読み込む。なければ・空ならば None を返す。"""
    if os.path.exists(CACHE_PATH):
        try:
            df = pd.read_parquet(CACHE_PATH)
            # code列を文字列に統一
            for col in ("code", "code_4"):
                if col in df.columns:
                    df[col] = df[col].astype(str)
            return df if not df.empty else None
        except Exception:
            return None
    return None


def get_cache_updated_at():
    """キャッシュの最終更新日時を返す。なければ None。"""
    if os.path.exists(CACHE_PATH):
        return datetime.fromtimestamp(os.path.getmtime(CACHE_PATH))
    return None


# ─── 価格データ（バルク・差分更新） ─────────────────────────────

def _load_prices():
    if os.path.exists(PRICES_PATH):
        return pd.read_parquet(PRICES_PATH)
    return pd.DataFrame()


def update_prices(progress_callback=None):
    """
    差分更新: 前回保存日の翌日〜今日分をバルクAPIで取得して追記する。

    1日1回のAPIコールで全銘柄のOHLCVが取得できるため、
    「銘柄ループ×日数」から「日付ループ」に変わり大幅に削減される。
    """
    existing = _load_prices()

    if not existing.empty:
        last_date = pd.to_datetime(existing["Date"]).max()
        from_dt   = last_date + timedelta(days=1)
    else:
        from_dt = datetime.today() - timedelta(days=400)  # 52週高値検出に必要

    to_dt = datetime.today()

    if from_dt.date() > to_dt.date():
        return existing  # 既に最新

    # 平日リスト（土日除外・祝日はAPIが空レスポンスを返すのでスキップ）
    date_strs = [
        d.strftime("%Y-%m-%d")
        for d in pd.date_range(from_dt, to_dt, freq="B")
    ]

    new_frames = []
    total = len(date_strs)
    for i, date_str in enumerate(date_strs):
        if progress_callback:
            progress_callback(i, total, f"📅 価格取得: {date_str}")
        try:
            data = _get("/equities/bars/daily", {"date": date_str})
            df   = pd.DataFrame(data.get("data", []))
            if not df.empty:
                new_frames.append(df)
        except Exception:
            pass  # 祝日など取得できない日はスキップ

    if not new_frames:
        return existing

    new_data = pd.concat(new_frames, ignore_index=True)
    result   = pd.concat([existing, new_data], ignore_index=True) if not existing.empty else new_data

    # 同日・同コードの重複除去
    result = result.drop_duplicates(subset=["Date", "Code"]).reset_index(drop=True)

    os.makedirs(os.path.dirname(PRICES_PATH), exist_ok=True)
    result.to_parquet(PRICES_PATH, index=False)
    return result


# ─── 財務データ（初回全件・以降差分） ───────────────────────────

def _load_fins():
    if os.path.exists(FINS_PATH):
        return pd.read_parquet(FINS_PATH)
    return pd.DataFrame()


def _load_fins_state() -> dict:
    """差分更新の状態。verified_through = 全開示を取得できたと確認済みの最終日（YYYY-MM-DD）。"""
    try:
        with open(FINS_STATE_PATH, encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def _save_fins_state(state: dict) -> None:
    os.makedirs(os.path.dirname(FINS_STATE_PATH), exist_ok=True)
    with open(FINS_STATE_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def _load_trading_dates():
    if not os.path.exists(PRICES_PATH):
        return []
    return pd.read_parquet(PRICES_PATH, columns=["Date"])["Date"].unique()


def _plan_fins_fetch_dates(disc_dates, trading_dates, today, verified_through=None) -> List[pd.Timestamp]:
    """
    差分更新で /fins/summary?date= を取得する日付リスト（昇順）を返す。APIコールなし。

    - verified_through がある場合:
      その日を含む直近 FINS_REFETCH_OVERLAP 取引日 〜 today の取引日
    - ない場合（状態ファイル導入前のキャッシュを修復する初回）:
      直近 FINS_BOOTSTRAP_LOOKBACK_DAYS 日で「開示ゼロの取引日」を探し、欠落が密集し始めた日の
      FINS_BOOTSTRAP_MARGIN 取引日前 〜 today を取得する（ボタンを押した日は当日分しか
      取れていない可能性があるため、区間内は開示の有無に関わらず取り直す）。
      密集区間より前の孤立した欠落日（大納会など）はその日だけ取り直す。
    prices の最終取引日より後の平日（当日の価格が未取得など）も取得対象に含める。
    """
    today = pd.Timestamp(today).normalize()
    td = [d for d in to_day_list(trading_dates) if d <= today]
    td_set = set(td)
    last_td = td[-1] if td else None

    extra = []
    if verified_through:
        vt = pd.Timestamp(verified_through).normalize()
        prior = [d for d in td if d <= vt]
        if len(prior) >= FINS_REFETCH_OVERLAP:
            start = prior[-FINS_REFETCH_OVERLAP]
        else:
            start = prior[0] if prior else vt
    else:
        since = today - pd.Timedelta(days=FINS_BOOTSTRAP_LOOKBACK_DAYS)
        gaps = find_disclosure_gaps(disc_dates, td, since, today)
        pos = {d: i for i, d in enumerate(td)}
        start = None
        for gap, nxt in zip(gaps, gaps[1:]):
            if pos[nxt] - pos[gap] <= FINS_GAP_CLUSTER_WINDOW:
                start = td[max(0, pos[gap] - FINS_BOOTSTRAP_MARGIN)]
                break
        if start is None:
            start = td[-FINS_REFETCH_OVERLAP] if len(td) >= FINS_REFETCH_OVERLAP else today
        extra = [g for g in gaps if g < start]

    rng = [d for d in pd.bdate_range(start, today)
           if d in td_set or last_td is None or d > last_td]
    return sorted(set(extra) | set(rng))


def update_fins(progress_callback=None, trading_dates=None, today=None):
    """
    財務データ更新。

    - fins_cache.parquet が存在しない → 全銘柄を1件ずつ取得（初回のみ）
    - 存在する → 前回の確認済み日以降の全取引日を /fins/summary?date= で取得して差分追記

    【2026-09-15 修正】旧実装は「今日の開示」1日分しか取得せず、データ更新ボタンを
    押さなかった日の開示が永久に欠落していた（2026-04-16〜09-15 で開示ゼロの取引日が
    約90日。7803/5254/4417 の本決算が未収録のまま成長率・PER が計算されていた）。
    全開示を取得できた最終日を data/fins_fetch_state.json に記録し、次回はそこから取り直す。
    状態ファイルが無い場合は、既存キャッシュの開示ゼロ取引日から欠落区間を推定して修復する。

    Args:
        trading_dates: 取引日の一覧（prices.parquet の Date）。None なら prices.parquet から読む
        today:         基準日（テスト用）。None なら実行日
    """
    today = pd.Timestamp(today if today is not None else datetime.today()).normalize()
    existing = _load_fins()

    if existing.empty:
        result = _fetch_fins_all(progress_callback)
        # 銘柄別の全件取得は実行時点までの開示をすべて含む → 前日までを確認済みとする
        if not result.empty:
            _save_fins_state({"verified_through": (today - pd.Timedelta(days=1)).strftime("%Y-%m-%d")})
        return result

    if trading_dates is None:
        trading_dates = _load_trading_dates()
    state    = _load_fins_state()
    verified = state.get("verified_through")

    disc_dates = pd.to_datetime(existing["DiscDate"], errors="coerce").dropna().unique()
    dates = _plan_fins_fetch_dates(disc_dates, trading_dates, today, verified)

    new_frames = []
    failed     = False
    for i, d in enumerate(dates):
        date_str = d.strftime("%Y-%m-%d")
        if progress_callback:
            progress_callback(i, len(dates), f"財務取得: {date_str}")
        try:
            rows = _get_all("/fins/summary", {"date": date_str})
        except Exception:
            failed = True   # 以降の日は確認済みにしない（次回この日から取り直す）
            continue
        if rows:
            new_frames.append(pd.DataFrame(rows))
        # 当日分は後から開示が追加されうるので確認済みにしない
        if not failed and d < today and (verified is None or date_str > verified):
            verified = date_str

    result = existing
    if new_frames:
        # 追記して重複除去（DiscNo がユニークキー。重なり区間の再取得分は新しい値を優先）
        result = pd.concat([existing] + new_frames, ignore_index=True)
        if "DiscNo" in result.columns:
            result = result.drop_duplicates(subset=["DiscNo"], keep="last").reset_index(drop=True)
        result.to_parquet(FINS_PATH, index=False)

    if verified and verified != state.get("verified_through"):
        _save_fins_state({**state, "verified_through": verified})
    return result


def _fetch_fins_all(progress_callback=None):
    """
    初回のみ実行: 全銘柄の財務データを1件ずつ取得して保存する。
    取得成功後は fins_cache.parquet が存在するため、次回以降は差分更新になる。
    """
    data      = _get("/equities/master")
    codes     = [row["Code"] for row in data.get("data", []) if row.get("Code")]
    all_fins  = []
    total     = len(codes)

    for i, code in enumerate(codes):
        if progress_callback:
            progress_callback(i, total, f"📊 財務取得: {code}")
        try:
            d  = _get("/fins/summary", {"code": code})
            df = pd.DataFrame(d.get("data", []))
            if not df.empty:
                # 全種別（FY/1Q/2Q/3Q）・全件保存（上方修正検出のため履歴が必要）
                all_fins.append(df)
        except Exception:
            pass

    result = pd.concat(all_fins, ignore_index=True) if all_fins else pd.DataFrame()
    os.makedirs(os.path.dirname(FINS_PATH), exist_ok=True)
    result.to_parquet(FINS_PATH, index=False)
    return result


# ─── バリュエーション指標（J-Quants 算出値・差分更新） ─────────────

def _load_valuation():
    if os.path.exists(VALUATION_PATH):
        return pd.read_parquet(VALUATION_PATH)
    return pd.DataFrame()


def update_valuation(progress_callback=None, today=None):
    """
    バリュエーション指標（/v2/equities/valuation）の差分更新 → data/valuation.parquet。

    J-Quants が決算短信と株価から日次で算出する EPS / BPS / ROE / PER / PBR / 時価総額
    （株数は自己株控除）。date 指定の 1 コールで全銘柄が取れるので、株価と同じく
    「最終日の翌日〜今日」を日付ループで取得する。自前計算の答え合わせに使う
    （scripts/audit_valuation.py）。

    株価（update_prices）と違い、取得に失敗した日で止める。失敗日を飛ばして後続日を保存すると
    最終日が先に進み、失敗日が二度と取得されないため（fins の取りこぼしと同じ構造）。
    データは日次 16:30 頃に更新される。それより前に取得した当日分は空で返り、次回また取り直す。

    Args:
        today: 基準日（テスト用）。None なら実行日
    """
    today = pd.Timestamp(today if today is not None else datetime.today()).normalize()
    existing = _load_valuation()
    if not existing.empty:
        from_dt = pd.to_datetime(existing["Date"]).max() + pd.Timedelta(days=1)
    else:
        from_dt = today - pd.Timedelta(days=VALUATION_INITIAL_DAYS)

    dates = pd.bdate_range(from_dt, today)
    new_frames = []
    for i, d in enumerate(dates):
        date_str = d.strftime("%Y-%m-%d")
        if progress_callback:
            progress_callback(i, len(dates), f"バリュエーション指標取得: {date_str}")
        try:
            rows = _get_all("/equities/valuation", {"date": date_str})
        except Exception:
            break   # 以降は保存しない（次回この日から取り直す）
        if rows:
            new_frames.append(pd.DataFrame(rows))

    if not new_frames:
        return existing

    frames = ([existing] if not existing.empty else []) + new_frames
    result = (pd.concat(frames, ignore_index=True)
                .drop_duplicates(subset=["Date", "Code"], keep="last")
                .reset_index(drop=True))
    os.makedirs(os.path.dirname(VALUATION_PATH), exist_ok=True)
    result.to_parquet(VALUATION_PATH, index=False)
    return result


def update_earnings_dates(progress_callback=None, today=None):
    """
    決算発表予定日（/fins/earnings-date、全プラン）の差分更新 → data/earnings_dates.parquet。

    予定日の公表・変更は削除されず新しい行として追加される仕様なので、公表日（date=）の日付ループで
    最終公表日〜今日を取得して追記する（最終公表日は当日分の追加公表に備えて取り直す）。
    失敗した日で止め、次回はそこから取り直す。過去分は一括ダウンロード（scripts/jquants_bulk.py）で入れる。
    決算短信の未収録チェック（fins_utils.disclosure_freshness）が使う。
    """
    today = pd.Timestamp(today if today is not None else datetime.today()).normalize()
    existing = (pd.read_parquet(EARNINGS_DATES_PATH) if os.path.exists(EARNINGS_DATES_PATH)
                else pd.DataFrame())
    if not existing.empty:
        from_dt = pd.to_datetime(existing["PubDate"]).max()
    else:
        from_dt = today - pd.Timedelta(days=45)

    dates = pd.bdate_range(from_dt, today)
    new_frames = []
    for i, d in enumerate(dates):
        date_str = d.strftime("%Y-%m-%d")
        if progress_callback:
            progress_callback(i, len(dates), f"決算発表予定日取得: {date_str}")
        try:
            rows = _get_all("/fins/earnings-date", {"date": date_str})
        except Exception:
            break   # 以降は保存しない（次回この日から取り直す）
        if rows:
            new_frames.append(pd.DataFrame(rows).astype(str))

    if not new_frames:
        return existing
    frames = ([existing.astype(str)] if not existing.empty else []) + new_frames
    result = (pd.concat(frames, ignore_index=True)
                .drop_duplicates(keep="last")
                .sort_values(["PubDate", "Code", "FQName"])
                .reset_index(drop=True))
    os.makedirs(os.path.dirname(EARNINGS_DATES_PATH), exist_ok=True)
    result.to_parquet(EARNINGS_DATES_PATH, index=False)
    return result


# ─── モメンタムシグナル計算（APIコールなし） ────────────────────

def _empty_mom():
    return {
        "mom_signal"      : False,
        "mom_signal_date" : "",
        "mom_signal_close": np.nan,
        "mom_vol_ratio"   : np.nan,
        "mom_gc"          : False,
        "mom_new_high"    : False,
        "mom_macd"        : False,
        "mom_above_ma200" : False,
        "mom_ma200_ratio" : np.nan,
        "mom_revision"    : np.nan,
        # SEPA
        "sepa_stage"      : 0,
        "sepa_ma_align"   : False,
        "sepa_ma200_trend": False,
        "sepa_from_low"   : np.nan,
        "sepa_from_high"  : np.nan,
        "sepa_rs"         : np.nan,
        # グランビル・ダウ理論
        "gran_g1"    : False,
        "gran_g2"    : False,
        "dow_uptrend": False,
    }


def _get_revision(fins_df, code, threshold_pct=20, window_days=30):
    try:
        # fins_df はコード別グループ化済みを前提（フィルター不要）
        cf = fins_df.copy()
        if cf.empty:
            return np.nan
        # 通期（FY）のみ対象（四半期間比較による誤検知を防ぐ）
        if "CurPerType" in cf.columns:
            cf = cf[cf["CurPerType"] == "FY"]
        cf["DiscDate"] = pd.to_datetime(cf["DiscDate"], errors="coerce")
        cf["FEPS"]     = pd.to_numeric(cf["FEPS"], errors="coerce")
        cf = (cf.dropna(subset=["FEPS", "DiscDate"])
                .query("FEPS > 0")
                .sort_values("DiscDate")
                .reset_index(drop=True))
        cutoff = pd.Timestamp.today() - pd.Timedelta(days=window_days)
        for i in range(1, len(cf)):
            if cf.loc[i, "DiscDate"] < cutoff:
                continue
            prev_eps = cf.loc[i - 1, "FEPS"]
            if prev_eps == 0:
                continue
            rev = (cf.loc[i, "FEPS"] - prev_eps) / abs(prev_eps) * 100
            if rev >= threshold_pct:
                return round(rev, 1)
        return np.nan
    except Exception:
        return np.nan


def _calc_sepa(close: pd.Series, topix_close: pd.Series) -> dict:
    """
    ミネルヴィニSEPA条件を評価してステージ分類と各指標を返す。

    Stage 2（買いゾーン）の条件:
      1. MA50 > MA150 > MA200（トレンド整列）
      2. MA200が上昇トレンド（20営業日前より高い）
      3. 現値 > MA50 > MA150 > MA200
      4. 52週安値から+25%以上
      5. 52週高値の75%以内（高値から-25%以内）
      6. RS（直近63日の対TOPIX相対パフォーマンス）が正

    Returns dict with sepa_stage, sepa_ma_align, sepa_ma200_trend,
                        sepa_from_low, sepa_from_high, sepa_rs
    """
    empty = {
        "sepa_stage": 0, "sepa_ma_align": False, "sepa_ma200_trend": False,
        "sepa_from_low": np.nan, "sepa_from_high": np.nan, "sepa_rs": np.nan,
    }
    if len(close) < 200:
        return empty

    ma50  = close.rolling(50,  min_periods=40).mean()
    ma150 = close.rolling(150, min_periods=120).mean()
    ma200 = close.rolling(200, min_periods=160).mean()

    c   = float(close.iloc[-1])
    m50 = float(ma50.iloc[-1])
    m150= float(ma150.iloc[-1])
    m200= float(ma200.iloc[-1])

    if any(np.isnan(v) for v in [c, m50, m150, m200]):
        return empty

    # 条件1: MAの整列
    ma_align = (m50 > m150 > m200)

    # 条件2: MA200が上昇トレンド（20営業日前より高い）
    ma200_20ago = float(ma200.iloc[-21]) if len(ma200) >= 21 and not np.isnan(ma200.iloc[-21]) else np.nan
    ma200_trend = (not np.isnan(ma200_20ago)) and (m200 > ma200_20ago)

    # 条件3: 現値 > MA50（MA150・200はma_alignで担保）
    above_all_ma = (c > m50) and ma_align

    # 条件4: 52週安値からの上昇率
    low_52w = float(close.iloc[-252:].min()) if len(close) >= 252 else float(close.min())
    from_low = (c - low_52w) / low_52w * 100 if low_52w > 0 else np.nan

    # 条件5: 52週高値からの下落率（-25%以内が望ましい）
    high_52w = float(close.iloc[-252:].max()) if len(close) >= 252 else float(close.max())
    from_high = (c - high_52w) / high_52w * 100 if high_52w > 0 else np.nan

    # 条件6: RS（直近63営業日の対TOPIX相対パフォーマンス）
    rs = np.nan
    if topix_close is not None and len(topix_close) >= 64:
        # 銘柄とTOPIXを同じ長さに揃える
        n = min(len(close), len(topix_close))
        stk = close.iloc[-n:].reset_index(drop=True)
        tpx = topix_close.iloc[-n:].reset_index(drop=True)
        if len(stk) >= 64 and float(stk.iloc[-64]) > 0 and float(tpx.iloc[-64]) > 0:
            stk_ret = (float(stk.iloc[-1]) / float(stk.iloc[-64]) - 1) * 100
            tpx_ret = (float(tpx.iloc[-1]) / float(tpx.iloc[-64]) - 1) * 100
            rs = round(stk_ret - tpx_ret, 1)

    # ステージ分類
    sepa_conditions = sum([
        ma_align,
        ma200_trend,
        above_all_ma,
        (not np.isnan(from_low))  and from_low  >= 25,
        (not np.isnan(from_high)) and from_high >= -25,
        (not np.isnan(rs))        and rs > 0,
    ])

    if above_all_ma and sepa_conditions >= 4:
        stage = 2  # 買いゾーン
    elif c > m200 and not ma_align:
        stage = 1  # 基盤形成中
    elif c < m200 and not ma_align and m50 < m150:
        stage = 4  # 下降トレンド
    else:
        stage = 3  # 天井圏・分配局面

    return {
        "sepa_stage"      : stage,
        "sepa_ma_align"   : ma_align,
        "sepa_ma200_trend": ma200_trend,
        "sepa_from_low"   : round(from_low,  1) if not np.isnan(from_low)  else np.nan,
        "sepa_from_high"  : round(from_high, 1) if not np.isnan(from_high) else np.nan,
        "sepa_rs"         : rs,
    }


def _calc_momentum_signals(cp, fins_df, code, topix_close=None, lookback=20, vol_mult=2.0):
    """直近lookback営業日以内のモメンタムシグナルを検出して辞書で返す。"""
    try:
        cp = cp.copy()
        cp["Date"] = pd.to_datetime(cp["Date"])
        cp = cp.sort_values("Date").reset_index(drop=True)
        if len(cp) < 60:
            return _empty_mom()

        # 分割対応: 生の C/Vo を末尾日スケールに正規化（cp とインデックス整合）
        close  = normalize_close(cp, dropna=False)
        volume = normalize_volume(cp, fillna=True)

        ma5        = close.rolling(5,   min_periods=1).mean()
        ma25       = close.rolling(25,  min_periods=1).mean()
        ma200      = close.rolling(200, min_periods=1).mean()
        gc         = (ma5 > ma25) & (ma5.shift(1) <= ma25.shift(1))
        high_252   = close.shift(1).rolling(252, min_periods=60).max()
        new_high   = close > high_252
        avg_vol    = volume.shift(1).rolling(20, min_periods=10).mean()
        vol_flag   = volume > avg_vol * vol_mult
        vol_ratio  = volume / avg_vol.replace(0, np.nan)
        ema12      = close.ewm(span=12, adjust=False).mean()
        ema26      = close.ewm(span=26, adjust=False).mean()
        macd_line  = ema12 - ema26
        sig_line   = macd_line.ewm(span=9, adjust=False).mean()
        macd_cross = (macd_line > sig_line) & (macd_line.shift(1) <= sig_line.shift(1))
        above_ma200 = close > ma200
        ma200_ratio = (close / ma200.replace(0, np.nan) - 1) * 100

        other  = gc.astype(int) + new_high.astype(int) + macd_cross.astype(int)
        signal = vol_flag & (other >= 1) & above_ma200

        # SEPA評価
        sepa = _calc_sepa(close, topix_close)

        # ─── グランビル法則（買いパターン）─────────────────────────────
        # G1: MA200を下から上にクロス（直近20日以内）
        gran_g1 = bool(((close > ma200) & (close.shift(1) <= ma200.shift(1))).iloc[-20:].any())
        # G2: MA200付近（±5%）から上昇（直近10日以内）
        near_ma200 = (close >= ma200 * 0.95) & (close <= ma200 * 1.05)
        gran_g2    = bool((near_ma200 & (close > close.shift(1))).iloc[-10:].any())

        # ─── ダウ理論（トレンド確認）────────────────────────────────────
        # 20日ローリング高値・安値が20日前より高ければ上昇トレンド
        roll_high = close.rolling(10, min_periods=5).max()
        roll_low  = close.rolling(10, min_periods=5).min()
        dow_uptrend = (
            len(roll_high) >= 42
            and not np.isnan(roll_high.iloc[-1])
            and not np.isnan(roll_high.iloc[-21])
            and float(roll_high.iloc[-1]) > float(roll_high.iloc[-21])
            and float(roll_low.iloc[-1])  > float(roll_low.iloc[-21])
        )

        recent = signal.iloc[-lookback:]
        if not recent.any():
            result = _empty_mom()
            result.update(sepa)
            result.update({"gran_g1": gran_g1, "gran_g2": gran_g2, "dow_uptrend": dow_uptrend})
            return result

        si = recent[recent].index[-1]  # 最も新しいシグナルの位置（0始まり）

        vr = float(vol_ratio.iloc[si])
        mr = float(ma200_ratio.iloc[si])
        return {
            "mom_signal"      : True,
            "mom_signal_date" : cp.loc[si, "Date"].strftime("%Y/%m/%d"),
            "mom_signal_close": round(float(close.iloc[si]), 1),
            "mom_vol_ratio"   : round(vr, 1) if not np.isnan(vr) else np.nan,
            "mom_gc"          : bool(gc.iloc[si]),
            "mom_new_high"    : bool(new_high.iloc[si]),
            "mom_macd"        : bool(macd_cross.iloc[si]),
            "mom_above_ma200" : bool(above_ma200.iloc[si]),
            "mom_ma200_ratio" : round(mr, 1) if not np.isnan(mr) else np.nan,
            "mom_revision"    : _get_revision(fins_df, code),
            **sepa,
            "gran_g1"    : gran_g1,
            "gran_g2"    : gran_g2,
            "dow_uptrend": dow_uptrend,
        }
    except Exception:
        return _empty_mom()


# ─── メトリクス計算（APIコールなし・ローカル処理） ───────────────

def _growth_vs_prior_fy(latest, cf_fy, fwd_val, nx_val, actual_col, zero_is_missing):
    """
    成長率(%) = 今期値 ÷「今期値の対象年度の直前に終わった FY の実績」- 1。

    今期値の優先順位: 今期予想（latest の F*）→ 来期予想（latest の NxF*。本決算直後）
    → 最新FY実績。対象年度の終了日（CurFYEn / NxtFYEn）より前に終わった FY 実績のうち
    最新のものを前期とする。決算期変更で期間長が違う場合は日数比で揃える。

    【2026-09-15 修正】旧実装は前期を常に cf_fy.iloc[1]（最新確定FYのさらに1期前）に
    していたため、予想値ベースでは 2 年分の伸びを 1 年の成長率として出していた
    （7803: FY26/6予想 ÷ FY24/6実績 = 利益 +586%。正しくは ÷ FY25/6 = +61%）。
    """
    if cf_fy.empty:
        return np.nan

    def _usable(v):
        return pd.notna(v) and not (zero_is_missing and v == 0)

    inclusive = False
    if _usable(fwd_val):
        curr, st, en = fwd_val, latest.get("CurFYSt"), latest.get("CurFYEn")
    elif _usable(nx_val):
        curr, st, en = nx_val, latest.get("NxtFYSt"), latest.get("NxtFYEn")
        if pd.isna(pd.to_datetime(en, errors="coerce")):
            # 来期の期間列が無いレコード: 最新レコードの年度そのものを前期とする（期間補正なし）
            st, en, inclusive = None, latest.get("CurFYEn"), True
    else:
        base = cf_fy.iloc[0]
        curr = pd.to_numeric(base.get(actual_col), errors="coerce")
        st, en = base.get("CurFYSt"), base.get("CurFYEn")

    end = pd.to_datetime(en, errors="coerce")
    if pd.isna(curr) or pd.isna(end):
        return np.nan
    fy_end = pd.to_datetime(cf_fy["CurFYEn"], errors="coerce")
    cand = fy_end[(fy_end <= end) if inclusive else (fy_end < end)]
    if cand.empty:
        return np.nan
    prev = cf_fy.loc[cand.idxmax()]
    prev_val = pd.to_numeric(prev.get(actual_col), errors="coerce")
    if pd.isna(prev_val) or prev_val == 0:
        return np.nan
    scale = period_length_scale(st, en, prev.get("CurFYSt"), prev.get("CurFYEn"))
    return (curr * scale - prev_val) / abs(prev_val) * 100


def _compute_metrics(code, prices_df, fins_df, info_row):
    """1銘柄分のスコア・指標を計算して辞書で返す。データ不足は None。
    prices_df・fins_df はこのコード専用に事前フィルター済みの想定。"""
    try:
        cp = prices_df  # build_stock_cache でコード別グループ化済み
        if len(cp) < 20:
            return None

        cp = cp.sort_values("Date").reset_index(drop=True)
        # 分割対応: 生の C/Vo を末尾日スケールに正規化
        close  = normalize_close(cp, dropna=True)
        volume = normalize_volume(cp, fillna=True)

        if len(close) < 20:
            return None

        latest_close  = float(close.iloc[-1])
        latest_volume = int(volume.iloc[-1])
        avg_vol = calc_avg_volume(volume)
        rsi     = calc_rsi(close)
        ma25    = calc_moving_average(close, 25)

        # 財務データ（build_stock_cache でコード別グループ化済み）
        cf = fins_df
        if cf.empty:
            return None
        cf = cf.sort_values("DiscDate", ascending=False).reset_index(drop=True)
        latest = cf.iloc[0]  # 最新レコード（Q3/Q2/FY いずれか）

        # FYレコード群（前期比較用）: 決算短信（実績）のみ・訂正重複は最新のみ
        # （CurPerType=='FY' には実績列が空の予想修正レコードも混入するため、
        #   そのまま使うと ROE/成長率/op_positive が壊れる）
        if "CurPerType" in cf.columns:
            cf_fy = dedupe_same_fy(filter_fy_statements(cf)).reset_index(drop=True)
        else:
            cf_fy = cf
        latest_fy = cf_fy.iloc[0] if not cf_fy.empty else latest

        # 株式分割対応: 開示日後のAdjFactor累積積で分割比率を取得
        # （per-share 値は「その値が載っているレコードの開示日」を起点に変換する）
        cp_dates = pd.to_datetime(cp["Date"], errors="coerce")
        cp_adj   = pd.to_numeric(cp["AdjFactor"], errors="coerce").fillna(1.0)

        def _sf_after(disc) -> float:
            d = pd.to_datetime(disc, errors="coerce")
            if pd.isna(d):
                return 1.0
            adj_after = cp_adj[cp_dates > d]
            return float(adj_after.prod()) if len(adj_after) > 0 else 1.0

        split_factor = _sf_after(latest.get("DiscDate"))

        # PER: FEPS（今期予想）→ NxFEPS（来期予想、FY確報済みの場合）→ 実績EPS の順で優先
        # FY確報発表後は FEPS が空になり NxFEPS に来期予想が入る
        # NxFEPS は開示時点で既に分割後ベースのため split_factor 不要
        feps   = pd.to_numeric(latest.get("FEPS"),   errors="coerce")
        nxfeps = pd.to_numeric(latest.get("NxFEPS"), errors="coerce")
        eps    = pd.to_numeric(latest_fy.get("EPS"),  errors="coerce")
        # 予想 per-share 値の分割スケール判定（FNP/ShOutFY と突合）。
        # 「開示日<分割日 → 必ず分割前」の一律ルールは、分割発表後・効力前に
        # 分割後ベースで予想を開示する会社（例: 6227）を誤判定し PER を 3 倍にする。
        fnp_latest = pd.to_numeric(latest.get("FNP"),     errors="coerce")
        sh_latest  = pd.to_numeric(latest.get("ShOutFY"), errors="coerce")
        fwd_mult = forecast_per_share_multiplier(feps, fnp_latest, sh_latest, split_factor)
        if pd.notna(feps) and feps > 0:
            per_base = feps * fwd_mult       # 今期予想EPS（開示スケールを判定して調整）
        elif pd.notna(nxfeps) and nxfeps > 0:
            per_base = nxfeps                # 来期予想EPS（開示時点で分割後ベース）
        elif pd.notna(eps) and eps > 0:
            # 実績EPS（フォールバック）: latest_fy の開示日を起点に分割調整
            per_base = eps * _sf_after(latest_fy.get("DiscDate"))
        else:
            per_base = np.nan
        per = latest_close / per_base if not np.isnan(per_base) else np.nan

        # PBR: 純資産・株数は「実績を持つ直近の財務諸表レコード」から取得
        # （最新レコードが予想修正だと Eq/ShOutFY が空で PBR が壊れるため遡って探す）
        eq_src = latest
        if pd.isna(pd.to_numeric(latest.get("Eq"), errors="coerce")) or \
           pd.isna(pd.to_numeric(latest.get("ShOutFY"), errors="coerce")):
            stmts = (cf[cf["DocType"].astype(str).str.contains("FinancialStatements", na=False)]
                     if "DocType" in cf.columns else cf)
            for _, cand in stmts.iterrows():
                if pd.notna(pd.to_numeric(cand.get("Eq"), errors="coerce")) and \
                   pd.notna(pd.to_numeric(cand.get("ShOutFY"), errors="coerce")):
                    eq_src = cand
                    break
        eq     = pd.to_numeric(eq_src.get("Eq"),      errors="coerce")
        sh_out = pd.to_numeric(eq_src.get("ShOutFY"), errors="coerce")
        sf_bs  = _sf_after(eq_src.get("DiscDate"))
        if pd.notna(sh_out) and sf_bs > 0:
            sh_out = sh_out / sf_bs    # 分割後は株数増加
        bps = eq / sh_out if (pd.notna(eq) and pd.notna(sh_out) and sh_out > 0) else np.nan
        pbr = latest_close / bps if (not np.isnan(bps) and bps > 0) else np.nan

        # ROE: 今期予想NP(FNP)があれば予想ROE=FNP/現Eq、なければ実績ROE=実績NP/平均Eq（株探方式）
        fnp    = pd.to_numeric(latest.get("FNP"),   errors="coerce")
        nxfnp  = pd.to_numeric(latest.get("NxFNp"), errors="coerce")  # 成長率計算でも使用
        np_val = pd.to_numeric(latest_fy.get("NP"), errors="coerce")
        eq_fy  = pd.to_numeric(latest_fy.get("Eq"), errors="coerce")
        if pd.notna(fnp) and fnp != 0:
            # 四半期予想 → FNP / 現在の自己資本（予想ROE）
            roe = fnp / eq_fy * 100 if (pd.notna(eq_fy) and eq_fy > 0) else np.nan
        else:
            # FY確報済み → 実績NP / 平均自己資本（株探・Yahoo方式）
            eq_prev = pd.to_numeric(cf_fy.iloc[1].get("Eq"), errors="coerce") if len(cf_fy) >= 2 else np.nan
            avg_eq  = (eq_fy + eq_prev) / 2 if (pd.notna(eq_fy) and pd.notna(eq_prev)) else eq_fy
            roe = np_val / avg_eq * 100 if (pd.notna(np_val) and pd.notna(avg_eq) and avg_eq > 0) else np.nan

        # 配当利回り: FDivAnn（今期予想）→ NxFDivAnn（来期予想、FY確報済み）→ DivAnn（実績）
        # NxFDivAnn は開示時点で分割後ベースのため split_factor 不要
        fdivann   = pd.to_numeric(latest.get("FDivAnn"),   errors="coerce")
        nxfdivann = pd.to_numeric(latest.get("NxFDivAnn"), errors="coerce")
        divann    = pd.to_numeric(latest.get("DivAnn"),    errors="coerce")
        if pd.notna(fdivann) and fdivann > 0:
            # 予想配当は予想EPSと同一レコード=同一ベース。EPS から得た fwd_mult を流用
            # （FEPS 欠損で判定不能なら fwd_mult=split_factor にフォールバック済み）
            div_ann = fdivann * fwd_mult
        elif pd.notna(nxfdivann) and nxfdivann > 0:
            div_ann = nxfdivann   # 既に分割後ベース
        elif pd.notna(divann):
            div_ann = divann * split_factor   # 実績配当は開示時点の分割前ベース
        else:
            div_ann = np.nan
        div_yield = div_ann / latest_close * 100 if (not np.isnan(div_ann) and latest_close > 0) else np.nan

        # 成長率: 予想純利益・売上 vs「その予想の対象年度の直前の FY 実績」
        # FNP→NxFNp→最新FY実績 の順で優先（FY確報済みの場合はNxFNpを使用）
        fsales   = pd.to_numeric(latest.get("FSales"),   errors="coerce")
        nxfsales = pd.to_numeric(latest.get("NxFSales"), errors="coerce")
        revenue_growth = _growth_vs_prior_fy(latest, cf_fy, fsales, nxfsales, "Sales",
                                             zero_is_missing=False)
        profit_growth  = _growth_vs_prior_fy(latest, cf_fy, fnp, nxfnp, "NP",
                                             zero_is_missing=True)

        # ─── Altman Z-score（近似版）────────────────────────────────────
        # 利用可能データで原式に近似: CFO→X1, NP→X2, OP→X3, Eq/負債→X4, Sales→X5
        # 日本株は原式より低く出るため参考値として保存（ハードフィルターは別途）
        ta_val  = pd.to_numeric(latest_fy.get("TA"),  errors="coerce")
        op_val  = pd.to_numeric(latest_fy.get("OP"),  errors="coerce")
        cfo_val = pd.to_numeric(latest_fy.get("CFO"), errors="coerce")
        altman_z = np.nan
        if pd.notna(ta_val) and ta_val > 0 and pd.notna(op_val) and pd.notna(np_val):
            sales_fy = pd.to_numeric(latest_fy.get("Sales"), errors="coerce")
            liab     = ta_val - (eq if pd.notna(eq) else 0)
            x3 = op_val  / ta_val
            x4 = eq / liab if (pd.notna(eq) and liab > 0) else np.nan
            x5 = sales_fy / ta_val if pd.notna(sales_fy) else np.nan
            x1 = cfo_val / ta_val  if pd.notna(cfo_val) else np.nan
            x2 = np_val  / ta_val
            z  = 3.3 * x3
            if pd.notna(x4): z += 0.6  * x4
            if pd.notna(x5): z += 1.0  * x5
            if pd.notna(x1): z += 1.2  * x1
            z += 1.4 * x2
            altman_z = round(z, 3)

        score = 0.0
        if not np.isnan(per) and per > 0:
            score += max(0, (20 - per) / 20 * 25)
        if not np.isnan(pbr):
            score += max(0, (1.5 - pbr) / 1.5 * 15)
        if not np.isnan(roe):
            score += min(roe / 20 * 20, 20)
        if not np.isnan(revenue_growth):
            score += min(revenue_growth / 20 * 20, 20)
        if not np.isnan(rsi):
            score += max(0, 10 - abs(rsi - 50) / 5)

        sig_score, sig_labels = calc_signal_score(close)
        mom = _calc_momentum_signals(cp, fins_df, code, topix_close=info_row.get("_topix_close") if info_row else None)

        return {
            "code"         : code,
            "code_4"       : code[:4],
            "close"        : round(latest_close, 1),
            "score"        : round(score, 1),
            "signal_score" : sig_score,
            "signals"      : ", ".join(sig_labels) if sig_labels else "−",
            "PER"          : round(per, 2)            if not np.isnan(per)            else np.nan,
            "PBR"          : round(pbr, 2)            if not np.isnan(pbr)            else np.nan,
            "ROE"          : round(roe, 2)            if not np.isnan(roe)            else np.nan,
            "div_yield"    : round(div_yield, 2)      if not np.isnan(div_yield)      else np.nan,
            "rev_growth"   : round(revenue_growth, 1) if not np.isnan(revenue_growth) else np.nan,
            "profit_growth": round(profit_growth, 1)  if not np.isnan(profit_growth)  else np.nan,
            "RSI"          : round(rsi, 1)            if not np.isnan(rsi)            else np.nan,
            "MA25"         : round(ma25, 1)           if not np.isnan(ma25)           else np.nan,
            "avg_volume"   : int(avg_vol),
            "latest_volume": latest_volume,
            "altman_z"     : altman_z,
            "op_positive"  : bool(pd.notna(op_val) and op_val > 0),
            "company_name" : info_row.get("CoName", "")                               if info_row else "",
            "market"       : info_row.get("MktNm", "")                                if info_row else "",
            "sector"       : info_row.get("S33Nm", info_row.get("S17Nm", ""))         if info_row else "",
            **mom,
        }
    except Exception:
        return None


def build_stock_cache(market_codes=None):
    """
    prices.parquet + fins_cache.parquet を読み込み、スコア・指標を計算して
    stock_cache.csv に保存する。APIコールは銘柄マスタ取得の1回のみ。

    Args:
        market_codes: 対象市場コードリスト（例: ["0111"]）。None なら全市場。
    Returns:
        pd.DataFrame
    """
    prices_df = _load_prices()
    fins_df   = _load_fins()

    if prices_df.empty or fins_df.empty:
        return pd.DataFrame()

    # 銘柄マスタ（1回のAPIコール）
    data       = _get("/equities/master")
    listed_df  = pd.DataFrame(data.get("data", []))

    if market_codes:
        listed_df = listed_df[listed_df["Mkt"].isin(market_codes)]

    codes    = listed_df["Code"].dropna().unique().tolist()
    info_map = {row["Code"]: row.to_dict() for _, row in listed_df.iterrows()}

    # TOPIX価格系列（RS計算用）: コード "13060" または "13010"（ETF代用）
    # 分割対応: TOPIX ETF も分割発生（13060 は 2026-03-30 1:10 分割）→ normalize_close 必須
    topix_close = None
    for topix_code in ["13060", "13010"]:
        tpx = prices_df[prices_df["Code"] == topix_code]
        if not tpx.empty:
            tpx_sorted = tpx.sort_values("Date").reset_index(drop=True)
            topix_close = normalize_close(tpx_sorted, dropna=False)
            break

    # info_map にTOPIX系列を埋め込む（_compute_metrics経由で_calc_sepaに渡す）
    for code in codes:
        if code in info_map:
            info_map[code]["_topix_close"] = topix_close

    # コード別グループを事前構築（ループ内の全行フィルターを排除）
    # 直近300行に限定: MA200=200日・52W高安値=252日をカバーしつつデータ量を削減
    prices_grouped = {
        code: grp.sort_values("Date").tail(300).reset_index(drop=True)
        for code, grp in prices_df.groupby("Code")
    }
    fins_grouped = {
        code: grp.reset_index(drop=True)
        for code, grp in fins_df.groupby("Code")
    }

    results = []
    for code in codes:
        row = _compute_metrics(
            code,
            prices_grouped.get(code, pd.DataFrame()),
            fins_grouped.get(code, pd.DataFrame()),
            info_map.get(code),
        )
        if row:
            results.append(row)

    df = pd.DataFrame(results) if results else pd.DataFrame()

    # PER / PBR / ROE / 時価総額は J-Quants のバリュエーション指標（予想ベース）に置き換える
    # （2026-09-18〜。自前計算は *_self 列に残す。経緯は services/valuation_source.py）
    valuation = _load_valuation()
    if not df.empty and not valuation.empty:
        as_of = pd.to_datetime(pd.Series(prices_df["Date"].unique())).max()
        df = apply_live_valuation(df, valuation, as_of)

    if not df.empty:
        os.makedirs(os.path.dirname(CACHE_PATH), exist_ok=True)
        df.to_parquet(CACHE_PATH, index=False)
    return df


# ─── 後方互換ラッパー ────────────────────────────────────────────

def fetch_all_stocks(market_codes=None, progress_callback=None):
    """
    後方互換API: 価格・財務データを更新してから stock_cache.csv を生成する。

    処理フロー:
      Phase1: update_prices()     → prices.parquet（バルク・差分）
      Phase2: update_fins()       → fins_cache.parquet（初回のみ全件、以降は前回確認済み日からの取引日数分）
      Phase3: build_stock_cache() → stock_cache.csv（ローカル計算）

    初回実行時は Phase2 で全銘柄の財務データを取得するため時間がかかります。
    2回目以降は Phase1・Phase2 とも前回更新からの経過日数分のコールで完了します。
    """
    fins_is_initial = not os.path.exists(FINS_PATH)

    # Phase1: 価格データ（バルク・差分）
    # 初回なら prices が約85コール、以降は経過日数分
    price_weight = 0.2 if fins_is_initial else 0.5

    def price_cb(i, total, msg):
        if progress_callback:
            frac = (i + 1) / max(total, 1) * price_weight
            progress_callback(min(int(frac * 100), 99), 100, msg)

    prices = update_prices(progress_callback=price_cb if progress_callback else None)
    trading_dates = prices["Date"].unique() if prices is not None and not prices.empty else None

    # Phase2: 財務データ
    def fins_cb(i, total, msg):
        if progress_callback:
            frac = price_weight + (i + 1) / max(total, 1) * (1.0 - price_weight - 0.05)
            progress_callback(min(int(frac * 100), 99), 100, msg)

    update_fins(progress_callback=fins_cb if progress_callback else None,
                trading_dates=trading_dates)

    # Phase2.5: バリュエーション指標（本番の PER/PBR/ROE/時価総額の出所）と決算発表予定日。
    # 失敗しても続行する（バリュエーションは直近 7 日以内の値で代用、予定日は推定ルールで代用される）
    for label, updater in (("バリュエーション指標", update_valuation),
                           ("決算発表予定日", update_earnings_dates)):
        if progress_callback:
            progress_callback(95, 100, f"{label}を取得中...")
        try:
            updater()
        except Exception as e:
            if progress_callback:
                progress_callback(95, 100, f"{label}の取得に失敗しました（続行）: {e}")

    # Phase3: メトリクス計算（APIコールなし）
    if progress_callback:
        progress_callback(96, 100, "⚡ スコア計算中...")

    return build_stock_cache(market_codes=market_codes)
