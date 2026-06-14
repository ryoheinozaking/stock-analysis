# -*- coding: utf-8 -*-
"""
バリュー株モード診断スクリプト

月次スナップショットでバリュー株パイプラインを再実行し、
各ファンダ指標の Rank IC（予測力）・Top N α・重みスイープを出力する。

Usage:
    .venv\\Scripts\\python.exe diagnose_value.py
    .venv\\Scripts\\python.exe diagnose_value.py --fwd 250 --top 20
    .venv\\Scripts\\python.exe diagnose_value.py --fwd 60 --snapshots 12  # 直近12ヶ月のみ

出力 CSV:
    data/diagnose_value/ic_by_factor.csv   -- Rank IC 集計
    data/diagnose_value/alpha_by_topn.csv  -- Top N 別 α
    data/diagnose_value/weight_sweep.csv   -- 重みスイープ
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv(".env")

import pandas as pd

from services.diagnose_value_service import run_diagnosis


# ════════════════════════════════════════════════════════════════════════
#  サマリー表示
# ════════════════════════════════════════════════════════════════════════

def _fmt_pct(v, fmt="+.1f"):
    return f"{v:{fmt}}%" if v is not None else "  N/A "


def print_summary(result: dict) -> None:
    ic_df    = result["ic_by_factor"]
    alpha_df = result["alpha_by_topn"]
    sweep_df = result["weight_sweep"]
    snaps    = result["snapshots"]
    fwd      = result["forward_days"]
    top_n    = result["top_n"]
    p_min, p_max = result["data_range"]

    n_valid = sum(
        1 for r in result["snapshot_results"] if "scored" in r
    )

    W = 78
    print(f"\n{'='*W}")
    print(f"  バリュー株モード Rank IC 診断")
    print(f"  スナップショット: {n_valid}/{len(snaps)} 件有効"
          f"  ({snaps[0]} - {snaps[-1]})")
    print(f"  価格データ: {p_min} - {p_max}")
    print(f"  forward: {fwd}日  /  Top N: {top_n}")
    print(f"{'='*W}")

    # Rank IC
    print(f"\n{'-'*W}")
    print(f"  [Rank IC 診断]  IC > +0.10 で実用レベル")
    print(f"{'-'*W}")
    print(f"  {'factor':<18} {'mean_IC':>8} {'median_IC':>10}"
          f"  {'pos/total':>10}  {'pos%':>6}  {'t_stat':>7}")
    print(f"  {'-'*60}")

    for _, row in ic_df.iterrows():
        mic = row["mean_ic"]
        flag = "  [OK]" if mic > 0.10 else ("  [NG]" if mic < -0.05 else "")
        t     = row["t_stat"]
        t_str = f"{t:+.2f}" if t is not None else "  N/A"
        print(
            f"  {row['factor']:<18} {mic:>+8.4f} {row['median_ic']:>+10.4f}"
            f"  {row['pos_months']:>3}/{row['total_months']:<4}"
            f"  ({row['pos_pct']:>5.1f}%)"
            f"  {t_str:>7}"
            f"{flag}"
        )

    # 旧バックテスト参考
    print(f"\n  参考 [CLAUDE.md 記載のバリューモード旧バックテスト]:")
    print(f"  Top 20 平均リターン:    +37.67%")
    print(f"  TPX α:                 +18.14%   <- これが本物か検証中")
    print(f"  勝率:                   86.1%")
    print(f"  期間:                   37 月次スナップショット")

    # Top N alpha
    print(f"\n{'-'*W}")
    print(f"  [Top N 別 alpha (vs TOPIX ETF / universe, fwd {fwd}日)]")
    print(f"{'-'*W}")
    print(f"  {'top_n':>6}  {'mean_ret':>9}  {'topix':>7}  {'a(TPX)':>7}"
          f"  {'univ':>7}  {'a(univ)':>7}  {'win%':>6}  {'n':>5}")
    print(f"  {'-'*72}")

    for _, row in alpha_df.iterrows():
        flag = " << 現在" if int(row["top_n"]) == top_n else ""
        print(
            f"  {int(row['top_n']):>6}  {_fmt_pct(row.get('mean_return')):>9}"
            f"  {_fmt_pct(row.get('topix_mean')):>7}"
            f"  {_fmt_pct(row.get('alpha')):>7}"
            f"  {_fmt_pct(row.get('universe_mean')):>7}"
            f"  {_fmt_pct(row.get('universe_alpha')):>7}"
            f"  {_fmt_pct(row.get('win_rate'), '.1f'):>6}"
            f"  {int(row.get('n', 0)):>5}{flag}"
        )

    # 重みスイープ
    print(f"\n{'-'*W}")
    print(f"  [Funda/Tech 重みスイープ (Top {top_n})]")
    print(f"{'-'*W}")
    print(f"  {'funda_w':>8}  {'tech_w':>7}  {'mean_ret':>9}  {'a(TPX)':>7}"
          f"  {'a(univ)':>7}  {'win%':>6}  {'n':>5}")
    print(f"  {'-'*65}")

    for _, row in sweep_df.iterrows():
        fw_val = float(row["funda_w"])
        tw_val = float(row["tech_w"])
        flag   = "  <- 現状" if (abs(fw_val - 0.6) < 1e-9 and abs(tw_val - 0.4) < 1e-9) else ""
        print(
            f"  {fw_val:>8.1f}  {tw_val:>7.1f}"
            f"  {_fmt_pct(row.get('mean_return')):>9}"
            f"  {_fmt_pct(row.get('alpha')):>7}"
            f"  {_fmt_pct(row.get('universe_alpha')):>7}"
            f"  {_fmt_pct(row.get('win_rate'), '.1f'):>6}"
            f"  {int(row.get('n', 0)):>5}{flag}"
        )

    # ファンダ・バリアント × 重みスイープ
    variant_df = result.get("variant_sweep")
    if variant_df is not None and not variant_df.empty:
        print(f"\n{'-'*W}")
        print(f"  [Funda variant x weight x Top N sweep]  universe_alpha 降順 上位15件")
        print(f"{'-'*W}")
        print(f"  {'variant':<18} {'fw':>4} {'tw':>4} {'top':>4}"
              f"  {'mean_ret':>9}  {'a(TPX)':>7}  {'a(univ)':>7}  {'win%':>6}  {'n':>5}")
        print(f"  {'-'*78}")
        for _, row in variant_df.head(15).iterrows():
            print(
                f"  {row['variant']:<18} {float(row['funda_w']):>4.1f} {float(row['tech_w']):>4.1f}"
                f" {int(row['top_n']):>4}"
                f"  {_fmt_pct(row.get('mean_return')):>9}"
                f"  {_fmt_pct(row.get('alpha')):>7}"
                f"  {_fmt_pct(row.get('universe_alpha')):>7}"
                f"  {_fmt_pct(row.get('win_rate'), '.1f'):>6}"
                f"  {int(row.get('n', 0)):>5}"
            )

    bonus_ic = result.get("bonus_ic")
    if bonus_ic is not None and not bonus_ic.empty:
        print(f"\n{'-'*W}")
        print("  [経営変化ボーナス 成分別 Rank IC]  (activist は look-ahead)")
        print(f"{'-'*W}")
        for _, r in bonus_ic.iterrows():
            la = "  <- look-ahead" if r.get("look_ahead") else ""
            print(f"  {r['factor']:<16} IC={r['mean_ic']:+.4f}  "
                  f"t={r['t_stat']}  pos%={r['pos_pct']}{la}")

    loo = result.get("bonus_leave_one_out")
    if loo is not None and not loo.empty:
        print(f"\n{'-'*W}")
        print("  [leave-one-out α (funda0.6/tech0.4 × Top20)]  α低下=ボーナスの限界寄与")
        print(f"{'-'*W}")
        for _, r in loo.iterrows():
            la = "  <- look-ahead" if r.get("look_ahead") else ""
            drop = r.get("alpha_drop_vs_full")
            drop_s = f"  Δ={drop:+.2f}" if drop is not None and pd.notna(drop) else ""
            print(f"  {str(r['variant']):<20} α={r['alpha']}  univ_α={r['universe_alpha']}{drop_s}{la}")

    out = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                       "data", "diagnose_value")
    print(f"\n{'-'*W}")
    print(f"  CSV 保存先: {out}/")
    print(f"    ic_by_factor.csv  alpha_by_topn.csv  weight_sweep.csv  funda_variant_sweep.csv")
    print(f"    bonus_ic.csv  bonus_leave_one_out.csv")
    print(f"{'='*W}\n")


def print_per_snapshot_summary(snapshot_results: list) -> None:
    """スナップショットごとの通過銘柄数 / 有効データ数をテーブル表示。"""
    print(f"\n  スナップショット別サマリー:")
    print(f"  {'date':>12}  {'filtered':>8}  {'fwd_ok':>7}")
    print(f"  {'-'*30}")
    for r in snapshot_results:
        n_filt  = r.get("n_filtered", 0)
        n_valid = int(r["scored"]["has_fwd_data"].sum()) if "scored" in r else 0
        err     = r.get("error", "")
        if err:
            print(f"  {r['as_of']:>12}  SKIP: {err}")
        else:
            print(f"  {r['as_of']:>12}  {n_filt:>8}  {n_valid:>7}")


# ════════════════════════════════════════════════════════════════════════
#  CLI エントリ
# ════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="バリュー株モード診断（Rank IC / Top N α / 重みスイープ / universe α）"
    )
    parser.add_argument(
        "--fwd", type=int, default=60,
        help="forward リターンの計算期間（日）。デフォルト: 60"
    )
    parser.add_argument(
        "--top", type=int, default=20,
        help="Top N（デフォルト: 20）"
    )
    parser.add_argument(
        "--snapshots", type=int, default=None,
        help="使用するスナップショット数（最新 N 件）。省略時は全件"
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="スナップショット別サマリーも表示する"
    )
    args = parser.parse_args()

    print(f"バリュー株モード診断 開始")
    print(f"  forward_days={args.fwd}d  top_n={args.top}"
          + (f"  snapshots=最新{args.snapshots}件" if args.snapshots else ""))
    print()

    result = run_diagnosis(
        forward_days=args.fwd,
        top_n=args.top,
        max_snapshots=args.snapshots,
        progress_cb=print,
    )

    if args.verbose:
        print_per_snapshot_summary(result["snapshot_results"])

    print_summary(result)


if __name__ == "__main__":
    main()
