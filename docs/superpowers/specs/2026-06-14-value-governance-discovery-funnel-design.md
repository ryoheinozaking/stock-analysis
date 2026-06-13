# バリュー深層分析動線「経営変化 × 出遅れ 発掘」設計

- 作成日: 2026-06-14
- 対象: `pages/7_pipeline_report.py` の tab2（バリューモード）+ `services/pipeline_service.py`（activist 列の露出）
- ステータス: 承認済み（設計）→ 実装計画へ

## 背景・問題

バリュー株モードの tab2「SEPA2絞り込み TOP10」で、深層分析の「優先精査 推奨候補」が**常にゼロ件**になる。

原因は**表示ロジックの設計ミスであって、戦略の問題ではない**:

- tab2 は `sepa_stage == 2`（確立した上昇トレンド）で絞った上に、さらに `signal == "BUY"` を要求する（[7_pipeline_report.py:793-835](pages/7_pipeline_report.py)）。
- バリューモードの BUY 条件は **RSI 30-50**（底値圏からの反転）+ close>MA25 + tech≥55 + total≥60。
- SEPA Stage2 入りした銘柄は通常 RSI>50（モメンタム）。実例: 9846 天満屋ストアは Stage2 だが RSI 87.5 → WATCH（RSI 範囲外）。
- 「Stage2（上昇継続中）」と「RSI 30-50（まだ底）」は**構造的に排反** → BUY 0 件は必然。

さらに、バリューで深層分析をかける目的は「バリュートラップか本物のカタリストか」の判定であり、その判定が最も必要なのは**安いのにまだ動いていない（=Stage2でない）WATCH銘柄**。Stage2 を要求すると判定対象を真っ先に捨てる。SEPA Stage2 絞り込みは成長株モード用の動線であり、バリューに流用したのが誤り。

### 重要な前提（戦略への非影響）

バリュー戦略の銘柄選定は `select_top_candidates` が **`total_score` 純順位**で Top20 を採るだけで、`signal`（BUY/WATCH）は選定に使っていない。よって「優先精査ゼロ件」は**実際のポートフォリオに一切影響していない**。本改修は**表示・動線レイヤーのみ**を対象とし、バックテスト α+9.4% の土台（配点・フィルタ・選定）は一切変更しない。

### 診断的根拠（なぜ「経営変化」を軸にするか）

`data/diagnose_value/ic_by_factor_fwd250.csv` より:

- funda_score IC = +0.179、ボーナス抜きコア value_funda_v2 IC = +0.146、生PBR |IC| = 0.146。
- **value_funda_v2(0.146) ≈ |PBR|(0.146)** → PSR/PER/op_margin は PBR に上乗せをほぼ生まない。
- **生PBR を超えている唯一の要素が経営変化ボーナス（0.146 → 0.179）**。

よって深層分析動線の絞り込み軸として「経営変化シグナル」を採るのは、戦略の α 上乗せ分と整合する。

## 設計

### スコープ

- 変えるのは **tab2 の中身**（表示・動線）と、それを支える **activist 列の露出**のみ。
- **配点・スコア式・ハードフィルタ・Top20 選定は変更しない**。
- **成長株モードの tab2 は現状維持**（SEPA2 絞り込み = 攻めの動線として正しい）。tab2 を `mode` で分岐させる。

### 候補生成ロジック（value mode）

- **母集団**: `scored`（フィルタ通過 全銘柄。現状約 614 件）
- **候補条件（AND）**:
  1. 経営変化シグナルあり（以下の **OR**）:
     - `activist == True`（アクティビスト保有）
     - `div_trend >= 1`（1期以上の増配）
     - `payout_ratio` が `[25, 70]` の範囲（健全な還元姿勢。既存ボーナス配点の帯と一致）
  2. 過熱していない: `rsi <= 60`。**`rsi` 欠損時は除外しない**（発掘網を狭めないため、欠損は候補に含める）
- **ランキング**: `total_score` 降順、上位 **10 件**をメイン表示
- **参考枠**: 「経営変化あり だが `rsi > 60`」（割安+還元姿勢だが既に買われ始め）を「参考（過熱気味・待ち）」として最大 **3 件**

> 利益モメンタム（V字転換 `op_turnaround`・増益 `op_trend`）は候補条件に**入れない**。「経営の意志（還元姿勢）」に絞るという決定に従い、利益改善はスコア側に委ねる。

### 表示と動線（value mode）

- success バナー「**→ 次にかけるべき分析（深層分析推奨）**」: 上位 5 件を `深層分析 <code_4>` 形式で列挙（コピペで深層分析スキル起動）。各行に会社名・総合スコア・該当した経営変化シグナルを併記。
- 各候補カードは既存 `_render_scorecard` を流用（経営変化チップは既存表示を踏襲）。
- タブ名: value = 「**経営変化×出遅れ 発掘**」 / growth = 「SEPA2絞り込み TOP10」（現状維持）。
- 空状態: 該当ゼロ件なら情報メッセージ（広い定義 + 全 universe 母集団なので実際はほぼ出ない）。

### データ準備（最小変更：`services/pipeline_service.py`）

`calc_funda_score` の value 分岐で既に `calc_governance_score_for_df` を呼んで governance を `score` に加算している。これを**列として残す**:

```python
if "code_4" in df.columns:
    governance = calc_governance_score_for_df(df, code_4_col="code_4")
    df["activist"] = governance > 0      # ← 追加（テスト可能な列に）
    score = score + governance
```

これにより tab2 のフィルタが単純な列演算になり、ユニットテストが書ける。growth モードでは `activist` 列は付かない（value 分岐内のため）が、tab2 の value 分岐でのみ参照するので問題ない。

### tab2 の構造（擬似コード）

```python
tab1, tab2 = st.tabs(["スコア TOP10", tab2_label(cached_mode)])

with tab2:
    if cached_mode == "value":
        render_value_discovery(scored, ai_stocks)   # 新規
    else:
        render_sepa2_funnel(scored, ai_stocks)       # 既存ロジックを関数に切り出し
```

- `render_value_discovery`: 上記候補条件で母集団を絞り、total_score 順 Top10 + 参考枠 + success バナーを描画。
- `render_sepa2_funnel`: 現状の sepa_stage==2 → BUY 動線をそのまま関数化（挙動不変）。

## テスト

`services/pipeline_service.py` の純粋関数として候補抽出ロジックを切り出し、合成 DataFrame で回帰テスト:

- 経営変化（activist / div_trend / payout）いずれか1つを持つ行が候補に入る。
- どのシグナルも持たない行は除外される。
- `rsi > 60` の経営変化あり行は「参考枠」に回り、メイン候補に入らない。
- `rsi` 欠損（NaN/None）の経営変化あり行はメイン候補に含まれる。
- ランキングが total_score 降順である。
- 利益モメンタムのみ（op_trend≥1 だが activist/div/payout なし）の行は候補に入らない（思想の確認）。

UI 描画（Streamlit）はテスト対象外。抽出ロジックを `select_value_discovery_candidates(scored) -> (main_df, reference_df)` のような関数に分離してテストする。

## スコープ外（YAGNI / 別議題）

以下は本改修に含めず、別途 α レビューで提案・検証:

- 経営変化ボーナスの分解診断（activist / 増配 / payout / V字 / 増益 のどれが IC を上げているか）。
- `op_margin` 10pt の符号問題（IC -0.06 で逆向きの疑い）。
- PSR/PER の冗長性整理。
- セクター内ランク・β低減（Top10 β=0.997 問題）。
- 成長株モードの Top3-5 集中・growth_engine_score 新設。

## 受け入れ基準

1. バリューモードで tab2 を開くと「経営変化×出遅れ」候補が（通常）1件以上表示され、success バナーに `深層分析 <code>` 動線が出る。
2. 成長株モードの tab2 は挙動・表示とも従来と同一。
3. 抽出ロジックのユニットテストが通る。
4. 配点・フィルタ・Top20 選定・バックテスト関連コードに差分がない。
