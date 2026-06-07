# 飛躍分析（Leap Analysis）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 深層分析（守備）と対をなす攻めの個別銘柄分析スキル `leap-analysis-jp` を新設し、深層分析が門前払いするグロース株を「成長エンジン実証」で選別して救い上げる。

**Architecture:** Claude Code スキル（Markdown プロンプト文書）。深層分析 `deep-analysis-jp/SKILL.md` と完全に同じ骨格（投資哲学 → 一次情報取得 → フレーム適用 → artifact → ダッシュボード HTML → wiki ingest → 要約）を踏襲し、中核を「拒否フィルター」から「成長エンジン実証 C + SEPA タイミング A + PEG 価格 B + 致命傷ガードレール」へ差し替える。

**Tech Stack:** Markdown スキルプロンプト / EDINET DB MCP（`get_kg_kpi_track_record`, `search_kg_kpi_commitments`, `get_segments`, `get_order_backlog` 他）/ `scripts/deep_analysis_helper.py`（既存・株価/テクニカル/SEPA/percentile）/ WebFetch / Obsidian vault（concept・entity・wiki ingest）/ Chart.js ダッシュボード HTML

---

## 重要な前提（実装者は必ず読む）

### 成果物は 3 つのリポジトリにまたがる

| 成果物 | 場所 | git 管理 |
|---|---|---|
| `leap-analysis-jp/SKILL.md` + `dashboard_template.html` | `C:\Users\ryohei\.claude\skills\leap-analysis-jp\` | 実装時に `git -C` で確認。管理下なら commit、そうでなければファイル作成のみ |
| 新規 concept 4 本 | `<vault>/wiki/concepts/` | vault は PostToolUse hook で auto-commit される（手動 commit 不要） |
| この plan / spec | `C:\Users\ryohei\stock_analysis\docs\superpowers\` | stock_analysis リポジトリ（main） |

`<vault>` = `C:\Users\ryohei\iCloudDrive\iCloud~md~obsidian\LLM Wiki\`

### TDD の翻訳

成果物は実行コードではなくプロンプト文書なので pytest は無い。各タスクの「検証」は以下に翻訳する:
- **構造検証**: 成果物を Read し、必須見出し・必須要素の存在を目視確認
- **リンク検証**: `[[...]]` wikilink が既存 or 新設 concept に解決するか Grep で確認
- **挙動検証**（最終 Task 9）: 実銘柄でスキルを試走し、設計憲法どおりの判定が出るか確認

### 設計の参照元

実装中、常に `C:\Users\ryohei\.claude\skills\deep-analysis-jp\SKILL.md`（42KB）を開いておき、各セクションの構造・トーン・禁則を踏襲する。**逐語コピーではなく「守備（拒否）→ 攻め（証拠）」へ翻案する。**

### commit ポリシー

このプロジェクトは「commit はユーザ確認を挟む」慣習。subagent-driven / executing-plans のレビュー checkpoint でユーザが確認した上で commit する。vault は auto-commit のため例外。

---

## File Structure

- **Create** `C:\Users\ryohei\.claude\skills\leap-analysis-jp\SKILL.md` — スキル本体。唯一の責務 = 飛躍分析の実行手順書
- **Create** `C:\Users\ryohei\.claude\skills\leap-analysis-jp\dashboard_template.html` — 成長 KPI 中心の Chart.js ダッシュボードテンプレ（深層分析テンプレ派生）
- **Create** `<vault>/wiki/concepts/成長エンジン実証.md` — 中核 C レンズの concept
- **Create** `<vault>/wiki/concepts/飛躍シナリオ.md` — §3 定量化手法の concept
- **Create** `<vault>/wiki/concepts/PEG（成長対比バリュエーション）.md` — 価格規律 B の concept
- **Create** `<vault>/wiki/concepts/致命傷ガードレール.md` — ガードレール 4 種の concept
- **Reference (Read only)** `deep-analysis-jp/SKILL.md`, `deep-analysis-jp/dashboard_template.html`, `<vault>/wiki/concepts/モメンタムトラップ.md`（frontmatter スキーマ）

---

## Task 1: スキルディレクトリ + frontmatter + 起動条件

**Files:**
- Create: `C:\Users\ryohei\.claude\skills\leap-analysis-jp\SKILL.md`

- [ ] **Step 1: ディレクトリ作成と参照元の確認**

Read: `C:\Users\ryohei\.claude\skills\deep-analysis-jp\SKILL.md`（行 1-54、frontmatter と「いつ使うか」を把握）

ディレクトリ作成（Bash）:
```bash
mkdir -p "/c/Users/ryohei/.claude/skills/leap-analysis-jp"
```

- [ ] **Step 2: frontmatter + 見出しを書く**

SKILL.md 冒頭に以下を書く。**description は明示起動限定**（深層分析の暗黙パターンとの衝突回避）:

```markdown
---
name: leap-analysis-jp
description: Perform an offensive growth-focused deep dive on a Japanese listed company — the attacking counterpart to deep-analysis-jp's defensive rejection filter. Selects "real growth" from "dream traps" by demanding proof that the growth engine is actually turning (KPIs, unit economics, segment growth, order backlog), then layers SEPA-stage timing and PEG price discipline, with four fatal-flaw guardrails. Writes a 6-section Japanese research report, saves to the LLM Wiki vault, and runs wiki-ingest. EXPLICIT TRIGGER ONLY (to avoid collision with deep-analysis-jp's implicit triggers). Triggers ONLY on explicit phrasings that name a specific Japanese company AND the word 飛躍/leap: "飛躍分析 <X>", "<X>の飛躍分析", "<X>を飛躍分析", "/飛躍分析 <X>", "leap analysis <X>", "<X>は化けるか", "<X>は飛ぶか". Do NOT trigger on generic "<X>って買い？/どう？" (those belong to deep-analysis-jp).
---

# 飛躍分析 (Leap Analysis JP)

深層分析（守備＝拒否フィルター）と対をなす**攻めの個別銘柄分析**。深層分析が門前払いするグロース・テーマ株の中から、「本物（成長エンジンが実際に回っている）」と「夢トラップ」を選別して救い上げる。設計憲法は **「規律あるグロース、もう大怪我はしない」**。
```

- [ ] **Step 3: 起動条件セクションを書く**

`## いつ使うか` を書く。**明示起動のみ**を強調し、禁則に「曖昧な投資判断質問は deep-analysis-jp に渡す」を入れる:

```markdown
## いつ使うか

### 起動条件（明示のみ・深層分析との衝突回避）

以下の**明示パターン**でのみ起動する:
- `飛躍分析 <X>` / `<X>の飛躍分析` / `<X>を飛躍分析(して)` / `/飛躍分析 <X>`
- `leap analysis <X>`
- `<X>は化けるか？` / `<X>は飛ぶか？` / `<X>の伸びしろは？`（X = 銘柄名 or 4 桁コード）

### 禁則（起動しない → deep-analysis-jp に渡す）
- 曖昧な投資判断質問（`<X>って買い？` `<X>はどう？` `<X>の今後は？`）→ これは深層分析（守備）の領域
- 銘柄名・ticker が出てこない質問
- 同セッションで同じ銘柄の飛躍分析を完了済み（再分析は明示時のみ）

### 銘柄識別ヒューリスティック
deep-analysis-jp と同一（4 桁数字 / vault entity 既存社名 / 周知の通称）。迷ったら確認する。
```

- [ ] **Step 4: 検証（構造 Read）**

Read: `leap-analysis-jp/SKILL.md`
Expected: frontmatter の `name: leap-analysis-jp`、description に "EXPLICIT TRIGGER ONLY"、本文に「明示のみ」「deep-analysis-jp に渡す」が存在する

- [ ] **Step 5: commit（checkpoint でユーザ確認後）**

```bash
git -C "/c/Users/ryohei/.claude" add skills/leap-analysis-jp/SKILL.md 2>/dev/null && git -C "/c/Users/ryohei/.claude" commit -m "feat(leap-analysis): スキル骨格と明示起動トリガーを追加" || echo "~/.claude is not a git repo — file created, skip commit"
```

---

## Task 2: 投資哲学（設計憲法 + 中核 C/A/B + 致命傷ガードレール）

これが飛躍分析の心臓。深層分析の `## 投資哲学` に対応するが、中身は攻めの規律。

**Files:**
- Modify: `leap-analysis-jp/SKILL.md`（Task 1 の続きに追記）

- [ ] **Step 1: 設計憲法ノートを書く**

`## 投資哲学（規律あるグロース）` 見出し直後に、深層分析の設計思想ノート（`deep-analysis-jp/SKILL.md` 行 57）と対をなすブロック:

```markdown
> **設計思想（このスキルの存在意義）**: 本スキルは「夢を語るレポート」生成器ではなく、**夢トラップと本物のグロースを選別するアクセル兼ブレーキ**である。深層分析が拒否フィルター（FOMO ブレーキ）なら、飛躍分析は証拠フィルター（投資を前に進めるアクセル）。ただし**実証を要求する**: 成長エンジンが実際に回っている数字（KPI・単位経済・セグメント成長）が無ければ門前払いする。ユーザの実弾の負け（[[464A QPSHD]] 高値掴み / 夢トラップの CANBAS ▲68% / サンバイオ ▲59%）は全て「実体ゼロのストーリー株を高値で掴んだ」こと。**飛躍分析は同じ土俵に、今度はちゃんとした装備で再参入する道具**。憲法は「規律あるグロース、もう大怪我はしない」——テンバガー狙いの分散張りはしない。
```

- [ ] **Step 2: 中核構造 3 層を書く**

```markdown
### 中核構造（3 層 + ガードレール）

買い候補と判定するには、以下 3 層を順に通す。**中核 C を通らないものは A/B を見るまでもなく門前払い**。

#### 中核 C：成長エンジンの実証（無ければ門前払い）

「ストーリー」ではなく「事業エンジンが実際に回っている数字」を要求する。以下を EDINET DB + helper で確認:

- **売上が実際に伸びているか**: 直近通期 + 直近 4Q の YoY 売上成長。目安 YoY ≥ 15%（業種で調整）。**売上が実質ゼロ/微小なら即門前払い（[[夢トラップ]]）**
- **伸びに利益 or 単位経済の裏付けがあるか**: 営業黒字 or 黒字化の明確な軌道。赤字でも「粗利率改善 × 営業レバレッジ（売上増 > 固定費増）」が数字で見えること。粗利ゼロ/単位経済が説明できない成長は夢トラップ
- **成長ドライバーのセグメントが実際に伸びているか**: `get_segments` でドライバー部門の売上/利益が YoY で拡大しているか
- **受注残の質**: `get_order_backlog` があれば YoY で積み上がっているか、かつ**補助金・一過性に依存していないか**（[[290A Synspective]] = 受注残 95% 補助金の反面教師）
- **経営が掲げた KPI の進捗が実績で裏打ちされているか**: `get_kg_kpi_track_record` / `search_kg_kpi_commitments` で中計目標 vs 実績

**ゲート判定**: 「売上が実際に伸びている」AND「その伸びに利益 or 単位経済の改善が伴う」の両方が揃わなければ → **門前払い（[[夢トラップ]] 認定）**。詳細は [[成長エンジン実証]]。

#### タイミング A：SEPA ステージ（高値追い禁止）

helper の `sector_comparison.self_signals` を使う:
- **Stage 2 初期 or 押し目を要求**。Stage 4（下降）や Stage 2 の極端な過熱（mom_new_high True + RSI percentile ≤ 5）は「待つ」に倒す
- 52 週新高値に張り付き + 出来高急増 + 信用倍率高位 = [[モメンタムトラップ]] 警戒（QPS 型の再現を防ぐ）
- 詳細は [[SEPA（ミネルヴィニ ステージ分析）]] / [[レラティブストレングス]]

#### 価格 B：PEG（払い過ぎ防止）

- **PEG = 予想 PER ÷ 期待成長率(%)**。1.0 前後が妥当、**2.0 超は払い過ぎ警戒**（グロースは質次第で 1.5-2.0 まで許容、ただし要根拠）
- 成長に対して価格が先行し過ぎていないかの定量チェック。詳細は [[PEG（成長対比バリュエーション）]]
```

- [ ] **Step 3: 致命傷ガードレール 4 種を書く**

```markdown
### 致命傷ガードレール（4 種・1 つでも該当で見送り or 厳格化）

「もう大怪我しない」憲法の最低防御線。深層分析のフル守備ゲートは課さないが、これだけは内蔵する:

| # | ガードレール | 判定基準 | 該当時の扱い |
|---|---|---|---|
| 1 | [[夢トラップ]] | 事業実体ゼロ/微小（売上実質ゼロ・PSR 異常・受注残が補助金依存） | 見送り（中核 C で既に弾かれる） |
| 2 | [[モメンタムトラップ]] | RSI percentile ≤ 5 + MA200 乖離大 + 信用倍率 > 10-15 倍 + 52 週新高値張り付き | 「待つ」or 安全マージン厳格化 |
| 3 | [[Going Concernリスク（グロース株）]] | GC 注記 / Altman Z 危険域 / 自己資本毀損 | 見送り |
| 4 | 増資希薄化 | 直近の公募増資・MSCB・新株予約権による希薄化、エクイティファイナンス頻発 | 減点 + サイジング縮小 |

詳細は [[致命傷ガードレール]]。
```

- [ ] **Step 4: 適用フレーム表を書く**

深層分析の「必須チェック」表（行 92-112）に対応する攻め版。守備フレームのうち致命傷系のみ流用し、攻めフレームを足す:

```markdown
### 適用フレームワーク（必須チェック）

| フレームワーク | 判定内容 |
|---|---|
| [[成長エンジン実証]] | 中核 C ゲート。売上の実成長 × 利益/単位経済の裏付け。無ければ門前払い |
| [[飛躍シナリオ]] | 基本/強気の到達 price target と倍率、実現トリガー（観測可能 KPI） |
| [[PEG（成長対比バリュエーション）]] | 成長率に対する価格の妥当性 |
| [[SEPA（ミネルヴィニ ステージ分析）]] | Stage 判定。高値追い禁止 |
| [[致命傷ガードレール]] | 夢/モメンタム/GC/希薄化の 4 チェック |
| [[株式分割対応の正規化]] | 過去 EPS/売上を分割調整しているか |
| [[能力の輪]] | このビジネスは理解可能領域か（攻めでも能力の輪は維持） |
```

- [ ] **Step 5: 検証（構造 Read）**

Read: `leap-analysis-jp/SKILL.md`
Expected: 「中核 C」「門前払い」「致命傷ガードレール」「PEG」「SEPA」の各見出しが存在し、中核 C ゲートが "両方揃わなければ門前払い" と明記されている

- [ ] **Step 6: commit（checkpoint 後）**

```bash
git -C "/c/Users/ryohei/.claude" add skills/leap-analysis-jp/SKILL.md && git -C "/c/Users/ryohei/.claude" commit -m "feat(leap-analysis): 投資哲学（中核C/A/B + 致命傷ガードレール）を追加" || echo "skip commit (not a git repo)"
```

---

## Task 3: 実行フロー（一次情報取得 + フレーム適用）

**Files:**
- Modify: `leap-analysis-jp/SKILL.md`

- [ ] **Step 1: Step 0 過去判定チェックを書く**

深層分析 行 118-126 を流用（攻め用に文言調整）。重大開示後の過去判定無効化は攻めでも必須:

```markdown
## 実行フロー

### Step 0: 過去判定の有効期限チェック（既存 entity / 過去判定がある場合のみ）

対象銘柄に vault 既存 entity ページや過去判定（深層分析・飛躍分析どちらも）がある場合、分析前に有効性を点検。前回分析日以降に重大開示（決算/業績修正/増資/TOB/分割/大株主異動/重大事業ニュース）があれば過去判定を一旦無効として再評価する。確認手段は `mcp__edinetdb__get_events` + `get_earnings` + WebFetch。
```

- [ ] **Step 2: Step 1 一次情報取得（攻め用）を書く**

中核 C に必要な KPI 系ツールを前面に出す:

```markdown
### Step 1: 一次情報取得（並列）

**EDINET DB（中核 C の実証データを最優先で取る）**:
- `mcp__edinetdb__get_financials` — 過去 5-7 期の売上/利益/EPS 時系列（成長の連続性）
- `mcp__edinetdb__get_earnings` — 直近決算（YoY 加速の確認）
- `mcp__edinetdb__get_segments` — セグメント別売上/利益（成長ドライバーの特定）
- `mcp__edinetdb__get_order_backlog` — 受注残（あれば。質と YoY 推移）
- `mcp__edinetdb__get_kg_kpi_track_record` — 経営 KPI の実績推移
- `mcp__edinetdb__search_kg_kpi_commitments` — 中計目標 vs 実績
- `mcp__edinetdb__get_company` / `get_shareholders` / `get_events` — 会社概要・株主・直近開示
- `mcp__edinetdb__get_cross_shareholdings` / `get_activist_positions` — （補助）

**WebFetch / WebSearch**:
- 直近 KPI 速報・成長ニュース・新製品/新セグメント進捗
- PTS・足元の値動き・**信用倍率**（[[モメンタムトラップ]] ガードレールの一次指標）
```

- [ ] **Step 3: Step 1.5 helper 実行を書く**

深層分析 行 148-220 の helper 節を流用（同一ヘルパー、local モード Bash 直実行）:

```markdown
### Step 1.5: J-Quants ヘルパー実行（株価/テクニカル/SEPA/percentile）

深層分析と同一ヘルパーを使う:
```bash
cd C:\Users\ryohei\stock_analysis
.venv\Scripts\python.exe scripts\deep_analysis_helper.py <ticker> --save
```
出力の `moving_averages` / `technical`(RSI/MACD) / `range_52w` / `volume` / `sector_comparison`(SEPA stage・mom_signal・RS・percentile) をタイミング A の判定に使う。**percentile は単一ルール**（低い = セクター上位/favorable）で読む。鮮度が 3 営業日超なら Streamlit「データ更新」を依頼。
```

- [ ] **Step 4: Step 2 フレーム適用を書く**

```markdown
### Step 2: フレームワーク適用

Task 2 の「適用フレームワーク」を全て適用。**中核 C ゲートを最初に判定**し、通らなければ「夢トラップ認定・門前払い」でレポートを短く締める（A/B の深掘り不要）。EPS/売上は [[株式分割対応の正規化]] に従い必ず分割調整。
```

- [ ] **Step 5: 検証（構造 Read）**

Read: `leap-analysis-jp/SKILL.md`
Expected: Step 0/1/1.5/2 が順に存在、Step 1 に `get_kg_kpi_track_record` `get_order_backlog` `get_segments`、Step 2 に「中核 C ゲートを最初に判定」が存在

- [ ] **Step 6: commit（checkpoint 後）**

```bash
git -C "/c/Users/ryohei/.claude" add skills/leap-analysis-jp/SKILL.md && git -C "/c/Users/ryohei/.claude" commit -m "feat(leap-analysis): 実行フロー（KPI一次情報取得 + helper + フレーム適用）を追加" || echo "skip"
```

---

## Task 4: レポート 6 セクション artifact テンプレ

**Files:**
- Modify: `leap-analysis-jp/SKILL.md`

- [ ] **Step 1: Step 3 artifact 構造を書く**

深層分析 行 233-353 の 6 セクションに対応する攻め版。**「失敗の解剖」→「飛躍の設計図」へ反転**:

```markdown
### Step 3: artifact 生成（日本語 Markdown）

```markdown
# {会社名}（{コード}）飛躍分析レポート

**作成日**: {YYYY-MM-DD}
**分析時点株価**: ¥{value}（{date} 終値）
**時価総額**: 約 ¥{value}
**上場市場**: 東証{プライム/スタンダード/グロース}
**業種**: {sector}

> 設計憲法: 規律あるグロース。中核 C（成長エンジン実証）を通らなければ門前払い。

---

## 1. 成長ストーリーの骨格
- 何を売って、なぜ伸びているか（一文で言える成長の核）
- TAM と現在地（市場規模に対するシェア、伸びしろの天井）
- 能力の輪: このビジネスは理解可能か

## 2. 成長エンジンの実証（中核 C・心臓部）
- 売上 YoY（通期 + 直近 4Q）と加速/減速の判定（表）
- 利益/単位経済の裏付け（粗利率トレンド・営業レバレッジ・黒字化軌道）
- セグメント別成長（ドライバー部門の特定）
- 受注残の質（あれば。補助金/一過性依存でないか）
- 経営 KPI の実績進捗（中計目標 vs 実績）
- **中核 C ゲート判定: 通過 / 門前払い（夢トラップ認定）を明言**

## 3. 飛躍シナリオ
- 基本シナリオ: 現成長率が 2-3 年継続 → 売上/EPS → 妥当 PER → 到達 price target（倍率明記）
- 強気シナリオ: 成長加速 or 新セグメント寄与 → upside（倍率明記）
- 各シナリオの**実現トリガー**（観測可能な KPI で明示）

## 4. タイミングと価格
- SEPA Stage 判定（helper self_signals）・MA/RSI/MACD・52 週位置・信用倍率
- 「今は押し目か高値追いか」を明言
- PEG（予想 PER ÷ 期待成長率）= 払い過ぎ判定

## 5. 致命傷ガードレール
- 夢 / モメンタム / GC / 希薄化の 4 チェックを各々 該当/非該当 で明示
- 1 つでも該当 → 見送り or 厳格化の理由を書く

## 6. 総合判断
- 「張る / 押し目を待つ / 見送り（夢トラップ）」を明言（逃げ言葉禁止）
- サイジングの目安（中リスク前提、致命傷該当があれば縮小）
- 監視する KPI トリガー 3-5 個（飛躍が現実化する観測点）
```
```

- [ ] **Step 2: 検証（構造 Read）**

Read: `leap-analysis-jp/SKILL.md`
Expected: 6 セクション見出しが全て存在、§2 に「中核 C ゲート判定: 通過 / 門前払い を明言」、§3 に「実現トリガー」、§6 に「逃げ言葉禁止」

- [ ] **Step 3: commit（checkpoint 後）**

```bash
git -C "/c/Users/ryohei/.claude" add skills/leap-analysis-jp/SKILL.md && git -C "/c/Users/ryohei/.claude" commit -m "feat(leap-analysis): 6セクション artifact テンプレを追加" || echo "skip"
```

---

## Task 5: ダッシュボードテンプレ（成長 KPI 中心）

**Files:**
- Create: `leap-analysis-jp/dashboard_template.html`
- Modify: `leap-analysis-jp/SKILL.md`（Step 4.5 として参照を追記）

- [ ] **Step 1: 深層分析テンプレをコピーして派生させる**

```bash
cp "/c/Users/ryohei/.claude/skills/deep-analysis-jp/dashboard_template.html" "/c/Users/ryohei/.claude/skills/leap-analysis-jp/dashboard_template.html"
```

- [ ] **Step 2: チャート構成を成長 KPI 中心に差し替える**

Read: `deep-analysis-jp/dashboard_template.html` でチャートカード構造と配色 CSS 変数を把握。以下の方針で `leap-analysis-jp/dashboard_template.html` を Edit:
- KPI strip: 直近終値 / 時価総額 / 売上 YoY / PEG / SEPA Stage / 投資判断 の 6 個
- Chart 1: 売上 + 営業利益の時系列（成長の連続性）
- Chart 2: 売上 YoY 成長率の推移（加速/減速）
- Chart 3: セグメント別売上（doughnut or 横棒）
- Chart 4: 粗利率/営業利益率トレンド（単位経済）
- Chart 5: 飛躍シナリオ table（基本/強気 → price target）
- Chart 6: 株価と MA + SEPA Stage 注記
- **配色は深層分析テンプレの CSS 変数を流用**（accent-red/gold/green/blue、Plotly デフォルト禁止）

- [ ] **Step 3: SKILL.md に Step 4.5 参照を追記**

深層分析 行 363-456 を流用し、テンプレパスを `leap-analysis-jp/dashboard_template.html` に差し替えた `### Step 4.5: 成長ダッシュボード HTML 生成` を書く。保存先は `<vault>/raw/conversations/{銘柄}-飛躍分析-{YYYY-MM-DD}/artifacts/{銘柄}_{コード}_成長ダッシュボード.html`。

- [ ] **Step 4: 検証（構造 Read + ブラウザ目視）**

Read: `leap-analysis-jp/dashboard_template.html`
Expected: KPI strip に「売上 YoY」「PEG」「SEPA Stage」、Plotly デフォルト色（`#636efa` 等）が含まれない

- [ ] **Step 5: commit（checkpoint 後）**

```bash
git -C "/c/Users/ryohei/.claude" add skills/leap-analysis-jp/ && git -C "/c/Users/ryohei/.claude" commit -m "feat(leap-analysis): 成長KPI中心ダッシュボードテンプレを追加" || echo "skip"
```

---

## Task 6: wiki ingest 連動 + entity 共有 + 要約

**Files:**
- Modify: `leap-analysis-jp/SKILL.md`

- [ ] **Step 1: Step 4（保存）と Step 5（wiki ingest）を書く**

深層分析 行 355-532 を流用。**entity 共有が肝**:

```markdown
### Step 4: artifact を raw/ に保存
保存先: `<vault>/raw/conversations/{銘柄}-飛躍分析-{YYYY-MM-DD}/artifacts/{銘柄}_{コード}_飛躍分析レポート.md`

### Step 5: 同セッションで wiki-ingest 連動
`/wiki-ingest` フローに従う:
1. `raw/.manifest.json` 更新
2. `wiki/sources/{銘柄} 飛躍分析 {YYYY-MM-DD}.md` 作成
3. **`wiki/entities/{コード} {銘柄}.md` は深層分析と共有**。既存なら追記し、攻めの判定を「## 飛躍分析判定」セクションとして加える。深層分析判定（守備）と飛躍分析判定（攻め）が 1 ページに併存する形にする。Quick Verdict 直下にダッシュボード HTML 直リンク 1 行（[!tip] callout は使わない）
4. 新規 concepts（Task 8 の 4 本）を作成/更新
5. `wiki/domains/投資.md` MOC 更新
6. `wiki/index.md` / `wiki/hot.md` / `wiki/log.md` 更新（log は先頭に追加）
```

- [ ] **Step 2: Step 6 要約を書く**

```markdown
### Step 6: ユーザに要約報告（200-300 字）
- 判定（張る / 押し目待ち / 見送り=夢トラップ）
- 中核 C ゲート: 通過 or 門前払いの理由
- 飛躍シナリオの倍率（基本/強気）
- 該当した致命傷ガードレール
- 監視 KPI トリガー
```

- [ ] **Step 3: 検証（構造 Read）**

Read: `leap-analysis-jp/SKILL.md`
Expected: Step 5-3 に「深層分析と共有」「## 飛躍分析判定」「攻めと守りが 1 ページに併存」、Step 6 に「中核 C ゲート: 通過 or 門前払い」

- [ ] **Step 4: commit（checkpoint 後）**

```bash
git -C "/c/Users/ryohei/.claude" add skills/leap-analysis-jp/SKILL.md && git -C "/c/Users/ryohei/.claude" commit -m "feat(leap-analysis): wiki ingest連動とentity共有・要約を追加" || echo "skip"
```

---

## Task 7: 禁則・トーン・既存事例プレースホルダ

**Files:**
- Modify: `leap-analysis-jp/SKILL.md`

- [ ] **Step 1: 禁則を書く**

深層分析 行 576-586 を攻め用に翻案:

```markdown
## 禁止事項
- **装飾系絵文字**（📊 📈 🎯 🔥 🌟 等）。機能系（✅❌⚠️🚫）は判定シグナルのみ可
- 「場合による」「検討の余地がある」等の逃げ言葉
- **中核 C を通らない銘柄を「成長期待」「将来性」で救済する**（夢トラップの正当化）= 最大の禁則
- [[モメンタムトラップ]] を「勢いがある」で買い推奨
- 株式分割未調整の EPS/売上で成長率・PER 計算
- 高値追い（SEPA Stage 4 / RSI percentile ≤ 5）を「飛躍」と書く
- サイジングを中リスク上限を超えて推奨する（憲法「もう大怪我しない」違反）
```

- [ ] **Step 2: トーンと既存事例の枠を書く**

```markdown
## トーン
- 自然な日本語のレポート調、Substance over performance
- 数字は単位明示、主要数値はテーブル化、結論は太字 + 機能系絵文字で明示

## 既存事例（蓄積予定）
飛躍分析の判定スペクトラム（張る / 押し目待ち / 門前払い=夢トラップ）を網羅するサンプルを、実行のたびにここへ追記する。初版は空でよい。反面教師として深層分析側の [[290A Synspective]]（受注残補助金）/ [[464A QPSHD]]（高値掴み）を「中核 C で弾かれる例」として参照する。

## 出典
- 設計: docs/superpowers/specs/2026-06-07-leap-analysis-design.md（stock_analysis リポジトリ）
- 対をなす守備スキル: deep-analysis-jp
```

- [ ] **Step 3: 検証（構造 Read）**

Read: `leap-analysis-jp/SKILL.md`
Expected: 禁則に「中核 C を通らない銘柄を成長期待で救済」が最大の禁則として存在、トーン・既存事例・出典が存在

- [ ] **Step 4: commit（checkpoint 後）**

```bash
git -C "/c/Users/ryohei/.claude" add skills/leap-analysis-jp/SKILL.md && git -C "/c/Users/ryohei/.claude" commit -m "feat(leap-analysis): 禁則・トーン・事例枠を追加（スキル本体完成）" || echo "skip"
```

---

## Task 8: 新規 concept ページ 4 本

**Files:**
- Create: `<vault>/wiki/concepts/成長エンジン実証.md`
- Create: `<vault>/wiki/concepts/飛躍シナリオ.md`
- Create: `<vault>/wiki/concepts/PEG（成長対比バリュエーション）.md`
- Create: `<vault>/wiki/concepts/致命傷ガードレール.md`

- [ ] **Step 1: frontmatter スキーマを確認**

Read: `<vault>/wiki/concepts/モメンタムトラップ.md`（行 1-12）で frontmatter スキーマ（type/title/created/updated/status/domain/tags/aliases）を把握。created/updated は `2026-06-07`。

- [ ] **Step 2: `成長エンジン実証.md` を書く**

骨子: ストーリーではなく「事業エンジンが実際に回っている数字」を要求する中核レンズ。売上実成長 × 利益/単位経済の裏付け。ゲート判定（両方揃わなければ門前払い）。反面教師 = 売上ゼロのパイプライン株。
関連リンク: `[[夢トラップ]]` `[[飛躍シナリオ]]` `[[PEG（成長対比バリュエーション）]]` `[[290A Synspective]]` `[[能力の輪]]`

- [ ] **Step 3: `飛躍シナリオ.md` を書く**

骨子: 基本/強気シナリオで到達 price target・倍率を定量化し、各シナリオに観測可能 KPI の実現トリガーを紐付ける手法。希望的観測との違い = トリガーが反証可能であること。
関連リンク: `[[成長エンジン実証]]` `[[PEG（成長対比バリュエーション）]]` `[[正規化EPS]]` `[[安全マージン]]`

- [ ] **Step 4: `PEG（成長対比バリュエーション）.md` を書く**

骨子: PEG = PER ÷ 期待成長率。グロース株の「払い過ぎ」を縛る価格規律。1.0 妥当 / 2.0 超警戒。バリュー流の割安要求が使えないグロースでの代替価格レンズ。
関連リンク: `[[飛躍シナリオ]]` `[[成長エンジン実証]]` `[[バリュートラップ]]` `[[モメンタムトラップ]]`

- [ ] **Step 5: `致命傷ガードレール.md` を書く**

骨子: 攻めでも内蔵する最低防御線 4 種（夢/モメンタム/GC/希薄化）。各判定基準と該当時の扱い。深層分析のフル守備ゲートとの違い（飛躍分析は致命傷のみ）。
関連リンク: `[[夢トラップ]]` `[[モメンタムトラップ]]` `[[Going Concernリスク（グロース株）]]` `[[成長エンジン実証]]`

- [ ] **Step 6: 検証（リンク解決 Grep）**

```bash
grep -rl "成長エンジン実証\|飛躍シナリオ\|PEG（成長対比\|致命傷ガードレール" "/c/Users/ryohei/iCloudDrive/iCloud~md~obsidian/LLM Wiki/wiki/concepts/"
```
Expected: 4 ファイルが存在。各 frontmatter に `type: concept`。vault は auto-commit のため手動 commit 不要。

---

## Task 9: 統合検証（リンク解決 + 実銘柄試走）

**Files:** なし（検証のみ）

- [ ] **Step 1: 全 wikilink の解決確認**

`leap-analysis-jp/SKILL.md` 内の全 `[[...]]` を抽出し、参照先が vault に存在するか確認:
```bash
grep -oP '\[\[[^\]]+\]\]' "/c/Users/ryohei/.claude/skills/leap-analysis-jp/SKILL.md" | sort -u
```
各リンクについて `<vault>/wiki/` 配下に対応ページがあるか確認。新設 4 concept + 既存（夢トラップ/モメンタムトラップ/SEPA/能力の輪 等）に全て解決すること。unresolved があれば該当 concept を追加 or リンク修正。

- [ ] **Step 2: 実銘柄で試走（反面教師 = ガードレール機能の確認）**

深層分析が「夢トラップ/見送り」にした既存グロース銘柄（例: `290A Synspective`）で飛躍分析を起動:
```
飛躍分析 290A
```
Expected: 中核 C ゲートで「受注残 95% 補助金依存・売上の質が伴わない」と判定し、**門前払い（夢トラップ認定）** でレポートが短く締まる。救済しないこと。

- [ ] **Step 3: 実銘柄で試走（本物 = 救い上げ機能の確認）**

売上が実際に伸びていて利益裏付けのあるグロース銘柄を 1 件選び起動。Expected: 中核 C 通過 → A（SEPA タイミング）→ B（PEG）→ ガードレール → 総合判断（張る/押し目待ち）まで到達し、サイジングが中リスク前提になっている。

- [ ] **Step 4: entity 共有の確認**

試走で生成された `wiki/entities/{コード} {銘柄}.md` を Read。Expected: 既存に深層分析判定があれば「## 飛躍分析判定」が併存、新規なら飛躍分析判定が記録されている。

- [ ] **Step 5: 完了報告**

試走 2 件の判定サマリと、unresolved link が無いことをユーザに報告。

---

## Self-Review（このプランを書いた後の自己点検結果）

**1. Spec coverage:** spec の全要素を Task にマップ済み —
- 設計憲法/ネーミング → Task 1-2 / 中核 C/A/B → Task 2 / 致命傷ガードレール → Task 2 / 6 セクション → Task 4 / データ供給（helper + EDINET KPI + WebFetch）→ Task 3 / ダッシュボード → Task 5 / wiki ingest + entity 共有 → Task 6 / 新規 concept 4 本 → Task 8 / 明示起動トリガー → Task 1 / 成功基準（リンク解決・試走）→ Task 9。発掘パイプライン棚卸しは spec で別トラック明記のためプラン対象外（正しい）。

**2. Placeholder scan:** 各 Step に実際の文面・判定基準・コマンド・期待結果を記載。concept 本文（Task 8）は逐語ではなく骨子 + 関連リンク指定（vault 文体は実装時に既存ページ踏襲が適切なため、これは手抜きではなく正当な指示）。

**3. Type consistency:** 用語を全 Task で統一 — 「中核 C / 門前払い / 致命傷ガードレール / 飛躍シナリオ / SEPA Stage / PEG」。concept ファイル名（`成長エンジン実証` `飛躍シナリオ` `PEG（成長対比バリュエーション）` `致命傷ガードレール`）が Task 2 の wikilink と Task 8 の Create と一致。entity 見出し「## 飛躍分析判定」が Task 6 と Task 9 で一致。
