# deep-analysis-jp 投資哲学レンズ追加 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** deep-analysis-jp スキルに守備精密化2レンズ（堀の耐久性=クリステンセン / サイクル位置=マークス）を中核を壊さず追加し、再帰性・モメンタムを買い根拠化しないガードレールを明文化する。

**Architecture:** SKILL.md（マークダウン）への6箇所の局所編集 + LLM Wiki vault concepts への1追記・3新規作成。コードもテストもない純粋なドキュメント変更。「検証」は Grep/Read による挿入確認と [[...]] リンク解決確認で行う。

**Tech Stack:** Markdown（Obsidian Flavored）、Edit/Write/Grep ツール。git 対象は spec/plan doc のみ（SKILL.md は `~/.claude/skills/` 配下で非 git、vault は iCloud 同期で非 git）。

**Spec:** `docs/superpowers/specs/2026-06-06-deep-analysis-philosophy-lenses-design.md`

---

## File Structure

| ファイル | 操作 | 責務 |
|---|---|---|
| `C:\Users\ryohei\.claude\skills\deep-analysis-jp\SKILL.md` | Modify ×6 | 設計思想ノート / 勝ちパターン節 / 必須表+注記 / §1 モート / §4.7 / §6 / §4.6 |
| `…\LLM Wiki\wiki\concepts\モメンタムトラップ.md` | Modify | 再帰性（ソロス）節の追記 + Related 追加 |
| `…\LLM Wiki\wiki\concepts\破壊的イノベーション（クリステンセン）.md` | Create | 堀の耐久性レンズの concept |
| `…\LLM Wiki\wiki\concepts\市場心理サイクル（ハワード・マークス）.md` | Create | サイクル位置レンズの concept |
| `…\LLM Wiki\wiki\concepts\バリュー+カタリスト.md` | Create | 勝ちパターンの concept |

**注記**: SKILL.md/vault は非 git。各タスクに git commit ステップは無い。Edit の old_string が一致しない場合のみ、該当セクションを Read して anchor を再導出する（編集前セクションのテキストは spec と本計画に転記済みだが、念のため）。

vault パス省略形 `…\LLM Wiki\` = `C:\Users\ryohei\iCloudDrive\iCloud~md~obsidian\LLM Wiki\`

---

## Task 1: SKILL.md 設計思想ノート挿入（1-A）

**Files:**
- Modify: `C:\Users\ryohei\.claude\skills\deep-analysis-jp\SKILL.md`

- [ ] **Step 1: 投資哲学見出し直後にノートを挿入**

Edit:
- old_string:
```
## 投資哲学（バフェット/マンガー流・日本市場適用）

### 基本原則
```
- new_string:
```
## 投資哲学（バフェット/マンガー流・日本市場適用）

> **設計思想（このスキルの存在意義）**: 本スキルは「賢く見えるレポート」生成器ではなく、**FOMO（高値掴み・テーマ追随）を止めるブレーキ**である。中核のバフェット/マンガー流は**拒否フィルター**として働く。以下に足す補助レンズ（堀の耐久性・サイクル位置）は**守備の精密化**であって攻撃エンジンではない。**再帰性・モメンタムは買いの根拠にしない**——それらは [[モメンタムトラップ]] の発生メカニズムであり、検出対象であって正当化材料ではない。複数レンズを「どの銘柄もどれかに合格」させる合理化に使うことを禁ずる。

### 基本原則
```

- [ ] **Step 2: 挿入確認**

Run: `Grep pattern="FOMO（高値掴み・テーマ追随）を止めるブレーキ" path="C:\Users\ryohei\.claude\skills\deep-analysis-jp\SKILL.md" output_mode="content"`
Expected: 1 ヒット

---

## Task 2: SKILL.md 「勝ちパターン: バリュー+カタリスト」節挿入（1-B）

**Files:**
- Modify: `C:\Users\ryohei\.claude\skills\deep-analysis-jp\SKILL.md`

- [ ] **Step 1: 基本原則の末尾と思考の規律の間に節を挿入**

Edit:
- old_string:
```
- 大きなリターンは買いでも売りでもなく**「待つこと」で生まれる**。忍耐は戦略である

### 思考の規律
```
- new_string:
```
- 大きなリターンは買いでも売りでもなく**「待つこと」で生まれる**。忍耐は戦略である

### 勝ちパターン: バリュー+カタリスト

中核の拒否フィルターを通った先で、**実証的に効いてきた攻め筋を1つだけ明文化する**: 「割安 × 触媒」。安く見えるだけでは不足（[[バリュートラップ]] 行き）、触媒——経営の意志・株主還元改定・東証 PBR 要請への実弾・アクティビスト参入・TOB/M&A——が伴って初めて PBR/PER の是正が現実に起きる。

- 実例: [[5016 JX金属]]（割安 × 株主還元方針改定 + 自社株 TOB + 東邦チタニウム完全子会社化）= +48%
- 触媒の所在は [[経営変化スコア]] / [[アクティビスト投資]] / [[東証 PBR 改善要請]] / [[自社株 TOB（公開買付）]] で捕捉
- 詳細: [[バリュー+カタリスト]]

これは攻撃エンジンの解禁ではない。「割安」を買い根拠にする前に**触媒の実在を要求する**という、もう一段の規律である。

### 思考の規律
```

- [ ] **Step 2: 挿入確認**

Run: `Grep pattern="### 勝ちパターン: バリュー\+カタリスト" path="C:\Users\ryohei\.claude\skills\deep-analysis-jp\SKILL.md" output_mode="content"`
Expected: 1 ヒット

---

## Task 3: SKILL.md 必須チェック表に行追加 + 条件付きレンズ注記（1-C）

**Files:**
- Modify: `C:\Users\ryohei\.claude\skills\deep-analysis-jp\SKILL.md`

- [ ] **Step 1: 表末尾に堀の耐久性行を追加し、表直後に条件付きレンズ注記を挿入**

Edit:
- old_string:
```
| [[株式分割対応の正規化]] | 過去 EPS を分割調整しているか、混入していないか |

---
```
- new_string:
```
| [[株式分割対応の正規化]] | 過去 EPS を分割調整しているか、混入していないか |
| [[破壊的イノベーション（クリステンセン）]] | この堀は次の競争ルールで負債に変わらないか（堀の耐久性）。崩壊兆候があれば見送り/[[too hard pile]] |

**条件付きレンズ（該当時のみ適用）**:
- [[市場心理サイクル（ハワード・マークス）]]: シクリカル/テーマ/急騰銘柄では「今が強欲・恐怖・正常化のどこか」を §4.7 で判定し、買い水準の厳しさを調整する

---
```

注意: この `| [[株式分割対応の正規化]] | … |` 行 + 直後の `---` は SKILL.md 内で必須チェック表の末尾にのみ存在する一意の anchor。

- [ ] **Step 2: 挿入確認**

Run: `Grep pattern="破壊的イノベーション（クリステンセン）.*堀の耐久性" path="C:\Users\ryohei\.claude\skills\deep-analysis-jp\SKILL.md" output_mode="content"`
Expected: 1 ヒット（表の行）

---

## Task 4: SKILL.md §1 経済的堀 bullet 差し替え（2-A）

**Files:**
- Modify: `C:\Users\ryohei\.claude\skills\deep-analysis-jp\SKILL.md`

- [ ] **Step 1: モート評価 bullet に堀の耐久性の問いを追加**

Edit:
- old_string:
```
- 経済的堀（モート）の評価
- 顧客は誰か、なぜ自社から買うか
```
- new_string:
```
- 経済的堀（モート）の評価 — **加えて「この堀は次の競争ルールで負債に変わらないか」（堀の耐久性 / [[破壊的イノベーション（クリステンセン）]]）を必ず問う**。今の強み（既存技術・既存顧客基盤・規模）が次世代の競争で足枷に転じる兆候はないか。堀崩壊の兆候があれば §6 で見送り/[[too hard pile]] に倒す
- 顧客は誰か、なぜ自社から買うか
```

- [ ] **Step 2: 挿入確認**

Run: `Grep pattern="この堀は次の競争ルールで負債に変わらないか" path="C:\Users\ryohei\.claude\skills\deep-analysis-jp\SKILL.md" output_mode="content"`
Expected: 1 ヒット（§1 内）

---

## Task 5: SKILL.md §4.7 サイクル位置 新設（2-B）

**Files:**
- Modify: `C:\Users\ryohei\.claude\skills\deep-analysis-jp\SKILL.md`

- [ ] **Step 1: §4.6 末尾（Top5 ベンチマーク段落）と §5 見出しの間に §4.7 を挿入**

Edit:
- old_string:
```
**Top 5 同業ベンチマーク**: `top5_by_signal_score_in_sector` で「同セクターで最も評価されている上位 5 銘柄」と比較。自銘柄が PER/PBR/ROE で大幅に劣る場合、「同業の中でなぜこれを選ぶか」を説明できなければ買い候補ではない。

## 5. 日本市場固有の文脈
```
- new_string:
```
**Top 5 同業ベンチマーク**: `top5_by_signal_score_in_sector` で「同セクターで最も評価されている上位 5 銘柄」と比較。自銘柄が PER/PBR/ROE で大幅に劣る場合、「同業の中でなぜこれを選ぶか」を説明できなければ買い候補ではない。

### 4.7 サイクル位置（シクリカル/テーマ/急騰銘柄では必須・[[市場心理サイクル（ハワード・マークス）]]）

「今この銘柄/セクターは市場心理サイクルのどこにいるか」を判定し、安全マージンの取り方を調整する。バフェット流の「価値」評価を補完する**二次的思考**のレンズ。

- **判定軸**: 強欲（多幸感・新規参入殺到・"今回は違う"ナラティブ）/ 正常化 / 恐怖（投げ売り・無関心）のどこか
- **根拠データ**: §4.6 セクター percentile（PBR/RSI percentile が割高×過熱なら強欲）/ 信用倍率（§4.5）/ 出来高急増 / メディア・個人の注目度（WebFetch）
- **使い方**:
  - 強欲フェーズ → 買い水準を**通常より厳しく**（安全マージンを厚く要求）
  - 恐怖フェーズ + 中核フィルター通過 → **積極**（マークス「最良の買いは皆が投げ売る時」）
  - 「価値は割安だが、サイクルは強欲の一方上り」= [[モメンタムトラップ]] 警戒
- **注意**: サイクル位置は買い"根拠"ではなく買い"水準"の調整材料。割高な強欲銘柄を「サイクルがまだ上」で正当化しない（再帰性の罠）

## 5. 日本市場固有の文脈
```

- [ ] **Step 2: 挿入確認**

Run: `Grep pattern="### 4.7 サイクル位置" path="C:\Users\ryohei\.claude\skills\deep-analysis-jp\SKILL.md" output_mode="content"`
Expected: 1 ヒット

---

## Task 6: SKILL.md §6 守備ゲート決定ドライバー化（2-C）

**Files:**
- Modify: `C:\Users\ryohei\.claude\skills\deep-analysis-jp\SKILL.md`

- [ ] **Step 1: §6 見出し直後に守備ゲートを挿入**

Edit:
- old_string:
```
## 6. 待つべきか・今か（総合判断）
直接的に結論を述べる:
```
- new_string:
```
## 6. 待つべきか・今か（総合判断）

判定に先立ち、以下の守備ゲートを明示的に通す:
- **堀の耐久性（必須）**: 堀崩壊の兆候があれば、バリュエーションに関わらず見送り/[[too hard pile]] に倒す
- **サイクル位置（該当時）**: 強欲フェーズなら買い水準を厳しく、恐怖フェーズなら積極に補正
- **3 系統トラップ判定**（バリュー / 夢 / モメンタム）: 該当するなら明示

その上で直接的に結論を述べる:
```

- [ ] **Step 2: 重複する旧トラップ判定行を削除**

Edit:
- old_string:
```
3 系統トラップ判定（バリュー / 夢 / モメンタム）に該当するなら明示。
「場合による」「検討の余地がある」は禁止。
```
- new_string:
```
「場合による」「検討の余地がある」は禁止。
```

- [ ] **Step 3: 挿入確認**

Run: `Grep pattern="判定に先立ち、以下の守備ゲートを明示的に通す" path="C:\Users\ryohei\.claude\skills\deep-analysis-jp\SKILL.md" output_mode="content"`
Expected: 1 ヒット
Run: `Grep pattern="3 系統トラップ判定" path="C:\Users\ryohei\.claude\skills\deep-analysis-jp\SKILL.md" output_mode="count"`
Expected: 1（守備ゲート内の1箇所のみ。旧行が消えている）

---

## Task 7: SKILL.md §4.6 ミネルヴィニ系利用の明確化（2-D）

**Files:**
- Modify: `C:\Users\ryohei\.claude\skills\deep-analysis-jp\SKILL.md`

- [ ] **Step 1: self_signals 利用ブロック末尾に1行追加**

Edit:
- old_string:
```
- Stage 2 + mom_new_high True + RSI percentile ≤ 5 = [[モメンタムトラップ]] の極致
```
- new_string:
```
- Stage 2 + mom_new_high True + RSI percentile ≤ 5 = [[モメンタムトラップ]] の極致
- **需給/モメンタムシグナル（SEPA stage・mom_signal・RS）は sector_comparison で取得済み**だが、これらは「業績変化 × 需給」の両方が揃った時のみ買い材料。需給単独を買いの免罪符にしない（[[SEPA（ミネルヴィニ ステージ分析）]] / [[レラティブストレングス]]）
```

- [ ] **Step 2: 挿入確認**

Run: `Grep pattern="需給単独を買いの免罪符にしない" path="C:\Users\ryohei\.claude\skills\deep-analysis-jp\SKILL.md" output_mode="content"`
Expected: 1 ヒット

---

## Task 8: モメンタムトラップ.md に再帰性（ソロス）節を追記（3-A）

**Files:**
- Modify: `C:\Users\ryohei\iCloudDrive\iCloud~md~obsidian\LLM Wiki\wiki\concepts\モメンタムトラップ.md`

- [ ] **Step 1: 「構成要素（5因子）」と「適用例」の間に発生メカニズム節を挿入**

Edit:
- old_string:
```
5. 正規化 PER（過去 5-7 年平均 EPS × 適正 PER）と現値 PER が **2 倍以上乖離**

## 適用例
```
- new_string:
```
5. 正規化 PER（過去 5-7 年平均 EPS × 適正 PER）と現値 PER が **2 倍以上乖離**

## 発生メカニズム: 再帰性（ソロス）

株価上昇そのものが企業行動とファンダを変える正のフィードバック——増資調達の容易化 → ナラティブ強化 → 採用力/顧客信用の向上 → さらなる株価上昇。これがモメンタムトラップを膨らませる動力。**重要: これは買いの根拠ではなく罠の発生機構**。再帰性を「だから上がり続ける」と読むのが高値掴みの典型（実例: [[464A QPSHD]] / [[290A Synspective]] の急騰局面）。再帰ループは外部資金・テーマ熱が尽きた瞬間に逆回転する。

## 適用例
```

- [ ] **Step 2: Related に市場心理サイクルを追加**

Edit:
- old_string:
```
- [[too hard pile]] — モメンタムトラップに繰り返し引っかかる業種は永続見送りも合理的
```
- new_string:
```
- [[too hard pile]] — モメンタムトラップに繰り返し引っかかる業種は永続見送りも合理的
- [[市場心理サイクル（ハワード・マークス）]] — サイクルの強欲フェーズ = モメンタムトラップ多発期
```

- [ ] **Step 3: 挿入確認**

Run: `Grep pattern="発生メカニズム: 再帰性（ソロス）" path="C:\Users\ryohei\iCloudDrive\iCloud~md~obsidian\LLM Wiki\wiki\concepts\モメンタムトラップ.md" output_mode="content"`
Expected: 1 ヒット

---

## Task 9: 破壊的イノベーション（クリステンセン）.md を新規作成（3-B-1）

**Files:**
- Create: `C:\Users\ryohei\iCloudDrive\iCloud~md~obsidian\LLM Wiki\wiki\concepts\破壊的イノベーション（クリステンセン）.md`

- [ ] **Step 1: 完全な内容で Write**

Write（以下を全文）:
```markdown
---
type: concept
title: "破壊的イノベーション（クリステンセン）"
created: 2026-06-06
updated: 2026-06-06
status: developing
domain: investing
tags: [concept, framework, moat, disruption, christensen]
aliases: [Disruptive Innovation, 破壊的イノベーション, 堀の耐久性, イノベーションのジレンマ]
---

# 破壊的イノベーション（クリステンセン）

## Definition

クレイトン・クリステンセンの「イノベーションのジレンマ」に基づく**堀の耐久性**レンズ。[[deep-analysis-jp]] では §1（事業の本質）の必須チェックとして「**この堀は次の競争ルールで負債に変わらないか**」を問う。

バフェット流は安定した堀（モート）を好むが、その裏返しとして**堀が破壊される瞬間を過小評価**しがち。優良企業ほど既存顧客・既存技術・既存収益構造に最適化されており、それが次世代の競争では足枷に転じる。これがイノベーションのジレンマ。

## 持続的 vs 破壊的

| | 持続的イノベーション | 破壊的イノベーション |
|---|---|---|
| 方向 | 既存指標で性能向上 | 別の価値基準を持ち込む |
| 勝者 | 通常は既存大手 | 新規参入者 |
| 既存大手の反応 | 得意（追随できる） | 軽視・対応遅れ（合理的判断ゆえに負ける） |
| 例 | より速い CPU | スマホが PC を侵食 / SaaS が pkg ソフトを侵食 |

破壊には 2 型:
- **ローエンド型破壊**: 過剰品質の隙を、安価で「十分」な代替が下から侵食
- **新市場型破壊**: 非消費（これまで使えなかった層）を取り込み、やがて主流市場へ

## 堀の耐久性チェック（深層分析での問い）

1. 今の強み（既存技術・顧客基盤・規模・流通）は、5-10 年後の競争ルールでも強みのままか
2. その強みに最適化された組織・収益構造が、新しい価値基準への転換を**遅らせる**方向に働かないか
3. 下（ローエンド）や横（非消費）から、別の価値基準を持つ代替が育っていないか
4. 経営陣は破壊の兆候を認識し、自己破壊（カニバリゼーション容認）を選べる体質か

崩壊兆候があれば、バリュエーションが割安でも §6 で見送り/[[too hard pile]] に倒す。

## 適用が効く領域

- 半導体製造装置・素材（プロセスノード転換・新材料で堀が移る）
- プラットフォーム（ハード世代交代・配信モデル転換）
- 金融（フィンテック・ネット専業による下からの侵食）
- SaaS 移行期の旧来型ソフト（[[SaaS移行期の収益の谷]]）

## Related

- [[能力の輪]] — 破壊の判定は能力の輪の内側でのみ可能。分からなければ [[too hard pile]]
- [[too hard pile]] — 規格競争中・破壊の最中の技術は積極的に積む
- [[モメンタムトラップ]] — 「新ストーリー」が実は自社の堀を破壊する側のこともある
- [[半導体サイクル感応度]] — 持続的/破壊的の両方が頻発する領域
- [[SaaS移行期の収益の谷]] — 破壊される側が移行で通る谷
- [[投資]]

## Sources

- Clayton Christensen "The Innovator's Dilemma"
- [[deep-analysis-jp]] スキル §1 堀の耐久性チェック（2026-06-06 導入）
```

- [ ] **Step 2: 作成確認**

Run: `Grep pattern="堀の耐久性チェック" path="C:\Users\ryohei\iCloudDrive\iCloud~md~obsidian\LLM Wiki\wiki\concepts\破壊的イノベーション（クリステンセン）.md" output_mode="content"`
Expected: 1 ヒット

---

## Task 10: 市場心理サイクル（ハワード・マークス）.md を新規作成（3-B-2）

**Files:**
- Create: `C:\Users\ryohei\iCloudDrive\iCloud~md~obsidian\LLM Wiki\wiki\concepts\市場心理サイクル（ハワード・マークス）.md`

- [ ] **Step 1: 完全な内容で Write**

Write（以下を全文）:
```markdown
---
type: concept
title: "市場心理サイクル（ハワード・マークス）"
created: 2026-06-06
updated: 2026-06-06
status: developing
domain: investing
tags: [concept, framework, cycle, market-psychology, howard-marks]
aliases: [Market Cycle, ハワード・マークス, 二次的思考, サイクル位置, 振り子]
---

# 市場心理サイクル（ハワード・マークス）

## Definition

ハワード・マークス（Oaktree）の思想に基づく**サイクル位置**レンズ。[[deep-analysis-jp]] では §4.7 として、シクリカル/テーマ/急騰銘柄で「**今この銘柄/セクターは市場心理サイクルのどこにいるか**」を判定し、安全マージンの取り方（買い水準の厳しさ）を調整する。

バフェット流の「価値」評価を否定するのではなく**補完**する。価値が割安でも、市場が強欲フェーズの一方上りなら買い水準を厳しくし、恐怖フェーズで皆が投げ売るなら積極になる。

## 2 つの核概念

### 二次的思考（Second-Level Thinking）
- 一次的思考: 「良い会社だから買い」
- 二次的思考: 「良い会社だが、それは皆が知っていて価格に織り込み済みでは？コンセンサスとどこで違う見方をしているか？」
- コンセンサスは情報であって福音ではない（[[SOUL.md 投資哲学]] と一致）

### 振り子（Pendulum）
市場心理は強欲↔恐怖の間を振れ、中庸に留まる時間は短い。
- **強欲**: 多幸感・新規参入殺到・「今回は違う」ナラティブ・リスク無視
- **正常化**: 中庸（通過点）
- **恐怖**: 投げ売り・無関心・優良企業まで叩き売られる

## サイクル位置の判定と使い方（深層分析 §4.7）

- **判定根拠**: §4.6 セクター percentile（PBR/RSI が割高×過熱なら強欲）/ 信用倍率 / 出来高急増 / メディア・個人の注目度
- **強欲フェーズ** → 買い水準を通常より厳しく（安全マージンを厚く要求）
- **恐怖フェーズ + 中核フィルター通過** → 積極（「最良の買いは皆が投げ売る時」）
- **制約**: サイクル位置は買い"水準"の調整材料であって買い"根拠"ではない。割高な強欲銘柄を「サイクルがまだ上だから」で正当化してはならない（それは再帰性の罠 = [[モメンタムトラップ]]）

## バフェット流との関係

- バフェット: 「価値」の絶対水準を見る
- マークス: 「今サイクルのどこか」を見る
- 両立: 内在価値で割安 **かつ** サイクルが恐怖寄り = 最良の買い場。割安だが強欲の只中 = 待つ

## Related

- [[安全マージン]] — サイクル位置で要求する安全マージンの厚みを変える
- [[正規化EPS]] — 強欲フェーズのピーク利益を正規化で割り引く
- [[モメンタムトラップ]] — 強欲フェーズの極致。再帰性で膨らむ
- [[反転思考]] — 「今回は違う」ナラティブを疑う
- [[バリュートラップ]] — 恐怖フェーズの安値が「割安」か「劣化」かは別途判定
- [[SOUL.md 投資哲学]] — コンセンサスは情報、独立判断
- [[投資]]

## Sources

- Howard Marks "The Most Important Thing" / "Mastering the Market Cycle"
- [[deep-analysis-jp]] スキル §4.7 サイクル位置（2026-06-06 導入）
```

- [ ] **Step 2: 作成確認**

Run: `Grep pattern="二次的思考（Second-Level Thinking）" path="C:\Users\ryohei\iCloudDrive\iCloud~md~obsidian\LLM Wiki\wiki\concepts\市場心理サイクル（ハワード・マークス）.md" output_mode="content"`
Expected: 1 ヒット

---

## Task 11: バリュー+カタリスト.md を新規作成（3-B-3）

**Files:**
- Create: `C:\Users\ryohei\iCloudDrive\iCloud~md~obsidian\LLM Wiki\wiki\concepts\バリュー+カタリスト.md`

- [ ] **Step 1: 完全な内容で Write**

Write（以下を全文）:
```markdown
---
type: concept
title: "バリュー+カタリスト"
created: 2026-06-06
updated: 2026-06-06
status: developing
domain: investing
tags: [concept, framework, value, catalyst, winning-pattern]
aliases: [Value plus Catalyst, バリュー+カタリスト, 割安×触媒, 勝ちパターン]
---

# バリュー+カタリスト

## Definition

[[deep-analysis-jp]] で明文化された**実証的勝ちパターン**: 「割安 × 触媒」。中核の拒否フィルター（バフェット/マンガー流）を通った先で、実際にリターンを生んできた攻め筋を 1 つだけ定式化したもの。

**安く見えるだけでは不足**（[[バリュートラップ]] 行き）。触媒——経営の意志・株主還元改定・東証 PBR 要請への実弾発射・アクティビスト参入・TOB/M&A——が伴って初めて、PBR/PER の是正が**現実の出来事として**起きる。

これは攻撃エンジンの解禁ではなく、「割安」を買い根拠にする前に**触媒の実在を要求する**もう一段の規律である。

## なぜ触媒が必要か

低 PBR・低 PER は「市場がまだ評価していない」とも「市場が正しく劣化を織り込んでいる」とも読める。両者を分けるのが触媒の有無:
- 触媒なし → 割安が割安のまま放置（バリュートラップ） or 構造劣化で更に下落
- 触媒あり → 是正が時間軸付きで進む（還元増・自社株消却・資本効率改善・支配権異動）

## 触媒の類型と捕捉手段

| 触媒 | 捕捉する concept / データ |
|---|---|
| 経営の意志（中計 ROE/PBR 目標・資本配分転換） | [[経営変化スコア]] |
| 株主還元改定（増配・累進配当・自社株買い） | [[自社株 TOB（公開買付）]] / fins の DPS 推移 |
| 東証 PBR 1 倍割れ是正要請への対応 | [[東証 PBR 改善要請]] |
| アクティビスト参入 | [[アクティビスト投資]] / EDINET 大量保有報告 |
| TOB / M&A / 親子上場解消 | get_events / 適時開示 |

## 実証事例

### [[5016 JX金属]]（+48%）
割安 × 触媒の教科書:
- 割安: 基礎材料・半導体材料の世界シェア企業ながら割安水準
- 触媒（同日 4 イベント = 東証 PBR 要請のショーケース）: [[JX金属 株主還元方針改定]]（性向 20→25% + 下限 ¥20）/ 自社株 ¥2,500 億 TOB / 東邦チタニウム完全子会社化 / ユーロ円建 CB
- 結果: 個人ポジション 300 株 +48.63%

## バリュートラップとの分岐点

同じ「PBR 1 倍割れ・高配当」表面シグナルでも、触媒の有無と裏のファンダ方向で判定が真逆:
- [[5016 JX金属]] = 割安 × 触媒（還元改定）= 勝ち
- [[6658 シライ電子工業]] = 割安に見えるが FY27 ▲58% 減益 = [[バリュートラップ]]（触媒どころか逆風）

## Related

- [[バリュートラップ]] — 割安単独・触媒なしの行き先
- [[経営変化スコア]] — 触媒の数値化（経営の意志）
- [[アクティビスト投資]] — 外部からの触媒
- [[東証 PBR 改善要請]] — 日本市場固有の大型触媒
- [[自社株 TOB（公開買付）]] — 還元・資本効率の実弾
- [[5016 JX金属]] — 勝ちパターンの実証事例
- [[安全マージン]] — 触媒があっても割安水準でしか買わない
- [[投資]]

## Sources

- [[deep-analysis-jp]] スキル 勝ちパターン節（2026-06-06 明文化）
- [[5016 JX金属]] の実トレード（+48%）
```

- [ ] **Step 2: 作成確認**

Run: `Grep pattern="割安 × 触媒の教科書" path="C:\Users\ryohei\iCloudDrive\iCloud~md~obsidian\LLM Wiki\wiki\concepts\バリュー+カタリスト.md" output_mode="content"`
Expected: 1 ヒット

---

## Task 12: 全体リンク解決検証 + plan doc コミット

**Files:**
- Verify only（編集なし）+ Commit: `docs/superpowers/plans/2026-06-06-deep-analysis-philosophy-lenses.md`

- [ ] **Step 1: SKILL.md 6箇所が全て入っているか一括確認**

Run（それぞれ output_mode="count" で 1 を期待）:
- `Grep pattern="設計思想（このスキルの存在意義）" path="…SKILL.md"`
- `Grep pattern="### 勝ちパターン: バリュー\+カタリスト" path="…SKILL.md"`
- `Grep pattern="### 4.7 サイクル位置" path="…SKILL.md"`
- `Grep pattern="判定に先立ち、以下の守備ゲート" path="…SKILL.md"`

Expected: 各 1

- [ ] **Step 2: 新設3 concept + モメンタムトラップ注記が存在するか確認**

Run: `Glob pattern="wiki/concepts/{破壊的イノベーション*,市場心理サイクル*,バリュー+カタリスト}.md" path="…\LLM Wiki"`（または 3 ファイルを個別 Read で存在確認）
Expected: 3 ファイル存在
Run: `Grep pattern="再帰性（ソロス）" path="…\モメンタムトラップ.md" output_mode="count"`
Expected: 1

- [ ] **Step 3: 新設/編集ページの [[...]] リンク先が全て実在するか確認**

新設3ページ + モメンタムトラップ追記が参照する concept/entity（[[能力の輪]] [[too hard pile]] [[モメンタムトラップ]] [[半導体サイクル感応度]] [[SaaS移行期の収益の谷]] [[安全マージン]] [[正規化EPS]] [[反転思考]] [[バリュートラップ]] [[SOUL.md 投資哲学]] [[経営変化スコア]] [[アクティビスト投資]] [[東証 PBR 改善要請]] [[自社株 TOB（公開買付）]] [[JX金属 株主還元方針改定]] [[5016 JX金属]] [[6658 シライ電子工業]] [[464A QPSHD]] [[290A Synspective]] [[SEPA（ミネルヴィニ ステージ分析）]] [[レラティブストレングス]] [[投資]]）は、本計画着手前の検証で全て実在を確認済み（spec の検証方法参照）。新設3ページ同士の相互参照（[[破壊的イノベーション（クリステンセン）]] [[市場心理サイクル（ハワード・マークス）]] [[バリュー+カタリスト]]）は Task 9-11 で作成されるため解決する。

確認: `[[投資]]` が `wiki/` 直下 or `wiki/domains/投資.md` に存在するか念のため Glob。
Run: `Glob pattern="**/投資.md" path="…\LLM Wiki"`
Expected: 1 件以上（MOC 本体）

- [ ] **Step 4: plan doc をコミット**

```bash
git -C C:\Users\ryohei\stock_analysis add docs/superpowers/plans/2026-06-06-deep-analysis-philosophy-lenses.md
git -C C:\Users\ryohei\stock_analysis commit -m "docs: deep-analysis-jp 投資哲学レンズ追加の実装計画"
```

---

## オプション（spec スコープ外・実行可否は要相談）

新設3 concept は SKILL.md（vault 外）からのみ参照されるため、vault 内では当面 orphan。orphan を嫌う場合、`wiki/domains/投資.md`（または `wiki/投資.md`）MOC の Frameworks セクションに 3 リンクを追記すると wiki-lint 的に健全。spec の承認スコープ外なので、実施する場合はユーザに確認してから。

---

## Self-Review

**1. Spec coverage:**
- 1-A 設計思想ノート → Task 1 ✓
- 1-B 勝ちパターン節 → Task 2 ✓
- 1-C 必須表+条件付き注記 → Task 3 ✓
- 2-A §1 モート → Task 4 ✓
- 2-B §4.7 → Task 5 ✓
- 2-C §6 守備ゲート → Task 6 ✓
- 2-D §4.6 ミネルヴィニ → Task 7 ✓
- 3-A モメンタムトラップ再帰性注記 → Task 8 ✓
- 3-B 新設3 concept → Task 9-11 ✓
- リンク解決検証 → Task 12 ✓
全 spec 要件にタスクが対応。ギャップなし。

**2. Placeholder scan:** TBD/TODO/「後で」なし。concept ページは全文を Write ステップに転記済み。

**3. Type/naming consistency:** concept ページ名・wikilink 表記は spec・既存 vault と一致（`[[破壊的イノベーション（クリステンセン）]]` 等の全角括弧含め統一）。frontmatter は `モメンタムトラップ.md` のスキーマ（type/title/created/updated/status/domain/tags/aliases）に準拠。

---

## 実行時の追加（2026-06-06、ユーザ承認 = 案A）

Task 12 のリンク解決検証で `[[deep-analysis-jp]]` が dead link（既存 vault 10 件 + 新規3ページ 3 件）と判明。vault の `wiki/meta/lint-report-2026-05-30.md` が既に P3 として指摘済みだった。ユーザ承認のもと**案A を実行**:

- 新規作成: `wiki/concepts/deep-analysis-jp スキル.md`（`aliases: [deep-analysis-jp, 深層分析スキル, Deep Analysis JP]`）
- 効果: `[[deep-analysis-jp]]` が alias 経由で resolve → 既存10件 + 今回3件 = 計13件の dead link を一括解消
- スキルの中核+補助レンズ・実行フロー・3系統トラップを要約し、関連 concept へクロスリンク
