# セクター回転検知ダッシュボード — 設計書

- 日付: 2026-07-26
- ステータス: 承認待ち
- 元ネタ: [KabuTrend /trend](https://kabutrend.com/trend) の機能を、自前データで再現する

---

## 1. 目的と背景

KabuTrend は日本株を約275の手作りテーマにグループ化し、テーマ単位で「資金の回転」を
日次観測するダッシュボード（月2,980円）。裏の事前計算済み JSON を解析した結果、その
計算ロジック自体は本プロジェクトの `data/prices.parquet`（全銘柄日次 OHLCV + 売買代金 Va）
と `data/stock_cache.parquet`（計算済みモメンタム・SEPA 指標）で再現可能と判明した。

KabuTrend の唯一の堀は「細かいテーマ・マスタ」だが、本ダッシュボードは **J-Quants の
S33 業種分類（34グループ）を軸**に開始する。回転検知の計算式は軸に依存しないため、
将来テーマ表を差し替えれば細かい軸へ移行できる設計にしておく。

### スコープの線引き（調査で確定）

| 段階 | 内容 | データ源 | 今回 |
|---|---|---|---|
| 段階1 | セクター回転・ランキング・銘柄需給シグナル | prices.parquet + stock_cache | **含む** |
| 段階2 | 信用倍率・信用残増減 | JPX 公式「銘柄別信用取引週末残高」(無料DL) | **含む** |
| 段階3 | リアルタイム板・VWAP・分足 | kabuステーションAPI（50銘柄上限・要口座） | **対象外**（別プロジェクト） |

- 段階3 は kabuステーションAPI が **同時50銘柄上限**のため市場横断スキャンに使えず、
  KabuTrend 型の市場横断ダッシュボードとは用途が異なる。将来「保有・監視50銘柄の
  リアルタイム版」として別途検討する。
- ユーザの J-Quants プランは Free/Light（信用データ非開放）のため、段階2 の信用残は
  J-Quants ではなく **JPX 公式の無料ダウンロード**（口座不要）から取得する。

---

## 2. アーキテクチャ

計算ロジックを純粋関数として `services/rotation_service.py` に隔離し、Streamlit ページ
（`pages/9_sector_rotation.py`）は薄い UI 層 + `@st.cache_data` ラッパーに徹する。
KabuTrend が事前計算 JSON で提供する内容を、本アプリは「純粋関数 + セッションキャッシュ」
で置き換える。都度計算でも実測 1.5 秒（初回ロード込み、90営業日 × 34業種）で実用範囲。

```
pages/9_sector_rotation.py   … UI 層（Streamlit + Plotly、@st.cache_data）
        │  呼び出し
        ▼
services/rotation_service.py … 純粋計算（隔離・テスト可能）
        │  直読み
        ▼
data/prices.parquet          … 全銘柄日次 OHLCV + Va（既存・Streamlitがメンテ）
data/stock_cache.parquet     … 銘柄別モメンタム/SEPA/sector（既存）
data/margin.parquet          … 段階2: JPX信用残（新規・下記パイプラインで生成）
```

### モジュール境界

- `rotation_service.py` は Streamlit に依存しない純粋関数のみ（`import streamlit` しない）。
  入力は DataFrame、出力は DataFrame/dict。これにより単体テストとバッチ化の両方が可能。
- ページ側が `@st.cache_data` でラップし、キャッシュ制御と UI 描画のみ担当。

---

## 3. データモデル

### 3.1 業種軸

`stock_cache.parquet` の `sector` 列（S33 業種名、34グループ）を銘柄→業種の対応表とする。
構成銘柄が 3 未満の業種は集計から除外（統計的に無意味なため）。`prices.parquet` の
`Code`（5桁）を `stock_cache.code` で業種に写像。写像できないコード（ETF・上場廃止等）は
`unknown` として除外。

### 3.2 日次セクター系列（prices.parquet 直読み、直近90営業日）

- **セクターリターン** = 構成銘柄の日次リターンの等加重平均。
  分割スケール破綻を避けるため `services/split_adjust.normalize_close` で末尾日スケールに
  正規化した close 系列から pct_change を取る（CLAUDE.md の分割対応ルール遵守）。
- **セクター売買代金** = 構成銘柄 `Va` の合計。

---

## 4. 収録機能

### A. 市況サマリー（画面上部）

- **テーマ温度ゲージ**（0-100）: 上昇業種比率と中央 breadth を合成
- KPI 行: 上昇業種比率(%)・出来高急増業種数・鮮度カウント（上昇中/勝ち続け/失速）
- **データ基準日**を明示。`prices.parquet` の最終日が古ければ Streamlit「データ更新」を促す

### B. セクター回転（4指標）

| 指標 | 計算式 | KabuTrend 対応 |
|---|---|---|
| 出来高急増 | 当日セクター売買代金 ÷ 直近20日中央値 = turnoverRatio。降順ランク | `turnoverRatio` |
| 鮮度 | 週リターン(5日)ランク・月リターン(20日)ランクを算出。`rankDelta = weekRank − monthRank`（負=最近急浮上）。**上昇中**(週良・月悪) / **勝ち続け**(両方良) / **失速**(週悪・月良) の3分類 | `freshness` |
| 資金流入 | 5成分の合成スコア（下記 4.B.1） | `fundFlow` |
| テーマ温度 | 上昇業種比率 + 中央 breadth を 0-100 に合成 | `themeTemperature` |

閾値（鮮度の分類境界など）は `rotation_service.py` 冒頭に定数として外出しし、後から
調整・検証できるようにする。初期値は KabuTrend の JSON から観測した値
（`winThreshold≈69`, `fallMonth≈40` 等）を参考にする。

#### 4.B.1 資金流入スコア（5成分の合成）

KabuTrend の fundFlow 5成分を、板情報のない日次データで近似する:

1. **売買代金ペース** (turnoverPace): 当日売買代金 ÷ 直近平均、0-100 正規化
2. **持続日数** (persistence): 平均を連続で上回った日数
3. **直近リターン** (flowReturn): 直近リターンのスコア化
4. **上昇銘柄比率** (breadth): 構成銘柄のうち上昇した割合
5. **上昇売買代金シェア** (flowDominance の近似): 上昇銘柄の売買代金 ÷ 業種売買代金

> **割り切り（承認済み）**: KabuTrend の flowDominance は板/約定方向を使うと推測されるが、
> 本アプリに板情報がないため「上昇銘柄の売買代金シェア」で代替する。

### C. ランキング（タブ切替、KabuTrend の /rankings 相当）

- タブ: 値上がり / 値下がり / 出来高急増 / 資金フロー / モメンタム
- **各銘柄にテーマ内順位（selfRank / 業種内N銘柄中X位）を付与** — 「使いやすいランク付け」の核

### D. 銘柄レベル需給シグナル（段階1の範囲）

- 5MA/25MA/75日線乖離率、52週新高値/安値、ゴールデン/デッドクロス(5/25・25/75)、
  出来高前日比・出来高倍率
- 大半は `stock_cache` の `mom_*` / `sepa_*` を再利用。不足分のみ prices.parquet から算出

### E. ドリルダウン

- 業種を選択 → 構成銘柄を需給シグナル + テーマ内順位付きで一覧 → `pages/2_stock_detail.py`
  へリンク。KabuTrend の「テーマボーナス」（個別モメンタム + 所属業種が強ければ加点）を再現。

---

## 5. 段階2: JPX 信用残パイプライン（新規）

### 5.1 データ源

JPX 公式「[銘柄別信用取引週末残高](https://www.jpx.co.jp/markets/statistics-equities/margin/05.html)」
（無料・口座不要）。週次（火曜16:30頃公表）、全信用銘柄の信用買残・売残・前週比を含む
Excel ファイル。J-Quants が再販している元データそのもの。

> **実装時に確定すべき詳細**: 正確なファイル形式（.xls/.xlsx）・ダウンロードURLパターン・
> 1ファイルの銘柄構成・過去何週分保持されるか。WebFetch では JPX が 403 を返すため、
> Claude Code シェルからの直接取得が SSL/403 で失敗する可能性がある。その場合は
> `prices.parquet` と同じく **ユーザのブラウザ/Streamlit 操作経由**で取得する運用にする。

### 5.2 パイプライン

```
JPX週次Excel  →  services 内のパーサ  →  data/margin.parquet
                                              │
rotation_service が読み込み → 信用倍率(買残/売残)・信用残増減(前週比) を算出
```

### 5.3 拡張フック

`rotation_service.py` に信用データ読み込みの関数境界を最初から用意する。margin.parquet が
存在すれば信用指標を有効化し、無ければ段階1 の指標のみ表示（グレースフルデグレード）。
これにより、JPX パイプラインの実装完了前でもダッシュボード本体は動作する。

---

## 6. UI レイアウト（Streamlit + Plotly）

装飾絵文字は使わない（機能系 ✅⚠️ のみ）。既存 `components/chart.py` が Plotly を使うため
本ページも Plotly で統一。

1. 上部: テーマ温度ゲージ + KPI 行 + データ基準日
2. 出来高急増セクション（横棒 + テーブル）
3. 鮮度セクション（上昇中 / 勝ち続け / 失速 の3カラム、順位移動を可視化）
4. 資金流入セクション（合成スコア順テーブル + 5成分の内訳）
5. 信用需給セクション（段階2。信用倍率・信用残増減ランキング。margin.parquet があれば表示）
6. ランキングセクション（タブ切替）
7. ドリルダウン: 業種選択 → 構成銘柄モメンタム/需給表 → 詳細ページへ

---

## 7. ファイル構成

- `services/rotation_service.py` — 純粋計算（隔離・テスト可能・信用フック内蔵）
- `pages/9_sector_rotation.py` — UI + `@st.cache_data`
- `tests/test_rotation_service.py` — 合成データによる単体テスト
- （段階2）JPX 信用残の取得・パースは `services/` 内に追加（実装時にモジュール名確定）
- （段階2）`data/margin.parquet` — 生成物

---

## 8. テスト方針

`tests/test_rotation_service.py` で合成 DataFrame を入力し、各計算関数を検証:

- セクターリターンの等加重平均が正しいか（分割日を跨いでもスケール破綻しないか）
- turnoverRatio が「当日 ÷ 中央値」で正しく出るか
- 鮮度の3分類が weekRank/monthRank の境界で正しく振り分くか
- 資金流入5成分の合成が 0-100 に収まるか
- 構成銘柄3未満の業種が除外されるか
- margin.parquet 不在時に信用指標がスキップされ段階1が動くか（グレースフルデグレード）

---

## 9. 非目標（YAGNI）

- リアルタイム板・VWAP・分足（段階3・kabuステーション）
- 細かいテーマ・マスタの構築（S33 業種で開始、将来差し替え可能に留める）
- 事前計算バッチ化（都度計算で実用速度のため。将来重ければ同じ純粋関数をバッチから呼ぶ）
- 米国株（KabuTrend の /us/trend 相当）
