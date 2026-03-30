# Python と C# の比較メモ

## 比較対象

比較対象は「JV-Link raw data downloader」としての v1 です。

対象機能:

- RACE の `setup`
- RACE の `update`
- `verify`
- `refresh-view`
- `doctor`

評価優先度:

1. 保守性
2. 配布容易性
3. 性能

## Python 現状

強み:

- すでに downloader の実動コードがある
- state / staging / publish のロジックが揃っている
- fake session を使った挙動検証がしやすい
- `pip` 配布と相性がよい

弱み:

- Windows COM と 32bit Python への依存が分かりにくい
- 配布物としては Python 環境準備が必要
- package 化は始まったが、実装本体はまだ root module に残る

## C# PoC の位置づけ

`csharp-poc/` には .NET x86 向けの比較用 PoC を置きます。

PoC の目的:

- x86 Windows CLI としての構成の素直さを見る
- COM まわりの API 境界を比較する
- storage/verify/view の実装負荷を比較する

PoC は比較用であり、本番採用決定そのものではありません。

## 比較結果

### アーキテクチャ

- Python
  - 既存実装があり、機能網羅度が高い
  - package 化は可能
  - ただし Windows/COM 依存を隠しきれない
- C#
  - Windows COM CLI としては自然
  - x86 指定や registry/COM 診断の実装は素直
  - ただし acquisition parity までの書き直しコストが高い

### 依存

- Python
  - 32bit Python
  - pywin32
  - JV-Link COM
- C#
  - .NET x86 runtime
  - JV-Link COM

### 実装量

- Python
  - すでに主要機能あり
  - 整理コストは中程度
- C#
  - runner/storage/session を一から揃える必要がある
  - parity までのコストは大きい

### テスト容易性

- Python
  - fake session を差し込みやすい
  - filesystem テストも軽い
- C#
  - xUnit/NUnit で整備すれば強い
  - ただし現時点では test harness 未整備

### 配布容易性

- Python
  - `pip` 配布はやりやすい
  - 利用者に 32bit Python を要求する
- C#
  - Windows CLI 配布に向く
  - `pip` 前提とは相性が弱い

### 実運用リスク

- Python
  - 既存コードを整理する方がリスクは低い
- C#
  - 移行中は二重実装リスクが高い

## 推奨

現時点の推奨は **Python を v1 の正式実装として維持する** です。

理由:

- すでに raw downloader として重要な挙動が揃っている
- 保守性の主要課題は「言語」より「責務整理」と「配布/診断不足」にある
- `pip` 配布を第一候補にするなら Python の継続が自然
- C# は将来「Windows 単体配布の installer」を強く求めるときの有力候補

つまり、現時点では

- Python: 正式実装
- C#: 比較用 PoC

という位置づけが最も現実的です。

## 採用後の方針

Python 側で進める作業:

- package 化
- `doctor` の標準化
- user config への移行
- README と導入手順の整備
- 実 COM acceptance 手順の明文化

C# 側で残す作業:

- x86 CLI skeleton の維持
- COM/session/archive の比較対象として保存
- standalone Windows 配布を本気で検討するときに再評価

