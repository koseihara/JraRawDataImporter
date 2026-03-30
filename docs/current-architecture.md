# 現状アーキテクチャ整理

## このアプリケーションが行うこと

このアプリケーションは、JRA-VAN JV-Link から raw ファイルを取得し、そのまま `.jvdat` として保存する downloader です。

責務は次の 4 つに限定されます。

- `download`: JV-Link から dataspec ごとに raw ファイルを取得する
- `resume`: 中断した run を再開する
- `verify`: 公開済み snapshot と object の整合性を検査する
- `publish view`: 人が見やすい `view/current/...` を生成する

このアプリは parser ではありません。
PostgreSQL 保存責務も持ちません。

後続の parser アプリは、この downloader が作る raw アーカイブを読む前提です。

## 現在のデータフロー

1. CLI が `setup` または `update` を受け付ける
2. `JobRunner` が dataspec 単位で run を開始する
3. `JvLinkSession` が `JVInit -> JVSetSavePath -> JVOpen -> JVStatus -> JVRead` を行う
4. `RawFileWriter` が取得中データを `runs/<run_id>/staging` に書く
5. `JobState` が `last_completed_filename` などの再開情報を保持する
6. `DataspecArchive` が staging を object 化し、`commits/<commit_id>` を作る
7. `refs/current.json` を更新して公開スナップショットを切り替える
8. `view/current/<format_code>/...` を再生成する
9. `verify` は `refs / commits / objects / runs` を検査する

## 現在の公開面

後続アプリが依存してよい公開面は、現在は次の 3 つです。

- `view/current/<format_code>/<logical_filename>.jvdat`
- `refs/current.json`
- `commits/<commit_id>/manifest.jsonl`

`objects/` は内部実装です。

## モジュール責務

- `main.py`
  - package CLI への shim
- `jvlink_raw_fetcher/cli.py`
  - CLI、設定解決、ログ設定、コマンド分岐
- `jvlink_raw_fetcher/app_config.py`
  - user config の保存先と優先順位
- `jvlink_raw_fetcher/platform.py`
  - 32bit 判定、Windows 判定、`doctor`
- `job_runner.py`
  - dataspec 実行オーケストレーション
- `jvlink_session.py`
  - JV-Link COM 境界
- `raw_writer.py`
  - staging への raw ファイル書き込み
- `job_state.py`
  - run 再開用状態
- `archive_store.py`
  - snapshot/objects/view/verify の中核
- `config.py`
  - dataspec、JVOpen option、encoding などの定数

## 技術的負債

### 1. core/storage/platform の実装がまだ root module に残っている

package CLI は追加したが、`job_runner.py` や `archive_store.py` はまだ root module です。
今後は package 内へ寄せる余地があります。

### 2. C# PoC は比較用であり、本番 parity には達していない

現在の C# PoC は platform/storage の比較を主眼にしており、完全な acquisition parity はまだありません。

### 3. 公開契約は事実上存在するが、schema version をまだ持っていない

`current.json` と `manifest.jsonl` はすでに事実上の handoff 契約ですが、version field はまだありません。

### 4. 実 COM を使う自動テストは未整備

fake session での Python テストは可能ですが、JV-Link 実環境 acceptance は手動確認が前提です。

## 目標形

この repo の目標は「別環境の Windows/x86/JV-Link 環境に入れて raw ファイルを取得できる OSS downloader」です。

そのための v1 製品境界は次で固定します。

- raw 取得
- 整合性検査
- 再開
- 人間向け view 公開

次の責務は持ちません。

- parser
- schema 正規化
- PostgreSQL への投入

