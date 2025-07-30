# ストレージメタデータ履歴  

## 本文書について

jogasakiのストレージメタデータ (`src/jogasaki/proto/metadata/storage.proto`) の変更履歴を記録する

## ストレージメタデータバージョン

- ストレージメタデータバージョンはテーブル・インデックスに関する永続化メタデータのフォーマットバージョンである
- `storage.proto` の `Storage::message_version` フィールドに格納する
- 原則として `storage.proto` に更新があった際にこのバージョン番号を 1 増加させる

## バージョン履歴

バージョンを追加する際は `src/jogasaki/constants.h` の `metadata_format_version` を更新する。
下表に `metadata_format_version` の値、コミット作成日、その変更が含まれるtsurugidbのリリース、コミットを示す。

| バージョン | 日付       | tsurugidbリリース | コミット | 備考 |
|:-------------------------|:-----------|:------------------|:--------|:------|
|  1  | 2022-10-13 | - | [a08463967874cfdb7088b8c839bf85e46d633837](https://github.com/project-tsurugi/jogasaki/commit/a08463967874cfdb7088b8c839bf85e46d633837) | deprecated - 使用するDBは存在しない |
|  2  | 2022-10-25 | - | [97c289d0f45c9d09180bdb8a2c355161653ff13c](https://github.com/project-tsurugi/jogasaki/commit/97c289d0f45c9d09180bdb8a2c355161653ff13c) | 同上 |
|  3  | 2022-12-18 | - | [db58cc5cffc12ebae5b3138e2e9a8ee414028eec](https://github.com/project-tsurugi/jogasaki/commit/db58cc5cffc12ebae5b3138e2e9a8ee414028eec) | 同上  |
|  10 | 2023-10-03 | 1.0.0-BETA1 | [0f7514b77328a69363a7f68557c0260dd6effe1c](https://github.com/project-tsurugi/jogasaki/commit/0f7514b77328a69363a7f68557c0260dd6effe1c) | GA (1.0.0) もこのバージョンを使用 |
|  11 | 2025-04-29 | 1.4.0 | [7b2dd1a5498f2cec3be068b29268e6c0fd3deccb](https://github.com/project-tsurugi/jogasaki/commit/7b2dd1a5498f2cec3be068b29268e6c0fd3deccb) | `description` の追加など |
|  12 | 2025-07-30 | 1.6.0 | [bc8d04c1d035af1d52e96ea84f574bf533c18c82](https://github.com/project-tsurugi/jogasaki/commit/bc8d04c1d035af1d52e96ea84f574bf533c18c82) | テーブルへ認証情報の追加など |


