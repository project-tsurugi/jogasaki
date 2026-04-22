# ALTER TABLE / ALTER INDEX RENAME の設計

2026-04-14 kurosawa

## 本文書について

本文書は `ALTER TABLE RENAME` および `ALTER INDEX RENAME` によるテーブル名・インデックス名の変更機能の設計を記述する。

前提として、`docs/internal/index-surrogate-id.md` に記述されたインデックスのサロゲートID導入（リリース1.8）は実装済みであり、本文書はその設計を踏まえて記述する。

## 背景

リリース1.8以降で新規作成されたテーブル・インデックスは、インデックス名とは独立したストレージキー（`storage_key` フィールド）を持つ。これにより、インデックス名を変更してもストレージキーが変わらないため、ALTER RENAMEの安全な実装が可能になった。

一方、リリース1.8より前に作成されたテーブル・インデックス（pre-1.8 テーブル）は `storage_key` フィールドを持たず、インデックス名がそのままストレージキーとして使われている。これらに対してもALTER RENAMEを実施できるよう、`storage_key` フィールドを追加する処理が必要になる。

## 実装方針

### 名前の変更

`ALTER TABLE RENAME`/`ALTER INDEX RENAME` の実行時には、下記フィールドのみを変更する。

- `IndexDefinition.name.element_name`（インデックス名 / プライマリインデックスの場合はテーブル名でもある）
- `TableDefinition.name.element_name`（プライマリインデックスに内包されるテーブル定義の名前）

`storage_key` フィールドは変更しない。これにより、名前変更後もストレージキーが保持され、sharksfin/shirakami 上のストレージとの対応が維持される。

### pre-1.8 テーブルへの対応

`storage_key` フィールドが存在しないインデックス（pre-1.8 テーブル・インデックス）に対して ALTER RENAME が実行された場合は下記の処理を行う。

1. `storage_key` フィールドを `IndexDefinition` に追加する
2. 変更前のインデックス名（`element_name`）をそのフィールドに書き込む

これにより、名前変更後もストレージキーが旧名前に固定され、sharksfin/shirakami 上のストレージとの対応が維持される。

### 名前ベースの依存関係の更新

`storage.proto` の `IndexDefinition` では、セカンダリインデックスからその所属テーブルへの参照が `StorageReference.name`（`StorageName` 型）という名前ベースの参照として記録されている。

テーブル名が変更された場合、そのテーブルに属するすべてのセカンダリインデックスの `table_reference.name` を新しいテーブル名に更新する必要がある。

この処理においてストレージキーは関与せず、`storage_key` フィールドはそのまま維持される。

### 更新対象フィールドの一覧

| フィールド | ALTER TABLE RENAME | ALTER INDEX RENAME |
|---|---|---|
| プライマリインデックスの `IndexDefinition.name.element_name` | 更新 | - |
| プライマリインデックスに内包される `TableDefinition.name.element_name` | 更新 | - |
| セカンダリインデックスの `IndexDefinition.name.element_name` | - | 更新 |
| セカンダリインデックスの `table_reference.name.element_name` | 更新（全セカンダリインデックス） | - |
| `storage_key` フィールド（存在する場合） | 変更しない | 変更しない |
| `storage_key` フィールド（存在しない場合） | 旧名前を設定して追加 | 旧名前を設定して追加 |

### インメモリ状態の更新

- yugawara の `basic_configurable_provider` が保持するインメモリのテーブル・インデックス定義についても、永続化メタデータへの書き込みと合わせて更新する。
  - SQLコンパイラをスロットリングして、ALTER 中に `basic_configurable_provider` が更新されないようにする必要があるかもしれない
- `storage_manager` が保持するインデックス名からエントリIDへのマップについても、インデックス名の変更に伴い対応するエントリのキーを更新する。ストレージキー自体はエントリ内の `storage_control` が保持しており、変更しないが、`storage_control` の `name_`フィールドはインデックス名を保持しているため更新する。

## テーブル閉塞

- ALTER RENAME によりテーブルやセカンダリインデックスの名前が変更される際は、テーブルとそのテーブルに対するすべてのセカンダリインデックスも閉塞する。
- 厳密にはインデックス名変更の場合は、当該インデックスのみの閉塞で可能だが、実装の簡素化のためまとめて閉塞する方式とする
  - DMLリクエストが使用するテーブルは管理しているが、セカンダリインデックスは管理していないため

## 初期見積もり

5d

## その他・注意点

- ALTER 実行によってスキーマ等が変更になるので、変更対象のインデックス・テーブルに関連する既存のprepared statementをinvalidateする必要がある
  - 方式としてはストレージにバージョン番号を付与してALTER実行によってインクリメントする。prepared statement 作成時のバージョンと実行時のバージョンを比較して不一致の場合はエラーにする。

