# TRUNCATE TABLE の設計

2026-03-10 kurosawa

## 本文書について

TRUNCATE TABLE 文の実装設計を記述する。

## 構文

```
<truncate-table>:
  TRUNCATE TABLE <table-name> [<column-identity-restart-option>]

<column-identity-restart-option>:
  RESTART IDENTITY
  CONTINUE IDENTITY
```

オプションは identity 列のリセットの有無のみである。
オプション無指定時のデフォルトは `CONTINUE IDENTITY` である。

## 実装方針

### ストレージの再作成による全件削除

TRUNCATE TABLE は対象テーブルの全行を削除する操作であるが、jogasaki では行を個別に削除するのではなく、ストレージエントリを作り直す方式で実装する。

- `TRUNCATE TABLE T1` が実行されると、jogasaki は論理的なテーブル T1 の定義を維持したまま、shirakami に新しいストレージを作成する
  - shirakami/limestone から見ると、古いストレージとは別の新しいストレージが作成されたという扱いになる
- jogasaki はテーブル T1 の内部マッピング(`storage_manager`の`storages_`, `storage_keys_`, `storage_names_`)を新しいストレージに切り替える
  - 以降の DML はすべて新しいストレージに対して操作を行う
- `T1` がセカンダリインデックスを持つ場合は、それらのセカンダリインデックスのストレージも同様に削除予約して再作成する

### 古いストレージの遅延削除

- 古いストレージは DROP TABLE と同様の遅延削除機構（`lazy-delete-storage.md` 参照）で削除される
  - TRUNCATE文実行時点で削除予約状態とし、メンテナンススレッドへ削除処理を委譲する
    - 名前は `storage_control::name_` から削除され、`storage_control::original_name_` にコピーされる 
    - 新しいストレージ用の `storage_control` を作成し、それが同じ名前を持つ
  - これによりTRUNCATE文は古いストレージの削除(`shirakami::delete_storage`)を待たずに完了をSQLクライアントへ戻す

### IDENTITY 列の処理

- `RESTART IDENTITY` が指定された場合、TABLE に紐づくすべてのシーケンス(暗黙定義の主キー列および generated as identity 列)を再作成する
  - 新しいシーケンスIDとシーケンス定義IDを割り当て、システムテーブル(`__system_sequences`)に登録する
  - 既存のシーケンスIDとシーケンス定義IDは削除する。具体的には下記のような処理になる。これらは同期的な処理であり、 TRUNCATE TABLE の完了までに終了する。
    - シーケンスIDはトランザクションエンジンが管理するもののため、トランザクションエンジンに削除を依頼する
    - シーケンス定義IDはjogasakiが管理するものであり、システムテーブルから削除する
    - ストレージメタデータからも削除する必要があるが、これはプライマリインデックスのストレージメタデータの再作成に含まれるので明示的に行う必要はない
- `CONTINUE IDENTITY`（デフォルト）が指定された場合、シーケンスは既存のものを引き続き使用する
  - 既存のシーケンスIDとシーケンス定義IDを引き継ぐ
  - システムテーブルには変更なし 
  - プライマリインデックスのストレージメタデータの再作成時に、シーケンス定義IDは既存のものを使用する

### DMLとの排他制御、ロールバックの扱い

- TRUNCATE文はDDL扱いとし、通常のDDL/DML排他制御( tsurugi-issues #1230 )に従う
- DROP TABLE と同様、ロールバックはサポートしない。TRUNCATE TABLEに使用されたトランザクションがアボートした場合、シーケンステーブルにゴミが残る可能性があるが、これもDROP TABLEと同様の制限である

## 初期見積もり

4d

## 実装詳細

- drop_table.{h, cpp} や create_table.{h, cpp} に習って truncate_table.{h, cpp} を新規作成する。
- truncate_tableはdrop_table処理とcreate_table処理を同時に行うような実装になる
- drop_tableやcreate_table処理と内容が重複する場合は共通関数として括りだして共通化する

## テスト

下記のテストシナリオを含める

- TRUNCATE TABLE によりテーブルの全行が削除される
- 暗黙定義の主キー列があるテーブルに対して TRUNCATE TABLE を実行して全行削除を確認、その後も問題なくINSERTが可能(RESTART/CONTINUE 両方試す)
  - TRUNCATE TABLE前後でシステムに登録されているsequenceの個数は変化しない
    - RESTARTの場合は既存のエントリが削除されて、新しいエントリが追加される
    - CONTINUEの場合は既存のエントリがそのまま残る
- generated as identity 列があるテーブルに対して TRUNCATE TABLE を実行して全行削除を確認、その後も問題なくINSERTが可能であることを確認。また、INSERT時にされる値が、RESTART/CONTINUE による仕様通りであることを確認

- DROP TABLE と同様に、lazy deleteや再起動後のリカバリのテストを実施する。

## その他

- DROP/CREATEとほぼ同じ動作になるため、TRUNCATE特有の利点は大きくないかもしれないが、テーブル/インデックス定義を維持できる点は運用前のテスト等で便利であると思われる (特にセカンダリインデックスやシーケンス列の定義がある場合)
