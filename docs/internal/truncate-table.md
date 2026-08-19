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

- `RESTART IDENTITY` が指定された場合、TABLE に紐づくすべてのシーケンス(暗黙定義の主キー列および generated as identity 列)の**値を初期値にリセット**する。
  - 既存のシーケンスIDとシーケンス定義IDは引き継ぐ
  - システムテーブルには変更なし 
  - プライマリインデックスのストレージメタデータの再作成時も、シーケンス定義IDは既存のものを使用する
  - 値のリセットは通常のシーケンス値更新と同様にトランザクションに関連付けて行われる
    - `sequence_manager::notify_updates()` によりコミット時に永続化される
    - `sequence::reset()` により、次回の `next()` 呼び出しが initial value を返すように in-memory の値を巻き戻す
      - そのように in-memory の値を巻き戻すことができないような初期値・increment の組み合わせのシーケンスに対しては、TRUNCATE TABLE は unsupported error となる
- `CONTINUE IDENTITY`（デフォルト）が指定された場合、シーケンスの値も含めて既存のものをそのまま引き続き使用する
  - 値を初期値に戻さない点のみ `RESTART IDENTITY` と異なる

### DMLとの排他制御、ロールバックの扱い

- TRUNCATE文はDDL扱いとし、通常のDDL/DML排他制御( tsurugi-issues #1230 )に従う
- DROP TABLE と同様、ロールバックはサポートしない。TRUNCATE TABLE で作成された新しいストレージや、削除予約された古いストレージは、TRUNCATE TABLEを実行したトランザクションがアボートしても取り消されない(ストレージの作成/削除予約自体がトランザクショナルでないため)。全行削除の効果はアボート後も残る

### 権限

TRUNCATE TABLE を実行するには、管理者権限またはテーブルのALTER権限が必要である。
現実装ではALTER権限が未実装のため、ALTER権限の代わりにCONTROL(ALL PRIVILEGES)権限があれば実行可能とする。

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
  - TRUNCATE TABLE前後でシステムに登録されているsequenceの個数・エントリ内容が変化しない
    - RESTART/CONTINUEのいずれの場合も既存のエントリ(`definition_id` → `seq_id`)がそのまま残る
- generated as identity 列があるテーブルに対して TRUNCATE TABLE を実行して全行削除を確認、その後も問題なくINSERTが可能であることを確認。また、INSERT時にされる値が、RESTART/CONTINUE による仕様通りであることを確認
- `初期値 - increment` が bigint の範囲外になるシーケンスを持つテーブルに対する `RESTART IDENTITY` 付き TRUNCATE TABLE が unsupported error になること、およびその文を実行したトランザクションがアボートされること(以降の利用が inactive transaction エラーになること)を確認

- DROP TABLE と同様に、lazy deleteや再起動後のリカバリのテストを実施する。

## 既知の制限

### TRUNCATE TABLE を実行したトランザクションがアボートした場合の挙動

`RESTART IDENTITY` によるシーケンス値のリセットは `seq_id`・`definition_id` を変更せず、システムテーブル(`__system_sequences`)へのエントリ更新を伴わない。これはシステムテーブルへのエントリの削除・再作成を行うと1トランザクションで複数回のTRUNCATE処理を実行した際にOCCエラーが発生し、それを避けるのが難しいためである。
そのため、TRUNCATE TABLE を実行したトランザクションがアボートした場合、システムテーブルのエントリが不正な状態で残るということはない。ただし、リセットされた値は当該トランザクションのコミットとともに永続化されるものであるため、アボートした場合は永続化されない。
このとき、in-memoryのシーケンスの値はリセットされたままとなるので、アボート直後のINSERT文等では初期値から値が採番されるが、その後Tsurugidbを再起動した場合はアボート前の永続値から採番が継続され、RESTART IDENTITY のリセット効果は失われる。(next()済みのシーケンスを持つトランザクションがアボートした場合と同様の扱いである。transaction_fail_ddl_test.truncate_restart_aborted に関連テストあり)

## その他

- DROP/CREATEとほぼ同じ動作になるため、TRUNCATE特有の利点は大きくないかもしれないが、テーブル/インデックス定義を維持できる点は運用前のテスト等で便利であると思われる (特にセカンダリインデックスやシーケンス列の定義がある場合)
