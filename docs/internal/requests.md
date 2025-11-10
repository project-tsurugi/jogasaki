# jogasakiのリクエスト

## 本文書について

jogasaki APIおよび内部で使われる「リクエスト」という用語について整理する

## 広義のリクエスト

`request.proto` で定義されているメッセージ `jogasaki.proto.sql.request.Request` で `tateyama::api::server::request` 経由で受け取るものを広義のリクエストと呼ぶ。jogasakiに対するAPI経由の要求を指す。

## 狭義のリクエスト

- jogasaki内部で `request_context` を使って処理されるものを狭義のリクエストと呼ぶ
  - SQLクエリやステートメントの実行要求
  - トランザクションの開始/終了要求
  - その他内部で `request_context` を使って処理される要求

## 広義のリクエストと狭義のリクエストの差分

狭義のリクエストは広義のリクエストのサブセットである。
下記は request_context を使わないため、狭義のリクエストには含まれない

  - prepare
  - explain
  - describe table
  - list tables
  - dispose (transaction/statement)
  - その他 jogasakiに対する情報要求 (GetErrorInfo, ExtractStatementInfo など)

## 狭義のリクエストの分類

- 狭義のリクエスト処理にはバリエーションがあるので、 `request_context` に要素を足したり、そのプロパティによって分岐を行う場合にはリクエストの分類に注意すべき
  - (狭義の)リクエストは下記の3つに分類される
    - SQLステートメントの実行要求
    - ダンプ・ロードの実行要求
    - それ以外。begin/commit/durable_callback処理などが該当する
  - ステートメントはクエリかそれ以外のSQL(DDLやクエリ以外のDML e.g. INSERT/UPDATE/DELETE)である
  - あるステートメントがクエリか、クエリでないかは emitの有無と等価。
    - これはchannelの有無では判定できない点に注意する
      - 例えばINSERT文は結果セットを返さないが、null_record_channelが作成されるためchannelは存在する
      - また結果セットを返すクエリが結果セットを要求しないAPIで実行された場合にもnull_record_channelが作成される

