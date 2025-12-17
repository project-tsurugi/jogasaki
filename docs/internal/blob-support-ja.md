# BLOB 内部設計メモ

2025-03-18 kurosawa
2025-12-14 kurosawa UDFサポートに伴う更新

## 本文書について

BLOBの実装時に追加された内部設計のうち、通常の型の追加と異なる点や、特に備忘として残したいものをまとめる

## 前提

TsurugiにおけるBLOBデータの扱いに関する下記のデザイン文書を前提知識とする

[IPC 接続における BLOB データ授受に関するデザイン](https://github.com/project-tsurugi/tateyama/blob/master/docs/internal/blob-ipc-design_ja.md)

[BLOB データの永続化に関するデザイン](https://github.com/project-tsurugi/tateyama/blob/master/docs/internal/blob-store-design_ja.md)

UDFサポートにともなうBLOBの取り扱いの追加については下記 issue の設計文書を参照のこと

[UDF の BLOB 対応](https://github.com/project-tsurugi/tsurugi-issues/issues/1341)

[BLOB中継サービス](https://github.com/project-tsurugi/tsurugi-issues/issues/1335)

## 用語

- LOB (BLOB)

  BLOB/CLOBといった大容量データ型の総称。BLOBという用語でこれを指すこともあり、その場合はCLOBもBLOBの一種と考える。

## BLOB 参照とロケータ

meta::field_type の既存の型と異なり、LOB列は実際のデータをレコード上に保持しない。
また、データ処理の進捗によってLOBデータ本体が存在する位置 (データストア内ファイルかクライアントが送信中のファイルかなど) が変化する。
そこで `lob_reference` クラスがこれらの状態に関するする情報を持ち、必要に応じてパス等の可変長部分を `lob_locator` という別クラスに格納するものとした。

- `lob_reference` (実際に使われるのは `blob_reference` / `clob_reference` )
  - レコード ( `record_ref` ) 上でBLOB/CLOB列のデータを示すもの(runtime type)
  - LOBデータがデータストアに保存されているか、クライアントから送信中 ( `lob_reference_kind::provided` ) か、中継サービスのセッションに保持されているかといった情報を持つ
    - データストア保存の場合は問い合わせることでデータ本体を取得することが可能
    - `provided` の場合は下記の `lob_locator` によってデータ本体の場所が示される
      - LOBデータを識別するID (BLOB ID) はこの状態では未発行
  - データストアに保存されている場合、仮登録済み ( `lob_reference_kind::resolved` ) と永続化済みのデータを取得したか( `lob_reference_kind::fetched` ) という区別がある
    - この参照を書き込みで使用する際、仮登録済みはそのままデータストアへ送信が可能だが、`fetched` は再登録の必要があるため
    - いずれの場合も BLOB ID を保持し、データストアにデータ本体を問い合わせることが可能
  - UDFがBLOB/CLOBを戻す際はBLOB中継サービスのセッションに保管される
    - データストアではなくBLOB中継サービスが独自でIDを採番する
    - この場合 `lob_reference` のproviderは `lob_data_provider::relay_service_session`、 kindは `lob_reference_kind::resolved` となる
    - 戻り値は関数式の評価が終わるまでに中継サービスのセッションから取り出され、データストアに仮登録される。
    - そのため、この状態のBLOB参照はレコードに格納されることはなく、生存する区間はあまり長くない。
- `lob_locator` (実際に使われるのは `blob_locator` / `clob_locator` )
  - LOBのデータ本体が存在する場所を示すもの(現状は特権モードのみを実装しており、実体はファイルパス)
  - `provided` の `lob_reference` とペアで使われ、`lob_reference` が `lob_locator` への参照をもつ
  - `lob_reference` は trivially copyable である必要があるため可変長部分を別クラスに切り出したもの
  - life cycle管理のため、リクエストが完了するまでホスト変数を保持する `variable_table` にownされる
  - 「クライアント側がファイルの所有権を渡そうとしているか」を示す `is_temporary` フラグも保持する

## LOBを含む式の評価とデータストアへの(仮)登録

LOBのデータストアへの仮登録は式の評価時に行う。このような設計とした背景を下記に説明する。

### LOBを含む典型的な式

LOBを含む典型的な式とその用途により、SQL実行エンジンでどのような処理が必要になるかを下記に示す

- BLOB/CLOB型の値

    - テーブルから取り出されたBLOB/CLOB型の列の値
      - この値がクライアントへ戻される場合はそのまま戻せばよい (BLOB IDを受け取ったクライアントはそれを使って内容をリクエストできる)
      - この値がINSERT-SELECT等で再度インデックスへ書き込まれる場合はデータストアへ再登録(BLOB IDの再取得)が必要 
    - placeholder (aka ホスト変数) としてBLOB/CLOB型の値が式に含まれる場合
      - インデックスへ書き込まれる場合はクライアント送付のデータをデータストアへ仮登録する必要がある
      - クライアントへ戻す場合は、SELECT完了後にもTXが有効な間はデータを保持し、問い合わせを受けてファイル位置を戻す処理が必要
        - jogasakiで実装しようとるするとlimestoneがblob_poolで行っているようなディレクトリの確保、ファイル作成、問い合わせ受付をすることになる
    - BLOB/CLOB型の値が関数から戻される場合
      - [UDF の BLOB 対応](https://github.com/project-tsurugi/tsurugi-issues/issues/1341) の記述のとおり
        - 戻り値は一旦中継サービスのセッションに保管される
        - 式評価時に再度取得されデータストアへ仮登録される

- BLOB/CLOB型のキャスト

    - BLOBから文字列(varbinary/varchar)への変換
        - LOBの種類( `lob_reference_kind` )によって、データストアまたはクライアントからパスを取得しファイルを読み出して文字列データ( `accessor::text` や `accessor::binary` ) を作成する
    - 文字列(varbinary/varchar)からBLOBへの変換
        - LOBがインデックスへ書き込まれる場合、作成したデータをデータストアへ仮登録する必要がある
        - LOBがクライアントへ戻される場合、データストアへの仮登録は必須ではないが、上記placeholder評価の場合と同様にTXが有効な間はクライアントから送られたデータを保持し、問い合わせを受け付ける必要がある


いずれの場合もLOBがインデックスへ書き込まれる際には書き込みエントリが使用するBLOB IDをトランザクションエンジンへ通知する必要がある。


### 評価時のBLOBデータの登録

上記の各パターンにシンプルに対応するために、下記のような方針でLOBを扱うものとした

- evaluatorがLOB型のplaceholderの評価を行った際に一旦データストアの `blob_pool` にデータを登録する
- キャストでLOB型の値が内部的に生成される場合も、データストアの `blob_pool` にデータを登録する
- 関数の戻り値もセッションから速やかにとりだしてデータストアの `blob_pool` にデータを登録する
- これらの値を最終的にインデックスに書き込まない場合は永続化が不要なのでこの登録は無駄になるが
  - データが必要な際にデータストアに問い合わせてデータを取得できるのでjogasaki側のコードが共通化できる

### 当初の検討案

- 当初案ではwrite直前やemit直前にデータを登録することにしていた
  - この場合、途中で評価は行ったが最終的にインデックス書きこれなかったデータは登録されないのでデータストア側の負担が軽くなる
- しかしCAST等でSQLの処理中にBLOBデータを使うケースを含めて考えると最初にBLOBデータが式評価であらわれたタイミングで登録したほうが後続が共通の扱いができるのでこのようにした

## LOBデータの状態( `lob_reference_kind` )の遷移

`lob_reference_kind` の各操作による遷移を下記に示す

- LOBデータがパラメータとしてリクエストと一緒に送信されると `provided` となる
- そのパラメータを含む式が式評価器によって評価されるとデータストアへ仮登録され `resolved` となる
  - `resolved` はそのままwrite/emit演算子等で インデックスやクライアントへ送信される
- データストアからテーブルレコードの一部として取得されたものは `fetched` となる
  - クライアントへ送信される際はそのまま、インデックスへエンコードされる直前にデータストアへ再登録( `limestone::api::blob_pool::duplicate_data` )され `resolved` に変換される
- データストアへ最終的に書き込みエントリと関連付けて渡されるのは `resolved` のもののみ

## エラーハンドリング

原則としてBLOB 処理に関するエラーは、SQL上のエラーではなく、jogasakiが仲介したBLOB参照に関して他のコンポーネントがエラーに遭遇した状況である。
そのため、SQLのエラーコードを使用せず、`tateyama.proto.diagnostics.Code` 以下のエラーを戻す。

jogasaki内部では `error_info` に載せるために `error_code` を使用するが、クライアントへ戻す際に `tateyama.proto.diagnostics.Code` に変換する。

- `IO_ERROR`

  - datastoreまたはjogasakiがファイルを読み書きする際にI/Oエラーが発生したことを示す
  - jogasaki内部では `error_code::lob_file_io_error`

- `ILLEGAL_STATE`
  - 無効な BLOB IDを操作しようとした (すでにBLOB削除済みである場合も含む) を示す
  - jogasaki内部では `error_code::lob_reference_invalid`

- `OPERATION_DENIED`
  - 特権モードにおける操作がtateyamaによって拒否された
  - jogasaki内部では `error_code::operation_denied`

ただしキャスト等の処理で純粋にSQL上のエラーとなる場合は、SQLエラーとして返す( `BLOB` から `varbinary` への変換でデータ長が `varbinary` の最大長を超えるなど)

## テストモック

- 単体テストのためにデータストアのテストモックを追加した
  - `src/jogasaki/datastore/blob_pool_mock.h` や `src/jogasaki/datastore/datastore_mock.h` によって単体テストをデータストアに依存せずに実施可能
  - ただしフルにデータストアの機能をシミュレートするわけではなくテストのための必要最小限にとどめている
  - 特に `limestone::api::log_channel::add_entry` 部分はモック実装していない
- 本番コードではこれらは使われず、 `datastore_prod.h` 経由でlimestoneが使われる

## その他・注意事項

- 上述の通り、現状では実行エンジンがLOBデータを生成した場合であっても式の評価時に一旦データストアに登録される。そのため `lob_data_provider` は `datastore` または `relay_service_session` であり、SQL実行エンジンが直接自分でLOBデータ本体を維持し、クライアントへ提供するケースはなく `sql` は使用されない

