# リクエストキャンセルの内部設計

2024-02-27 kurosawa

## リクエストのキャンセルとは

- セッションのクローズ時など、処理中のリクエストをキャンセルし可能な限り早く完全な完了を行いたいことがある。
  - 詳細は[ジョブのキャンセルに関するデザイン](https://github.com/project-tsurugi/tateyama/blob/master/docs/internal/job-cancel-design_ja.md)等を参照
- 本文書はキャンセル要求に対するjogasakiの振る舞いと内部設計について記述する

## キャンセル要求に対するjogasakiのふるまい

- jogasakiは `tateyama::api::server::response::check_cancel()` を適当なタイミングで確認し、キャンセルが要求された際には処理を諦めて安全かつ速やかにレスポンスを戻すようにする
- キャンセルされたことは `tateyama::api::server::response::error()` にdiagnosticsコード( `OPERATION_CANCELED` )を送信して通知する

  - あらゆるリクエストはこのエラーを戻す可能性がある
  - これが戻った場合は「処理を実行途中であきらめた」ことを通知するのみで、リクエストがどこまで進行したかは不定となる
    - 極端なケースではリクエストが正常に完了しているが `OPERATION_CANCELED` が戻ることもある
  - リクエストが使用するトランザクションに対する副作用も不定だが、少なくとも利用不可な状態とするためにトランザクションに対してアボート要求を行う 
      - トランザクションがアクティブな状態であればアボート予定 (`going-to-abort`) 状態へ遷移しトランザクションは閉塞する
      - CCへのアボート要求は適切なタイミングで行われる
      - 必ずしもアボートさせられるわけではない。特にコミット処理が先行した場合など
      - 詳細は[トランザクション終了要求の並行処理](https://github.com/project-tsurugi/jogasaki/blob/master/docs/internal/concurrent_end_tx.md) および [SQL実行エンジン内におけるトランザクションの状態管理](tx-state.md) を参照
  - `response` オブジェクト経由で `OPERATION_CANCELED` を通知したのち、すぐに「完全に終了」の処理( `request`, `response` を保持する `shared_ptr` の破棄)を行う
    - 逆にいうと `request`, `response` を使用しているスレッドがある間は `OPERATION_CANCELED` 通知を送らない
  - 理想的には `OPERATION_CANCELED` だけを特別扱いするのではなく、他のエラー処理(SQL実行エンジンとしてのエラーも含む)も同様にする
    - `tateyama::api::server::response::body()` および `tateyama::api::server::response::error()` でエラー通知後、即座に完全に終了する

## 内部設計

- jogasakiの内部エラーコード `status::request_canceled`, `error_code::request_canceled` を追加
- これらのエラーコードを使用し、他のエラー処理と同様のパスでエラー処理を行う
- ただしクライアントへ戻す前に `jogasaki::api::impl::service` のレイヤでこのコードをdiagnosticsコード( `OPERATION_CANCELED` )に変換する

### 対象とするリクエストおよび `check_cancel()` 確認箇所

- DAGに関するもの
  - group/aggregateの同期部分でcheck_cancelを行う
  - キャンセル対象：group/aggregate演算子を含むSQL文
- プロセスに関するもの
  - scan/findが取得するエントリごとにcheck_cancelを行う
  - take演算子が取得するレコードの毎行ごとにcheck_cancelを行う
  - キャンセル対象：scan/find/take_*演算子を持つプロセスルートに持つもの
- writeに関するもの
  - writeタスクがkvsへ処理を行う前にcheck_cancelを行う
  - キャンセル対象：VALUES句を持つinsert(でサブクエリが使われていないもの)
- begin waitに関するもの
  - condition taskの条件確認時にcheck_cancel()も一緒に確認する
  - wait開始以前にエラーを戻すパスが追加になる
  - キャンセル対象： LTX/RTXに対するトランザクション開始の要求
- precommitに関するもの
  - トランザクションエンジンへのprecommit要求を行った後、コールバック前の状態でもjogasakiレベルでキャンセルを可能にする
  - トランザクションエンジンがコールバックを呼び出せる状態を維持しつつ、`request` と`response` に関連するリソースを破棄する
  - precommit結果は無視するのでキャンセル後のトランザクション状態は不定
  - キャンセル対象： precommit完了前のコミットリクエスト
- durable waitに関するもの
  - precommit後にdurable waitしているトランザクションをキャンセルする
    - durability callbackにおいてdurability_manager::check_cancelを呼び出す
    - その中でwaiting中のrequest_contextをチェックし、cancelフラグが立っていれば、teardownを行う
      - そのエントリをwaiting listから除去する。実際にdurableになったタイミングは無視され、durable callbackは実行されない。
    - durability callbackが呼び出されないとキャンセルできない
  - キャンセル対象： precommit完了後、永続化前のコミットリクエスト

### キャンセル対象外の処理

現状では下記の処理に対してはキャンセルが実装されておらず、キャンセル要求を行っても `OPERATION_CANCELED` が戻ることはない

- explain, DDL
  - リクエストスレッドで迅速に処理が完了するのでキャンセルを実装していない

