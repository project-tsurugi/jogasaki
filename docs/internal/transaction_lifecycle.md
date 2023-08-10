# transaction lifecycle

2023-08-11 kurosawa

## 本文書について

`jogasaki::transaction_context` のownershipについて内部設計を記述する

## jogasaki API におけるトランザクション

- jogasakiはトランザクションの実体を `transaction_context` として保持するが、これは外部へ暴露せず、トランザクションハンドル `transaction_handle` によってのみアクセスできる
- jogasakiはbegin要求によってトランザクションを開始し、トランザクションハンドル `transaction_handle` を戻す
  - このハンドルは `transaction_context` のアドレス
  - `transaction_context` のライフサイクルを管理するため、並列ハッシュマップにハンドルをキーにして `std::shared_ptr<transaction_context>` を格納する

- トランザクションを使用するAPIはトランザクションハンドルを渡す
  - このハンドルが妥当なものかをチェックするために、並列ハッシュマップにアクセスして `std::shared_ptr<transaction_context>` を取得し、それ経由で使用する
  - これによって別スレッドからトランザクションハンドルのdispose要求があっても、使う側が`shared_ptr`で持っているので `transaction_context` オブジェクトがdeleteされてしまうことが避けられる
  - ハンドルが妥当でない(すでに解放済み)の場合は `TransactionNotFound` エラーを戻す

- commit/rollback要求はトランザクションの生存期間に影響しない、これらの処理完了後もtransaction_contextオブジェクトは維持されていて、エラー情報の取得等が可能
  - 例外として、commit要求の`auto_dispose`オプションがが `true` の場合はcommit処理成功後に、下記dispose transactionの処理が実行される。
- jogasakiはdispose transaction要求(または `auto_dispose=true` なcommit要求の成功)によって、トランザクションの破棄を予約する
  - この要求のタイミングで破棄されるとは限らない
  - 並列ハッシュマップから `std::shared_ptr<transaction_context>` を含むエントリが削除されるので、実際にdeleteされるタイミングは外部で保持される `std::shared_ptr<transaction_context>` によって決まる
- `std::shared_ptr<transaction_context>`が破棄されたあとはトランザクションハンドルは無効な状態となる
  - 再度使われた場合は`TransactionNotFound`エラー

  ## 内部実装メモ

  - `executor.h` の関数は `std::shared_ptr<transaction_context>`を受け取る
    - つまり呼出側があらかじめ並列マップ等から `std::shared_ptr` を取得しておくことを要請する
    - 関数内部では `request_context` 作成時に `request_context` ともそのownershipを共有する
    - jobの完了まで `request_context`は生存するので、job実行中にdispose transactionされても `transaction_context` がdeleteされることはない
  - ハンドルの有効性をチェックする必要があるため `transaction_handle` には `transaction_context` と `database` のアドレスを持たせている
    - `transaction_handle` 経由でのアクセスはコストがあるため、内部の処理では `transaction_context` を直接使用すべき
  - 性能面から、`transaction_handle` にハンドルの妥当性チェックを行わない関数 (`_uncheck`接尾辞をもつもの)がある。これを使用する場合は、参照先の`transaction_context`が未開放である確認は呼び出し側の責任。

