# トランザクションの排他的制御について

## この文書について

* トランザクション処理をほかのトランザクション処理が稼動していない状態で実行することで、abort しないように動作させるための手順を共有する

## 排他的制御の指定について

トランザクションを開始する際に、オプション (`TransactionOption.priority`) に以下のいずれかの値を指定する。

* `TransactionPriority.TRANSACTION_PRIORITY_UNSPECIFIED` (既定)
  * トランザクションの抑制を行わない
* `TransactionPriority.INTERRUPT`
  * 新規のトランザクションの開始を抑制する
  * 現在稼働しているトランザクションを即座に停止 (abort) し、停止後にこのトランザクションを開始する
  * このトランザクション開始後は、新規のトランザクションを開始できる
* `TransactionPriority.WAIT`
  * 新規のトランザクションの開始を抑制する
  * 現在稼働しているすべてのトランザクションが終了後に、このトランザクションを開始する
  * このトランザクション開始後は、新規のトランザクションを開始できる
* `TransactionPriority.INTERRUPT_EXCLUDE`
  * `TransactionPriority.INTERRUPT` に加え、このトランザクション終了後まで新規のトランザクションの開始を抑制する
* `TransactionPriority.WAIT_EXCLUDE`
  * `TransactionPriority.WAIT` に加え、このトランザクション終了後まで新規のトランザクションの開始を抑制する

## 閉塞時の動作について

* トランザクションの閉塞 (トランザクションの開始を抑制する) 状態において、新しいトランザクションを開始しようとした場合、トランザクションの開始に失敗する
  * `TransactionOption.priority` にいかなる値を指定しても、閉塞時に新しいトランザクションを開始できない
  * 開始に失敗する場合、原因となったトランザクションの情報を表示する
    * この際、原因となったトランザクションのラベル (`TransactionOption.label`) 等を表示することが望ましい
* トランザクションの閉塞には、ある種の特権を要求してもよい