# コミット時の待ち合わせについて

## この文書について

* コミット時の待ち合わせの種類について定義する

## 待ち合わせの種類

* コミット時の待ち合わせは `Commit.notification_type : CommitStatus` メッセージによって規定する
* クライアントからコミットのリクエストがあった場合、コミット時の待ち合わせの種類に応じて、コミット完了の通知を遅延させる
  * コミット状態が `Commit.notification_type` に指定された状態に達しのちに、クライアントへレスポンスを返す
* `CommitStatus` には以下の種類がある
  * `CommitStatus.COMMIT_STATUS_UNSPECIFIED`
    * 既定の待ち合わせを行う (既定値)
  * `CommitStatus.ACCEPTED`
    * システムエラー以外でコミットが失敗しないことが保証された状態
      * pre-commit 完了後
  * `CommitStatus.AVAILABLE`
    * コミット内容がほかのトランザクションから利用可能になった状態
      * group-commit 完了後
  * `CommitStatus.STORED`
    * コミット内容がローカルディスクに記録された状態
      * WAL 書き込み後
  * `CommitStatus.PROPAGATED`
    * コミット内容が適切なノードに複製され、消失の危険がほぼなくなった状態
      * 過半数のノードにログを保存したのち
      * 単一ノード構成の場合、 `STORED` と同等
* 備考
  * TBD
    * `ACKNOWLEDGED`
      * コミットの成否はともかく、受付が完了した状態
    * `SHIPPED`
      * コミットの保存はともかく、別ノードに送付した状態

## 既定値に関する定義

* `CommitStatus.COMMIT_STATUS_UNSPECIFIED` が指定された場合、ほかのいずれかの `CommitStatus` に読み替えて待ち合わせを行う
* 読み替える先は以下の方法で定義できる (優先度が高い順)
  * トランザクション内の設定
    * `SET LOCAL TRANSACTION_COMMIT_NOTIFICATION TO XXX`
    * 未指定の場合はセッションの設定を利用
  * セッション内の設定
    * `SET TRANSACTION_COMMIT_NOTIFICATION TO XXX`
    * 未指定の場合はデータベースの設定を利用
  * データベース内の設定
    * `transaction` セクションの `commit_notification` 属性
    * 既定値は `STORED`
