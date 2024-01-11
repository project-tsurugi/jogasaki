# jogasaki診断情報の出力

## 現状の制約

実行中のタスクやタスクスケジューラーを停止させずに診断情報を収集しているため、厳密にある一時刻の情報を出力しているわけではない

## 現状出力している情報

jogasakiタスクスケジューラが各種キューに保持しているタスクの数とその内容

- jogasaki診断情報開始マーカー `/:jogasaki print diagnostics start`
- jogasaki診断情報終了マーカー `/:jogasaki print diagnostics end`
- job_count: 実行中のジョブ数
  - jobs.job_id: ジョブの識別子
  - jobs.job_kind: ジョブの種別
  - jobs.job_status: ジョブの状態
  - jobs.sql_text: SQLを実行するジョブの場合はその内容
  - jobs.transaction_id: ジョブがトランザクションを使用している場合はそのID
  - jobs.channel_status: ジョブが出力用チャネルを使用している場合はその状態 (未実装)
  - jobs.channel_name: ジョブが出力用チャネルを使用している場合はその名前 (未実装)
  - jobs.task_count: ジョブの配下で稼働中のタスク数

- worker_count: jogasakiタスクスケジューラーが稼働させているワーカー数(構成パラメーターsql thread_pool_sizeで変更可能)
- workers.worker_index: ワーカーごとのインデックス(0-base)
- workers.queues: ワーカー配下にある各種キュー
  - workers.queues.sticky: スティッキーキュー(transaction操作を伴い、特定のワーカーのみから実行される必要があるタスクが保持される)
  - workers.queues.local: ローカルキュー(上記以外の通常のタスクが保存される)

- 各キューに対するプロパティ
  - task_count: キューに存在するタスクの個数
  - tasks: キューに存在するタスクのリスト
    - id: タスクの識別子(TBD)
    - kind: タスクの種別(開発者向けデバッグ用)
    - sticky: タスクがスティッキー(transaction操作を伴い、特定のワーカーのみから実行される必要がある)か
    - job_id: タスクが所属するジョブの識別子(TBD)

- conditional_worker.thread.physical_id: conditional workerが使用するスレッドの識別子
- conditional_worker.queue: conditional taskのキュー情報
- durable_wait_count: durabile完了を待っているトランザクションの数
- durable_waits: durable完了を待っているトランザクションの情報
  - durable_waits.transaction: トランザクションの識別子
  - durable_waits.job_id: トランザクションが使用されているジョブの識別子
  - durable_waits.marker: トランザクションがdurable完了となる予定のdurability markerの値

出力例
```
/:jogasaki print diagnostics start
job_count: 2
jobs:
  - job_id: 000000000010e0e5
    job_kind: execute_statement
    job_status: finishing
    sql_text: insert into sbtest1(id, k, c, pad) values(:id, :k, :c, :pad)
    transaction_id: TID-0000001c00000086
    channel_status: undefined
    channel_name:
    task_count: 0
  - job_id: 000000000010e0e4
    job_kind: execute_statement
    job_status: finishing
    sql_text: insert into sbtest1(id, k, c, pad) values(:id, :k, :c, :pad)
    transaction_id: TID-0000001000000088
    channel_status: undefined
    channel_name:
    task_count: 0
worker_count: 2
workers:
  - worker_index: 0
    queues:
      local:
        task_count: 0
      sticky:
        task_count: 1
        tasks:
        - id: 0000000103f54fb7
          kind: write
          sticky: true
          job_id: 00000000000001ca
  - worker_index: 1
    queues:
      local:
        task_count: 0
      sticky:
        task_count: 0
conditional_worker:
  thread:
      physical_id: 7f337c7e8640
  queue:
        task_count: 0
durable_wait_count: 2
durable_waits:
  - transaction id: TID-0000001000000088
    job_id: 000000000010e977
    marker: 43641
  - transaction id: TID-0000004a00000082
    job_id: 000000000010e88e
    marker: 43641       
/:jogasaki print diagnostics end
```


## これから出力するかもしれない情報

SQLスコアボード
実行中のタスクに関する情報
タスクの内容に関するより詳細な情報(task kindだけでなく、SQL操作のどの部分を担うタスクなのかという情報)
