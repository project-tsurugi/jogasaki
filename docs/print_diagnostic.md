# jogasaki診断情報の出力

## 現状の制約

実行中のタスクやタスクスケジューラーを停止させずに診断情報を収集しているため、厳密にある一時刻の情報を出力しているわけではない

## 現状出力している情報

jogasakiタスクスケジューラが各種キューに保持しているタスクの数とその内容

- jogasaki診断情報開始マーカー `/:jogasaki print diagnostics start`
- jogasaki診断情報終了マーカー `/:jogasaki print diagnostics end`
- worker_count: jogasakiタスクスケジューラーが稼働させているワーカー数(構成パラメーターsql thread_pool_sizeで変更可能)
- workers.worker_index: ワーカーごとのインデックス(0-base)
- workers.queues: ワーカー配下にある各種キュー
  - workers.queues.sticky: スティッキーキュー(transaction操作を伴い、特定のワーカーのみから実行される必要があるタスクが保持される)
  - workers.queues.delayed: 遅延キュー(待ち状態や、条件付きタスクなど、頻繁な起動が必要でないタスクが保持される)
  - workers.queues.local: ローカルキュー(上記以外の通常のタスクが保存される)

- 各キューに対するプロパティ
  - task_count: キューに存在するタスクの個数
  - tasks: キューに存在するタスクのリスト
    - id: タスクの識別子(TBD)
    - kind: タスクの種別(開発者向けデバッグ用)
    - sticky: タスクがスティッキー(transaction操作を伴い、特定のワーカーのみから実行される必要がある)か
    - delayed: 遅延タスク(頻繁な起動が必要でないタスク)か
    - job_id: タスクが所属するジョブの識別子(TBD)

出力例
```
/:jogasaki print diagnostics start
worker_count: 2
workers:
  - worker_index: 0
    queues:
      local:
        task_count: 0
      sticky:
        task_count: 1
        tasks:
        - id: 18446744073709551615
          kind: write
          sticky: true
          delayed: false
          job_id: 67
      delayed:
        task_count: 0
  - worker_index: 1
    queues:
      local:
        task_count: 0
      sticky:
        task_count: 0
      delayed:
        task_count: 1
        tasks:
        - id: 20038
          kind: wrapped
          sticky: true
          delayed: true
          job_id: 54
/:jogasaki print diagnostics end
```


## これから出力するかもしれない情報

SQLスコアボード
実行中のタスクに関する情報
タスクの内容に関するより詳細な情報(task kindだけでなく、SQL操作のどの部分を担うタスクなのかという情報)
