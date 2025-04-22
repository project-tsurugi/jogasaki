# タスクスケジューラ統計情報

2025-04-22 kurosawa

## 本文書について

タスクスケジューラが停止時に表示する統計情報について説明する

## タスクスケジューラの統計情報 

- SQL実行エンジンのshutdown時に統計情報が下記のようなログで表示される

```
I20250422 15:41:14.754374 2690592 stealing_task_scheduler.cpp:133] /:jogasaki:scheduler:stealing_task_scheduler:stop Task scheduler statistics {"duration_us":235683342,"worker_count":3,"workers":[{"worker_index":0,"count":307,"sticky":110,"steal":43,"wakeup_run":64,"suspend":330},{"worker_index":1,"count":111,"sticky":0,"steal":25,"wakeup_run":25,"suspend":264},{"worker_index":2,"count":17,"sticky":0,"steal":6,"wakeup_run":8,"suspend":253}]}
```

- ワーカーごとのタスクの処理数やスティールの回数、サスペンド回数などがみれる
- これらを見ることで、各ワーカーに均等にタスクが分配されなかったり休止回数が多すぎるといった異常な状況を推測することができる

## 統計情報の項目

- `duration_us` 

  - タスクスケジューラの開始から終了までの時間(us)

- `worker_count` 

  - タスクスケジューラ内のワーカーの個数 (`sql.thread_pool_size`と等しくなる)

- `worker_index` 

  - ワーカーのインデックス(0-origin)

- `count`

  - 起動中にワーカーが処理したタスクの総数

- `sticky`

  - 起動中にワーカーが処理したstickyタスクの個数

- `steal`

  - ワーカーがstealによって処理したタスクの個数

- `wakeup_run`

  - ワーカーがsuspend状態から復帰し、少なくとも1個以上のタスクを実行した回数

- `suspend`

  - ワーカーがsuspend状態に入った回数
  - suspendから復帰してすぐに再suspendに入った回数も数える


