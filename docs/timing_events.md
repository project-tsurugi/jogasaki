# jogasakiタイミングイベントログメッセージ

2023-03-22 kurosawa


## 当文書について

jogasakiがログメッセージに出力するタイミングイベントの一覧をまとめる

## タイミングイベント

jogasakiは下記のイベントをverbose log level 35 (jogasaki::log_debug_timing_event)で出力する


|  ログプレフィックス  |  イベント内容  | 備考 |
| ---- | ---- | ---- |
| /:jogasaki:timing:transaction:starting | トランザクションを開始しようとする | |
| /:jogasaki:timing:transaction:starting_end | トランザクションを開始を受け付けさせた | |
| /:jogasaki:timing:transaction:started | トランザクションを開始した| |
| /:jogasaki:timing:committing | トランザクションをコミットしようとする | |
| /:jogasaki:timing:committing_end | トランザクションのコミットを受け付けさせた | |
| /:jogasaki:timing:committed | トランザクションのコミットが完了した| |
| /:jogasaki:timing:transaction:finished | トランザクションが終了した | 要求されたリクエスト種別によらず、トランザクション終了のみを表すイベント。 `status` の値 `committed` / `aborted` によって終了したトランザクションの状態を示す。 |

出力例
```
I0316 07:46:02.838364 502928 database.cpp:822] /:jogasaki:timing:transaction:starting job_id:00000000000017f1 options:{type:ltx write_preserves:{ T0 T1 }}
I0316 07:46:02.838768 501283 database.cpp:795] /:jogasaki:timing:transaction:starting_end job_id:00000000000017f1
I0316 07:46:02.860045 501284 database.cpp:815] /:jogasaki:timing:transaction:started TID-0000000000000020 job_id:00000000000017f1

I0316 07:46:02.823669 502928 transaction.cpp:397] /:jogasaki:timing:committing TID-000000000000001f job_id:00000000000017f0
I0316 07:46:02.823817 501281 transaction.cpp:350] /:jogasaki:timing:committing_end TID-000000000000001f job_id:00000000000017f0
I0316 07:46:02.823943 501282 transaction.cpp:390] /:jogasaki:timing:committed TID-000000000000001f job_id:00000000000017f0

I0330 16:27:42.184736 643047 transaction.cpp:446] /:jogasaki:timing:transaction:finished TID-0000000000000010 status:committed
```

## 細粒度タイミングイベント

jogasakiは下記のイベントをverbose log level 37 (`jogasaki::log_debug_timing_event_fine`)で出力する

|  ログプレフィックス  |  イベント内容  | 備考 |
| ---- | ---- | ---- |
|/:jogasaki:timing:job_accepted | ジョブを受け付けた | ジョブID, リクエスト種、SQL文字列(*1)、トランザクションオプション、トランザクションID等も出力する |
|/:jogasaki:timing:job_submitting | ジョブをスケジューラに投入しようとする (スケジューラを利用する場合のみ) | ジョブIDを出力する |
|/:jogasaki:timing:job_started | ジョブがスケジューラ上で開始された (スケジューラを利用する場合のみ) | ジョブIDを出力する|
|/:jogasaki:timing:job_finishing | ジョブが完了しようとする | ジョブIDを出力する。ジョブの実行結果をstatus (success/failure) として出力する。|


リクエスト種を下表に示す

|  リクエスト種 |  内容  | 備考 |
| ---- | ---- | ---- |
|prepare | preparedステートメントの作成| |
|begin | トランザクション開始 | |
|commit | コミット | |
|rollback | ロールバック | |
|execute_statement | クエリ・ステートメント等の実行 | |
|dispose_statement  | preparedステートメントの破棄 | |
|dump  | ダンプの実行 | |
|load  | ロードの実行 | |
|explain | 実行プラン表示 | |
|describe_table  | テーブルメタデータの取得 | |
|batch | TBD | |

出力例
```
I0322 07:50:37.409651 1330675 request_logging.cpp:39] /:jogasaki:timing:job_accepted job_id:000000000000003b kind:execute_statement tx:TID-0000000600000001 sql:{select * from T0} tx_options:{<empty>}
I0322 07:50:37.409933 1330675 request_logging.cpp:49] /:jogasaki:timing:job_submitting job_id:000000000000003b
I0322 07:50:37.410038 1330668 request_logging.cpp:55] /:jogasaki:timing:job_started job_id:000000000000003b
I0322 07:50:37.448936 1330668 request_logging.cpp:61] /:jogasaki:timing:job_finishing job_id:000000000000003b status:success
```

注
(*1) ログの増加を減らすため、長いSQL文は適当な長さにtruncateされる。またログメッセージを一行に収めるために、改行等の制御文字は置換えられることがある。

