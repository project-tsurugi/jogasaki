# プロセス実行機設計

2020-05-20 kurosawa (status: メンタルモデル整理のためのメモ)

2020-05-30 kurosawa (IF定義に合わせて文書更新)

## 登場クラス

### process_executor

プロセスステップの準備および実行を担うもの
下記processorによって表現されるプロセスの処理内容に対し、適切なreader/writer/scan_info等の部品をtask_contextを通して与えて起動するしくみ。
測定用も用意しtask外でもスタンドアロンで動くようにする。

### processor

プロセス内のグループ/レコードに対する一連の処理内容(ロジック)を司るもの
リレーショナル演算子グラフ等で表された内容から生成される。

### task_context

実行するタスクのスコープを規定し、処理データの範囲を定義するとともに実行状態を保持する手段も提供する

processorに対してreader/writer/scan_infoを提供することによって処理対象の入力データの範囲を規定する。
processor実装が扱う、実行状態保持用のオブジェクトであるwork_contextの格納・取得手段を提供する。

実行スレッドがアサインされてからreader/writerなどのI/Oオブジェクトが決まるケースがあるのでprocessorへreader/writerを直接渡さず、このオブジェクトを経由する。

### task 

プロセス処理を並列化した最小実行単位
process::step::flowからcreate_tasks()によって必要な並列度の個数作成される

taskには処理ロジックと対象データの範囲が決まっており、対象データの全ての処理が終わるとタスク完了となる。
対象データの範囲は規定されているが、対象データの分量はタスク開始時に固定的に決まるとは限らない。
例えば「あるレンジスキャン条件(scan_info等のオブジェクトで定義)によって得られるデータ全て」や「あるreaderから読出し可能なデータ全て」といった与えられ方がある。

### task_scheduler

taskにスレッドをアサインしスケジューリングを担う
必要に応じてprocess_executorと協調して適切なスレッド割当を行う(情報を提供しスレッドに最適な入力データをを割り当てる)

タスク実行のためにはスレッドが割り当てられるがスレッド割当(attach)による実行開始とスレッド割当終了(detach)とタスクの実行(開始-終了)とは直交している。
タスクは最初にスレッドにattachされて実行された時点で開始(running)となり、終了を意図してスレッドからdetachされた場合に終了(completed)となる。
sleepを意図してスレッドからdetachされた場合sleepingとなり、外部から起こされてrunningに戻るまでその状態が継続する。

TODO: taskのキャンセルの扱い

タスクの状態(not started/running/sleeping/completed)とそれを実行するスレッドの状態(attached/detached)の組合せとして下記の組合せがありえる。

- 未実行(threadがdetached、taskはnot started)
- 実行中(threadがattached、taskはrunning)
- 待機中(threadがdetached、taskはrunning) <- yieldはここへの遷移
- 休止中(threadがdetached、taskはsleeping) <- sleepはここへの遷移
- 完了済(threadがdetached、taskはcompleted)

定義から、下記の組合せはありえない
attachedかつnot started
attachedかつcompleted
attachedかつsleeping

## 参考

asakusaのVertexProcessorインターフェース周辺
https://github.com/asakusafw/asakusafw-compiler/tree/428fef5a123ed504d85c6251541ca06ee8c31740/dag/runtime/api/src/main/java/com/asakusafw/dag/api/processor

VertexProcessorのテストハーネス
https://github.com/asakusafw/asakusafw-compiler/tree/428fef5a123ed504d85c6251541ca06ee8c31740/dag/runtime/api/src/test/java/com/asakusafw/dag/api/processor/testing