# プロセス実行機設計

2020-05-20 kurosawa (status: メンタルモデル整理のためのメモ)

## 登場クラス

### process_executor

プロセスステップの準備および実行を担うもの
下記processorによって表現される一連のプロセスの処理内容に対し、適切なreader/writer等の部品をprocessor_contextを通して与えて起動するしくみ。
測定用も用意しtask外でもスタンドアロンで動くようにする。

### processor

プロセス内のグループ/レコードに対する一連の処理内容を司るもの(のインターフェース)
リレーショナル演算子グラフ等で表された内容から生成される。
（最初の実装はインタプリタによる演算子グラフの実行）

### process_context

processorに対してreader/writer等必要なI/Oオブジェクトの提供を担うもの。
processorがtask内で実行されてスレッドがアサインされてからreader/writerが決まるケースがあるのでprocessorへreader/writerを直接渡さず、このオブジェクトを経由する。

### task 

プロセス処理を並列化した最小実行単位
process::step::flowからcreate_tasks()によって必要な並列度の個数作成される

### task_scheduler

taskにスレッドをアサインしスケジューリングを担う
必要に応じてprocess_contextと協調して適切なスレッド割当を行う(情報を提供しスレッドに最適な入力データをを割り当てる)

## 参考

asakusaのVertexProcessorインターフェース周辺
https://github.com/asakusafw/asakusafw-compiler/tree/428fef5a123ed504d85c6251541ca06ee8c31740/dag/runtime/api/src/main/java/com/asakusafw/dag/api/processor

VertexProcessorのテストハーネス
https://github.com/asakusafw/asakusafw-compiler/tree/428fef5a123ed504d85c6251541ca06ee8c31740/dag/runtime/api/src/test/java/com/asakusafw/dag/api/processor/testing