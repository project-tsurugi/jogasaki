# プロセスタスクのイールド処理

## この文書について

プロセスタスク実行中に中断してタスクをリスケジュールする「イールド処理」に関する設計と実装上の考慮点を記載する。

## 背景

演算子の処理中にワーカースレッドをブロックすると、そのワーカーが持つ他のタスクが実行されなくなり、スループットが低下する。
これを避けるため、演算子が処理を中断する必要があると判断した場合は、実行コンテキストを保管してタスクをリスケジュールし、ワーカーを他のタスクに譲る実行モデルを採用する。

## プロセスタスクの前提知識

- プロセスを実行するタスクは、1個以上の関係演算子から構成される演算子ツリーを実行する
  - ツリールートの演算子は `scan`, `find`, `take_flat`, `take_group`, `take_cogroup` のいずれかで、これらはプロセスタスクの開始点となり、プロセスが行うデータ処理の範囲を規定し、その開始から完了まで一連の処理をドライブする (activeな演算子)
  - それ以外の演算子( `filter` など)は駆動される側の演算子で、上流の演算子から呼び出されて処理を行う(passiveな演算子)
- 関係演算子はタスクコンテキスト情報としてその処理に必要な変数を入力として受け取り、必要な処理を行う。上流の演算子は下流の演算子が必要とする変数を生成し、下流の演算子を呼び出す
- プロセスが終了する際、各演算子の終了処理を `operator_base::finish()` で行う
  - このタイミングで関係演算子のコンテキスト等のリソースを解放する
  - activeな演算子が `finish()` の呼出を開始し、各演算子が下流へ伝搬させる
- 演算子オブジェクト自体は状態を持たない。各演算子に対応する演算子コンテキストオブジェクトがあり、演算子の状態や変数を保持する
  - 演算子コンテキストはタスクコンテキストに保存され、演算子が必要なときにアクセスされる

## イールド設計

- coroutineライブラリは使用せず(実行時オーバーヘッドを考慮)、各演算子が演算子コンテキストへ再開時に必要となる変数や状態を保存する

### 基本動作フロー

1. **演算子の処理開始**: 演算子はその演算子本体に必要な通常の処理を実行
2. **下流演算子の呼出**: 演算子は適当なタイミングでデータを下流へ送信して処理させるために下流演算子を呼出する
  - `context_state` を `calling_child` に設定して下流演算子を呼び出す 
  - 下流演算子から戻された `operation_status_kind` によって下流演算子の実行結果を確認
    - `ok`: 下流演算子が正常に実行された
      - `context_state` は `running_operator_body` に戻す
    - `yielding`: 下流演算子がイールドを要求
      - 下流呼出までのフレームを保管し、本演算子も `operation_status_kind::yielding` を上流へ戻し、タスクをリスケジュール
      - `context_state` は `calling_child` を維持
    - `aborted`: 下流演算子がエラーにより失敗
      - 本演算子も上流へ `operation_status_kind::aborted` を返し、リクエストの実行を中止する
      - `context_state` は `aborted` に設定する

### イールド処理フロー

1. **処理の継続判断**: 演算子は処理を継続できるかを自律的に判断
  - **継続可能な場合**: そのまま処理を継続
  - **継続不可能な場合**: コンテキストを保管し、タスクをリスケジュール
    - イールドする演算子は、自身の `context_state` の状態を `yielding` に設定
    - イールドする位置の前後で必要な変数や状態を演算子コンテキストに保存する
      - イールド位置を文字列 `<yield-point-name>` で識別し、`frame_<yield-point-name>` という構造体へ保存する
        - 例
          - `filter_context::frame_calling_downstream`
          - `apply_context::frame_stream_next`
    - 現在の演算子実行を `operation_status_kind::yielding` で終了し、スケジューラへリスケジュールを要求する
2. **タスクの再実行と復元**
  - スケジューラーが再度このタスクを選択したとき、タスクは root 演算子から呼び出しを開始
  - 呼び出された演算子は自身のコンテキスト状態を確認：
    - **`running_operator_body`**: 新規入力(レコード・グループ・コグループ)にたいする通常の処理を実行
    - **`calling_child`**: 下流の演算子がイールドしている。子演算子の呼出位置から再開する。上記の `frame_calling_downstream` を使用して変数状態を復帰させ、下流演算子の呼出を再開
    - **`yielding`**: 自分がイールドした箇所から再開する。上記の `frame_<yield-point-name>` を使用して変数状態を復帰させ演算子ロジックを再開

### コンテキスト状態

各演算子のコンテキストは、イールド処理の制御に使用される状態 ( `context_state` ) を持つ：

- **コンテキスト状態**
  - **`running_operator_body`**: 通常の状態。演算子本体の処理を実行中
  - **`yielding`**: この演算子が直接イールドを要求した状態
  - **`calling_child`**: 子演算子を呼び出している状態。下流の演算子(直接の子とは限らない)がイールド中の状態も含む
  - **`aborted`**: エラー発生等により演算子が処理途中で中止された状態

- **演算子呼出の戻り値** ( `operation_status_kind` )
  - **`ok`**: 正常に処理が完了
  - **`yielding`**: イールドしてタスクをリスケジュールする
  - **`aborted`**: タスクを中止(エラー発生等)

**状態遷移:**
- 初期状態・通常処理時: `running_operator_body`
- 子演算子呼び出し直前: `calling_child` に設定
- 子演算子呼び出しから戻った場合: 戻り値により遷移
  - `ok`: `running_operator_body` に戻す
  - `yielding`: `calling_child` を維持して上流へ `yielding` を返す
  - `aborted`: `aborted` に設定して上流へ `aborted` を返す
- イールド時: `yielding` に設定。親演算子には `operation_status_kind::yielding` を返す
- 処理再開時
  - ツリーのルートから演算子呼出が再開され、 `yielding` 状態の演算子まで再帰的に呼び出される
  - `yielding` 状態の演算子は `running_operator_body` に設定され、保存した状態から処理を再開

### 関係演算子の実装パターン

- 関係演算子の呼出は、その演算子が入力として扱うデータ単位によって `process_record`, `process_group`, `process_cogroup` のいずれかの関数で行われる(以下これらを総称して `process_IN` と呼ぶ)
- `process_IN` は必要な演算子コンテキストオブジェクトを生成し(タスクコンテキストに保存)それを使って `operator()(operator_context*, task_context*)` を呼び出す
- `operator()` 内で `context_state` に応じた処理を行う
  - 関数の先頭やwhile/forループの先頭など、適切な位置で `context_state` を確認し、状態に応じた処理を行う
  - `running_operator_body` の場合は通常の処理を行う
  - `calling_child`/`yielding` の場合は保存した状態から再開するために goto文でイールド位置へジャンプする
    - 関数ローカルな変数があるとgotoできないのでフレームオブジェクトに保存する 
    - こうすることで、直線的なコード構造を維持しつつ、途中から再開可能にする
      - 将来的にはフェーズに分け進捗とともに状態機械で管理することを検討してもよい
- `finish()` で演算子の終了処理を行うとともに下流へ `finish()` を呼び出すのは既存コードと同じ
  - イールド中の演算子も `finish()` で終了処理を行う必要があるため、

## 他のイールド処理との関係

- スキャンの結果セットのサイズが上限を超える場合、タスクをイールドされる
  - 詳しくは次issueを参照 https://github.com/project-tsurugi/tsurugi-issues/discussions/705
  - scan演算子は上流の演算子がないため上流コンテキストの保存は必要なく既存コードで実装済み
  - 今回のイールド処理の実装とも整合させるように、scan演算子のイールド処理も `context_state` で状態を変更する 
- 結果セットの書き込みに使用するライターの数が上限を超える場合にもタスクがイールドされる
  - プロセスタスクの実行開始時に `writer_pool` から `writer_seat` をacquireし、それができない場合にイールドするようにする
    - acquireしたものはプロセスタスクの終了時にreleaseされる 
    - 詳しくは [limit_resultset_writers.md](./limit_resultset_writers.md) を参照
  - 今回のイールド処理(プロセスイールド)とは独立した処理
    - プロセスイールドで中断したタスクのコンテキストからも `writer_seat` は一旦releaseされ、再開後に再度acquireする

## 既知の問題と解決案

### aggregate_group::finish() でのイールド無視

#### 問題

COUNT(DISTICT) 処理を行う aggregate_group 演算子は、上流シャッフルからの入力が空であった場合でも新しい行をを生成して下流へ送る必要がある。
この処理が `aggregate_group::finish()` で行われているが、 `finish()` 呼出時に下流の演算子がイールドする可能性を考慮していないために問題が生じている。具体的には下記。

`aggregate_group::finish()` は、シャッフルからの入力が空であった場合(`empty_input_from_shuffle == true`)に、空グループの集計結果を生成し、下流の演算子に対して `process_record()` を呼び出す。
この呼び出しで下流演算子がイールド(`operation_status_kind::yield`)を返した場合、現在の実装はその戻り値を無視して処理を継続し、`downstream->finish()` を呼び出す。

結果として:
- 下流演算子の `process_record()` がイールドしたまま完了していない状態で `finish()` が呼び出される
- 下流演算子は処理途中(未完了)の状態でリソース解放される
- タスクがリスケジュールされても `finish()` への再入パスが存在しないため、未処理のレコードはそのまま失われる

**影響範囲:** `aggregate_group` の下流に `apply` 演算子など、`process_record()` でイールドし得る演算子が存在するクエリ。

**再現テスト:** `test/jogasaki/api/sql_yield_finish_test.cpp` 内の  
`sql_yield_finish_test.aggregate_group_finish_empty_shuffle_with_yield` を参照。  
このテストは期待される正しい動作をアサートしているため、現在の実装では FAIL する。  
同ファイルの `aggregate_group_finish_empty_shuffle_no_yield` は TVF がイールドしない基本ケースであり、現在の実装でも PASS する。

**類似問題のある演算子:** 調査した結果、`finish()` 内で `process_record()` を下流呼出しているのは現時点では `aggregate_group` のみである。

#### 解決案

**案A: `finish()` のシグネチャを変更してイールド可能な関数として再設計する**

`operator_base::finish(abstract::task_context*)` の戻り値を `void` から `operation_status` に変更し、`finish()` が下流の `process_record()` または `finish()` からイールドを受け取った場合に上流へ伝播する。  
呼出元(activeな演算子)も `finish()` の戻り値を確認してイールド時にリスケジュールするよう変更する必要がある。  
`aggregate_group_context` に「finish フェーズ内での下流呼出状態」を保存するための `context_state` 管理とフレーム保存が必要になる(`context_state` に `finishing` 状態を追加)。  
`finish()` がイールドした場合はタスクコンテキストに `finishing` 状態を保存し、タスク再開時に `aggregate_group` がこの状態を検出して `finish()` の処理を再開できるようにする。  
この場合も activeな演算子側が `finish()` の戻り値を確認してリスケジュールを行う変更が必要となる。

**案B: `empty_input_from_shuffle` 時の処理を `process_group()` へ移行する**

activeな演算子(`take_group` など)が `finish()` を呼び出す前に、`empty_input_from_shuffle` フラグを確認し、空入力時の代替グループを `process_group()` として合成して `aggregate_group::process_group()` を呼び出すようにする。  
こうすることで、空入力時の下流 `process_record()` 呼び出しが `finish()` ではなく通常のイールド対応済みパスで行われる。

## その他

- 現状では `buffer` 演算子が未実装のため、 関係演算子ツリーは実は線形なリストである
  - 将来的に `buffer` 演算子が実装されると、`buffer` は複数の子供を持ち、ツリー構造をなす
