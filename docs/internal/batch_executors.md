# batch executor 内部設計

2023-07-29 kurosawa

## この文書について

パラメーターセットをバルクで渡しステートメントを一括で実行するためのbatch apiの内部設計に関するメモ

## 用語 

* バッチ

パラメーターをバルクで渡し同一のステートメントを繰り返し実行する仕組みのこと
いわゆる業務バッチ処理とは直接関係ない

* (パラメーター)ファイル

バッチ実行のためのパラメーターが格納されているファイルを指す

* ブロック

パラメーターファイルの分割単位。ファイルは複数のブロックに分割され、ブロック単位で並列に読み込みが可能。

## ロードとの関連

* non-transactional loadはbatch executorの枠組みを使用している
* batch executorはloadだけではなく、より広くファイル・通信路からパラメーターセットを受け取ってバルク実行するためのもの

## batch executor オブジェクトモデル

batch executionは下記のオブジェクト階層からなるツリーを動的に組み換えながら実行される
上の項目が下の項目の親で、親と子は1:N (N=0, 1, ...)。ただしbathc block executorとstatement間は 1:0,1 。

* batch executor
  * batch実行全体を司るもの
  * ツリーのルート
  * batchが完全に完了するまで開放されない
* batch file executor
  * 特定のファイルに関する実行を司るもの
  * ファイル内の全ブロックが完了後に開放される
* batch block executor
  * 特定のファイルの特定のブロックに関する実行を司るもの
  * ブロック内の全ステートメントが完了後に開放される
* statement 
  * ステートメントの1回の実行を司るもの
  * C++のクラスでは表現されず、実行のスケジュールで生成され、完了コールバックで開放とみなす

## 並列化モデル

### 並列化パラメーター

* `max_concurrent_files`

1つのbatch executorが最大で同時に処理するファイル数

* `max_concurrent_blocks_per_file`

1つのbatch file executorが最大で同時に処理するブロック数

### 並列実行モデル
- batch executorは `max_concurrent_files` 個 以下のファイルへ処理を並列化する
- batch file executorは `max_concurrent_blocks_per_file` 個以下のブロックへ処理を並列化する
- batch block executorが同時に処理するステートメントは最大で1つ
- ステートメントの完了時にステートメントコールバックによってステートメント完了がblock executorへ通知される
  - batch block executorは残りステートメントがあるかどうか確認し、あればその実行をスケジュールする
  - なければ blockの処理を完了として、上位(file executor)に完了を通知する
  - file executorは残りブロックがあるか確認し、あればその実行(ステートメント含む)をスケジュールする
  - なければ fileの処理を完了として、上位(batch executor)に完了を通知する
  - batch executorは残りファイルがあるか確認し、あればその実行(`max_concurrent_blocks_per_file`個のブロックを含む)をスケジュールする
  - なければ batchの処理を完了として、バッチ完了コールバックによって呼び出し側へ完了を通知する

## ツリーノードの状態遷移

* created
  * 初期状態
  * ノードが作成された状態
* activated
  * ノードが実行中の状態
  * createdから遷移
    * (statement以外のノード) activatedな子が1個以上追加された
    * (statementノード) statementを実行がスケジュールされ、その実行完了コールバックが呼ばれることが保証されている
* deactivated 
  * ノードの実行が完了した状態
  * activatedから遷移
    * (statement以外のノード) activatedな子が0個になった
    * (statementノード) statement実行完了コールバックが呼ばれた
  * createdから遷移
    * activatedな子を追加しようとしたが、実行すべきステートメントがなかった
* disposed
  * 終了状態
  * deactivatedから遷移
    * (batch executor) 呼出側がownしているbatch executorをdestructした
    * (batch executor, statement以外) 親へのrelease要求によりノードが開放された
    * (statement) statement実行完了コールバックのオブジェクトがdestructされた

## ownership, object life-cycle

* 基本的に親が子ノードに対するC++オブジェクトをownする
* 子ノードは自分の処理が完了したら親にreleaseメソッドで破棄を要求する
* ルート(batch executor)は使う側がshared_ptrで保持する
* かつこのshared_ptrをステートメント完了コールバックにownさせて、ツリー全体の開放が全コールバック完了後になることを保証する
  * shared_ptrの渡しやすさのために、batch executorはenable_shared_from_this()を使用する
* 正常処理の場合は必要に応じてノードが追加・削除されるることを期待する。
  * これによってメモリフットプリントを抑えつつ多数ファイル・ブロックからなるバッチを実行できる。
  * 異常処理の場合はこの限りではなく、開放処理はツリーをまとめて一括で破棄する

## 完了処理・例外処理

* 正常終了・異常終了(エラー)に関わらずバッチ完了のコールバック関数によって終了が通知される
  * 正常終了の場合はステートメントの完了コールバック内で、それ以上作業する処理(ステートメント)がないとなった状態でバッチ完了のコールバックを呼び出す
  * 異常処理の場合はエラーが発生した時点でエラー情報を格納し、バッチ完了コールバックを実行する。
    * ステート管理の構造(batch_execution_state)に異常終了状態であるフラグをセットする(error_aborting)
    * 仕掛り中のステートメントはこのフラグを確認し、速やかに処理をやめて終了する

## タスクとジョブの関係

ジョブケジューリングの観点からは、batch実行は下記の２つがネストしたジョブとみなせる
* batch全体を実行するジョブ
* batchで呼び出される各ステートメントを実行するジョブ

後者は通常のステートメントのジョブ実行とほぼ同じもので、コールバック実行によってステートメント完了をbatch_block_executorに通知する。
batch_block_executorは必要に応じて上流のbatch_file_executor/batch_executorにブロック・ファイルの処理完了を通知する。通知を受けた側で、次の実行候補(fileかblock)があればその構造を生成し、それらに紐づくstatementをスケジュールする。
このようにbatch executorのほとんど(下記bootstrap部分を除く)はステートメントのジョブのコールバック内で稼働する。

batch全体を実行するジョブの構成要素は下記である。

* batch用bootstrapタスク
* teardownタスク

batch用bootstrapタスクによって、batch_executorのブートストラップが行われ、初期化、必要なファイル・ブロック・ステートメントの実行がスケジュールされる。
ブートストラップが完了後はステートメントのコールバックによってbatch executorが呼び出され、処理すべきステートメントがなくなるとbatch executorが完了してteardownタスクがスケジュールされる。
teardownタスクは通常のジョブと同じもので、これによってジョブ完了のコールバックが呼び出され、batch全体のジョブが完了となる。

