# jogasaki外部インターフェース

2020-05-19 kurosawa (status: memo)

## 主な方針
- umikongoで使いにくかった部分を改善するとともに、実行エンジンの方式の違い(push/pull)を反映させデータ取得部分は非同期にリクエストを要求・結果を取得する方式とする

- トランザクションをcallbackで受け渡し方式を止める
  callback使いにくかったのとPgとの整合性のため

- exceptionによるエラー処理をやめステータスコードによる応答結果を返す方式とする
  上記callbackとの合わせ技でexceptionの処理が二重に必要な事もありキレイでなかった

## 外部API
下記にAPIを構成する主なオブジェクトとメンバ関数をリストする
特に記述がない関数呼出はthread unsafeなものとする

### database

データベースを司るオブジェクト。通常、プロセスに一個のみを作成して使用する。

* start(options)
データベースの開始: スレッドプール初期化など資源の確保を行う

* stop()
データベースの終了: 資源を開放し終了する

* open_session(options) -> session
  * データベースとの接続セッションを確立する: データ受け渡し用のバッファ等の確保など相互作用を行うためのセッション環境を準備する
  * スレッドセーフ

### session

データベースと呼出側コードとの相互作用を行うセッションを司る
セッションは一時点でトランザクションを実行中か実行中でないかのいずれかの状態を持つ
(複数トランザクションを同時に実行中という状態はない)

* begin(options)
新規トランザクションを開始する

* execute_statement(string) -> result
SQLステートメント(データに変更を加えうるリクエスト)の実行を要求する。
実行エンジン側のSQL処理完了を待たずにresultオブジェクトが戻され、resultに対して完了の確認等が可能になる

* execute_query(string) -> result
SQLクエリ(データに変更を加えず読み出し要求リクエスト)の実行を要求する。
実行エンジン側のSQL処理完了を待たずにresultオブジェクトが戻され、resultに対して完了の確認、読出データの取得等が可能になる

(TODO)複数ステートメントを並行に処理できるようにする。jdbcのStatementオブジェクトのような感じ。
abort/commit時に実行中のステートメントの処理はどうなるかを決める


* commit(sync = false)
コミット要求を行う。
sync=falseの場合はグループコミットと関係なく制御が戻る。
syncがtrueであればグループコミット完了まで待つ(TXエンジンがサポートしている場合)

* abort(rollback = true)
トランザクションのアボート要求を行う。
実行中のステートメントが有る場合はキャンセル要求が投げられる。
rollback=falseの場合は即座に制御がもどる。
rollback=trueの場合はrollback処理完了後に制御がもどる。

* close()
セッションを終了させる。実行中のトランザクションが有る場合はabort()

### result 

* get_result_set(index) -> result_set
resultに関連づけられているindex番目のresult_setを取得する。
(複数result_setを返すケースはストプロ等を想定しておりなんらかの方法でindexが呼出側と共有される)

* completed(wait = false) -> bool
実行リクエストが完了したか
wait = trueの場合は完了を待機する。

### result_set

実行リクエストの結果を司るオブジェクト

(TODO)iteratorでのアクセスを可能にしたい。アクセスが軽量でalgorithmが使える。next()で取得する方法よりも柔軟性が高く上位のレイヤが実装が楽そう。順次データを受け取る場合は一度にiterateできる範囲が限られる可能性がある。get_more_pagesなどで順次ページごとに追加のiteratorを取得する。その際すでにある範囲をキープするかinvalidにするかoptionで選べるようにする。

* get_metadata() -> metadata
result setの列定義情報を持つmetadataオブジェクトを取得する

* available()
* wait()
* next()

### metadata

* type(index) -> type

### type 

* kind() -> kind
type kindを示すscoped enumを返す

* option() -> option
typeに付随するオプショナルな情報を返す(一部のtypeのみ)
