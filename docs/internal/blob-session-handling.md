# BLOBセッションの取り扱い

## 前提知識

- jogasakiがUDF(user defined function)呼出し等の過程でBLOBデータを送受信する際、その処理をBLOB中継サービスに依頼する
- BLOB中継サービスとのやりとりはBLOBセッションに紐づくので、送受信開始前にセッションを取得し使い終わったらdisposeする
  - BLOB sessionはsession idで識別される
- BLOBセッションの作成時にはトランザクションIDを渡す必要がある

## 設計

- `src/jogasaki/relay/` ディレクトリに `blob_session_container` というRAIIのためのオブジェクトを用意する
  - これはblob_sessionのポインタを保持することができる
    - ただしポインタの所有権は持たず、あくまでdisposeの責任を持つだけとする
  - dispose() メソッドを持ち、呼ばれたら保持しているセッションをdisposeする
  - dispose()は明示的に呼ぶことも可能だが、デストラクト時までに呼ばれなかったらデストラクト時に呼ぶ 
    - 現状はtask_contextやwork_contextにdispose()関数がないため、デストラクト時にdisposeをするようにする
    - 将来的には統一的にdispose()を実施する仕組みを用意したい
  - RAIIオブジェクトなのでコピー禁止、ムーブ禁止とする
    - work_contextのメンバ変数として直接保持するため、現状ムーブの必要性がない

- task contextとblob_session_containerが1:1になるようにする保持する
  - 具体的にはimpl::work_context に blob_session_container とその参照へのアクセサを追加する
  - context_helperにも同様にアクセサを追加

- BLOBセッションの取得はwork_context作成時ではなく、BLOBデータの送受信が必要になったタイミングで遅延初期化する
  - この遅延初期化はblob_session_containerが行う
  - blob_session_container に get_or_create() メソッドを用意して、BLOBセッションの取得・作成を行う
    - 初回アクセス時にセッションを作成し、blob_session_containerに格納する
    - 以降のアクセスでは既存のセッションを再利用する
  - work_contextは同時に1ワーカーからしか触らないのでこれが可能

- BLOBセッションの作成時にはトランザクションIDが必要になる。これはwork_contextの`transaction_`メンバから取得する

- `global::relay_service()`でBLOB中継サービスのインスタンスを取得する
  - このサービスを通じてBLOBセッションの作成・disposeを行う

- BLOBセッションを利用するのはUDFの呼び出し時が主なので、evaluator_contextにblob_session_container*を持たせて、UDF呼出し時にそこからBLOBセッションを取得できるようにする

