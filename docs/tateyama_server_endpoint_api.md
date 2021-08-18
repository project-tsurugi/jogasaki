# Tateyama Server Endpoint API案

2021-08-05 kurosawa

2021-08-16 kurosawa コメントを反映

2021-08-17 kurosawa bufferからwriterベースに変更

## この文書について

Tateyama AP基盤においてServer EndpointとServer APの境界となるAPI設計について記述する

## 前提

- QEX-3/JSP-4の作業で境界とするAPI
- このフェーズではまだIPC EndpointとSQL実行エンジンは密に稼働するが、将来的にEndpointとAPの疎結合を実現するために 当APIを経由したアクセスを設計・実装する
- ターゲットAPがSQLエンジンしかないのでルーティングはまだ行わない
  - そのためヘッダの一部などは未使用なものも入っている

## 用語
- 基本的に「UDF/Java サービスアーキテクチャ境界デザイン」文書に準じる
- エンドポイント
特に指定がない場合はサーバーエンドポイントを指す。
- API 
特に指定がない場合はエンドポイントAPIを指す
- エンドポイントAPI
サーバーAPIの別名
サーバアプリケーションがメッセージ(リクエストやレスポンス)の内容を読み書きするためのAPI
エンドポイントごとに異なる実装を持つことができる(IPC/Restなど)

## APIデザイン

### 基本的なデータの取り扱い

tateyamaはAP基盤であり、そのAPIはAPに特有のデータ構造に依存すべきではない。
当APIではルーティング等AP基盤の機能に必要なデータは外部に見える形で扱うがAP特有のデータはprotobuf/messagepackなどによってシリアライズされたデータを取り扱う

### service 

Server Endpoint APIはリクエストとレスポンスの実装、およびそれを受け取るAP基盤の関数`service`によって実現される。

```
status service(
  std::shared_ptr<request const> req, 
  std::shared_ptr<response> res
);
```

- Endpointがrequestとresponseを実装しservice関数を呼び出す
- AP基盤はrequestのヘッダ情報をもとに適切なサーバーAPにこれらを転送する
- `service`関数は通知後にすぐ戻る。呼出側はresponseオブジェクトのcomplete()関数による完了通知を待ち、戻された情報にアクセスする。どのタイミングでアクセス可能になるかについては下記responseセクションを参照。
- AP基盤とAP(実行エンジンなど)はrequestのコマンドに応じた内容を実行し、responseのメンバ関数を呼出して結果を戻す
- Endpoint/AP基盤間でownership管理の手間を軽減するためにrequest/responseはshared_ptrによって保持する

### request

virtual class `request`によってIFが定義される
Endpointごとに独自の実装をもつ
下記にrequestクラスがアクセサを提供する構成要素を示す

- headerアクセサ
  - headerはtateyamaのレイヤでAP基盤がrouting等に使用するためのプロパティ群をさす
  - QEX-4以降に使用予定でありQEX-3では未実装

  > std::size_t session_id();

  - セッションの識別子を取得する

  > std::size_t application_id();
  - リクエスト送付先のアプリケーションの識別子を取得する

- payloadアクセサ

  > std::string_view payload();

  - payloadはAPの管理するデータであり、tateyama上は不透明(opaque)なバイナリ列
  - jogasaki-tsubakuro間においては、tsubakuroのprotocol.requestメッセージをシリアライズしたものを使用する
    https://github.com/project-tsurugi/tsubakuro/blob/master/modules/common/src/main/protos/request.proto
  - jogasakiとtsubakuroはこのencoder/decoderを共有する

### response 

virtual class `response`によってIFが定義される
下記にresponseクラスがアクセッサを提供する各構成要素を示す

- headerアクセサ

  - headerはtateyamaのレイヤでAP基盤がrouting等に使用するためのプロパティ群をさす
  - status codeとerror message以外はQEX-4以降に使用予定でありQEX-3では未実装
  - header/bodyが確定するタイミングはoutput channelを持つケースとそうでない場合で異なる
    - output channelを持たない場合、response::complete()呼出し時点でheaderの値は確定する
    - output channelを持つ場合、responseから取得されたdata_channelの全てがrelease_channel()された時点でheaderの値が確定する。
      - それまではエラーによってヘッダやボディの内容が変更される可能性がある
      - それまではstatus codeには一時的な状態を表すコード"running"が戻される

  > void session_id(std::size_t session);
  - セッションの識別子を設定する

  > void requester_id(std::size_t id);
  - リクエスタの識別子を設定する

  > void code(response_code st);
    - tateyamaのレイヤでのステータスを設定する
    - APのレイヤでエラーが発生した場合、ここにはAPでエラーが起きた事を示すステータスコードが戻され、エラー詳細を示す情報はbodyに格納される

  > void message(std::string_view msg);
    - コンソールメッセージ用のエラー出力文字列を設定する

- bodyアクセサ
  - bodyはAPの管理するデータであり、tateyama上は不透明(opaque)なバイナリ列
  - jogasaki-tsubakuro間においては、tsubakuroのprotocol.responseメッセージをシリアライズしたものを使用する
    https://github.com/project-tsurugi/tsubakuro/blob/master/modules/common/src/main/protos/response.proto
  - jogasakiとtsubakuroはこのencoder/decoderを共有する
  - bodyの内容は下記関数によって設定される：

    ```
    status body(std::string_view body);
    ```
    - `body`で指定された内容をresponse bodyとして設定する

- output channelアクセサ
  - AP出力がある場合のみ、data_channelインターフェースを下記関数によって取得可能

    > status acquire_channel(std::string_view name, data_channel*& ch);

    - APが複数出力を持つ場合のために名前付きとしている
    - APがjogasakiで実行しているものがSQLステートメントの場合は複数出力はなく、出力名はこの関数の呼び出し側が決定し、bodyの一部として戻す(protocol.Response.ExecuteQuery.nameフィールドを使用)

    > status release_channel(data_channel& ch);
      - 与えられたdata channelに属する全ライタに対する書き込み完了を通知
      - これ以降はこのdata channelから新しくwriterがacquireされることもない事を宣言する

### data_channel

- 任意の長さを持つアプリケーションの出力を呼び出し側へ共有するためのライタ群を提供するクラス
- スレッドセーフ

```
status acquire(writer*& w);
```
- writerオブジェクトを取得する。

```
status release(writer& w) 
```
- ライタに対する書き込み完了を通知
- ライタに対するアクセス権を返却し、これ以降呼出側は`w`に対してアクセス不可能になる
- 処理途中でエラー等によりライタが不要になった際も呼出側はこの関数によって返却する
- write後にcommitされていないデータがあった場合、そのデータが使用される保証はない。呼び出し側はrelease前に適切にcommitを呼んで置くこと。

### writer

レスポンス構築時にデータを書き込むためのクラス
スレッドセーフでない

```
status write(char const* data, std::size_t sz);
```
- `data`から開始する`sz`バイトの内容をアプリ出力として追記する
- 書き込みフォーマットについては、serializer/deserializerをtsubakuro/jogasakiで共有する
- 内部的なバッファにスペースがなくて書けない場合はブロックする。読出し側が進行して内部バッファが空くのを待つ。

```
status commit();
```
- 現時点までに書き込んだ内容を確定させ、読出し可能であることを通知する
- 呼出側は適切な単位(レコード区切りなど呼び出し側が待つことなく処理可能な単位)およびタイミングでこの関数を呼び、読出し側を進行させることが期待される

## その他・考慮点

- 現状のprotocol.RecordMetaメッセージをprotocol.ExecuteQueryメッセージ内に移動する予定。クエリの実行時にresponse bodyとしてRecordMetaを入手可能とするため。
- ORDER BY句のように出力結果の順序が問題になるさいは単一writerによって結果が書かれることを想定している。複数writerの結果を順序付けるような機能は現時点ではない。