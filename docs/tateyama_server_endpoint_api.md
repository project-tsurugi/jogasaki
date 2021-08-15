# Tateyama Server Endpoint API案

2021-08-05 kurosawa
2021-08-06 kurosawa 型の扱いを更新
2021-08-06 kurosawa response.state変数を追加
2021-08-06 kurosawa output channelの記述追加
2021-08-06 kurosawa shared_ptrによる受け渡しに変更
2021-08-06 kurosawa 用語追加
2021-08-11 kurosawa protobufによるpayload定義を追加
2021-08-12 kurosawa data_channelとmanagement_channelを分離
2021-08-13 kurosawa 
2021-08-13 kurosawa state変数を削除しcomplete()関数へ変更

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
  - それぞれ適当なタイミングで不要になったshared_ptrを破棄してownershipを返却する。典型的なタイミングは：
    - AP基盤側: serviceによって開始された非同期処理を終えた時点でresponse::complete()を呼んでshared_ptrを破棄
    - Endpoint: responseはその内容をconsume完了した後。requestに関しては保持する必要がなければ`shared_ptr<request>`を作成してすぐにmoveでわたしてもよい。

### request

virtual class `request`によってIFが定義される
Endpointごとに独自の実装をもつ
下記にrequestクラスがアクセサを提供する構成要素を示す

- header (TBD)
  - session id : integer
  - target app id : integer
- payload
  - tateyama上は不透明(opaque)なバイナリ列
  - tsubakuroのprotocol.requestメッセージをシリアライズしたもの
    https://github.com/project-tsurugi/tsubakuro/blob/master/modules/common/src/main/protos/request.proto
  - jogasakiとtsubakuroはこのencoder/decoderを共有する

### response 

virtual class `response`によってIFが定義される
下記にresponseクラスがアクセッサを提供する各構成要素を示す

- header
  - session id : integer (TBD)
  - requester id : integer (TBD)
  - status code : integer
    - tateyamaのレイヤでのステータスを返す
    - APのレイヤでエラーが発生した場合、ここにはAPでエラーが起きた事を示すステータスコードが戻され、エラー詳細を示す情報はbodyに格納される
  - error message : string
    コンソールメッセージ用のエラー出力文字列
  - headerが読み取り可能になるタイミングはoutput channelを持つケースとそうでない場合で異なる
    - output channelを持たない場合、response::complete()呼出し時点でheaderの値は確定する
    - output channelを持つ場合、responseから取得されたdata_channelの全てがcomplete()された時点でheaderの値が確定する。
      - それまではエラーによってstatusコードが変わる可能性がある
- body
  - tateyama上は不透明(opaque)なバイナリ列
  - tsubakuroのprotocol.responseメッセージをシリアライズしたもの
    https://github.com/project-tsurugi/tsubakuro/blob/master/modules/common/src/main/protos/response.proto
  - jogasakiとtsubakuroはこのencoder/decoderを共有する
  - response::complete()の呼出し時点でresponse bodyは確定する。それ以前はbodyを読み出してはいけない。

- output channel : data_channelオブジェクト
  - 不確定な長さを持つAP出力を交換するためのインターフェース。下記data_channelクラスを参照
  - AP出力がある場合のみ、出力名を指定してresponseから取得可能
    - APが複数出力を持つ場合のために名前付きとしている
    - APがjogasakiの場合は複数出力はなく、出力名はbodyに戻されたchannel名(protocol.Response.ExecuteQuery.name)で取得されるdata_channelを使用する

```
std::shared_ptr<data_channel> const& channel(std::string_view name);
```
- 新規の名前付きdata channelを取得する
- `ordered`で所属するbuffer群を順序付きで扱うべきかを示すフラグをセットする。ordered=trueによって取得されたものを順序付きdata_channelと呼ぶ。

```
status stage(data_channel& ch);
```
- 与えられたdata channelに属する出力バッファに対する書き込み完了を通知(各bufferはstageされる)
- これ以降はこのdata channelから新しくbufferがacquireされることもない事を宣言する

### data_channel

- アプリケーションの出力を呼び出し側へ共有するためのバッファ群を提供するクラス
- 順序付きor順序なしのbuffer群を管理するクラス
- スレッドセーフ

```
buffer& acquire(std::size_t size);
```
書き込み領域のサイズを指定して、bufferオブジェクトを取得する。
戻されるbufferは少なくとも指定したサイズのcapacityを持つ事が保証される。
順序付きchannelの場合はこの関数の呼出順にbufferには内部的に通し番号が振られる。Endpoint側はこの順序でバッファを整列しデータ使用する必要がある。(e.g. ORDER BY句のあるSQL文)

```
status stage(buffer& buf) 
```
- バッファに対する書き込み完了を通知
- バッファに対するアクセス権を返却し、これ以降呼出側は`buf`に対してアクセス不可能になる
- `buf`にはこの関数の実行前にset_sizeによって書き込み済みサイズがセットされている必要がある

### buffer

レスポンス構築時にデータを書き込むためのバッファを抽象化したクラス。
スレッドセーフでない

```
unsigned char* data();
```
- 書き込み可能な領域の先頭を指すポインタを返す
- 書き込みフォーマットについては、serializer/deserializerをtsubakuro/jogasakiで共有する

```
std::size_t capacity();
```
- 書き込み可能な領域の最大サイズを返す。

```
void set_size(std::size_t sz);
```
- バッファに書き込んだバイト数を通知する
- data_channel::stage()実行前にこの関数呼び出しによって書き込みサイズを設定しておく必要がある

## その他・考慮点

- 要求するbuffer sizeに制限が必要かどうか要確認
