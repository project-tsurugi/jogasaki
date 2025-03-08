# current timestamp等の設計

## 本文書について

[issue](https://github.com/project-tsurugi/tsurugi-issues/issues/183) で実施した、 current_timestamp, localtimestampなどの現在時刻を取得する関数の設計に関するメモ

## 関数の仕様

issue 記載の下記のとおり。 `current_time` については `TIME WITH TIME ZONE` の取り扱いが不明なので今回は未実装とした。

* `current_date`
  * 概要: ローカル時刻における現在の日付を返す
  * 仮引数: なし
  * 返戻型: `DATE`
* `current_time`
  * 概要: 世界時刻における現在の時刻を返す
  * 仮引数: なし
  * 返戻型: `TIME WITH TIME ZONE`
  * 備考: `TIME WITH TIME ZONE` 関連のため対応不要
* `localtime`
  * 概要: ローカル時刻における現在の時刻を返す
  * 仮引数: なし
  * 返戻型: `TIME WITHOUT TIME ZONE`
* `current_timestamp`
  * 概要: 世界時刻における現在の日時を返す
  * 仮引数: なし
  * 返戻型: `TIMESTAMP WITH TIME ZONE`
* `localtimestamp`
  * 概要: ローカル時刻における現在の日時を返す
  * 仮引数: なし
  * 返戻型: `TIMESTAMP WITHOUT TIME ZONE`

### 注意

* OSが提供するシステムクロックはUTCにおける時刻を返すため、ローカル時刻を取得するためにはなんらかのタイムゾーン情報を利用する必要があり、「システムタイムゾーン」を使用する。
  * 本来「ローカル時刻」は特定のタイムゾーンによらない時刻であり、システムタイムゾーンに直接関連するわけではないが、UTC時刻からの変換においては何らかのタイムゾーンを使用する必要があるので、システムタイムゾーンを使用する。
  * ここで、システムタイムゾーンはtsurugi.iniの `session.zone_offset` によって決まるタイムゾーンを指す。

* 上の `current_date` の記述では「ローカル時刻における」となっていて、名前が `current`で始まるのと齟齬があるように見えるが、`localtimestamp` の日付部分は `current_timestamp` をシステムタイムゾーンで取得した際の日付部分と一致するので、どちらで考えてもよい (`localtimestamp` も内部的にUTC時刻からローカルに戻す時点でシステムタイムゾーンを使っている)

* 基本的に postgresqlに合わせた仕様となっているようなので、迷った場合は postgresql の挙動を確認するのがよさそう

## 実装上の注意点・配慮点

* 現在時刻をevaluator内で使用する必要があったため、`transaction_context` を `evaluator_context` に含めるようにした
  * 「式評価」と「トランザクション」の結びつきが密になりすぎる欠点はあるが、BLOB処理でも式評価時にトランザクションが必要になるため、式評価にトランザクションは必要なものとすることにした

* `std::chrono::steady_clock` で `now()` を使用して time pointを取得しても、UTC時刻のような絶対時刻を表すわけではない。今回の変更ではUTC時刻の取得が必要だったため、clockを使用しているほとんどの箇所を `std::chrono::system_clock` に変更した。

