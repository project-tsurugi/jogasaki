# jogasakiに構成パラメータを追加する 

2024-08-07 kurosawa

## 本文書について

jogasakiは実行時の挙動を制御するための構成パラメータを提供している。本文書は新規に構成パラメータを追加する際に必要なコード変更点を示す。

## tsurugi.iniとconfigurationクラス

tsurugi DB全体ではtsurugi.iniファイルによって構成パラメータを提供している。
jogasakiはconfigurationクラスによって、これらを含むパラメータのスーパーセットを保持し、実行時の挙動を構成可能にする。tsurugi.ini由来でないものは外部に公開しないもので、主に動作確認・テスト用である。

tsurugi.iniファイルの内容は api/resource/bridge の内部で上位フレームワークから渡されたものを解析してconfigurationクラスに設定される。
https://github.com/project-tsurugi/jogasaki/blob/e6de63be570fabd9a607a6f65ee7129c3d43148f/src/jogasaki/api/resource/bridge.cpp#L137

設定されたconfigurationクラスはdatabaseオブジェクトが保持する。
https://github.com/project-tsurugi/jogasaki/blob/e6de63be570fabd9a607a6f65ee7129c3d43148f/src/jogasaki/api/impl/database.h#L299

またdatabaseにアクセスできない場合のためにグローバル領域にもセットされる。
https://github.com/project-tsurugi/jogasaki/blob/e6de63be570fabd9a607a6f65ee7129c3d43148f/src/jogasaki/executor/global.h#L85

configurationオブジェクトはセッターを持つがデータベースの稼働中(api::database::start()実行後)に変更することは想定していない。
テストで使用する場合はSetUp内部で作成したconfigurationオブジェクトをdatabaseに設定して使用する。
https://github.com/project-tsurugi/jogasaki/blob/e6de63be570fabd9a607a6f65ee7129c3d43148f/test/jogasaki/api/sql_test.cpp#L80

## 構成パラメータの追加 

tsurugi.iniに公開しない構成パラメーターの場合はconfigurationクラスに必要なメンバ(およびgetter/setter)を追加し、必要な箇所でdatabaseやグローバルからconfigurationを取得して使用する。

tsurugi.iniに公開する構成パラメータの場合はこれに加えてbridgeでの読み出しを行う部分を追加する。
https://github.com/project-tsurugi/jogasaki/blob/e6de63be570fabd9a607a6f65ee7129c3d43148f/src/jogasaki/api/resource/bridge.cpp#L137
また、tateyama-bootstapのコードがtsurugi.iniのデフォルト値を保持しており、ここにエントリが存在しないと起動時に警告がでるので、こちらにもエントリとデフォルト値を設定する。
https://github.com/project-tsurugi/tateyama-bootstrap/blob/53716bab52190f17e953d9179655e56030b8285f/src/tateyama/configuration/bootstrap_configuration.cpp#L24
デフォルト値(等号の右辺)は省略することも可能だが、その場合は適宜resource/bridge.cpp内で値が取得できなかった際の処理を各必要がある。下記は `thread_pool_size` に何も設定されなかった場合の例である。
https://github.com/project-tsurugi/jogasaki/blob/e6de63be570fabd9a607a6f65ee7129c3d43148f/src/jogasaki/api/resource/bridge.cpp#L147
上記のコード変更を行ったのち、tateyamaのドキュメントも更新する
https://github.com/project-tsurugi/tateyama/blob/d4a0d6cff5863fc556cec0dbec6574e29e6086ee/docs/config_parameters.md

## パラメータの命名規則

tsurugi.iniに追加するもののうち、internalなものは `dev_` プレフィックスを付加する。外部ユーザーに公開する場合はもとの名前のままとする。

configurationクラスのパラメータ名はtsurugi.iniの名前と同じものを使用する、ただし、internal用でも `dev_` プレフィックスは省略する。

## パラメータ内容のログ出力

tsurugi.iniに追加したものについては、tsurugi.iniの内容が正しく実行時に反映されているかをユーザーが確認する目的でサーバーログに出力する。
https://github.com/project-tsurugi/jogasaki/blob/e6de63be570fabd9a607a6f65ee7129c3d43148f/src/jogasaki/api/impl/database.cpp#L151

configurationに追加したものについては、database開始時にデフォルト値と異なるものをサーバーログに主力しているが、こちらは開発者のデバッグ用途である。
https://github.com/project-tsurugi/jogasaki/blob/e6de63be570fabd9a607a6f65ee7129c3d43148f/include/jogasaki/configuration.h#L453

## tusurgi.iniのデフォルト値とconfigurationのデフォルト値

tusurgi.iniのデフォルト値とconfigurationのデフォルト値(configurationのデフォルトコンストラクタで詰められる値)は基本的に一致させているが、一部例外もある。
これは、単体テストケースではconfigurationの設定が主に使用されるため、テスト実行時の事情により本番環境と異なる構成で行いたい場合がある(テスト実行に時間がかかりすぎるなど)ためである。
本番環境でサーバープロセスとして起動する場合は、tsurugi.iniとそのデフォルト値が使用されるので、configurationのデフォルト値は使用されない。

## リファレンス

[tsurugi.iniファイル](https://github.com/project-tsurugi/tateyama/blob/d4a0d6cff5863fc556cec0dbec6574e29e6086ee/docs/config_parameters.md)

[configurationクラス](https://github.com/project-tsurugi/jogasaki/blob/e6de63be570fabd9a607a6f65ee7129c3d43148f/include/jogasaki/configuration.h)

