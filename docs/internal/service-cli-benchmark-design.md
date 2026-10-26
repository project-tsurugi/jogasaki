# service_cli / service_benchmark 改善設計

2026-10-26 kurosawa

## 本文書について

`examples/service_cli` と `examples/service_benchmark` はjogasakiのSQLサービスAPIを経由して挙動や性能を確認するためのツールだが、しばらくメンテナンスされておらず一部機能が動作しなくなっている。これらを改善し、性能分析およびSQL/トランザクション挙動の分析に使えるようにするための設計方針を記述する。

## スコープ

今回の作業は以下の3点とする。

1. 壊れているservice_cli/service_benchmarkを動作させ、使える状態にする
2. 基本的な部品をservice_cli/service_benchmarkの双方から使えるよう共通化する
3. service_cliのREPLおよびスクリプトの文法・入力形式を更新し、tgsql/tgsqlmux互換とする

### 対象とするもの

- service_cliの不具合修正 (テーブル登録、`ExecuteResult` の処理、日付・時刻パラメーターでのabort、auto commitの動作、応答待ちのタイムアウト)
- service_benchmarkの不具合修正 (集計誤り、`db_->start()` の戻り値未確認、古いフラグ説明)
- 共通部品: `service_client`、gflags定義と起動処理、テーブル準備とデータ投入
- tgsql/tgsqlmux互換のスクリプト/REPL (複数セッション、`--\sleep`、`--\wait`、`--\expect_error` を含む)
- 動作確認のためのスクリプト実行のctest登録

### 対象外とするもの (将来の課題)

- service_benchmarkの機能拡張: リトライとアボート数の集計、レイテンシのパーセンタイル、ワークロード定義ファイル、JSON/CSV出力、オーバーヘッド計測モード、`api::database` 直接呼び出しモード
- simple-benchmark互換のベンチマーク (「将来の拡張方針」に記載)
- CPU割当ての改善 (付録に記載)
- tgsqlにない機能の拡張 (ホスト変数とprepared statementは現行機能相当を `\prepare`/`\execute` として維持するにとどめる)
- BLOB/CLOB、Ctrl-Cによるキャンセル
- 期待結果コメント (`--[3]` など) による自動照合
- examples配下の他のcli (sql_cli, scan_cli など)。参考にするが変更しない

## 現状

### 共通

- 両ツールとも同一プロセス内で `jogasaki::api::database` を起動し、`jogasaki::api::impl::service` にtateyamaのmock (`test_request`/`test_response`) で protobuf リクエストを直接投げる
  - SQLサービスAPI以下は本物だが、tateyamaのendpoint (IPC/stream)、セッション管理、ルーティングは通らない
- begin/commit/execute とその結果のデコード処理が両ツールで重複している

### service_benchmark

- 固定4ワークロード (`--insert`/`--upsert`, `--update`, `--query`, `--query2`) を複数クライアントスレッドで実行し、スループットと平均ターンアラウンドを出力する
- OCC/LTX/RTX の選択、DDLによるテーブル作成と2次索引、データ準備 (`--prepare_data`/`--load_from`)、スケジューラ関連パラメーターを指定可能
- 問題点
  - エラー時 (OCCの競合によるcommit失敗を含む) は即 `std::abort()` する。リトライやアボート数の集計がない
  - 集計誤り: トランザクション時間が `commit_ns_ + statement_ns_ + commit_ns_` になっている (`begin_ns_` の誤り)。"avg ... ns/thread" は合計をスレッド数で割っただけ。集計している `records_` を表示していない
  - レイテンシは平均のみで、分布や begin/statement/commit ごとの内訳がない
  - SQLは固定で、prepared statementは1つしか持てない

### service_cli

- 1文字コマンド (`b`/`c`/`a`/`p`/`q`/`s`/`v`/`x`/`w`/`l`/`e`) による対話シェル、および `--input_file` によるスクリプト実行
- 動作確認で判明した問題
  1. 組み込みテーブルを古い方法 (`add_benchmark_tables(provider)` + 非推奨の `register_kvs_storage`) で登録しているため、ストレージ管理側に登録されず `TARGET_NOT_FOUND_EXCEPTION` になる。同梱スクリプトが使うT0/T1/qa_t*は登録すらされていない
  2. 文の実行結果 (`ExecuteResult`) を `decode_result_only` で読もうとしており、成功時も `command returned none` と表示される。一意制約違反などのエラーも原因が表示されない
  3. auto commit が明示的に `b` で開始したトランザクションに対しても文ごとにcommitしてしまう
  4. DATE/TIME_OF_DAY/TIME_POINT のパラメーターを渡すと `std::abort()` する (`to_value` が未実装)
  5. 応答待ちのタイムアウトが100ms固定
  6. トランザクションを1本しか持てず、複数トランザクションの交互操作ができない
  7. LTX/RTX の指定ができない、複数行のSQLが書けない、実行時間が表示されない

## 設計方針

### 共通化の範囲

CLIはリッチな機能、benchmarkは低オーバーヘッドを求めるが、両者の要求がぶつかるのは「1リクエストごとに必ず通る処理」(エンコード、service呼び出し、完了待ち、成否判定) だけである。この部分を薄い共通層とし、それより上の機能は各ツールで個別に持つ。

方針は「準備段階はいくらリッチでもよい。1リクエストごとの処理だけは薄く保つ」とする。

```
┌ service_cli ──────────────────┐ ┌ service_benchmark ────────────┐
│ スクリプト/REPLの解析              │ │ ワークロードのループ               │
│ セッション(トランザクション)の管理    │ │ リトライ・アボートの集計            │
│ 結果の表示・診断情報の表示          │ │ 計測(ヒストグラム)・結果出力        │
└───────────────┬───────────────┘ └───────────────┬───────────────┘
┌───────────────┴─────────────────────────────────┴───────────────┐
│ service_client (共通・薄い層)                                         │
│   begin(options) / prepare / execute_statement / execute_query       │
│   commit / rollback / explain                                         │
├──────────────────────────────────────────────────────────────────┤
│ 起動・データ準備 (共通)                                                 │
│   gflags から configuration を作る / DB起動 / テーブル準備 / データ投入  │
└──────────────────────────────────────────────────────────────────┘
```

#### service_client

- 配置: `examples/common` に静的ライブラリを作り、各ツールからリンクする。`mock/` の `common` ライブラリ (`jogasaki-common-utils`) はテスト用のため使わない
- 1つのservice_clientオブジェクトは1つのsession_idを持ち、送るリクエストにそれを付ける
- 設計上の制約
  - 出力しない、スレッドを作らない、セッションやトランザクションの一覧を持たない
  - 戻り値は軽量な結果型 (成功/エラーコード)。成功時は文字列を作らない
  - 診断情報 (エラーメッセージ、reason_code、競合したキーやストレージなど) は失敗時に呼び出し側が求めたときだけ取り出す
  - クエリ結果のコールバックはテンプレートか関数参照で受け、`std::function` を必須にしない
  - 完了待ちの方法 (スピン待ち/ブロック待ち、タイムアウト) を引数で選べる。CLIはブロック待ち・長いタイムアウト、benchmarkはスピン待ちを使う

#### 起動・データ準備

- スケジューラ関連のgflags定義 (スレッド数、アフィニティ、stealing など) を共通ライブラリにまとめ、両ツールで共有する
  - 静的ライブラリ内のgflags定義は、そのオブジェクトファイルが参照されないとリンク時に落ちる。定義は起動処理と同じ翻訳単位に置く
- 組み込みテーブル (テスト用T0/T1/T2、ベンチマーク用TPC-C風テーブル) は `utils::add_test_tables()` / `utils::add_benchmark_tables()` (`mock/jogasaki/utils/add_test_tables.cpp`) で登録する
- `--prepare_data`、`--load_from` も共通化する。これらは組み込みテーブルが登録されているときだけ有効

#### 共通化しないもの

- CLI: 入力の解析、表示形式、セッション管理、auto commitの扱い
- benchmark: 実行ループ、統計、リトライ方針、出力形式
- スクリプトを解釈する部分はservice_cliの中に置く。将来ベンチマークのワークロード定義に使う必要が出たら、その時点で共通ライブラリへ移す

### service_cli のスクリプト/REPL

既存のtgsqlおよびtgsqlmux (`tsurugi-issues/test-code-sql/tgsqlmux.pl`) のスクリプトを変更なしで実行できることを目標とする。

これにより、QAやユーザーから届くtgsql/tgsqlmux形式の再現手順を同一プロセスで実行してデバッガやperfで調べられる。また同じスクリプトを実サーバ (tgsql) とservice_cliの両方で実行することで、SQLエンジン側の問題か通信/セッション層の問題かを切り分けられる。

旧形式の1文字コマンド (`b`, `c`, `p`, `q`, `s`, `v` など) は廃止する。`examples/service_cli/scripts/` の同梱スクリプトは新形式に書き直す。

#### 起動と入力

- 起動オプションはgflagsの書き方とし、意味だけtgsqlに合わせる
  - `--transaction=short|long|read|manual` (既定 short)、`--write_preserve=T1,T2`、`--auto_commit` (既定 false) など
- 組み込みテーブルは既定では登録しない (tgsqlと同様にDDLで作る前提)。`--builtin_tables` (on/off) を指定するとテスト用とベンチマーク用の両方を登録する
- 入力元
  - `--input_file` で指定したファイル
  - 標準入力が端末でない場合は標準入力をスクリプトとして読む (例: `cat x.sql | service_cli`)
  - それ以外は対話モード
- 対話モードの履歴は `$HOME/.service_cli_history` に保存する
- 無操作時の自動終了 (`--exit_on_idle`) は既定で無効とし、オプションとして残す

#### tgsqlと合わせる部分

文法は `tanzawa/docs/grammar-rule.md` に従う。

- 文は `;` で区切る (EOFも区切り)。コメントは `--`, `//`, `/* */`。文字列リテラル・引用符付き識別子中の `;` は区切りとしない
- `BEGIN [LONG] TRANSACTION` とトランザクションオプション (`READ ONLY [DEFERRABLE]`, `WRITE PRESERVE`, `READ AREA INCLUDE/EXCLUDE`, `INCLUDE DDL`, `EXECUTE PRIOR|EXCLUDING ...`, `AS <label>`, `WITH k=v,...`)
- `COMMIT [WAIT FOR ACCEPTED|AVAILABLE|STORED|PROPAGATED]`, `ROLLBACK`, `EXPLAIN`
- トランザクションの動作はtgsqlのconsoleと同じとする
  - `BEGIN` なしで実行した文は暗黙のトランザクション (種別は `--transaction` で指定) で実行し、文ごとにcommitする
  - `BEGIN` で明示的に開始したトランザクションは `COMMIT`/`ROLLBACK` まで続く
  - `--auto_commit` を指定すると、明示的に開始したトランザクションでも文ごとにcommitする
- メタコマンド
  - `\exit`: 後片付け (残っているトランザクションのrollbackとprepare済みの文の破棄) をしてから終了する
  - `\halt`: 後片付けをせずに即座に終了する
  - `\status`: 各セッションの状態 (トランザクション、実行中の文、経過時間) を表示する
  - `\help`, `\show table`, `\timing` を実装する
  - `\connect` は同一プロセス内で動くため何もしないコマンドとして受け付ける
- SQLの中身は解析しない。prepareして応答の `has_result_records` を見てクエリか文かを判断する (tgsqlの `BasicSqlProcessor` と同じ方式)

#### tgsqlmuxと合わせる部分 (セッションと並行実行)

- `NAME:` で送り先のセッションを指定する
  - 行頭の `NAME: <文>` はその行だけをNAMEに送る
  - `NAME:` だけの行は以降の送り先をNAMEに切り替える
  - 初めて出現した名前で新しいセッションを作る。初期セッション名は `T0`
- 名前が指すのはセッションであり、セッションは同時に1本のトランザクションを持つ
- セッションごとに別のsession_idを割り当てる (1から順に振る。0はjogasakiで「セッションなし」を意味する)
  - jogasakiはsession_idごとにトランザクションと文の置き場を分けるため、実サーバと同様にセッション間で分離される
  - prepare済みの文はセッションごとに持つ
  - mockの `test_request` はセッション単位の情報をリクエストごとに持つため、実サーバのセッション終了時の後片付けは再現されない。代わりにservice_cliが終了時に、残っているトランザクションをrollbackし、prepare済みの文を破棄する
- 実行方式: セッションごとに専用スレッドと入力キューを持つ
  - 同じセッションの文は順番に処理し、別のセッション同士は並行に進む
  - `use_preferred_worker_for_current_thread` により、セッションごとに別の担当ワーカーが付き、実際に複数のクライアントがいる状況に近くなる
  - 完了はプロンプト文字列ではなく実際の応答で判定する
- スクリプト実行時の待ち
  - 送り先セッションが処理中ならその完了を待つ (tgsqlmuxと同じ)
  - `--wait_timeout` (既定60秒、0で無制限) を超えたら全セッションの状態 (実行中の文と経過時間) を表示し、エラーとして終了する
  - スクリプトの最後では、明示しなくても全セッションの完了を待つ
- 対話モードでは待たない。文を送り先セッションのキューに積んですぐ次の入力を受け付け、結果は届いた順に表示する。状態は `\status` で確認する
- `;` までの未完了の文はセッションごとに保持する
- `--\sleep 秒数` を受け付ける

#### 独自拡張

- 原則として `--\xxx` 形式とする。tgsql/tgsqlmuxではコメントとして無視されるため、service_cli向けに書いたスクリプトもそのまま実サーバで実行できる
  - `--\wait`: 全セッションの完了を待つ。`--\wait NAME` で指定セッションのみ待つ
  - `--\expect_error`: 直後の文 (送り先セッションで次に実行される文) が失敗することを期待する。失敗すれば正常、成功すればエラーとして数える。エラーの種類は照合しない
- `--\sleep` はtgsqlmux由来、`--\wait` と `--\expect_error` はservice_cli独自である

#### `--\wait` と `--\sleep` の使い分け

- tgsqlmuxが文を送る前に待つのは「送り先セッションが前の文を処理し終えたか」だけである。同じセッション内の順序は保証されるが、別のセッションに切り替えるときは直前のセッションの文の完了を確認しない
  - そのためtgsqlmuxでセッション間の順序を合わせるには `--\sleep` で時間をおくしかなく、保証にはならない
- service_cliでは `--\wait` でセッション間の順序を確実に保証できる
- `--\wait` はtgsqlmuxではコメントとして無視される。スクリプトはエラーにならずに最後まで進むが、待ちは入らない
  - 送り先セッションの完了は必ず待つため、多くの場合は影響しない
  - 影響するのは `--\wait T1` の直後に別のセッションへ文を送る書き方である。service_cliではT1の完了後に送られるが、tgsqlmuxでは完了を待たずに送られる
  - 順序が結果に影響する箇所をtgsqlmuxでも実行したい場合は、`--\wait` に加えて `--\sleep` を併用する
- `--\sleep` はservice_cliでも残す。エポックの進行など時間の経過そのものが必要な場面があり (例: `issue849_snapshot_tombstone.sql` でOCCのCOMMITと次のLTXのBEGINの間に置かれた `--\sleep 1`)、これは `--\wait` では置き換えられない
- 実行結果に影響する拡張は `\xxx` 形式とし、tgsqlでは動かないことを許容する
  - `\prepare <name> [<var>:<型> ...] AS <SQL>;` で文をprepareする。型名はSQLの型名 (`INT`, `BIGINT`, `DOUBLE`, `VARCHAR`, `DECIMAL`, `DATE` など) で書く
  - `\execute <name> [<var>=<値> ...];` でprepare済みの文を実行する。`NULL` を指定できる
- 表示切り替え (生のバイト列など) は `\set` で行う

#### 出力とエラー

- 結果の表示 (行の書式、`(1 row)` など) はtgsqlに近づける。型ごとの値の書式 (DECIMAL、日付、NULLなど) は実装時にtgsqlの書式を調べて合わせる。それ以外 (ログ行など) は独自の形式とする
- 1つの文の結果 (結果の行、件数、エラー) をひとまとまりとして一度に出力し、他のセッションの出力が途中に割り込まないようにする。各行にセッション名を付ける。タイムスタンプはオプションで抑止でき、diffを取りやすくする
- エラー時はエラーコード、メッセージ、診断情報 (reason_code、競合したストレージやキーなど、応答に含まれるもの全部) を常に表示する
- `EXPLAIN` はjogasakiが返すJSONをそのまま表示する
- エラーが起きても次の文に進む (tgsqlのconsoleやtgsqlmuxと同じ)。`--stop_on_error` で打ち切る。エラーが1件でもあれば終了コードを0以外にする (`--\expect_error` で期待されたエラーは数えない)

#### 実装構造

- 入力を「コマンド構造体」に変換する解析部と、それを実行する実行部を分ける。対話入力とファイル入力で同じ経路を通し、端末なしでテストできるようにする
- メタコマンドは表 (名前、ハンドラ、説明) で登録し、helpはそこから生成する
- 入力の行編集と履歴は既存のlinenoise-ngを使う
- 作らないもの: タブ補完、シンタックスハイライト、変数やループを持つスクリプト言語、SQLの構文解析

#### 対象外

- BLOB/CLOB (`\store blob` など)。BLOB/CLOBの列を含む結果は型名とサイズ程度だけを表示する
- Ctrl-Cによるリクエストのキャンセル。Ctrl-Cは従来どおりプロセスを終了させる。必要になれば `\cancel NAME` のようなコマンドとして別途検討する
- 期待結果コメント (`--[3]` など) による結果内容の自動照合

### service_benchmark

今回の対象:

- service_clientを使うように変更し、変更前後でスループットが変わらないことを確認する
  - 合格基準: releaseビルドで `--query` と `--insert` を `--clients` 1と4で各3回実行し、スループットの中央値の差が5%程度以内であること (WSL2上の数値のぶれを考慮した許容差)。確認は移行時の1回のみで、継続的な監視はしない
  - 比較を正しく行うため、移行時はsession_idを付けない (現状どおり) まま移す。比較が終わってから、クライアントごとに別のsession_idを使うように切り替える。現状は全クライアントがセッションなしの共通の置き場を使っており、実サーバの動きと異なるうえ、その置き場の排他処理が計測結果に混ざる可能性があるため
- 集計誤りを修正する
- `db_->start()` の戻り値を確認し、古いフラグ説明を直す

将来の課題:

- 競合などによるアボートを数えてリトライする (abortしない)
- レイテンシのヒストグラム/パーセンタイル、begin/statement/commitごとの内訳を出力する
- 結果をJSON/CSVでも出力できるようにする
- 任意のSQLやワークロード定義ファイルを受け付ける。定義はservice_cliと同じ「SQL+パラメーター」の書き方とし、開始前に一度解析してprepare済みの文と値の生成方法に変換しておくことで実行時のコストをなくす
- ツール自身のオーバーヘッドを確認するため、serviceを呼ばずエンコードと結果判定だけを行うモード、またはエンコード時間を別に記録する機能を設ける
- (検討) query_bench_cliのように `api::database` を直接呼ぶモードを設け、service経由との差からservice層自体のオーバーヘッドを測る

### 注意点

- 本ツールはendpointを通らないため、SQL層の性能を見るためのものである。endpointを含む全体性能はtgsqlやtanzawa/simple_benchmarkなど別の手段で見る
- protobufのエンコードは本来クライアントプロセス側のコストだが、同一プロセスで動かす以上避けられない。またmockの `wait_completion` はスピン待ちのため、クライアントスレッドがサーバーのワーカーとCPUを取り合う。クライアントスレッドのアフィニティ (`--client_initial_core`) で分離する

## 進め方

段階ごとにコミット/PRを分ける。

1. 共通部品とservice_cliの最小限の修正
   - `examples/common` の静的ライブラリ (service_client、共通のgflags/起動処理、テーブル準備とデータ投入) を作成する
   - service_cliの不具合 (テーブル登録、`ExecuteResult` の処理、日付・時刻パラメーターでのabort、応答待ちのタイムアウト) をservice_clientを使って修正する。この時点ではまだ旧形式のコマンドのまま
2. service_benchmarkの共通部品への移行
   - service_clientに移行し、上記の合格基準で移行前後のスループットを比較する
   - 集計誤り、`db_->start()` の戻り値確認、フラグ説明を修正する
   - 比較後、クライアントごとのsession_idに切り替える
3. service_cliのスクリプト/REPLをtgsql/tgsqlmux互換に作り直す
   - `tsurugi-issues/test-code-sql/tgsqlmux/*.sql` の7本を手元で実行して確認する
4. ctestへの登録
   - ctestで回すスクリプトはjogasaki用に新しく書く (tsurugi-issuesのスクリプトはコピーしない。リポジトリ間の依存を避けるため、またissueの再現スクリプトは待ち時間で順序を合わせているなどctestでは不安定になりやすいため)
   - 意図的にエラーを起こすテストは `--\expect_error` を使う

service_benchmarkの機能拡張 (リトライ、パーセンタイル、ワークロード定義など) は今回のスコープ外とし、上記完了後に別途検討する。

## 将来の拡張方針: ベンチマークのバリエーション

### simple-benchmark互換

simple-benchmark (`project-tsurugi/simple-benchmark`) のCBで問題が報告された際に、ローカルでも同じ条件で再現させたいことがある。そのためsimple-benchmarkと互換のベンチマークを用意することを将来の拡張とする。

- simple-benchmarkは `Insert`, `Update`, `Select`, `Delete`, `UpsertSameKey`, `KvsGet`/`KvsPut` などのシナリオを持ち、単一テーブル/複数テーブルの変種がある。レイテンシはヒストグラムで集計し、結果をファイルに出力する
- 互換の範囲は、シナリオ名、主要なオプション、テーブル定義、出力形式 (CBの結果と比較できること) とする
- endpointを通らないため、endpointやセッション層に起因する問題はローカルでは再現しない。再現しないこと自体が問題の切り分けに使える

### バリエーションごとにプログラムを分ける

ベンチマークのバリエーションは無数にありうるため、service_benchmark 1つにすべてを詰め込まず、ニーズに応じて別のプログラムとして作る。

- 1つのプログラムに詰め込むと、モードごとの分岐やオプションの組み合わせが増え、計測ループに余計な処理が入りやすくなる。プログラムを分ければ、それぞれのループを単純に保てる
- プログラム名は連番 (service_benchmark2 など) ではなく目的を表す名前とする (例: simple-benchmark互換なら service_sbench)
- 既存のservice_benchmarkは `simple_benchmark_client.md` に記述された簡易ベンチマークとして残す
- 各プログラムは共通部品 (`service_client`、gflags定義と起動処理、テーブル準備とデータ投入) を使う
- ベンチマーク同士で共有したい部品 (クライアントスレッドの起動と開始同期、CPU割当て、レイテンシ集計、結果出力など) は、2つ目のベンチマークを作る時点で共通化する。今回のスコープでは先回りして作らない

## 付録: service_benchmark のCPU割当て

CPU割当ての最適解はbenchmarkの作り (クライアントの待ち方、ワーカーの動作モードなど) に依存するため、ここでは現状と基本的な考え方のみを記す。

### CPUを使い続けるスレッド

- クライアントスレッド: mockの `wait_completion` がスピン待ちのため、待機中もコアを1つ使い続ける
- ワーカースレッド: benchmarkの既定は `--busy_worker=true` で、タスクがなくてもスピンする
- 割当て対象外のスレッド: shirakamiのエポックスレッド、limestone、watcher、メンテナンススレッドなど。固定されずOSに任される

クライアント数+ワーカー数が物理コア数に近づくとスピンするスレッド同士がコアを取り合い、計測値が大きくぶれる。

### 現状の仕組み

- ワーカー: `force_numa_node`、`assign_numa_nodes_uniformly`、`core_affinity` (`initial_core` から連番で論理CPUに固定) のいずれか1つを指定する。同時指定はエラー (詳細は [core_assign_params.md](core_assign_params.md))
- クライアント: `--client_initial_core=-1` (既定) ではNUMAノードに均等に割当て、番号を指定するとその番号から連番で論理CPUに固定する
- クライアントとワーカーの組: `use_preferred_worker_for_current_thread=true` (既定) では、クライアントスレッドが最初に投げたリクエストの時点で `next_worker()` により担当ワーカーが決まる。クライアントが最初のリクエストを投げる順番は不定なため、どのクライアントがどのワーカーと組むかは実行ごとに変わる

### 注意点

- service_benchmarkの既定値は `assign_numa_nodes_uniformly=true` のため、`--core_affinity=true` だけを指定すると `database::start()` が失敗する。benchmarkは `start()` の戻り値を確認していないため、失敗に気付かず先に進む。コア単位で固定するには `--assign_numa_nodes_uniformly=false --core_affinity=true` を指定する。フラグの説明文 "setting true automatically sets core_affinity=true" も古い
- 「連番で固定」の意味は論理CPU番号の振り方に依存する
  - SMTの兄弟が (0,1), (2,3), ... と並ぶマシン (例: Ryzen 9 7950X) では、連番で割当てると2つのスレッドが同じ物理コアを共有する
  - 兄弟が n と n+コア数 で並ぶマシン (多くのIntelサーバー) では、連番でも物理コアは分かれる
  - 実際の並びは `lscpu -e` や `/sys/devices/system/cpu/cpuN/topology/thread_siblings_list` で確認する
- L3キャッシュの共有範囲に注意する。AMD Zen系はCCD (Core Complex Die) ごとにL3を持ち、7950Xでは物理コア0-7 (論理CPU 0-15) と8-15 (論理CPU 16-31) が別のL3になる。SMTの兄弟は同じ物理コアのためL1/L2/L3をすべて共有する
- NUMAノードが1つのマシンでは、ノード単位の割当て (既定) は実質的に固定しないのと同じになる
- WSL2などの仮想環境では、割当て先は仮想CPUになり、ホストのスケジューリングの影響も受ける。またキャッシュ構成が正しく見えない場合がある (7950X上のWSL2ではL3が32MB×1、全CPU共有と報告される)。正式な計測は物理マシンで行う

### 推奨する構成

| 目的 | ワーカー | クライアント | 備考 |
|---|---|---|---|
| 性能の比較・回帰確認 (基本) | 物理コア単位でコアに固定 | ワーカーとは別の物理コアで、組になるワーカーと同じL3に置く | クライアント数+ワーカー数は物理コア数より1-2少なくし、割当て対象外のスレッド用に空けておく |
| 本番に近い条件 | 固定しない (tsurugidbの既定) | 固定しない | 上との差でCPU割当ての効果が分かる |
| NUMAの実験 (複数ノードのサーバー) | `force_numa_node` またはノード均等 | 組になるワーカーと同じノード | |

クライアントとワーカーをSMTの兄弟に置く配置は、L1/L2を共有するため受け渡しは速いが、両方スピンしているため実行資源を取り合う。基本は別の物理コアとし、兄弟に置く配置は実験の一つとして扱う。

### benchmark側の改善案

`db_->start()` の戻り値確認と古いフラグ説明の修正は今回のスコープで行う。それ以外は将来の課題とする。

- `db_->start()` の戻り値を確認し、設定が矛盾していたら停止する。矛盾する既定値と古いフラグ説明を直す
- 起動時に実際の割当て (各スレッドの `sched_getcpu()`、物理コア、クライアントとワーカーの組) を表示し、SMTの兄弟同士やクライアントとワーカーの重なりを警告する
- クライアントのCPUを個別に指定できるようにする (例: `--client_cpus=16,18,20`)
- 開始前にクライアントを1つずつ順番にウォームアップさせ、クライアントiが必ずワーカーiと組むようにする
- ワーカーのCPUを個別に指定するにはjogasaki/tateyamaの変更が必要なため範囲外とする。当面はプロセス全体を `taskset` でCPU範囲に閉じ込め、`initial_core` を物理コアの境目に合わせて対応する

## 参考

- `tanzawa/docs/grammar-rule.md`: tgsqlの文法
- `tanzawa/modules/tgsql/README.md`: tgsqlの起動オプション
- `tsurugi-issues/test-code-sql/README.md`, `tgsqlmux.pl`: tgsqlmuxの仕様と実装
- `docs/internal/simple_benchmark_client.md`: service_benchmarkの元になった簡易ベンチマーククライアントの説明
- `examples/scan_cli`: 対話モードでデータを保持したままオプションを変えて再実行する仕組み (`o`/`p`/`r` コマンド)
- `examples/query_bench_cli`: `api::database` を直接呼ぶベンチマーク
