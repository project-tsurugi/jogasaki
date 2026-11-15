# service_cli

同一プロセス内で起動したjogasakiに対して、SQLサービスAPI経由でSQLを実行する対話シェル/スクリプト実行ツール。tgsqlのスクリプトとtgsqlmuxのセッション指定 (`NAME:`) をそのまま受け付ける。

主な用途:

- QAやユーザーから届くtgsql/tgsqlmux形式の再現手順を、同一プロセスで実行してデバッガやperfで調べる
- 同じスクリプトを実サーバ (tgsql) とservice_cliの両方で実行し、SQLエンジン側の問題か通信/セッション層の問題かを切り分ける
- 複数セッションでのトランザクションの交互実行を確認する

SQLサービスAPI以下は本物だが、tateyamaのendpoint (IPC/stream)、セッション管理、ルーティングは通らない。設計の詳細は [docs/internal/service-cli-benchmark-design.md](../../docs/internal/service-cli-benchmark-design.md) を参照。

## ビルドと実行

`-DBUILD_EXAMPLES=ON` でビルドすると `<build>/examples/service_cli/cli` ができる。

```sh
# 対話モード
examples/service_cli/cli

# スクリプトファイルを実行
examples/service_cli/cli --input_file script.sql

# 標準入力が端末でなければスクリプトとして読む
cat script.sql | examples/service_cli/cli
```

入力元は次の順で決まる。

1. `--input_file` で指定したファイル
2. 標準入力が端末でない場合は標準入力
3. それ以外は対話モード (linenoise-ngによる行編集。履歴は `$HOME/.service_cli_history`)

既定では一時ディレクトリにDBを作り、終了時に削除する。テーブルはtgsqlと同様にDDLで作る前提で、組み込みテーブルは登録しない (`--builtin_tables` で登録できる)。

## スクリプトの例

```sql
-- 暗黙のトランザクション: 文ごとにcommitされる
CREATE TABLE T0 (C0 BIGINT PRIMARY KEY, C1 DOUBLE);
INSERT INTO T0 (C0, C1) VALUES (1, 1);

-- 明示的なトランザクション: COMMIT/ROLLBACKまで続く
BEGIN LONG TRANSACTION WRITE PRESERVE T0;
INSERT INTO T0 (C0, C1) VALUES (2, 2);
COMMIT;

SELECT * FROM T0 ORDER BY C0;
```

出力 (`--display_implicit` 既定のまま):

```
T0: > CREATE TABLE T0 (C0 BIGINT PRIMARY KEY, C1 DOUBLE);
T0: start transaction implicitly. type=OCC transaction_id=TID-0000000000000004
T0: execute succeeded
T0: transaction commit finished implicitly.
...
T0: > SELECT * FROM T0 ORDER BY C0;
T0: start transaction implicitly. type=OCC transaction_id=TID-0000000000000006
T0: [C0: BIGINT, C1: DOUBLE]
T0: [1, 1]
T0: [2, 2]
T0: (2 rows)
T0: transaction commit finished implicitly.
```

各行の先頭は送り先のセッション名。スクリプト実行時は入力した文を `> ` の後にエコーする。1つの文の結果 (結果の行、件数、エラー) はまとめて出力され、他のセッションの出力が途中に割り込まない。

## 文法

文法はtgsql (`tanzawa/docs/grammar-rule.md`) に従う。

- 文は `;` で区切る (ファイルの終わりも区切りになる)。複数行にわたって書ける
- コメントは `--`、`//`、`/* */`。文字列リテラルや引用符付き識別子の中の `;` は区切りとしない
- SQLの中身は解析しない。prepareした応答を見て、クエリか文かを判断する

### 文

| 文 | 説明 |
|---|---|
| `<sql>;` | SQLを実行する |
| `BEGIN [LONG] TRANSACTION [options];` | トランザクションを開始する |
| `COMMIT [WAIT FOR ACCEPTED\|AVAILABLE\|STORED\|PROPAGATED];` | commitする |
| `ROLLBACK;` | rollbackする |
| `EXPLAIN <sql>;` | 実行計画をjogasakiが返すJSONのまま表示する |
| `\prepare <name> [<var>:<type> ...] AS <sql>;` | プレースホルダー付きの文をprepareする (service_cli独自) |
| `\execute <name> [<var>=<value> ...];` | prepare済みの文を実行する (service_cli独自) |

`BEGIN` のオプション:

- `READ ONLY [DEFERRABLE|IMMEDIATE]`、`READ WRITE`
- `WRITE PRESERVE <tables>`
- `READ AREA INCLUDE <tables> EXCLUDE <tables>`
- `INCLUDE DDL` (`INCLUDE DEFINITION[S]`)
- `EXECUTE PRIOR|EXCLUDING [DEFERRABLE|IMMEDIATE]`
- `AS <label>`
- `WITH k=v,...` (受け付けるが無視する)

`START [LONG] TRANSACTION` も使える。`READ ONLY IMMEDIATE` はLTX、`READ ONLY [DEFERRABLE]` はRTXとして開始する。

### prepared statement

```sql
\prepare ins c0:INT c5:VARCHAR c6:DATE c8:TIMESTAMP
    AS INSERT INTO TT (C0, C5, C6, C8) VALUES (:c0, :c5, :c6, :c8);
\execute ins c0=1 c5='a b' c6=2024-01-02 c8=2024-01-02T03:04:05.123;
\execute ins c0=2 c5=NULL c6=NULL c8=NULL;
```

- 型名はSQLの型名で書く: `BOOLEAN`、`INT`/`INTEGER`、`BIGINT`、`REAL`/`FLOAT`、`DOUBLE`、`DECIMAL`、`CHAR`/`CHARACTER`/`VARCHAR`、`VARBINARY`、`DATE`、`TIME`、`TIMESTAMP` など
- 値に空白を含む場合は `'...'` で囲む (`''` で `'` を表す)。`NULL` を指定できる
- 日付は `YYYY-MM-DD`、時刻は `HH:MM:SS[.fraction]`、タイムスタンプは `YYYY-MM-DDTHH:MM:SS[.fraction]`
- prepare済みの文はセッションごとに持つ

### メタコマンド (行末まで)

| コマンド | 説明 |
|---|---|
| `\show table [<table name>]` | テーブルの一覧、またはテーブル定義を表示する |
| `\status` | 各セッションの状態 (トランザクション、実行中の文と経過時間) を表示する |
| `\timing [on\|off]` | 文の実行時間の表示を切り替える |
| `\help` | ヘルプを表示する |
| `\exit`, `\quit` | 残っているトランザクションのrollbackとprepare済みの文の破棄をしてから終了する |
| `\halt` | 後片付けをせずに即座に終了する |
| `\connect`, `\disconnect` | 同一プロセス内で動くため何もしない (tgsqlのスクリプト互換のため受け付ける) |

## トランザクションの動作

tgsqlのconsoleと同じ。

- `BEGIN` なしで実行した文は暗黙のトランザクションで実行し、文ごとにcommitする。暗黙のトランザクションの種別は `--transaction` で指定する (`manual` なら暗黙に開始しない)
- `BEGIN` で明示的に開始したトランザクションは `COMMIT`/`ROLLBACK` まで続く
- `--auto_commit` を指定すると、明示的に開始したトランザクションでも文ごとにcommitする
- 明示的なトランザクション中に文がエラーになると、そのトランザクションは無効になり、後続の `COMMIT` はエラーになる

## 複数セッション (tgsqlmux互換)

`NAME:` で送り先のセッションを指定する。初めて出現した名前で新しいセッションを作る。初期セッション名は `T0`。

- 行頭の `NAME: <文>` はその行だけをNAMEに送る
- `NAME:` だけの行は以降の送り先をNAMEに切り替える
- 各セッションは別のsession_idを持ち、同時に1本のトランザクションを持つ
- セッションごとに専用スレッドがあり、同じセッションの文は順番に、別のセッション同士は並行に実行される

```sql
CREATE TABLE tb1 (pk INT PRIMARY KEY);
INSERT INTO tb1 VALUES (1);
--\wait

Tx1:
 BEGIN LONG TRANSACTION WRITE PRESERVE tb1;
 INSERT INTO tb1 VALUES (2);
Tx2:
 BEGIN LONG TRANSACTION WRITE PRESERVE tb1;
 SELECT * FROM tb1;
 INSERT INTO tb1 VALUES (3);
 --\expect_error
 COMMIT;
Tx1:
 COMMIT;
--\wait
T0:
SELECT * FROM tb1 ORDER BY pk;
```

### 待ちの動作

- スクリプト実行時
  - 文を送る前に、送り先セッションが処理中ならその完了を待つ (tgsqlmuxと同じ)
  - 別のセッションに切り替えるときは、直前のセッションの完了を待たない
  - `--wait_timeout` (既定60秒) を超えたら全セッションの状態を表示してエラー終了する
  - スクリプトの最後では全セッションの完了を待つ
- 対話モード
  - 入力した行で文を送ったセッションの完了を `--prompt_wait` (既定1秒) まで待ってからプロンプトを出す
  - 時間内に終わらない場合 (他のトランザクション待ちなど) は待つのをやめてプロンプトを出す。後から届いた結果は届いた順に表示される。状態は `\status` で確認する

### ディレクティブ (`--\xxx`)

tgsql/tgsqlmuxではコメントとして無視されるため、service_cli向けに書いたスクリプトもそのまま実サーバで実行できる。

| ディレクティブ | 説明 |
|---|---|
| `--\sleep <seconds>` | 指定秒数待つ (tgsqlmux由来) |
| `--\wait [<name>]` | 全セッション (またはNAMEのみ) の完了を待つ |
| `--\expect_error` | 送り先セッションで次に実行される文が失敗することを期待する。失敗すれば正常、成功すればエラーとして数える (エラーの種類は照合しない) |

`--\wait` と `--\sleep` の使い分け:

- セッション間の順序を保証したい箇所には `--\wait` を書く。service_cliは同一プロセスで動くためtgsqlmuxより速く、既存のtgsqlmuxスクリプトが暗黙に頼っていた順序 (DDLなど短い処理は次のセッションに切り替わる前に終わっている) が崩れることがある
- `--\wait` はtgsqlmuxでは無視されるため、tgsqlmuxでも順序を合わせたい場合は `--\sleep` を併用する
- エポックの進行など、時間の経過そのものが必要な場面では `--\sleep` を使う

## エラー

- エラー時はエラーコード、メッセージ、診断情報 (reason_code、競合したストレージやキーなど) を表示する
- エラーが起きても次の文に進む。`--stop_on_error` で打ち切る
- エラーが1件でもあれば終了コードは0以外になる (`--\expect_error` で期待されたエラーは数えない)

```
Tx2: > COMMIT;
Tx2: error: CC_EXCEPTION: serialization failed transaction:TID-0000000100000002 shirakami response Status=ERR_CC {reason_code:CC_LTX_PHANTOM_AVOIDANCE, ...}
Tx2:   supplemental text: {"source_file":".../executor.cpp:977"}
Tx2: (error is expected)
```

## オプション

service_cli固有のオプション:

| オプション | 既定値 | 説明 |
|---|---|---|
| `--input_file` | (空) | 実行するスクリプトファイル |
| `--transaction` | `short` | 暗黙に開始するトランザクションの種別: `short` (OCC)、`long` (LTX)、`read` (RTX)、`manual` (暗黙に開始しない) |
| `--write_preserve` | (空) | `--transaction=long` で暗黙に開始するトランザクションのwrite preserve (カンマ区切りのテーブル名) |
| `--auto_commit` | `false` | 明示的に開始したトランザクションでも文ごとにcommitする |
| `--stop_on_error` | `false` | エラーで処理を打ち切る |
| `--wait_timeout` | `60000` | スクリプト実行時にセッションの完了を待つ時間 (ms)。0で無制限 |
| `--prompt_wait` | `1000` | 対話モードでプロンプトを出す前にセッションの完了を待つ最大時間 (ms)。0で無制限 |
| `--display_implicit` | `true` | 暗黙のトランザクションの開始・終了のメッセージを表示する (tgsqlの `display.implicit` 相当) |
| `--timestamp` | `false` | 各行に時刻を表示する |
| `--raw` | `false` | 結果を生のバイト列でも表示する |
| `--history_file` | (空) | 履歴ファイル。空なら `$HOME/.service_cli_history` |
| `--exit_on_idle` | `-1` | 対話モードで指定秒数操作がなければ終了する。-1で無効 |
| `--test_build` | `false` | ビルド確認用 (起動して終了する) |

DBの場所、組み込みテーブル、スケジューラ設定などの共通オプションは [service_common](../service_common/README.md#共通のgflags) を参照。

エンジンのINFOログは結果の表示に混ざらないよう既定で抑止している (`--minloglevel=1`)。表示する場合は `--minloglevel=0` を指定する。

## 同梱スクリプトとテスト

`scripts/*.sql` はctestに `service-cli-script-<name>` として登録され、想定外のエラーなく完了することを確認する。

| スクリプト | 内容 |
|---|---|
| `basic.sql` | 暗黙/明示的なトランザクション、prepared statement、クエリ |
| `sessions.sql` | 複数セッションでのLTXの並行実行 |
| `errors.sql` | エラーとエラー後のトランザクションの状態 |
| `types.sql` | 各種の型のパラメーターと結果 |
| `qa_tables.sql` | 2次索引を使うクエリ |

意図的にエラーを起こす文には `--\expect_error` を付ける。

## 対象外

- BLOB/CLOB (`\store blob` など)
- Ctrl-Cによるリクエストのキャンセル (Ctrl-Cはプロセスを終了させる)
- 期待結果コメント (`--[3]` など) による結果の自動照合
- タブ補完、シンタックスハイライト、変数やループを持つスクリプト言語
