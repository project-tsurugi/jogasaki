# service_benchmark

同一プロセス内で起動したjogasakiに対して、SQLサービスAPI経由で固定のワークロードを複数のクライアントスレッドから実行し、スループットと平均ターンアラウンドを測る簡易ベンチマーク。

SQLサービスAPI以下は本物だが、tateyamaのendpoint (IPC/stream)、セッション管理、ルーティングは通らない。SQL層の性能を見るためのものであり、endpointを含む全体の性能はtgsqlやsimple-benchmark (ローカルでの再現は [nsbench](../nsbench/README.md)) など別の手段で見る。

設計の詳細は [docs/internal/service-cli-benchmark-design.md](../../docs/internal/service-cli-benchmark-design.md) を参照。

## ビルドと実行

`-DBUILD_EXAMPLES=ON` でビルドすると `<build>/examples/service_benchmark/cli` ができる。

```sh
# point queryを1クライアントで5秒間
examples/service_benchmark/cli --query --prepare_data=100

# updateを4クライアント・4ワーカーで、LTXを使って10秒間
examples/service_benchmark/cli --update --prepare_data=100 --clients=4 --thread_count=4 --ltx --duration=10000

# DDLでテーブルを作り、2次索引ありでinsert
examples/service_benchmark/cli --insert --ddl --secondary
```

## ワークロード

`--insert`、`--upsert`、`--update`、`--query`、`--query2` のいずれか1つを指定する。テーブルはTPC-C風の組み込みテーブル (`--ddl` を指定した場合はDDLで作成したテーブル) を使う。

| モード | SQL |
|---|---|
| `--insert` | `INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id) VALUES (...)` |
| `--upsert` | `INSERT OR REPLACE INTO NEW_ORDER ...` |
| `--update` | `UPDATE STOCK SET s_quantity = :s_quantity WHERE s_i_id = :s_i_id AND s_w_id = :s_w_id` |
| `--query` | `SELECT d_next_o_id, d_tax FROM DISTRICT WHERE d_w_id = :d_w_id AND d_id = :d_id` (point query) |
| `--query2` | `SELECT no_o_id FROM NEW_ORDER WHERE no_d_id = :no_d_id AND no_w_id = :no_w_id ORDER BY no_o_id` (複数行) |

- 各クライアントは `--statements` 個の文を1トランザクションとして実行し、これを `--duration` の間 (または `--transactions` 回) 繰り返す
- クライアントごとに別のセッション (session_id) を使う。文はクライアントごとに開始前に一度だけprepareする
- `--duration` で実行する場合、時間が来るとトランザクションの途中でも文の発行をやめてcommitする
- `--update`/`--query`/`--query2` は対象のレコードがないと空振りになるため、通常は `--prepare_data` または `--load_from` でデータを投入する。`--update` は `--prepare_data` で投入された範囲のキーを選ぶ
- `--insert` はクライアントごとに異なる `no_w_id` を使い、主キーが衝突しないようにしている
- 応答はスピン待ちで待つ (クライアントスレッドは待機中もCPUを1つ使い続ける)
- エラー (OCCの競合によるcommit失敗を含む) が起きるとその時点で `abort()` する。リトライやアボート数の集計はしない

## 出力

既定ではログに出力する。

```
duration: 1,017 ms
  avg. begin-commit interval : 1,016,127,731 ns/transaction
  avg. statements interval : 1,504,167 ns/statement
executed: 1 transactions, 664 statements, 1,992 query result writes
throughput: 0 transactions/s, 652 statements/s
throughput/thread: 0 transactions/s/thread, 652 statements/s/thread
avg turn-around: transaction 1,017,000,000 ns, statement 1,531,626 ns
```

| 項目 | 意味 |
|---|---|
| `avg. begin-commit interval` | begin、文の実行、commitにかかった時間の合計をトランザクション数で割ったもの |
| `avg. statements interval` | 文の実行時間の合計を文の数で割ったもの |
| `executed` | 実行したトランザクション数、文の数、クエリ結果のレコード数 |
| `throughput` | 全クライアント合計の秒あたりの件数 |
| `throughput/thread` | クライアントあたりの秒あたりの件数 |
| `avg turn-around` | 経過時間 × クライアント数 ÷ 件数 |

`--md` を指定するとMarkdownの表で標準出力に出す。

```
|stmt|tx type|duration(ms)|threads|clients|statements/tx|executed stmts|throughput(stmts/s/thread)|
|-|-|-|-|-|-|-|-|
|UPDATE|OCC|546|1|1|1000|240|439|
```

## オプション

service_benchmark固有のオプション:

| オプション | 既定値 | 説明 |
|---|---|---|
| `--insert`/`--upsert`/`--update`/`--query`/`--query2` | `false` | ワークロード (上記) |
| `--statements` | `1000` | 1トランザクションあたりの文の数 |
| `--duration` | `5000` | 実行時間 (ms) |
| `--transactions` | `-1` | クライアントあたりのトランザクション数。-1なら `--duration` を使う |
| `--clients` | `1` | クライアントスレッド数 |
| `--client_initial_core` | `-1` | クライアントスレッドを指定したコアから連番で固定する。-1ならNUMAノードに均等に割り当てる |
| `--ltx` | `false` | OCCの代わりにLTXを使う (`--rtx` と同時に指定できない) |
| `--rtx` | `false` | OCCの代わりにRTXを使う (`--ltx` と同時に指定できない) |
| `--client_idle` | `0` | 各文の実行後にクライアントが空ループ (`_mm_pause`) を回す回数 |
| `--ddl` | `false` | 組み込みテーブルの代わりにDDLでテーブルを作る。`--secondary` に必要 |
| `--secondary` | `false` | 2次索引を使う |
| `--md` | `false` | 結果をMarkdownの表で出力する |
| `--verify` | `false` | クエリ結果のレコードをデシリアライズする (内容は `--v` などでデバッグログを有効にすると表示される)。`--clients=1` が必要 |
| `--minimum` | `false` | 最小構成 (1ワーカー、1クライアント、insertを1文1トランザクション) で実行する。ビルド確認用 |

共通オプションは [service_common](../service_common/README.md#共通のgflags) を参照。service_benchmarkでは以下の既定値を変えている。

| オプション | service_benchmarkの既定値 |
|---|---|
| `--thread_count` | `1` |
| `--assign_numa_nodes_uniformly` | `true` |
| `--busy_worker` | `true` |
| `--builtin_tables` | `--ddl` を指定しない限り常に `true` |

## CPU割当て

クライアントスレッド (スピン待ち) とワーカースレッド (`--busy_worker=true`) はどちらもCPUを使い続けるため、クライアント数+ワーカー数が物理コア数に近づくと計測値が大きくぶれる。

- ワーカーをコア単位で固定するには、既定の `--assign_numa_nodes_uniformly=true` を外して `--assign_numa_nodes_uniformly=false --core_affinity=true --initial_core=N` を指定する (同時指定はDBの起動エラーになる)
- クライアントは `--client_initial_core` でワーカーとは別のコアに固定する

```sh
examples/service_benchmark/cli --query --load_from db/default/w1 --thread_count 1 --duration 10000 \
    --assign_numa_nodes_uniformly=false --core_affinity=true --initial_core=30 --client_initial_core=3
```

SMTの兄弟やL3キャッシュの共有範囲、WSL2での注意点、推奨する構成は設計文書の「付録: service_benchmark のCPU割当て」を参照。

## テスト

ctestに各モード (`service-benchmark-<mode>`)、LTX/RTX、DDL+2次索引、複数クライアントを短時間で実行するテストを登録している。

## 対象外 (将来の課題)

- 競合などによるアボートの集計とリトライ
- レイテンシのヒストグラム/パーセンタイル、begin/statement/commitごとの内訳
- JSON/CSV出力
- 任意のSQLやワークロード定義ファイル

ベンチマークのバリエーションは本プログラムに詰め込まず、目的ごとに別のプログラムとして作る方針 (例: simple-benchmark互換の [nsbench](../nsbench/README.md))。
