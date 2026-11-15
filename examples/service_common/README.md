# service_common

SQLサービスAPIを同一プロセス内で呼び出すツール ([service_cli](../service_cli/README.md)、[service_benchmark](../service_benchmark/README.md)、[nsbench](../nsbench/README.md)) が共有する静的ライブラリ (`service-common`)。

設計の背景は [docs/internal/service-cli-benchmark-design.md](../../docs/internal/service-cli-benchmark-design.md) を参照。

## 構成

| ファイル | 内容 |
|---|---|
| `service_client.h/.cpp` | SQLサービスへのリクエスト送信 (begin/prepare/execute/query/commit/rollback/explain、テーブルメタデータ取得など) |
| `service_setup.h/.cpp` | 共通のgflags定義、`jogasaki::configuration` の作成、DBの起動・停止、組み込みテーブルの登録とデータ投入 |
| `../common/utils.cpp` | データのロード・ダンプ (`--load_from`) |

`../common` に置かないのは、他のexamples (aggregate_cli、join_cli など) が `../common/*.cpp` をGLOBで取り込んでおり、gflagsの多重定義などになるため。

## service_client

- 1つの `service_client` は1つのsession_idを持ち、送るリクエストすべてにそれを付ける。複数セッションを扱う場合はセッションごとに `service_client` を作る (session_idは1以上。0はjogasakiで「セッションなし」を意味する)
- リクエストはtateyamaのmock (`test_request`/`test_response`) を使って `jogasaki::api::impl::service` に直接渡す。SQLサービスAPI以下は本物だが、endpoint (IPC/stream) やセッション管理は通らない
- 1リクエストごとの処理を薄く保つための制約
  - 出力しない、スレッドを作らない、セッションやトランザクションの一覧を持たない
  - 戻り値は軽量な `call_result`。エラーメッセージや診断情報 (reason_code、競合したストレージやキーなど) は失敗時に `diag()` で取り出す
  - クエリ結果はコールバックで受ける
- 応答の待ち方を `wait_kind` で選べる
  - `spin`: ビジーループで待つ (ベンチマーク向け。CPUを1つ使い続ける)
  - `block`: 条件変数で待ち、応答の完了時に起こされる (CLI向け)
  - タイムアウトはミリ秒で指定し、0で無制限

## 共通のgflags

`service_setup.cpp` で定義し、各ツールで同じ名前・意味で使える。ツールによって既定値を上書きしているものがある (各ツールのREADMEを参照)。

### データベース

| オプション | 既定値 | 説明 |
|---|---|---|
| `--location` | `TMP` | データベースのディレクトリ。`TMP` を指定すると一時ディレクトリを作り、終了時に削除する |
| `--epoch_duration` | `0` | kvsのエポック間隔 (us)。0でkvsの既定値 (shirakamiは40000us)。指定した場合はこのライブラリでsharksfinのDBを開いてjogasakiに渡す |
| `--commit_response` | (空) | 既定のcommit応答: `accepted`、`available`、`stored`、`propagated`。空なら既定 (stored) |
| `--builtin_tables` | `false` | 組み込みテーブル (テスト用のT0/T1/T2など、ベンチマーク用のTPC-C風テーブル) を登録する |
| `--prepare_data` | `0` | 組み込みテーブルに投入するレコード数。0で投入しない。`--builtin_tables` が必要 |
| `--load_from` | (空) | 初期データを読み込む生成済みDBファイルのディレクトリ。`--builtin_tables` が必要 |

`--prepare_data` で投入するレコードの主キー列は `[0, 100)` の値を繰り返す (`prepared_data_modulo`)。

### スケジューラ

| オプション | 既定値 | 説明 |
|---|---|---|
| `--thread_count` | `5` | ワーカースレッド数 |
| `--single_thread` | `false` | シリアルスケジューラで実行する |
| `--core_affinity` | `false` | ワーカーを `--initial_core` から連番でコアに固定する |
| `--initial_core` | `1` | `--core_affinity` の開始コア番号 |
| `--assign_numa_nodes_uniformly` | `false` | ワーカーをNUMAノードに均等に割り当てる |
| `--force_numa_node` | `-1` | すべてのワーカーを指定したNUMAノードに割り当てる。-1で無効 |
| `--partitions` | `5` | プロセスあたりのパーティション数 |
| `--steal` | `true` | タスクのstealingを有効にする |
| `--stealing_wait` | `1` | stealingの前にローカルキューを確認する回数の係数 |
| `--task_polling_wait` | `0` | ワーカーのビジーループでの待ち方/時間 |
| `--use_preferred_worker_for_current_thread` | `true` | 要求元のスレッドに応じて担当ワーカーを選ぶ |
| `--enable_hybrid_scheduler` | `true` | serial-stealingハイブリッドスケジューラを有効にする |
| `--lightweight_job_level` | `0` | 軽量とみなすジョブのレベル |
| `--busy_worker` | `false` | タスクがなくてもワーカーをサスペンドさせない |
| `--watcher_interval` | `1000` | watcherスレッドの起床間隔 (us) |
| `--worker_try_count` | `1000` | サスペンド前にタスクキューを確認する回数 |
| `--worker_suspend_timeout` | `1000000` | サスペンドしたワーカーが起床するまでの時間 (us) |
| `--scan_block_size` | `100` | scanが他のタスクに譲るまでに処理する最大レコード数 |
| `--scan_yield_interval` | `1` | scanが他のタスクに譲るまでの最大時間 (ms) |
| `--scan_default_parallel` | `4` | scanタスクの最大並列数 |
| `--max_result_set_writers` | `64` | result set writerの最大数 |

`--core_affinity`、`--assign_numa_nodes_uniformly`、`--force_numa_node` は同時に指定できない (DBの起動が失敗する)。詳細は [docs/internal/core_assign_params.md](../../docs/internal/core_assign_params.md) を参照。

## 注意

- 静的ライブラリ内のgflags定義は、そのオブジェクトファイルが参照されないとリンク時に落ちる。そのため定義は起動処理 (`fill_configuration` など) と同じ翻訳単位に置いている
- 本ライブラリを使うツールはendpointを通らないため、SQL層の挙動・性能を見るためのものである。endpointを含む全体の挙動はtgsqlやsimple-benchmarkなど別の手段で確認する
