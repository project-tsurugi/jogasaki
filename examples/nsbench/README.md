# nsbench

simple-benchmark (`project-tsurugi/simple-benchmark`、以下sbench) と互換のベンチマークを、同一プロセス内で起動したjogasakiに対してSQLサービスAPI経由で実行するツール (native simple bench)。

sbenchのCBで問題が報告された際に、ローカルで同じ条件を再現するために使う。コマンド名、主要なオプション、テーブル定義とSQL、出力形式 (CBの結果と比較できること) をsbenchに合わせている。

endpoint (IPC/stream) やセッション層は通らないため、それらに起因する問題はnsbenchでは再現しない。再現しないこと自体が問題の切り分けに使える。

設計の詳細は [docs/internal/service-cli-benchmark-design.md](../../docs/internal/service-cli-benchmark-design.md) の「simple-benchmark互換 (nsbench)」を参照。

## ビルドと実行

`-DBUILD_EXAMPLES=ON` でビルドすると `<build>/examples/nsbench/nsbench` ができる。

```sh
nsbench <command>... [options]
```

```sh
# テーブルを作り、insertとpoint selectを実行
nsbench prepare insert select --operations 10000

# 4テーブルを作り、テーブルごとに1スレッドでinsert/update
nsbench prepare insert-mt update-mt --tables 4 --operations 100000

# sysbenchエンティティで、LTXを使って10秒間update
nsbench prepare insert update -e sysbench -t LTX --operations 100000 -d 10
```

### 複数コマンドとDBの場所

複数のコマンドを並べると、同じDB上で順に実行する。

既定の `--location=TMP` では実行ごとに一時ディレクトリにDBを作り、終了時に削除する。そのため、sbenchのように `prepare` と各ベンチマークを別プロセスで実行するとテーブルが存在しない。1回の実行にまとめるか、`--location` で永続ディレクトリを指定する。

```sh
nsbench prepare insert --location /tmp/nsbench-db --operations 10000
nsbench select --location /tmp/nsbench-db --operations 10000 --scan_range 10
```

コマンドごとにオプションを変えることはできない。変えたい場合は `--location` を指定して別々に実行する。

## コマンド

| コマンド | 説明 |
|---|---|
| `prepare` | テーブル (`--tables` 個) を作り直す。`--index` で2次索引を作る。DDLは `--transaction` に関わらずOCCで実行する |
| `insert`, `insert-st`, `insert-mt` | 主キーを連番で、それ以外の列を乱数で生成してinsert (`--insert_mode UPSERT` でinsert or replace) |
| `update`, `update-st`, `update-mt` | 主キーを連番で指定し、それ以外の列を乱数で更新 |
| `select`, `select-st`, `select-mt` | `--scan_range` に応じてfull scan/point read/range scan。結果はすべて読み捨てる |
| `delete`, `delete-mt` | `--scan_range` に応じてall delete/point delete/scan delete。`delete-mt` はall deleteのみ |
| `noptx`, `noptx-st` | 文を実行せずにbegin/commitだけを繰り返す |

サフィックスの意味:

- なし: 1スレッドで `--table_id` のテーブルを使う。主キーは `--id_origin` から始まる
- `-st`: `--threads` 個のスレッドが `table_id=1` のテーブルを使う。スレッドiの主キーは `i * --id_coefficient` から始まる
- `-mt`: `--tables` 個のスレッドがそれぞれテーブルiを使う。主キーは1から始まる

スレッドは一斉に開始する。各スレッドは別のセッション (session_id) を使う。

sbenchの `kvs-*`、`connect*`、`upsert-samekey*`、`stub-*` は対象外。

## エンティティ

| `--entity` (`-e`) | テーブル |
|---|---|
| `lkv` (既定) | `lkv<N>(k bigint primary key, v bigint not null)` |
| `sysbench` | `sbtest<N>(id int primary key, k int not null, c char(120) not null, pad char(60) not null)` |

`<N>` はテーブルID (1から)。`--without-pk` で主キーなしのテーブルを作る。`lob` エンティティは対象外。

LTXでは対象テーブルをwrite preserveに指定する。`--read_area inclusive` を指定すると対象テーブルをread area (inclusive) にも指定する。

## 出力

sbenchのログと同じ列と書式 (`"1,234"` 形式) で標準出力に出す。

```
insert-st 1000 operations with '2 threads' start
report of thread-1
Benchmark,Sub,Elapsed(ms),Operations,OPS,Ope/Tx,TPS,Avg-Tx(μs),Avg-Bc(μs),Min-Tx(μs),Min-Bc(μs),Max-Tx(μs),Max-Bc(μs),50Pctl-Tx(μs),50Pctl-Bc(μs),95Pctl-Tx(μs),95Pctl-Bc(μs),Tx
insert,INSERT,"48,083","1,000","21","1","21","48,066","1,348",...,OCC
<1ms,1-3ms,3-5ms,5-10ms,10-20ms,20-40ms,40-80ms,80-150ms,150-300ms,>300ms,total,-1ms(%),...,300ms-(%),
0,0,0,919,9,49,15,1,0,7,1000,0.0,0.0,0.0,91.9,92.8,97.7,99.2,99.3,99.3,100.0
report of thread-2
...
Benchmark,Sub,Elapsed(ms),Threads,Operations,OPS,Ope/Tx,TPS,Avg-Tx(μs),...,Tx
insert-st,INSERT,"48,077","2","2,000","42","1","42","48,060","1,344",...,OCC
0,0,0,1840,16,98,30,2,0,14,2000,0.0,0.0,0.0,92.0,92.8,97.7,99.2,99.3,99.3,100.0
```

- `Tx`: トランザクション開始からコミット完了までのレイテンシ。`Bc`: トランザクション開始からコミット直前までのレイテンシ。パラメーターはinsert/update/deleteではトランザクション開始前に生成して計測に含めず、selectではトランザクション中に乱数で生成する (sbenchと同じ)
- `Elapsed`: スレッドごとのシナリオ全体 (セッション作成と文のprepareを含む) の時間。`-st`/`-mt` ではスレッドの平均
- 集計はsbenchの `StatsUtil` と同じ: パーセンタイルはnearest-rank、複数スレッドの集計では件数は合計、最小/最大は全体、平均はスレッドごとの平均の平均、50/95パーセンタイルはスレッドごとの50/95パーセンタイルの値の50/95パーセンタイル。OPSは経過秒数が1未満なら1秒として割る
- 各結果の後にTxのレイテンシのヒストグラム (件数と累積%) を出す
- `-st`/`-mt` はスレッドごとの結果 (`report of thread-N`) と集計結果を出す

`-o` を指定するとCSVファイルに追記する (`-h` でヘッダーも書く)。`-st`/`-mt` でファイル名が `.csv` で終わる場合は、スレッドごとの結果を `_t<N>.csv`、ヒストグラムを `_hist.csv` に出す (sbenchと同じ)。

エラーが起きるとその時点でベンチマークを中止し、0以外で終了する。リトライはしない (sbenchと同じ)。

## オプション

sbenchと同じ名前のオプションを受け付ける。短縮形 (`-t`、`-e`、`-d`、`-i`、`-o`、`-h`、`-c`) とハイフンを含む名前 (`--without-pk` など) は解析前にgflagsの形式に変換する。真偽値オプションの値を別引数で与える形 (`--without-pk true`) も受け付ける。列挙値 (`OCC`/`LTX`/`RO`、`UPSERT` など) は大文字小文字を区別しない。

| オプション | 既定値 | 説明 |
|---|---|---|
| `--transaction`, `-t` | `OCC` | `OCC`/`short`、`LTX`/`long`、`RO`/`read`/`readonly` |
| `--entity`, `-e` | `lkv` | `lkv` または `sysbench` |
| `--duration`, `-d` | `0` | 実行時間 (秒)。経過すると残りの操作があっても終了する。0なら操作が終わるまで |
| `--interval`, `-i` | `0` | トランザクション間の待ち時間 (ms)。insertのみ有効 (sbenchと同じ) |
| `--output`, `-o` | (空) | 結果を追記するCSVファイル |
| `--header`, `-h` | `false` | CSVファイルにヘッダーを書く |
| `--operations` | `0` | 操作数。0なら `--duration=0` のとき100、それ以外は1000000000 |
| `--operations_per_tx` | (コマンドによる) | 1トランザクションあたりの操作数。0なら全操作を1トランザクションで実行する。既定はinsert/updateが1、select/deleteが0 |
| `--scan_range` | (コマンドによる) | select: 0 full scan、1 point read (既定)、2以上 range scan。delete: 0 all delete (既定)、1 point delete、2以上 scan delete |
| `--read_area` | `none` | LTXのread area: `none` または `inclusive` |
| `--table_id` | `1` | サフィックスなしのコマンドで使うテーブルID |
| `--id_origin` | `1` | サフィックスなしのコマンドの主キーの開始値 |
| `--id_coefficient` | `10000000` | `-st` で各スレッドに割り当てる主キーの範囲 (sbenchのシステムプロパティ `idCoefficient` 相当) |
| `--insert_mode` | `INSERT` | `INSERT` または `UPSERT` |
| `--threads` | `1` | `-st` のスレッド数 |
| `--tables` | `1` | `prepare` と `-mt` のテーブル数 |
| `--max_value` | `0` | select: 条件に指定する値の最大値。0なら `--operations` |
| `--select_columns` | `*` | selectする列 (`*` またはカンマ区切り)。sbenchと同じくカンマを空白に置き換える |
| `--condition_column` | (空) | selectの条件に使う列。空なら主キー列 |
| `--index` | (空) | prepare: 2次索引を作る列 (カンマ区切り) |
| `--without-pk` | `false` | prepare: 主キーなしのテーブルを作る |
| `--connection`, `-c`, `--session_timeout`, `--session_shutdown`, `--auto_inc` | | スクリプトの互換のため受け付けるが無視する |

nsbench独自のオプション:

| オプション | 既定値 | 説明 |
|---|---|---|
| `--client_spin` | `true` | クライアントの応答待ちをビジーループにする。falseなら条件変数で待ち、応答の完了時に起こされる (CPUを占有しない) |

エンジンの設定は [service_common](../service_common/README.md#共通のgflags) の共通オプションを使い、既定値は `jogasaki::configuration` のまま (CBのサーバーの既定設定に近づけるため)。ただし以下を変えている。

| オプション | nsbenchの既定値 | 理由 |
|---|---|---|
| `--epoch_duration` | `3000` (us) | sbenchが動くtsurugidbの既定 (tsurugi.ini) に合わせる。shirakami自体の既定は40msで、コミット完了待ちのレイテンシがこれに引っ張られる |
| `--minloglevel` | `1` | エンジンのINFOログが結果に混ざらないようにする |

commit応答の種類は `--commit_response` (共通オプション) で変えられる。

## sbenchとの違い

- full scanのselectは、sbenchでは先頭の1件だけを読むが、nsbenchでは結果をすべて読み捨てる
- sbenchで実行時例外になる設定 (例: `select-st` で `--operations_per_tx` を省略した場合の集計時のゼロ除算) は、補正後の値を使って動くようにした
- スレッドは一斉に開始する (sbenchはスレッドプールに順に投入する)
- sbenchのnoptxは `sbtest<N>` 固定だが、nsbenchではエンティティのテーブルを使う (LTXのwrite preserveの対象)

## テスト

ctestに `nsbench-lkv`、`nsbench-sysbench`、`nsbench-st`、`nsbench-mt`、`nsbench-ltx` を登録している。各コマンドを少ない操作数で実行し、エラーなく終了することを確認する。
