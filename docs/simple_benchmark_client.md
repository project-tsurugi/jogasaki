# 簡易ベンチマーククライアント

TPC-Cよりも簡単なワークロードを実行する簡易クライアントによってtsubakuro-jogasaki間のボトルネックを分析する

- 簡易クライアントはオプションでクライアントスレッド数を指定することができる
- insert/update/queryのいずれかの動作モードを指定し、それぞれのステートメント(prepare済み)を繰り返し実行することができる
- 一定時間または一定トランザクション数の実行後に停止し、かかった時間からスループット(tps)を計算し表示する。
- データの準備を簡単にするため、jogasaki-benchmarks/tpccで生成されたデータを利用する
- 簡単のため、warehouseとクライアントスレッドは1:1で固定とする。

## 初期データ

insert/update/query共通で、jogasaki-benchmarksによって初期データを用意した後にベンチマークを開始する。

> jogasaki-tpcc --generate --dump --warehouse <# of warehouses>

実行ディレクトリに作成されたdbディレクトリをコピーしてクライアント起動時にロードして使用する。


## 使用ステートメント

実行時パラメーター：
`n_statements`でトランザクション毎の実行ステートメント数(デフォルトで3)を表す

### insert 

insertモードはトランザクション毎に`n_statements`個の下記INSERT文を実行する。

```
INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id) VALUES (:no_o_id, :no_d_id, :no_w_id)
```
ここでno_w_idは実行中のスレッドに固有の値を使用し、初期データと衝突しない範囲のno_d_idとno_o_idを生成して挿入する。(例えばno_o_idを3001からインクリメントするなど)

### update

updateモードはトランザクション毎に`n_statements`個の下記UPDATE文を実行する。

```
UPDATE STOCK SET s_quantity = :s_quantity WHERE s_i_id = :s_i_id AND s_w_id = :s_w_id
```

ここでs_w_idは実行中のスレッドに固有の値を使用し、初期データの範囲にマッチするs_i_idを生成して更新する。

### query

queryモードはトランザクション毎に`n_statements`個の下記SELECT文を実行する。

```
SELECT no_o_id FROM NEW_ORDER WHERE no_d_id = :no_d_id AND no_w_id = :no_w_id ORDER BY no_o_id
```

no_w_idはスレッドに固有の値を使用し、初期データの範囲にマッチするno_d_idを生成してクエリする。
