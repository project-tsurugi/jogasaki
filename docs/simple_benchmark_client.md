# 簡易ベンチマーククライアント

TPC-Cよりも簡単なワークロードを実行する簡易クライアントによってtsubakuro-jogasaki間のボトルネックを分析する

- 簡易クライアントはオプションでクライアントスレッド数を指定することができる
- insert/update/queryのいずれかの動作モードを指定し、それぞれのステートメント(prepare済み)を繰り返し実行することができる
- 一定時間または一定トランザクション数の実行後に停止し、かかった時間からスループット(tps)を計算し表示する。
- データの準備を簡単にするため、対象テーブルはTPC-CのNEW_ORDER表のみ使用する
- 簡単のため、warehouseとクライアントスレッドは1:1で固定とする。

## 初期データ

insert/update/query共通で、下記のような初期データを用意した後にベンチマークを開始する。

### 初期データにおいて調整可能なパラメーター

n_warehouses : warehouse数 = クライアントスレッド数
n_districts :  district数(デフォルトで10000)
n_statements : トランザクション毎の実行ステートメント数(デフォルトで3)

### 初期データの値

初期データとして、NEW_ORDER表に各列が下記の値を持つ(スレッド数 * n_districts * 100)個のレコードを用意する

no_w_id : スレッドID
no_d_id : 0-n_districts
no_o_id : 0-99

(キャッシュに全データが乗ることを防ぐため、n_districtsは最低でも10000程度で、スレッド毎に100Mレコード以上を想定する。)

## 使用ステートメント

### insert 

insertモードはトランザクション毎にn_statements個の下記INSERT文を実行する。

```
INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id) VALUES (:no_o_id, :no_d_id, :no_w_id)
```
ここでno_w_idは実行中のスレッドIDを使用し、初期データと衝突しない範囲のno_d_idとno_o_idをランダムに生成して挿入する。

### update

updateモードはトランザクション毎にn_statements個の下記UPDATE文を実行する。
(no_o_idの符号を反転させる単純なUPDATE)

```
UPDATE NEW_ORDER SET no_o_id=-no_o_id WHERE no_d_id=:no_d_id AND no_w_id=:no_w_id AND no_o_id=:no_o_id "
```

ここでno_w_idは実行中のスレッドIDを使用し、初期データの範囲にマッチするno_d_idとno_o_idをランダムに生成して更新する。

### query

queryモードはトランザクション毎にn_statements個の下記SELECT文を実行する。

```
SELECT no_o_id FROM NEW_ORDER WHERE no_d_id = :no_d_id AND no_w_id = :no_w_id ORDER BY no_o_id
```

no_w_idはスレッドIDを使用し、初期データの範囲にマッチするno_d_idをランダムに生成してクエリする。
初期データの定義から、100レコード程度の出力が得られることを期待している。




