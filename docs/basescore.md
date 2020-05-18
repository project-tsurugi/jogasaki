# shuffle exchange性能ベーススコア

2020-05-18 kurosawa

# この文書について

10億件or100億件のレコードを100コアでshuffle(group)処理時の各区間の時間をベーススコアとして記録する。

# 測定環境/測定条件

- サーバーcli01 (hyper threading off)
- jogasaki (commit# 39bafd530596994e925c3d1409fdfc97d437c7b2)を使用
- レコードサイズ16 bytes (std::int64_t + double, キーはstd::int64_t)

# 測定項目
下記の各処理にかかった経過時間(ms)を記録する

## prepare
レコードを生成し上流プロセス内部に確保したバッファに保存する作業

## produce(write)
prepareされたデータをプロセスがwriterにwriteし、shuffle exchangeへ渡す作業

## produce(pregroup)
write時にinput partitionに蓄えられたレコードをpre-sortする作業
(consumeがpriority queueベースで有る場合のみ必要となる)

## transfer
shuffle前半で作成したinput partitionを後半の処理(source)側に送出する処理

## consume
shuffle後段のpost group処理 - 下流プロスがreaderを取得し全件readを行う

## 測定結果

### シングルスレッド、シングルパーティション

1 partition, 1 thread

|  項目| 10M recs/partition  | 100M recs/partition | コメント |
| ---- | ---- | --- | -- |
|  prepare |  217  | 1921  | |
|  produce(write) | 375 | 3629  | |
|  produce(sort) | 2304 | 23043    | consume priority queueの場合のみ発生 |
|  transfer | 0 | 0 | 短時間のため測定不能 |
|  consume sorted vector|5794  | 83442  | |
|  consume priority queue|2428  | 33162  | |
単位(ms)

### シングルスレッド、マルチパーティション

100 partition, 1 thread
10M recs/partition

|  項目| 全スレッド  | スレッド平均 | コメント |
| ---- | ---- | --- | -- |
|  prepare |257704 | 185 | |
|  produce(write) |63845 | NA  | 差を使って計算しているので平均はNot Available |
|  produce(sort) |196047 | NA    | consume priority queueの場合のみ発生 差を使って計算しているので平均はNot Available |
|  transfer | 7 | - |  |
|  consume sorted vector|502806|5027   | |
|  consume priority queue|232484|2324 | |
単位(ms)

### マルチスレッド、マルチパーティション

100 partition, 100 thread
10M recs/partition

単位(ms)
|  項目| 全スレッド  | スレッド平均 | コメント |
| ---- | ---- | --- | -- |
|  prepare |  7505  | 5841  | |
|  produce(write) |4949 | NA  | 差を使って計算しているので平均はNot Available|
|  produce(sort) |3444 | NA    | consume priority queueの場合のみ発生 差を使って計算しているので平均はNot Available |
|  transfer | 7 | - |  |
|  consume sorted vector|14428|12968   | |
|  consume priority queue|676708|629976 | **sorted vectorに比べてかなり遅い** |
単位(ms)
