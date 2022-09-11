# shuffle exchange性能ベーススコア

2020-05-18 kurosawa
2020-05-19 kurosawa comparator修正後に再測定

# この文書について

10億件or100億件のレコードを100コアでshuffle(group)処理時の各区間の時間をベーススコアとして記録する。

# 測定環境/測定条件

- サーバーcli01 (hyper threading off)
- jogasaki (88237ed50909a61cb60e5f7863a216256368f523)を使用
- レコードサイズ16 bytes (std::int64_t + double, キーはstd::int64_t)

# 測定項目
下記の各処理にかかった経過時間(ms)を記録する

## prepare
レコードを生成し、上流プロセス内部で確保したバッファに保存

## produce(write)
prepareされたデータをプロセスがwriterにwriteする処理
(flush時に発生するpregroup処理の時間は含まない。下記に分離して計算)

## produce(sort)
write時にinput partitionに一定量蓄えられたレコードをpre-sortする作業
(consumeがpriority queueベースで有る場合のみ必要となる)

## transfer
shuffle前半で作成したinput partitionを後半の処理(source)側に送信する処理

## consume
shuffle後段のpost group処理 - 下流プロスがreaderから全件readを行う

## 測定結果

### シングルスレッド、シングルパーティション

1 partition, 1 thread
10M recs/partition または 100M recs/partition
単位ms

|  項目| 10M recs/partition  | 100M recs/partition | コメント |
| ---- | ---- | --- | -- |
|  prepare |  215  | 1916  | |
|  produce(write) | 387 | 3803  | |
|  produce(sort) | 1540 | 15388 | |
|  transfer | 0 | 0 | 短時間のため測定不能 |
|  consume w/sorted vector|4281| 62632  | |
|  consume w/priority queue|1902  | 27600  | |

### シングルスレッド、マルチパーティション

100 partitions, 1 thread
10M recs/partition
単位ms

|  項目| 全パーティション合計  | パーティション毎平均 | コメント |
| ---- | ---- | --- | -- |
|  prepare |18400 | 184 |合計は平均から逆算  |
|  produce(write) |47300 | 473 | 合計は平均から逆算 |
|  produce(sort) |135800 | 1358  | 合計は平均から逆算 |
|  transfer | 7 | - |  |
|  consume w/sorted vector|446439|4463| 内訳はsortが95%以上 sorted pointerの併合は5%未満|
|  consume w/priority queue|215869|2158| |

### マルチスレッド、マルチパーティション

100 partitions, 100 threads
10M recs/partition
単位ms

|  項目| 全スレッド(最速-最遅)  | スレッド平均 | コメント |
| ---- | ---- | --- | -- |
|  prepare |  6997 | 5728 | |
|  produce(write) |5463| 2604  | |
|  produce(sort) |1712 | NA    | 差によって計算しているので平均はNot Available |
|  transfer | 7 | - |  |
|  consume w/sorted vector|12144|11739 | |
|  consume w/priority queue|4305|3816 |  |

### 付録 (実行コマンド一覧)

シングルスレッド、シングルパーティション
```
GLOG_v=1 ./group-cli --upstream_partitions 1 --downstream_partitions 1 --records_per_partition 10000000 --thread_pool_size 1
GLOG_v=1 ./group-cli --upstream_partitions 1 --downstream_partitions 1 --records_per_partition 10000000 --thread_pool_size 1 --shuffle_uses_sorted_vector
```

シングルスレッド、マルチパーティション
```
GLOG_v=1 ./group-cli --upstream_partitions 100 --downstream_partitions 100 --records_per_partition 10000000 --thread_pool_size 1
GLOG_v=1 ./group-cli --upstream_partitions 100 --downstream_partitions 100 --records_per_partition 10000000 --thread_pool_size 1 --shuffle_uses_sorted_vector
```

マルチスレッド、マルチパーティション
```
GLOG_v=1 ./group-cli --upstream_partitions 100 --downstream_partitions 100 --records_per_partition 10000000 --thread_pool_size 100
GLOG_v=1 ./group-cli --upstream_partitions 100 --downstream_partitions 100 --records_per_partition 10000000 --thread_pool_size 100 --shuffle_uses_sorted_vector
```