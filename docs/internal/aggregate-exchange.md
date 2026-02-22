# aggregate エクスチェンジとインクリメンタル集約関数のメモ

2026-02-18 kurosawa

## 本文書について

aggregate exchange およびインクリメンタル集約関数の内部設計について特に重要な点を記載する

## 前提とする takatori 演算子

jogasaki の aggregate exchange ( namespace `executor::exchange::aggregate` ) は `takatori::plan::aggregate` に対応するオペレータであり、その内容に基づいて集約処理を実行する。

`takatori::plan::aggregate` は下記の情報を保持する。

- 入力列 (`source_columns`): group_keys または集約関数の引数として使用される列
- グループキー列 (`group_keys`): グループ化に使用するキー列
- 出力列 (`destination_columns`): group_keys または集約関数の格納先列から構成される列
- 集約要素リスト (`aggregations`): 各要素は下記を含む
  - 集約関数
  - 引数列のリスト
  - 格納先列 (`destination`): 集約結果を格納する出力列

各要素間には以下の制約がある。

- `group_keys` は `source_columns` の部分集合である
- 各集約関数の引数列のリストも `source_columns` の部分集合である
- `destination_columns` は `group_keys` と各集約要素の格納先列 (`destination`) から構成される

## レコードメタデータ

集約関数の処理を段階的に行うため、aggregate エクスチェンジは下記の 3 種類のレコードを使用する。

### 入力レコード

- `takatori::plan::aggregate` の `source_columns` に対応する列を持つレコード
- 列順まで一致する必要はないが、現実装では同じ順序としている

### 中間レコード

- aggregate exchange の各段階 (下記 pre / mid aggregation を参照) で使用されるレコード
- key 部分と value 部分があり、group key を key 部分として、集約演算のための中間フィールド (intermediate fields) を value 部分として持つ
- key 部分にはvalue 部分へのポインタも格納する

### 出力レコード

- aggregate exchange の post フェーズ (下記参照) で使用されるレコード
- group key と最終結果フィールドを持つ

## インクリメンタル集約関数

aggregate exchange で使用される集約関数。入力レコードを段階的に集約できるよう、pre / mid / post の 3 フェーズに分けて処理を行う。

#### pre aggregation

aggregate exchange の入力レコードをキーと演算フィールド (intermediate fields) に分割し、
パーティション (`input_partition`) 内でハッシュテーブルを用いて局所的に集約するフェーズであり、
上流プロセスの writer の書込み操作によって駆動される。

- 入力: 入力レコード
- 出力: 中間レコード

#### mid aggregation (intermediate aggregation)

pre フェーズの結果をマージするフェーズであり、下流プロセスの reader の読出し操作によって駆動される。

- 入力: 中間レコード
- 出力: 中間レコード

#### post aggregation

mid フェーズの出力から最終結果フィールドを算出するフェーズ。
例えば平均値を求める関数の場合、総和を保持するフィールドと個数を保持するフィールドから最終的な平均値を算出する。

- 入力: 中間レコード
- 出力: 出力レコード

## aggregate exchange の処理フロー

### 入力フェーズ

上流プロセスが `writer` 経由でレコードを書き込む。
`input_partition` が書き込まれたレコードを受け取り、ハッシュテーブル (`tsl::hopscotch_map`) を使って重複キー排除を行う。
ハッシュテーブルの key/value はそれぞれ中間レコードの key部分と value部分であり、長さは別途保持されるので生ポインタのみが格納される。
key/value の実体は `data::record_store` オブジェクトを経由でページプールから確保したメモリに格納される。
後段の reader が keyを読み出す際にはソートされている必要があるため、keyの生ポインタはポインタテーブルにも格納される。

入力レコードのキーが既存エントリと一致する場合、pre aggregation を即座に実行し、既存のエントリと新しいレコードを
pre アグリゲーション関数を用いてマージする。
全入力レコードを保持する必要がある group エクスチェンジと異なり、ここで単一レコードにされるのでメモリ使用量を削減できる。

ハッシュテーブルのロードファクターが一定の閾値を超えた場合、
`input_partition::flush()` が呼ばれ、`write()` で逐次構築されたポインタテーブルをソートして後段で使用可能にする。
ハッシュテーブルは `clear()` して次の入力レコードへ再利用する。

### 転送フェーズ

入力フェーズが全パーティションに対して完了した後、転送フェーズにおいて `input_partition` が
エクスチェンジ前段 (`sink`) から後段 (`source`) へ移動される (group exchange と同様)。

### 出力フェーズ

下流プロセスが `reader` 経由でグループとそのメンバーを読み出す。
複数の `input_partition` 同士を mid aggregation 関数によってマージするとともに、
post aggregation 関数で最終値を算出し出力する。

## 注意事項

- `input_partition` のハッシュテーブルはページプールから確保したメモリを使用する。
  ハッシュテーブルのサイズ上限はページサイズ (`memory::page_size`) に基づいており、
  超過した場合はポインタテーブルへの退避を経て新しいテーブルに移行する。
  このため 1 つの `input_partition` は複数のハッシュテーブル (ポインタテーブル) を持つ場合がある。

- 本書の対象としている集約関数はインクリメンタルなものだが、非インクリメンタル集約関数もあり、
  これは `aggregate_group` オペレータ (プロセスステップ内での集約) で使用される。
