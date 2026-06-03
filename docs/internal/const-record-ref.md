# constant record_ref の追加

2026-06-03 kurosawa

## 本文書について

`record_ref` の定数バージョンである `const_record_ref` を追加する設計を記述する。

## 本文書の状態

proposed

## 背景・概要

既存のコードでは `record_ref` は常に変更可能であったが、実際にはホスト変数由来のレコードや、上流 basic block で定義された変数 region を格納するレコードなど、変更してはいけないレコードも多く存在する。これらを `const_record_ref` として区別することで、コードの安全性と可読性を向上させる。

## 設計

- `const_record_ref` クラスを追加する。これは `record_ref` と同様のインターフェースを持つが、変更操作を提供しない。

- `record_ref` は暗黙的に `const_record_ref` に変換可能とする

- `copy_nullable_field` など共通関数において、ソースレコードを示す引数を `const_record_ref` に変更する

## 実施計画

### 方針

一度にすべてを変更するのではなく、影響範囲を絞って段階的に進める。

1. 最初のフェーズは **`variables_view::ref(region_id)` が返す値を `const_record_ref` にする** ことを起点とし、そこからコンパイルエラーが生じる箇所のみを修正する。

- `variables_view::ref()` （region_id なし = 現在ブロック）→ 書き込み可能 → `record_ref` を返す（変更なし）
- `variables_view::ref(region_id r)` （上流ブロック指定）→ 読み取り専用 → `const_record_ref` を返すよう変更する

2. 影響範囲の小さい関数から順次変更していく

3. 影響範囲の大きいものは適当なタイミングでまとめて変更する

### 保留: 変更対象となる主要な関数・インターフェース

ソース（読み取り専用）引数を `const_record_ref` に変更すべき代表的な関数・インターフェースのうち、影響範囲が大きいために保留中のものを以下に示す。これらは段階的に変更していく予定。

#### レコード書き込みインターフェース

書き込み先へ渡す「ソースレコード」を示す引数。インターフェース変更が実装クラス全体に波及するため影響範囲が大きい。

| 関数 | ファイル | 変更対象引数 |
|------|---------|------------|
| `record_writer::write(rec)` (+ 全実装) | `src/jogasaki/executor/io/record_writer.h` | `rec` |
| `file_writer::write(ref)` (+ 全実装) | `src/jogasaki/executor/file/file_writer.h` | `ref` |
| `input_partition::write(record)` (group/aggregate/forward) | `src/jogasaki/executor/exchange/*/input_partition.h` | `record` |
| `input_partition::push(record)` (forward) | 同上 | `record` |


#### 集約関数

`aggregator_type` 関数型（および `incremental::builtin` の具体的実装 `sum`, `count_pre`, `count_mid`, `max`, `min`, `identity_post` 等）の `source` 引数。関数型エイリアス自体の変更が必要となるため、全登録済み集約関数への波及に注意。

| 関数/型 | ファイル | 変更対象引数 |
|---------|---------|------------|
| `aggregator_type` (関数型エイリアス) | `src/jogasaki/executor/function/incremental/aggregator_info.h` | 第4引数 `source` |
| `builtin::sum`, `count_pre`, `max`, `min` 等 | `src/jogasaki/executor/function/incremental/builtin_functions.h` | `source` |

#### インデックスアクセス

| 関数 | ファイル | 変更対象引数 |
|------|---------|------------|
| `aggregate_info::extract_key(record)` | `src/jogasaki/executor/exchange/aggregate/aggregate_info.h` | `record` |
| `aggregate_info::output_key(mid)` | 同上 | `mid` |
| `group_info::extract_key(record)` | `src/jogasaki/executor/exchange/group/group_info.h` | `record` |
| `group_info::extract_sort_key(record)` | 同上 | `record` |
| `group_info::extract_value(record)` | 同上 | `record` |
| `primary_target::prepare_encoded_key(ctx, source, out)` | `src/jogasaki/index/primary_target.h` | `source` |
