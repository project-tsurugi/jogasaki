# buffer 演算子の実装設計

2026-05-22 kurosawa

## 本文書について

`takatori::relation::buffer` に対応する jogasaki の演算子実装設計を記述する。

## 本文書の状態

implemented

## 背景・概要

### takatori における buffer

`takatori::relation::buffer` は、1つの入力リレーションを複数の下流演算子から共有入力として利用できるようにする演算子である（[relational-operators.md](https://github.com/project-tsurugi/takatori/blob/master/docs/ja/relational-operators.md) 参照）。

- 入力数: 1
- 出力数: 2以上（`buffer::size()` で取得）
- 列の公開: すべて
- プロパティ: なし

jogasaki は `buffer::size()` が返す任意個数の下流に対応する。各下流を **下流演算子i**（output_ports()[i]）と呼ぶ。

## 実装上の特殊点

### 下流が複数ある

他のすべての演算子は下流演算子を1つしか持たないが、buffer だけは複数持つ。これは jogasaki の operator モデルにおける唯一の例外である。

```
上流
  │
[buffer]
  ├─ [下流演算子0] ─ ... ─ [emit/write0]
  ├─ [下流演算子1] ─ ... ─ [emit/write1]
  └─ [下流演算子N-1] ─ ... ─ [emit/writeN-1]
```

## 基本概念：basic block と variable_table の関係

### basic block の定義

演算子ツリーは buffer 演算子によって **basic block** に分割される。

- 各 basic block は、buffer 演算子のすぐ下流の演算子を先頭とし、次に現れる buffer 演算子（その演算子自身を含む）までを含む一連の演算子列である
- 先頭ブロック（一番上流のブロック）は、ツリーの根演算子（scan / take_flat など）を先頭とし、最初の buffer 演算子で終わる
- buffer 演算子がなければ、プロセス全体が1つの basic block になる

```
[scan]
  |
[project]   <- block 0
  |
[buffer]    <- block 0 の末尾
  +- [filter] - [emit0]    <- block 1
  +- [write1]              <- block 2
```

各演算子には `block_index` が割り当てられており、その演算子がどの basic block に属するかを示す。

### block_index の採番ルール

- プロセスのブロックは `block_index = 0` から始まり、`create_block_variables_definition` によって連番で割り当てられる
- プロセスの作成時に作られてからは不変なので、ブロックに関する情報(variable_tableやvariable_table_info)はstd::vector などに格納しインデックスまたはポインタでアクセスできる

### block index の抽象化

block index は basic block に対する採番だが、basic block以外も同様に扱うための抽象化である region id を導入する (variable-region.md 参照)。
演算子のコードからは直接 block index を扱わず、抽象化した region id を使う。region_id は軽量な trivially copyable なクラスであり、block index から変換して使用する。
`variable_table` の内部など、コードの位置によっては、block index を直接扱う箇所もあるが、必要がなければ region_id を使うことが推奨される。

### variable_table と variable_table_info

各 basic block には、以下の2種類のオブジェクトが対応する。

**`variable_table`（実行時データ）**

- その basic block で **新たに定義された変数のみ** を格納するストレージ
- block 間で互いに独立しており、他の block の `variable_table` を知らない
- 実行時に `work_context` が `variable_table` の配列を保持する。block index や region_id で各要素にアクセスできる
- メタデータとして、下記 `variable_table_info` の参照を持つ

**`variable_table_info`（コンパイル時メタデータ）**

- その basic block で新たに定義された変数のオフセット等のメタデータを `record_meta` で管理する
- **親子構造** を持ち、子は親を知っているが親は子を知らない
- buffer の下流ブロックの `variable_table_info` は、上流ブロック（buffer を含むブロック）の `variable_table_info` を親とする

### 変数検索と value_info

演算子が変数 `v` のオフセットを調べる際は、自分のブロックの `variable_table_info::at(v)` を呼び出す。

- 自分のブロックで定義された変数 → 自分の `variable_table_info` が解決し、**自分の region_id** を含む `value_info` を返す
- 上流ブロックで定義された変数 → 親（または祖先）に委譲し、その祖先の **region_id** を含む `value_info` を返す

`value_info` には以下の情報が含まれる：

| フィールド | 説明 |
|-----------|------|
| `region` | 変数が実際に格納されているリージョンを示す region_id を取得可能 |
| `value_offset` | その `variable_table` 内でのバイトオフセット |
| `nullity_offset` | nullity ビットのオフセット |
| `index` | `record_meta` 上のフィールドインデックス |

演算子は `value_info.region()` を使って `work_context::variables()` で得た variable_table_list から正しい `variable_table` を取得し、そこから値を読み書きする。

### buffer と variable_table の関係

buffer は変数テーブルのデータをコピーしない。下流ブロックは `value_info.region()` 経由で上流ブロックの `variable_table` を直接参照する。buffer 自身がすべきことは下流演算子を順番に呼び出すことのみである。

```
block 0 の variable_table: [col_a, col_b, col_c]  <- scan で定義
block 1 の variable_table: [new_col_x]             <- block 1 の project で新たに定義
block 2 の variable_table: []                      <- block 2 では新変数なし

block 1 の演算子が col_a を参照する場合:
  block_info().at(col_a) -> value_info{ region_id=0, offset=... }  (親委譲)
  work_context.variables().at(region_id{0}) -> block 0 の variable_table
  ref.get_value<T>(offset) -> col_a の値
```

### variable_table_list と variables_view

**`variable_table_list`（実行時データ）**

プロセス内で使用するすべての `variable_table` をまとめて管理するもの。実行時に `work_context` がこれを保持し、block_index をキーとして各 `variable_table` にアクセスできる。

**`variables_view`（アクセスビュー）**

`variable_table_list` への参照と region_id をまとめて保持する軽量なビュークラス。関係演算子内部での関数間での変数群の受け渡しに使用する。これまで `variable_table` の参照を渡していた箇所は `variables_view` を渡すように変更する。
`variables_view` には `ref` や `at` などの便利関数を定義して、内部的で使われている `variable_table_info` や `variable_table` を意識せずに変数にアクセスできるようにする。`at(var)` は `variable_table` の保持する `variable_table_info` の親委譲チェーンを辿って `value_info` を返す。

例えば、 `variables_view` には `ref(region_id region = {}) -> accessor::record_ref` 関数があり、region_id を指定することでそれが担当する `small_record_store` の `record_ref` を得ることができる。region_id を省略した場合は自身が保持する region_id を使用する。

`variables_view` が保持する region_id は、その演算子がアクセス可能な変数の端点（basic block ツリーの末端）を表す。変数への **書き込み** 時はこの region_id をそのまま使用できるが、**読み取り** 時は `ref(region_id)` の形で親や祖先の region_id を明示的に指定する必要がある。

region_id を正しく指定して呼出すのは呼出側の責任で、variables_viewがアクセスを許可しない region_id を指定した場合はassert_with_exceptionで失敗する。

下流の演算子は上流のブロックの変数を書き替えることができない。書き換え可能なのは必ず ref() によって得られる `record_ref` のみである。

### 演算子コンテキストからの変数アクセスの統一

これまで演算子コンテキストには `input_variables()` と `output_variables()` として別々に変数アクセスを提供していたが、`ctx.variables()` という単一の `variables_view` に統一する。

`value_info` にはすでに `region` が含まれているため、読み取り時は `ctx.variables().ref(vinfo.region())` で正しい `record_ref` を取得し、書き込み時は `ctx.variables().ref()` でアクセスする。

```cpp
// 変数 v への実行時アクセスイメージ（新方式）
auto vinfo = block_info().at(v);                          // 親委譲込みで解決
auto ref = ctx.variables().ref(vinfo.region());           // 正しいブロックの record_ref を取得
auto value = ref.get_value<T>(vinfo.value_offset());      // 値の読み出し

// 自ブロックの変数への書き込み
auto out_ref = ctx.variables().ref();                     // 自ブロックの record_ref（region 省略）
out_ref.set_value<T>(vinfo.value_offset(), value);
```
