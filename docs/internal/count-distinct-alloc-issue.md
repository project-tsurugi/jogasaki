# `COUNT(DISTINCT ...)` のメモリ確保問題に対する設計

## 目的

大量の入力に対する `COUNT(DISTINCT ...)` で発生する、以下の二つのメモリ確保問題を解消する。

- [tsurugi-issues #945](https://github.com/project-tsurugi/tsurugi-issues/issues/945):
  distinct 値が多い場合にハッシュ表の確保が失敗する
- [tsurugi-issues #1528](https://github.com/project-tsurugi/tsurugi-issues/issues/1528):
  入力レコードが多い場合に `value_store` の null フラグ領域の確保が失敗する

両者は Jogasaki のページサイズ（2 MiB）に関係するが、原因と修正箇所は異なる。

## Issue #945: distinct 用ハッシュ表

### 原因

`count_distinct` は `tsl::hopscotch_set` に distinct 値を格納する。
このコンテナのバケット配列は単一の連続領域であり、再ハッシュ時には 2 MiB を超えることがある。

従来使用していた `monotonic_paged_memory_resource` は、一回の allocation で一ページを超える
連続領域を提供できない。このため、distinct 値が増えてバケット配列が一ページを超えると
`std::bad_alloc` が発生していた。

### 設計

`page_or_heap_memory_resource`（`jogasaki/memory/page_or_heap_memory_resource.h`）を使用する。

- 要求サイズが一ページ以下の場合は、`monotonic_paged_memory_resource` から確保する。
- 要求サイズが一ページを超える場合は、`new_delete_resource()` から確保する。
- deallocation は allocation と同じ要求サイズを受け取るため、サイズによって確保元へ振り分ける。

これにより、小さいバケット配列では page pool を再利用しつつ、一ページを超える連続領域も
確保できる。ページから確保した領域は monotonic 同様に個別解放されず、resource の破棄時に
プールへ返却される。ヒープから確保した領域は deallocate で解放されるため、呼び出し側が
解放する必要がある。

### 初期バケット数

初期バケット数には、一ページに収まる最大の二のべき乗を使用する。

`tsl::hopscotch_set` は、要求されたバケット数を二のべき乗に切り上げたうえで、末尾の
neighborhood 用に追加のバケットを確保する。このため、初期バケット数を次の式で求める。

```cpp
page_fitting_buckets = round_down_to_power_of_two(
    page_size / sizeof(bucket_type) - (neighborhood_size - 1));
```

バケット配列がこのサイズを超えて成長した場合は、ヒープ上へ再配置される。
より少ない初期バケット数を使用し、少ないグループ数のクエリに対する使用メモリを削減することも可能だが、本対応では既存のもの(1ページに収まる初期バケット数)を維持し、最適化は将来の検討項目とする。

## Issue #1528: `value_store` の null フラグ

### 原因

`COUNT(DISTINCT ...)` の計算前には、グループ内のすべての値と null フラグが
`value_store` に格納される。

従来の null フラグは一件につき一バイトを使用し、すべてのフラグが単一の連続領域にあることを
前提としていた。入力件数が一ページに格納できる件数を超えると、次のページの allocation が
非連続なアドレスを返し、`value_store` が内部エラーを送出していた。

### 設計

null フラグをビットパックし、複数の連続範囲（range）として管理する。

- `std::uint8_t` 一個に八件分の null フラグを格納する。
- null フラグ用 resource から一バイトずつ確保する。
- 直前のブロックと連続していれば、現在の null range を延長する。
- 非連続なら新しい null range を開始する。
- `reset()` では range と書き込みカーソルを初期状態へ戻す。

これにより、null フラグのメモリ使用量を従来の約 1/8 にし、ページ境界を越えた格納を可能にする。

### イテレータ

値と null フラグではページ境界が一致しないため、イテレータはそれぞれ独立したカーソルを持つ。

- 値カーソルは従来どおり value range を走査する。
- null カーソルは null range、range 内のブロック位置、ブロック内のビット位置を走査する。
- `operator++()` は両方のカーソルを一件ずつ進める。
- `is_null()` は現在の null ブロックから対象ビットを読み出す。
- イテレータの等価比較には値カーソルの位置を使用する。null カーソルは値カーソルから一意に
  決まる同期状態として扱う。

`value_store` の公開されている append、begin、end、`is_null()` の使用方法は変更しない。

## 検証

以下の観点をテストする。

- null/非 null が混在する場合のビット境界をまたぐ走査
- value range と null range の境界が異なる場合の走査
- null フラグが複数 range に分割された場合の走査
- 空、単一値、単一 null、および `reset()` 後の走査
- 一ページを超えるバケット配列が必要な distinct cardinality の集計
- 対応する全データ型の既存 `COUNT(DISTINCT ...)` テスト

## 残課題

一ページを超えるハッシュ表はプロセスヒープを使用するため、極端に大きな distinct cardinality
では `std::bad_alloc` が発生し得る。クエリー単位のメモリ上限と、リソース不足時の適切な
エラーコードへの変換は、本対応の範囲外とする。
