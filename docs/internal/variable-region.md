# variable region — 変数アクセスの統一設計

2026-05-31 kurosawa

## 本文書について

jogasaki の関係演算子が扱う変数の種類を統一的な仕組みで管理・アクセスするための設計を記述する。

## 本文書の状態

proposed 

region_id という名前は一部実装済み

---

## 背景：現状の問題

演算子が参照・更新する値の格納先は現在、以下の種類が混在しており、演算子コードはそれぞれに個別対応している。

| 種類 | 現在の表現 | 読み書き |
|------|-----------|---------|
| 上流ブロックのストリーム変数 | `variables_view::ref(block_id)` | 読み込みのみ |
| 自ブロックのストリーム変数 | `variables_view::ref()` | 読み書き可 |
| ホスト変数 | `variable_table_info const*` + 別途 `record_ref` | 読み込みのみ |
| index から decode した key/value | operator ごとに用意された `record_ref` 変数 | 読み込みのみ（encode 時は書き込み先） |
| ad-hoc `record_meta` ベースのレコード | 各演算子が独自に管理 | 演算子依存 |

この結果、演算子コード（`emit`, `write_existing`, `aggregate_group` 等）は
- ストリーム変数には `variables_view` 経由
- ホスト変数には `host_variable_info` ポインタ経由
- index 由来データには専用の `record_ref` 変数経由

とそれぞれ別ルートでアクセスしており、コードが煩雑になっている。

---

## 目的

すべての変数を **variable region（変数リージョン）** という統一概念でまとめ、演算子コードが変数の出自を意識せずにアクセスできるようにする。

---

## 設計概要：variable region

### リージョンとは

**variable region** は、1 つの `record_ref`（+対応する `record_meta`）に対応する名前付きメモリ領域である。

- 1 リージョン = 1 `record_ref`
- `value_offset` / `nullity_offset` はリージョンが決まって初めて意味を持つ
- リージョンはプロセス内でユニークな `region_id` によって識別される

### region_id

`block_index`（buffer-operator.md で導入）を発展させた識別子。

```cpp
enum class region_kind {
    undefined,
    basic_block,   // ストリーム変数のベーシックブロック（従来の block_index ）
    host_variable,   // ホスト変数
    // TODO: 必要に応じて追加
};

class region_id {
public:
    constexpr region_id() noexcept = default;           // undefined
    constexpr region_id(std::size_t index) noexcept;    // implicit: basic_block（後方互換）
    constexpr region_id(region_kind kind, std::size_t index) noexcept;

    [[nodiscard]] constexpr region_kind kind() const noexcept;
    [[nodiscard]] constexpr std::size_t index() const noexcept;
    [[nodiscard]] constexpr explicit operator bool() const noexcept; // true = valid
    // operator==, !=, << ...
};
```


`region_id` は trivially copyable で、フィールドメタデータ（`field_info`, `emit_field`, `update_field`, `aggregate_group_argument` 等）に直接埋め込める。

### region_store

プロセス実行中に生存するすべてのリージョンを管理するストア。

```cpp
class region_store {
public:
    // リージョンを登録し、region_id を発行する（コンストラクション時）
    region_id add(record_meta const& meta, record_ref ref);

    // 登録済みリージョンの record_ref を取得する（実行時アクセス）
    [[nodiscard]] accessor::record_ref ref(region_id id) const;
};
```

`variables_view` は `region_store` への参照と「自ブロックの `region_id`」を保持するビューに進化する。

### フィールド参照

各フィールドは次の情報を持つ。

```
(region_id, value_offset, nullity_offset)
```

これにより `field_info`, `emit_field`, `update_field`, `aggregate_group_argument` 等は
`source_block_index_` の代わりに `region_id_` を保持する。

演算子コードはフィールドのリージョン種を気にせず、次のように統一的にアクセスできる。

```cpp
// 読み取り（リージョン種に依らない共通コード）
auto src_ref = ctx.variables().ref(f.region_id_);
auto value = src_ref.get_value<T>(f.source_offset_);

// 書き込み（自ブロック = 書き込み可能なリージョン）
auto dst_ref = ctx.variables().ref();
dst_ref.set_value<T>(f.target_offset_, value);
```

### 各リージョン種の役割

| region_kind | 内容 | record_ref のライフタイム | 読み書き |
|-------------|------|------------------------|---------|
| `basic_block` | ストリーム変数 | プロセス実行全体 | 自ブロックのみ書き込み可 |
| `host_variable` | ホスト変数 | リクエスト全体 | 読み込みのみ |
| `index_record` | インデックス由来のkey/valueレコード | デコード結果のライフタイムに依存 | 読み書き両方 |

`index_record` は必要に応じて追加するリージョン種。インデックス由来の値を統一的なアクセス方法で扱えるようになる。

