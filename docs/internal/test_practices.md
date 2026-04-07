# テストの書き方

jogasakiプロジェクトにおけるテストコードの推奨記法をまとめたドキュメント。

---

## クエリ結果の確認は `basic_record` の比較で行う

**ポリシー:** クエリ結果の検証は、期待値となる `basic_record` を構築して `EXPECT_EQ` で比較する。`record_ref` のアクセサを使ってフィールドを個別にチェックしない。

**良い例:**

```cpp
std::vector<mock::basic_record> result{};
execute_query("select c0 from t", result);
ASSERT_EQ(1, result.size());
EXPECT_EQ((mock::typed_nullable_record<kind::decimal>(
    std::tuple{meta::decimal_type(5, 3)},
    runtime_t<meta::field_type_kind::decimal>(1, 0, 1230, -3)
)), result[0]);
```

**悪い例:**

```cpp
std::vector<mock::basic_record> result{};
execute_query("select c0, c1 from t", result);
ASSERT_EQ(1, result.size());
// NG: record_ref を使ってフィールドを個別にチェックしている
auto ref = result[0].ref();
EXPECT_EQ(42, ref.get_value<std::int32_t>(0));
EXPECT_EQ(accessor::text{"hello"}, ref.get_value<accessor::text>(1));
```

---

## `field_type` の作成には `type_helper.h` のヘルパ関数を使う

**ポリシー:** テストコードで `field_type` を作成する際は、`jogasaki/meta/type_helper.h` で定義されているヘルパ関数（`int4_type()`、`decimal_type()`、`character_type()` など）を使う。`field_type` のコンストラクタを直接呼び出さない。

**良い例:**

```cpp
auto fm = meta::decimal_type(5, 3);
auto r1 = mock::typed_nullable_record<kind::decimal>(std::tuple{fm}, val1);
auto r2 = mock::typed_nullable_record<kind::decimal>(std::tuple{fm}, val2);
```

**悪い例:**

```cpp
// NG: field_enum_tag を使って field_type コンストラクタを直接呼び出している
auto ft = meta::field_type{meta::field_enum_tag<meta::field_type_kind::int4>};

// NG: make_shared でオプションを渡して field_type コンストラクタを直接呼び出している
auto fm = meta::field_type{std::make_shared<meta::decimal_field_option>(5, 3)};
```

---

## テストマクロにテンプレート式を渡す場合は追加の括弧で囲む

**ポリシー:** `ASSERT_EQ` / `EXPECT_EQ` などの gtest マクロにテンプレート引数を持つ式（`create_nullable_record<A, B>(...)` など）を直接渡す場合、テンプレート引数のカンマがマクロの引数区切りとして誤認される。式全体をさらに `()` で囲んで回避する。

**良い例:**

```cpp
ASSERT_EQ((create_nullable_record<kind::int8, kind::int8>(2, 3)), result);
```

**悪い例:**

```cpp
// NG: プリプロセッサがカンマをマクロ引数の区切りと誤認してコンパイルエラーになる
ASSERT_EQ(create_nullable_record<kind::int8, kind::int8>(2, 3), result);

// NG: ローカル変数に逃がすのは冗長
auto expected = create_nullable_record<kind::int8, kind::int8>(2, 3);
ASSERT_EQ(expected, result);
```
