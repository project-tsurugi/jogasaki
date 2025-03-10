# スカラ関数の追加実装

2025-03-10 nishimura

## この文書について

SQL実行エンジンのスカラ関数追加実装に必要な作業内容について記述する。
この文章は実際にsubstringを追加した場合の例を元に示す。


## 関連ファイル

src/jogasaki/executor/function/scalar_function_kind.h
src/jogasaki/executor/function/builtin_scalar_functions_id.h
src/jogasaki/executor/function/builtin_scalar_functions.h
src/jogasaki/executor/function/builtin_scalar_functions.cpp


### scalar_function_kind.h

下記scalar_function_kind enumクラスに追加する関数名(substring)を末尾に追加

```CPP
enum class scalar_function_kind : std::size_t {
    undefined = 0,
...
    substring
};
```
to_string_view関数に追加する関数名(substring)を末尾に追加

```CPP
[[nodiscard]] constexpr inline std::string_view to_string_view(scalar_function_kind value) noexcept {
    using namespace std::string_view_literals;
    using kind = scalar_function_kind;
    switch (value) {
        case kind::undefined: return "undefined"sv;
...
        case kind::substring: return "substring"sv;
    }
    std::abort();
}
```

### builtin_scalar_functions_id.h

scalar_function_id enumクラスにidを追加する。
追加するidは末尾のid+1

```CPP
enum scalar_function_id : std::size_t {
...
    id_11004,
    id_11005,
    id_11006
};
```
### builtin_scalar_functions.cpp

add_builtin_scalar_functions関数に登録したいスカラ関数のインターフェース(substring)を
追加する

```CPP
void add_builtin_scalar_functions(
    ::yugawara::function::configurable_provider& functions,
    executor::function::scalar_function_repository& repo
) {
...

    /////////
    // substring
    /////////
    {
        //  scalar_function_infoの引数はそれぞれ
        //    scalar_function_kind enum  -> scalar_function_kind::substring
        //    内部ロジックを実装した関数 -> builtin::substring
        //    引数の個数                 -> 3
        //  を表している
        auto info = std::make_shared<scalar_function_info>(
            scalar_function_kind::substring, builtin::substring, 3);
        //関数名
        auto name = "substring";
        //登録したscalar_function_id
        auto id   = scalar_function_id::id_11006;
        repo.add(id, info);
        functions.add({
            id,
            name,
            //戻り値の型
            t::character(t::varying),
            //引数の型
            {t::character(t::varying), t::int8(), t::int8()},
        });
        // 引数の個数が異なるなど複数のインターフェースを持つ場合は
        // 対応するインターフェース分登録する
        id = scalar_function_id::id_11006;
        repo.add(id, info);
        functions.add({
            id,
            name,
            t::character(t::varying),
            {t::character(t::varying), t::int8()},
        });
    }
}
```

namespace builtin内に内部ロジックを実装した関数(substring)を追加する。

```CPP
namespace builtin {
...
data::any substring(evaluator_context& ctx, sequence_view<data::any> args) {
...
}
```

### builtin_scalar_functions.h

builtin_scalar_functions.cppで記述した関数宣言(substring)を追加

```CPP
namespace jogasaki::executor::function {

namespace builtin {

data::any substring(
    evaluator_context& ctx,
    sequence_view<data::any> args
);


}  // namespace builtin

}  // namespace jogasaki::executor::function
```


