# 演算子テストケースの構造改善設計

## 背景と課題

演算子（filter、project、offer など）を単体でテストするには、コンパイラや `operator_builder` が通常の実行パスで生成するさまざまなデータ構造を事前に揃える必要がある。既存のテストケースはこれらを各テスト関数内で都度構築していた。

その結果、以下の問題が生じている。

### 問題1: テストコードの大部分が前提構築

1 テストケースあたり 100〜200 行のうち、検証ロジックは数行に過ぎず、残りのほとんどが前提データ構造の構築コードである。何をテストしているのかが見通しにくい。

### 問題2: テストの横展開不足

前提構築コストが高いため、テストケースが少数（多くの場合 1〜2 件）に留まっている。演算子内部のコードパス（NULL 入力、境界値、エラーパスなど）を網羅するテストケースが書かれていない。

### 問題3: 恣意的な周辺演算子の配置

演算子のテストであるにもかかわらず、`processor_info` 構築のために scan や offer を周辺に配置する必要があり、テスト対象ではない演算子が混入した不自然な構成になっている（例：filter のテストに scan を書く）。

---

## 演算子のカテゴリ

テストハーネスの設計にあたり、演算子を 2 つの軸で分類する。

### 軸1: active vs passive

| カテゴリ | 説明 | 代表演算子 |
|---|---|---|
| **active** | オペレータツリーのルートであり、自ら下流をドライブする | `take_flat`, `take_group`, `take_cogroup`, `scan`, `find`, `values` |
| **passive** | 上流から駆動され、受け取ったレコードを処理して下流に渡す | `filter`, `project`, `offer`, `emit`, `join_find`, `join_scan` など |

passive な演算子をテストするには、上流から入力を供給するスタブが必要になる。これを `add_upstream_xxx_provider` 関数群として提供する（後述）。

active な演算子は自身がツリーのルートであるため、上流スタブは不要だが、KVS などの外部リソースや exchange reader の準備が必要になる場合がある。

### 軸2: 入力の種類（record / group / cogroup）

演算子が受け取る入力の種類によって、対応する基底クラスと必要な上流スタブが決まる。

| 入力種別 | 基底クラス | 上流スタブ関数 | 主な演算子 |
|---|---|---|---|
| **record** | `record_operator` | `add_upstream_record_provider` | `filter`, `project`, `offer`, `emit`, `join_find`, `join_scan` |
| **group** | `group_operator` | `add_upstream_group_provider` | `aggregate_group`、group exchangeの下流にくる演算子 |
| **cogroup** | `cogroup_operator` | `add_upstream_cogroup_provider` | `join`（take_cogroup の下流にくる演算子） |

---

## 上流スタブ関数

passive な演算子のテストでは、テスト対象演算子の上流に入力を供給するスタブ演算子をプロセスグラフに差し込む必要がある。このスタブ配置を以下の関数群として `operator_test_utils` に用意する。

### `add_upstream_record_provider(columns, types)`

record を下流に供給するスタブをプロセスグラフに追加し、`n` 個の stream variable とその型を登録する。返り値として、接続済みのストリーム変数列を返す。

- 内部実装の選択肢: `scan` 演算子、`values` 演算子、`take_flat` 演算子など
- テスト記述者は実装の詳細（何の演算子が上流にいるか）を意識しない

```cpp
// 使用例
auto vars = add_upstream_record_provider({t::int8{}, t::int8{}, t::int8{}});
// vars[0], vars[1], vars[2] がstream variableとして利用可能
```

オプションで実装種別を切り替えられるようにする（デフォルト: `values` か `scan`）。

### `add_upstream_group_provider(key_types, value_types)`

group exchange を準備し、`take_group` を上流に配置して group を下流に供給する。`group_operator` を実装する演算子（`aggregate_group` など）のテストで使用する。

```cpp
// 使用例
auto [key_vars, value_vars] = add_upstream_group_provider(
    {t::float8{}, t::int4{}},   // キー列の型
    {t::int8{}}                 // 値列の型
);
```

内部では以下を構築する：
- `takatori::plan::group` exchange および exchange columns
- `relation::step::take_group` 演算子のプロセスへの挿入
- 対応する変数バインディング

### `add_upstream_cogroup_provider(groups)`

複数の group exchange を準備し、`take_cogroup` を上流に配置して cogroup を下流に供給する。`cogroup_operator` を実装する演算子（join など）のテストで使用する。

```cpp
// 使用例（2入力 join のケース）
auto [left_vars, right_vars] = add_upstream_cogroup_provider({
    { {t::int8{}, t::int4{}}, {t::float8{}} },  // group 0: キー2列, 値1列
    { {t::int8{}, t::int4{}}, {t::int8{}}  },   // group 1: キー2列, 値1列
});
```

内部では以下を構築する：
- 各入力に対応する `takatori::plan::group` exchange
- `relation::step::take_cogroup` 演算子のプロセスへの挿入
- 全グループの変数バインディング

---

## 改善の目的

テストハーネスが用意すべき前提構築を以下の 2 カテゴリに整理し、個別テストケースに記述する内容をスリムにする。

| カテゴリ | 内容 |
|---|---|
| **A** | 演算子をプロセス内で動かすにあたって必要で、演算子の特性によらないもの |
| **B** | 演算子特有の準備が必要だが、個別テストケース間では差異がなく、演算子ごとにほぼ共通のもの |

---

## カテゴリ A: 演算子によらない共通の前提

以下は演算子の種類に関係なく、ほぼ毎回同一のコードが繰り返されていた部分。

### A-1. メモリリソースの初期化

```cpp
memory::page_pool pool_{};
memory::lifo_paged_memory_resource resource_{&pool_};
memory::lifo_paged_memory_resource varlen_resource_{&pool_};
```

### A-2. mock の `task_context` 生成

```cpp
mock::task_context task_ctx{ {}, {}, {}, {} };
```

### A-3. `compiled_info` と `processor_info` の構築

```cpp
yugawara::compiled_info c_info{expression_map_, variable_map_};
processor_info p_info{p0.operators(), c_info};
```

### A-4. `variable_table` のセットアップ

```cpp
auto& block_info = p_info.vars_info_list()[op.block_index()];
variable_table variables{block_info};
```

これらはテストフィクスチャ（`operator_test_utils` クラス）のメンバとして保持し、`create_processor_info()` のような 1 行の呼び出しで完結させる。

---

## カテゴリ B: 演算子ごとに共通の準備

各演算子のテストでは毎回同一パターンの準備が書かれているが、テストケース間では差異がない部分。

### B-1. ストレージ・テーブル・インデックスの登録

```cpp
// 現状のパターン（毎テストに繰り返し）
std::shared_ptr<storage::configurable_provider> storages = std::make_shared<...>();
std::shared_ptr<storage::table> t0 = storages->add_table({ "T0", { ... } });
std::shared_ptr<storage::index> i0 = storages->add_index({ t0, "I0" });
```

`operator_test_utils::create_table()` と `create_primary_index()` に集約済み（scan_test で活用済み）。filter_test や project_test などにも横展開する。

### B-2. プラン・プロセスグラフの構築

```cpp
takatori::plan::graph_type p;
auto&& p0 = p.insert(takatori::plan::process{});
```

`operator_test_utils` のコンストラクタで初期化済み（`plan_`, `process_` メンバ）。各テストはこれを利用する。

### B-3. 上流・下流スタブ演算子の配置

テスト対象演算子の上流・下流にスタブを配置するパターン。

**上流スタブ**: `add_upstream_record_provider` / `add_upstream_group_provider` / `add_upstream_cogroup_provider` として新規に追加する（前述）。filter のテストが scan を意識しなくて済む。

**下流スタブ（verifier）**: テスト対象演算子の下流には `offer` などの実演算子を置かない。代わりに、全テスト共通の検証用モック演算子（verifier）を配置する。verifier はラムダを受け取り、演算子から呼ばれるたびにそのラムダを実行する。これにより、各テストケースは「下流で何を確認したいか」だけをラムダとして記述すればよい。

```cpp
// 全テスト共通（フィクスチャが自動でセット）
add_downstream_verifier([&](/* 入力種別に応じた引数 */) {
    // 各テストケースが記述する検証ロジック
});
```

入力種別に応じて以下の 3 種を用意する（`verifier.h` の `verifier` / `group_verifier` / `cogroup_verifier` に対応）：

| 関数名 | 対応する入力種別 | lambda シグネチャ |
|---|---|---|
| `add_downstream_record_verifier(fn)` | record | `void()` |
| `add_downstream_group_verifier(fn)` | group | `void(bool last_member)` |
| `add_downstream_cogroup_verifier(fn)` | cogroup | `void(cogroup<Iterator>&)` |

### B-4. 変数バインディング

```cpp
variables().bind(c0, t::int8{});
variables().bind(c1, t::int8{});
// ...（列数だけ繰り返す）
```

`add_types()` / `add_column_types()` / `add_key_types()` に集約済み（scan_test で活用済み）。他演算子テストへも横展開する。`add_upstream_xxx_provider` の内部で呼ぶことで、利用者は意識しない。

### B-5. 演算子コンテキストの生成と演算子の挿入

`filter_context`, `project_context` などの演算子コンテキストの生成や、`add_filter` / `add_project` などの演算子挿入ヘルパは、演算子ごとに必要な引数（条件式、列定義など）が異なる。そのため、これらは `operator_test_utils` には含めず、**各テストファイル（`filter_test.cpp`、`project_test.cpp` など）のフィクスチャクラス**に実装する。

`operator_test_utils` から提供されるメモリリソース（`resource_`, `varlen_resource_`）や `processor_info_` を利用することで、各テストファイルのヘルパ実装は簡潔になる。

---

## 現状と改善後の比較

### 現状の filter_test::simple（概略）

```cpp
TEST_F(filter_test, simple) {
    // ストレージ・テーブル・インデックス構築（20行）
    // forward exchange 構築（10行）
    // プラン・グラフ・scan・filter・offer 構築（40行）
    // 変数バインディング（15行）
    // compiled_info / processor_info 構築（5行）
    // variable_table 構築（5行）
    // task_context 構築（5行）
    // メモリリソース（5行）
    // filter_context 構築（3行）
    // 変数への値セット（10行）
    // --- ここからがテスト本体 ---
    s(ctx);
    ASSERT_TRUE(called);
}
```

合計 ~120 行のうち、テスト固有のロジックは ~20 行。

### 改善後のイメージ

```cpp
// filter_test.cpp のフィクスチャクラスに定義するヘルパ（B-5）
// add_filter, make_filter_context は filter_test.cpp 固有の実装
class filter_test : public test_root, public operator_test_utils {
public:
    // 上流の record provider が供給する stream variables を受け取り、
    // filter 演算子をプロセスに挿入して downstream に接続する
    relation::filter& add_filter(
        std::unique_ptr<scalar::expression> cond,
        record_operator* downstream
    );

    // filter_context をメモリリソース込みで生成する
    filter_context make_filter_context(relation::filter& op);
};

// テストケース本体
TEST_F(filter_test, simple) {
    // B-3: 上流スタブ（record provider）を用意し、ストリーム変数を得る
    //      filter は passive な record_operator なので add_upstream_record_provider を使う
    //      上流が scan なのか values なのかはテストケースが意識しない
    auto vars = add_upstream_record_provider({t::int8{}, t::int8{}, t::int8{}});
    auto c0 = vars[0]; auto c1 = vars[1]; auto c2 = vars[2];

    // B-3/B-5: verifier を下流に配置し、filter 演算子を挿入
    //          add_filter・add_downstream_record_verifier は filter_test.cpp のヘルパ
    bool called = false;
    auto& filter_op = add_filter(
        condition_expr,
        add_downstream_record_verifier([&]{ called = true; })
    );

    // A-3: compiled_info / processor_info
    create_processor_info();

    // B-5: コンテキスト生成（filter_test.cpp のヘルパ）
    auto ctx = make_filter_context(filter_op);

    // --- テスト本体 ---
    set_int8(ctx, c1, 11); set_int8(ctx, c2, 10);   // c1 == c2 + 1 → true
    filter_op(ctx);
    ASSERT_TRUE(called);

    called = false;
    set_int8(ctx, c1, 20); set_int8(ctx, c2, 22);   // c1 != c2 + 1 → false
    filter_op(ctx);
    ASSERT_TRUE(! called);
}
```

ボイラープレートが大幅に削減され、テストの意図が明確になる。`scan` が上流にいること、`offer` が下流にいることをテストコードが一切意識しない。verifier のラムダが検証ロジックの唯一の記述場所となる。

---

## 実装方針

### フィクスチャの整備

`operator_test_utils`（`test/jogasaki/operator_test_utils.h`）はすでに scan_test で活用されており、同クラスへの機能追加を軸に進める。

**`operator_test_utils` に追加・拡充すべき機能（テスト間共通）：**

| メソッド | 目的 |
|---|---|
| `add_upstream_record_provider(types, impl=...)` | 指定した型列の record を供給する上流スタブをプロセスに追加し、stream variables を返す |
| `add_upstream_group_provider(key_types, value_types)` | group exchange + take_group を構築し、key/value stream variables を返す |
| `add_upstream_cogroup_provider(groups)` | 複数 group exchange + take_cogroup を構築し、各グループの stream variables を返す |
| `add_downstream_record_verifier(fn)` | `void()` を受け取る verifier を下流に配置し、参照を返す |
| `add_downstream_group_verifier(fn)` | `void(bool)` を受け取る group_verifier を下流に配置し、参照を返す |
| `add_downstream_cogroup_verifier(fn)` | `void(cogroup<Iterator>&)` を受け取る cogroup_verifier を下流に配置し、参照を返す |
| `set_value<T>(ctx, var, val)` | コンテキストの `variable_table` の特定変数に値をセットする |
| `set_null(ctx, var, is_null)` | コンテキストの null フラグをセットする |

**各テストファイルのフィクスチャクラスに実装する機能（演算子固有）：**

| メソッド | 実装場所 | 目的 |
|---|---|---|
| `add_filter(expr, downstream)` | `filter_test.cpp` | filter 演算子を挿入し、downstream に接続する |
| `make_filter_context(op)` | `filter_test.cpp` | `filter_context` をメモリリソース込みで生成する |
| `add_project(columns, downstream)` | `project_test.cpp` | project 演算子を挿入し、downstream に接続する |
| `make_project_context(op)` | `project_test.cpp` | `project_context` をメモリリソース込みで生成する |
| （他の演算子も同様） | 各テストファイル | 演算子固有の引数・型を持つため各ファイルに委ねる |

### テスト移行方針

1. `operator_test_utils` に必要なメソッドを追加する。
2. 各演算子テストのフィクスチャを `operator_test_utils` を継承するよう変更する（scan_test パターンに倣う）。
3. 既存テストを新しいフィクスチャを使って書き換え、テスト本体部分のみが残るようにする。
4. 書き換えによって浮いたコードパスの余力を使い、NULL 入力・型境界・エラーパスなどの追加テストケースを補完する。

### 対象演算子テストファイルと分類

| ファイル | active/passive | 入力種別 | 上流スタブ | 状態 |
|---|---|---|---|---|
| `scan_test.cpp` | active | record | 不要（KVSが入力源） | `operator_test_utils` 使用済み（ベースライン） |
| `filter_test.cpp` | passive | record | `add_upstream_record_provider` | 移行対象 |
| `project_test.cpp` | passive | record | `add_upstream_record_provider` | 移行対象 |
| `offer_test.cpp` | passive | record | `add_upstream_record_provider` | 移行対象 |
| `find_test.cpp` | active | record | 不要（KVS が入力源） | 移行対象 |
| `join_scan_test.cpp` | passive | record | `add_upstream_record_provider` | 移行対象 |
| `join_find_test.cpp` | passive | record | `add_upstream_record_provider` | 移行対象 |
| `join_test.cpp` | passive | cogroup | `add_upstream_cogroup_provider` | 移行対象 |
| `take_flat_test.cpp` | active | record | 不要（exchange が入力源） | 移行対象 |
| `take_group_test.cpp` | active | group | 不要（group exchange が入力源） | 移行対象 |
| `take_cogroup_test.cpp` | active | cogroup | 不要（group exchange 群が入力源） | 移行対象 |
| `aggregate_group_test.cpp` | passive | group | `add_upstream_group_provider` | 移行対象 |
| `write_existing_test.cpp` | passive | record | `add_upstream_record_provider` | 移行対象 |

---

## まとめ

| | 現状 | 改善後 |
|---|---|---|
| 1テストケースの行数 | ~100〜200行 | ~20〜40行 |
| テスト本体の割合 | ~10〜20% | ~50〜70% |
| テストケース数 | 演算子あたり 1〜3件 | コードパス網羅が可能な件数へ |
| ボイラープレートの所在 | 各テストに散在 | `operator_test_utils` に集約 |
| 上流実装の依存 | テストコードに scan 等が露出 | `add_upstream_xxx_provider` で隠蔽 |
| 下流実装の依存 | テストコードに offer 等が露出 | `add_downstream_xxx_verifier` で隠蔽・lambda で検証ロジックを注入 |

---

## 実装済みパターンと設計知見

以下は `project_test.cpp` と `filter_test.cpp` を `operator_test_utils` ベースで書き直した際に確立した知見をまとめたものである。

---

### executor 構造体パターン

passive な record 演算子のテスト（`project`, `filter` など）では、実行時オブジェクトを束ねる `*_executor` 構造体と、それを生成する `make_*_executor()` メソッドを各テストファイルのフィクスチャクラスに定義する。

```cpp
struct filter_executor {
    filter op_;
    variable_table variables_;
    mock::task_context task_ctx_;
    filter_context ctx_;

    filter_executor(
        filter op_arg,
        variable_table_info const& block_info,
        memory::lifo_paged_memory_resource* res,
        memory::lifo_paged_memory_resource* varlen_res,
        request_context* req_ctx
    ) :
        op_{std::move(op_arg)},
        variables_{block_info},
        task_ctx_{{}, {}, {}, {}},
        ctx_{&task_ctx_, variables_, res, varlen_res}
    {
        ctx_.task_context().work_context(std::make_unique<impl::work_context>(
            req_ctx, 0, op_.block_index(), nullptr, nullptr, nullptr, nullptr, false, false
        ));
    }
};
```

**設計上の制約**: `ctx_` は `variables_` と `task_ctx_` への参照を内部に保持する。そのため構造体はコピーもムーブもできない。C++17 の guaranteed copy elision（prvalue return）を利用することで、`make_*_executor()` から返却する際もコピー・ムーブが発生しない。

```cpp
filter_executor make_filter_executor(...) {
    // ...
    return filter_executor{std::move(op), ...};  // prvalue はコピー・ムーブを起こさない
}
```

---

### work_context は passive record 演算子テストで必須

`filter`, `project` などの passive record 演算子は `context_helper::blob_session_container()` を呼び出す。これは内部で `work_context_->blob_session_container()` を null チェクなしに呼ぶ。そのため、`task_context` に `work_context` をセットしていないとクラッシュする。

`work_context` のセットアップはコンストラクタ本体で行う（初期化リストでは `op_` が未構築なため `op_.block_index()` が使用できない）：

```cpp
// コンストラクタ本体内で行う（初期化リスト内では不可）
ctx_.task_context().work_context(std::make_unique<impl::work_context>(
    req_ctx, 0, op_.block_index(), nullptr, nullptr, nullptr, nullptr, false, false
));
```

---

### `emplace_operator<Op>()` テンプレートで operator node を挿入する

プロセスグラフへの演算子ノード挿入には `emplace_operator<Op>()` テンプレートを使う。`process_.operators().emplace<Op>(...)` の冗長な記述を省略できる。

```cpp
// 使用例
auto& flt = emplace_operator<relation::filter>(std::move(cond));
auto& prj = emplace_operator<relation::project>(
    relation::project::column{varref(in[0]), out[0]},
    relation::project::column{varref(in[1]), out[1]}
);
```

---

### `add_upstream_record_provider` と `input_definition`

`add_upstream_record_provider(meta_ptr)` は `take_flat` を上流に追加し、各列の変数を `variable_map_` に登録する。返り値は `(take_flat&, input_definition)` のペアで、`input_definition` には stream variables のリストと `record_meta` が入っている。

```cpp
auto [up, in] = add_upstream_record_provider(input.record_meta());
// in[0], in[1], ... が stream variables
// in.meta_ が record_meta（set_variables() に渡す）
```

`record_meta()` の戻り値型は `maybe_shared_ptr<record_meta>`（`takatori::util::maybe_shared_ptr`）であり、`std::shared_ptr` への暗黙変換はない。`add_upstream_record_provider` のパラメータも `maybe_shared_ptr<record_meta>` で受け取る。

---

### `set_variables` / `get_variables` の使い方

```cpp
// upstream から読み出した record_ref の値を変数ブロックへコピーする
set_variables(ex.variables_, in, input_record.ref());

// 変数ブロックから値を basic_record として取り出して比較する
auto result = get_variables(ex.variables_, {out[0], out[1]});
ASSERT_EQ(expected, result);
```

内部では `utils::copy_nullable_field()` を使って nullable フィールドをコピーする。
