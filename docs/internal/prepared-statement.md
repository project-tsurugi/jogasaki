# prepared statementの概念と実装

2025-07-06 kurosawa

## この文書について

この文書はSQL実行エンジンにおけるprepared statementの概念と仕様、その内部設計について記述する

## prepared statementとは

prepared statementは、SQLクエリをあらかじめパースし、コンパイルして再利用可能な形で保存する仕組みである。これにより、同じクエリを異なるパラメータで繰り返し実行する際のパフォーマンスを向上させることができる。

### API概要

Jogasakiにおけるprepared statementは以下のAPIを通じて利用される：

#### 1. Preparation Phase（準備フェーズ）
```cpp
// SQLステートメントの準備
status database::prepare(
    std::string_view sql,
    statement_handle& handle
);

// パラメータ付きステートメントの準備
status database::prepare(
    std::string_view sql,
    std::unordered_map<std::string, api::field_type_kind> const& variables,
    statement_handle& handle
);
```

#### 2. Resolution Phase（解決フェーズ）
```cpp
// パラメータを束縛してexecutable statementを作成
status database::resolve(
    statement_handle handle,
    maybe_shared_ptr<api::parameter_set const> parameters,
    std::unique_ptr<api::executable_statement>& statement
);
```

### API上の規約

- **Thread Safety**: `statement_handle`は複数スレッドから安全に共有可能
- **Ownership**: `statement_handle`は軽量なハンドルで、実際のステートメントは`database`が管理
- **Lifecycle**: prepared statementは明示的に破棄されるまで、またはデータベースが閉じられるまで有効
- **Parameter Binding**: パラメータは型安全に束縛される必要がある
- **Immutability**: prepared statementは一度作成されると変更不可

### 典型的な使用パターン

```cpp
// 1. ステートメントの準備
std::string sql = "SELECT * FROM users WHERE id = :user_id";
std::unordered_map<std::string, api::field_type_kind> variables{
    {"user_id", api::field_type_kind::int8}
};
statement_handle handle;
ASSERT_EQ(status::ok, db->prepare(sql, variables, handle));

// 2. パラメータセットの作成
auto params = api::create_parameter_set();
params->set_int8("user_id", 42);

// 3. executable statementの作成
std::unique_ptr<api::executable_statement> executable;
ASSERT_EQ(status::ok, db->resolve(handle, params, executable));

// 4. 実行（別途実行APIを使用）
```

## prepared statementの内部設計

### アーキテクチャ概要

prepared statementの実装は以下の層から構成される：

1. **Public API Layer** (`include/jogasaki/api/`)
2. **Implementation Layer** (`src/jogasaki/api/impl/`)
3. **Planning Layer** (`src/jogasaki/plan/`)

### 核となるクラス

#### Public API Classes

**`api::executable_statement`**
- コンパイル済みで実行可能なステートメントの抽象ベースクラス
- `meta()`メソッドで出力メタデータにアクセス
- non-copyable, non-movableで安全性を確保

**`api::statement_handle`**
- prepared statementへの軽量なハンドル
- trivially copyableで高速な複製が可能
- ハッシュ値計算に対応し、連想コンテナでの使用が可能

#### Implementation Classes

**`api::impl::prepared_statement`**
- `plan::prepared_statement`をAPI層のメタデータでラップ
- 外部writer用のメタデータを管理
- 結果レコードの有無を判定する`has_result_records()`メソッドを提供

**`api::impl::executable_statement`**
- public `api::executable_statement`インターフェースの実装
- `plan::executable_statement`とコンパイル時メモリリソースを保持
- パラメータセットの管理と実行ボディへのアクセスを提供

#### Planning Classes

**`plan::prepared_statement`**
- パース済みTakatoriステートメントを保持
- コンパイル情報とhost variableを管理
- SQLテキストと実行メタデータのmirror containerを保存

**`plan::executable_statement`**
- 完全にコンパイル済みの実行オペレータを保持
- host variable用のvariable tableを含む
- DDL検出と実行タイプチェックを提供

### ライフサイクルと実行フロー

#### Phase 1: Preparation（SQL → Prepared Statement）

1. **API Entry Point**: `database::prepare(sql, statement_handle&)`
2. **Compilation Context Setup**: ストレージプロバイダーを含むコンパイラコンテキストを作成
3. **SQL Parsing**: Mizugakiパーサーを使用してSQLをASTに変換
4. **Semantic Analysis**: Yugawaraコンパイラーが型チェックと最適化を実行
5. **Mirror Preprocessing**: 実行メタデータコンテナを作成
6. **Storage**: prepared statementを並行ハッシュマップまたはセッションストアに保存

#### Phase 2: Resolution（Prepared Statement → Executable Statement）

1. **Parameter Binding**: プレースホルダー変数を実際の値で解決
2. **Host Variable Creation**: パラメータ用のvariable tableを作成
3. **Final Compilation**: 実行可能なオペレータにコンパイル
4. **Memory Resource Association**: 実行用メモリリソースを関連付け

### ストレージとキャッシュシステム

#### Statement Storage

**`api::impl::statement_store`**
- TBBを使用したスレッドセーフな並行ハッシュマップ
- セッションスコープでのprepared statementの保存
- セッション破棄時の自動クリーンアップ
- `tateyama::api::server::session_element`インターフェースを実装

#### キャッシュ戦略

- **Global Cache**: セッションレスprepared statement用
- **Session Cache**: セッションスコープprepared statement用
- **Concurrent Access**: スレッドセーフティのためTBB concurrent hash mapを使用
- **Memory Management**: shared_ptrによるRAIIベースのクリーンアップ

### メモリ管理

#### 専用メモリリソース

- **`memory::lifo_paged_memory_resource`**: コンパイル用メモリに使用
- **NUMA-aware allocation**: 効率的なメモリ使用のためページプールを活用
- **Resource lifecycle**: executable statementのライフタイムに紐付け

#### メモリ割り当てパターン

- **Compilation Phase**: 一時オブジェクト用の専用メモリリソースを使用
- **Execution Phase**: executable statementとともにメモリリソースを移動
- **Cleanup**: executable statement破棄時の自動リソースクリーンアップ

### パラメータシステム

#### Host Variables

- **Type System**: `api::field_type_kind`を通じたすべてのSQLデータ型をサポート
- **Variable Provider**: Yugawaraのconfigurable providerシステムを使用
- **Parameter Sets**: 強い型付けされたパラメータコレクション
- **Variable Tables**: 実行時変数ストレージとアクセス

### エラーハンドリング

#### エラー伝播

- **Compilation Errors**: `error::error_info`を通じた詳細なエラー情報
- **Parse Errors**: Mizugakiパーサー診断情報
- **Semantic Errors**: Yugawaraコンパイラー診断情報
- **Runtime Errors**: KVSと実行層エラー

### 外部システム統合

#### コンパイルパイプライン

1. **Mizugaki Parser**: SQLテキスト → AST
2. **Yugawara Compiler**: AST → 最適化済み実行プラン
3. **Jogasaki Compiler**: 実行プラン → Jogasakiオペレータ
4. **Mirror Processing**: メタデータ抽出と最適化

#### 外部依存関係

- **Takatori**: コアIRとステートメント表現
- **Yugawara**: SQLコンパイルと最適化
- **Mizugaki**: SQLパースィング
- **Sharksfin**: ストレージ抽象化

## その他、注意事項

### パフォーマンスの考慮事項

- **Two-Phase Compilation**: preparationとresolutionの分離によりパラメータ再利用を最適化
- **Memory Resource Management**: 専用アロケータによる効率的なメモリ使用
- **NUMA Awareness**: マルチソケットシステムでのメモリアクセス最適化
- **Concurrent Access**: 高並行性環境での安全な共有アクセス

### 制限事項

- **Immutability**: prepared statementは一度作成すると変更不可
- **Parameter Type Safety**: パラメータの型は準備時に決定され、実行時に変更不可
- **Session Lifecycle**: セッションスコープのステートメントはセッション終了時に自動破棄

### 開発時の注意点

- **Memory Management**: メモリリソースのライフサイクルを適切に管理
- **Error Handling**: すべてのコンパイル段階でのエラーハンドリングを実装
- **Thread Safety**: 並行アクセスを考慮した設計
- **Testing**: パラメータ化クエリの包括的テストを実施