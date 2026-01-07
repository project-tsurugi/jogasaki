# 表値関数実装作業 (Jogasaki)

## この文書について

この文書は、Tsurugi における表値関数機能の jogasaki 内部実装作業を管理するためのものです。
[表値関数の仕様](./internal/.local/table-valued-function_ja.md) に基づき、jogasaki での実装タスクをリストします。

## 目標

- UDTF (User-Defined Table-valued Function) を SQL から呼び出せるようにする
- `APPLY` 演算子を実装し、左表の各行に対して表値関数を呼び出して結合する
- 最初のフェーズでは同期的な実行モデルを実装する
  - 表値関数の結果をすべて取得してから次の処理に進む
  - ワーカースレッドを占有する単純な実装

## 前提条件

以下の機能は UDF 領域の担当者が実装するため、jogasaki 側では利用可能であることを前提とします：

- `generic_record_stream` クラス（UDF 用のストリーム機能）
- `generic_client::call_server_streaming_async()` メソッド
- ストリーミングレコードのバッファ管理

## 設計原則

### UDF 実装からの抽象化

apply 演算子は UDF 特有のデータ構造（`generic_record_stream`, `generic_record`）に直接依存しません。
代わりに、以下の抽象的なインターフェースを使用します：

- **`data::any_sequence`**: 表値の1行を表す
  - 内部実装: `std::vector<any>` でデータを保持
  - 列値のシーケンスとして扱える
  - `view()` メソッドで `takatori::util::sequence_view<any>` を返す
  
- **`data::any_sequence_stream`**: 表値（複数行）を表すストリーム
  - `try_next(any_sequence&)`: 次の行を非ブロッキングで取得
  - `next(any_sequence&, timeout)`: 次の行をタイムアウト付きで取得
  - `close()`: ストリームをクローズ
  - `generic_record_stream` と同様のインターフェース

### 統合ポイント

- **ビルトイン表値関数**: `any_sequence_stream` を直接構築して返す
- **UDTF**: `generic_record_stream` から `any_sequence_stream` へデータを移し替えるアダプターを実装
  - このアダプターは UDF 統合レイヤー（`udf_functions.cpp` など）で実装
  - apply 演算子からは見えない

## 設計概要

### apply 演算子の動作

`APPLY` 演算子は以下のように動作します：

```python
# 同期版（第1フェーズ）
def apply_operator_sync(left_input: tuple) -> None:
    # 表値関数を呼び出し、any_sequence_stream を取得
    right_stream: any_sequence_stream = func(arg_1(left_input), arg_2(left_input), ...)
    right_records = collect_all(right_stream)  # すべて取得（各要素は any_sequence）
    
    if right_records.is_empty():
        if is_outer:
            output = left_input + null_columns_for_right()
            proceed(output)
    else:
        for right_row: any_sequence in right_records:
            output = left_input + right_row
            proceed(output)
```

### 表値関数の識別

- takatori の `relation::apply` ノードから表値関数を呼び出す
- 関数デスクリプタの `function_kind::server_streaming` で UDTF を識別
- yugawara の関数宣言に `function_feature::table_valued` フラグが設定される

### テスト戦略

- UDF 呼び出しを伴わないビルトイン表値関数を用意してテストを行う
- モック関数として、固定の表値を返す関数を実装
- 単体テストで `CROSS APPLY` / `OUTER APPLY` の動作を確認

## 実装タスク（第1フェーズ：同期実行）

### 1. any_sequence / any_sequence_stream の実装

**目的**: UDF 特有のデータ構造から apply 演算子を抽象化する

**タスク**:
- [x] `data::any_sequence` クラスの定義
  - `takatori::util::sequence_view<any>` をラップ
  - 列値のシーケンスとして行データを表現
  - イテレーション機能の提供
- [x] `data::any_sequence_stream` クラスの定義
  - 抽象インターフェース（純粋仮想クラス）
  - `try_next(any_sequence&)` メソッド
  - `next(any_sequence&, timeout)` メソッド
  - `close()` メソッド
  - `any_sequence_stream_status` enum の定義（`ok`, `error`, `end_of_stream`, `not_ready`）
- [x] ビルトイン用の具象実装クラス
  - `mock_any_sequence_stream`: メモリ上のデータを返すシンプルな実装
  - テスト用モック関数で使用

**成果物**:
- `src/jogasaki/data/any_sequence.h`
- `src/jogasaki/data/any_sequence.cpp`
- `src/jogasaki/data/any_sequence_stream.h`
- `src/jogasaki/data/any_sequence_stream.cpp`
- `src/jogasaki/data/mock_any_sequence_stream.h`
- `src/jogasaki/data/mock_any_sequence_stream.cpp`

### 2. 表値関数リポジトリの追加

**目的**: 表値関数を管理するリポジトリを実装する

**タスク**:
- [x] `table_valued_function_repository.h` の作成
- [x] `table_valued_function_repository.cpp` の実装
- [x] `table_valued_function_info` クラスの定義
  - 関数名、引数型、戻り値の表型などの情報を保持  - 関数本体: `std::function<std::unique_ptr<any_sequence_stream>(...)>` のような形式- [x] 既存の `scalar_function_repository` を参考に実装
- [x] リポジトリへの登録・検索 API の実装

**成果物**:
- `src/jogasaki/executor/function/table_valued_function_info.h`
- `src/jogasaki/executor/function/table_valued_function_info.cpp`
- `src/jogasaki/executor/function/table_valued_function_repository.h`
- `src/jogasaki/executor/function/table_valued_function_repository.cpp`

### 3. モック表値関数の実装（テスト用）

**目的**: UDF を使わずにテスト可能なモック表値関数を実装する

**タスク**:
- [x] `builtin_table_valued_functions.h` の作成
- [x] モック関数の実装
  - 各関数は `mock_any_sequence_stream` を構築して返す
  - 例: `mock_table_func(INT4) -> TABLE(c1 INT4, c2 INT8)` のような固定値を返す関数
  - 空の表を返す関数（OUTER APPLY のテスト用）
  - 複数行を返す関数
  - エラーを返す関数（エラーハンドリングのテスト用）
- [x] ビルトイン表値関数の登録処理
- [x] 関数 ID の定義（`builtin_table_valued_functions_id.h`）

**成果物**:
- `src/jogasaki/executor/function/builtin_table_valued_functions.h`
- `src/jogasaki/executor/function/builtin_table_valued_functions.cpp`
- `src/jogasaki/executor/function/builtin_table_valued_functions_id.h`

### 4. apply 演算子の実装

**目的**: `relation::apply` ノードに対応する実行演算子を実装する

**タスク**:
- [x] `ops/apply.h` の作成
  - apply 演算子クラスの定義
  - `apply_kind::cross` / `apply_kind::outer` の対応
- [x] `ops/apply.cpp` の実装
  - 左表からの入力を受け取る
  - 表値関数を呼び出し、`any_sequence_stream` を取得
  - `any_sequence_stream::next()` から全レコードを同期的に取得
  - 左表の行と右表の行（`any_sequence`）を結合
  - OUTER APPLY の場合の NULL 埋め処理
  - エラーハンドリング
- [x] `operator_builder.cpp` の `operator()(const relation::apply&)` 実装
  - 現在は未実装（`throw_exception`）
  - apply 演算子インスタンスの生成
  - 表値関数リポジトリから関数情報を取得

**成果物**:
- `src/jogasaki/executor/process/impl/ops/apply.h`
- `src/jogasaki/executor/process/impl/ops/apply.cpp`
- `src/jogasaki/executor/process/impl/ops/operator_builder.cpp` の修正

### 5. any_sequence からの値抽出と変数アサイン

**目的**: apply 演算子で `any_sequence` から列値を取得し、出力変数にアサインする

**タスク**:
- [x] `any_sequence` のイテレーション処理
- [x] `any` 型から具体的な型への変換
- [x] 出力変数への値のアサイン
  - 左表の列 + 右表の列を連結した出力レコードの生成
  - 既存の変数アサイン機構（`accessor` など）との統合
- [x] OUTER APPLY 時の NULL 列の生成
- [x] 型チェックとエラーハンドリング

**成果物**:
- `ops/apply.cpp` 内の結合処理実装

### 6. UDF 関数リポジトリへの統合

**目的**: UDTF を yugawara の関数定義から jogasaki に登録する

**タスク**:
- [ ] `udf_functions.h` への UDTF サポート追加
  - `add_udf_table_valued_functions()` 関数の追加
- [ ] `udf_functions.cpp` での UDTF 登録処理
  - Server Streaming RPC を持つ関数を識別
  - `function_kind::server_streaming` の判定
  - `function_feature::table_valued` フラグの確認
  - `generic_record_stream` から `any_sequence_stream` へのアダプター実装
    - `udf_any_sequence_stream` クラス: `any_sequence_stream` のUDF用実装
    - `generic_record_stream` をラップし、`generic_record` を `any_sequence` に変換
  - 表値関数リポジトリへの登録
- [ ] `database.cpp` からの呼び出し
  - 既存の `add_udf_functions()` を参考に実装
  - `configuration::mock_table_valued_functions()` が `true` の場合のみ `add_builtin_table_valued_functions()` を呼び出す

**成果物**:
- `src/jogasaki/executor/function/udf_functions.h` の修正
- `src/jogasaki/executor/function/udf_functions.cpp` の修正
- `src/jogasaki/executor/function/udf_any_sequence_stream.h` (アダプタークラス)
- `src/jogasaki/executor/function/udf_any_sequence_stream.cpp`
- `src/jogasaki/api/impl/database.cpp` の修正

### 7. テストの作成

**目的**: apply 演算子の動作を検証するテストを作成する

**タスク**:
- [x] `any_sequence` / `any_sequence_stream` の単体テスト
  - `mock_any_sequence_stream` の動作確認
- [ ] apply 演算子の単体テスト `apply_test.cpp`
  - モック表値関数を使用したテスト
  - CROSS APPLY の動作確認
    - 1行の左表 × N行の右表 → N行の出力
    - 左表が空の場合 → 空の出力
    - 右表が空の場合 → 空の出力
  - OUTER APPLY の動作確認
    - 右表が空の場合 → 左表の行 + NULL 列で出力
  - エラーケース
    - 関数呼び出しエラー
    - ストリームからのエラーレコード
- [ ] UDF アダプターのテスト
  - `udf_any_sequence_stream` の動作確認
  - `generic_record_stream` からの変換が正しく行われることを確認
- [x] 統合テスト
  - SQL レベルの統合テスト `sql_apply_test.cpp` を作成
  - CROSS APPLY / OUTER APPLY の動作検証
  - モック表値関数を使用したエンドツーエンドテスト

**成果物**:
- `test/jogasaki/data/any_sequence_test.cpp`
- `test/jogasaki/executor/process/ops/apply_test.cpp` (スケルトンのみ、DISABLED)
- `test/jogasaki/executor/function/udf_any_sequence_stream_test.cpp`
- `test/jogasaki/api/sql_apply_test.cpp` (統合テスト)
- `mock/jogasaki/udf/mock_udtf_server.h` (必要に応じて)
- `mock/jogasaki/udf/mock_udtf_server.cpp` (必要に応じて)

### 8. ドキュメント・コメントの追加

**目的**: 実装の意図と動作を明確にする

**タスク**:
- [ ] 各クラス・関数への doxygen コメント追加
- [ ] パフォーマンス特性の記録
  - 同期実行であることの明記
  - 表値関数の結果をすべてメモリに保持することの注記

**成果物**:
- ソースコード内のコメント

## 将来の拡張（第2フェーズ：非同期実行）

第2フェーズでは、以下の機能を実装する予定です（現時点ではタスク化しません）。

### 非同期 apply 演算子の設計

**目的**: 表値関数の実行中にワーカースレッドを解放し、他のタスクに譲る

**概要**:

```python
# 非同期版（第2フェーズ）
def apply_operator_async(left_input: tuple) -> None:
    # 表値関数を呼び出し、any_sequence_stream を取得
    right_stream: any_sequence_stream = func(arg_1(left_input), arg_2(left_input), ...)
    schedule_task(apply_operator_continue, left_input, right_stream, true)

def apply_operator_continue(left_input: tuple, right_stream: any_sequence_stream, first_time: bool) -> None:
    right_row = any_sequence()
    status = right_stream.try_next(right_row)
    
    if status == status_type::end_of_stream:
        if first_time and is_outer:
            output = left_input + null_columns_for_right()
            proceed(output)
    elif status == status_type::ok:
        output = left_input + right_row
        proceed(output)
        schedule_task(apply_operator_continue, left_input, right_stream, false)
    elif status == status_type::not_ready:
        # レコードが未準備 → 待機してリスケジュール
        schedule_task_with_wait(apply_operator_continue, left_input, right_stream, first_time)
```

**必要な機能**:
- タスクスケジューリング機構の拡張
  - `schedule_task()` の実装
  - タスクコンテキストの保存・復元
- プロセスコンテキストの拡張
  - タスクキューの管理
  - コールバック登録機能
- apply 演算子の状態管理
  - 左表の行、ストリーム、first_time フラグの保持
  - ステートマシンの実装

**メリット**:
- 長時間実行される UDTF でもワーカースレッドをブロックしない
- スループットの向上
- システム全体のレスポンス性の向上

**課題**:
- 実装の複雑性が増加
- デバッグが困難になる可能性
- メモリ管理（状態の保持）

## 参考資料

- [表値関数の仕様](/home/kuro/git/jogasaki/docs/internal/.local/table-valued-function_ja.md)
- [UDF コンセプト](/home/kuro/git/jogasaki/docs/internal/.local/udf-concept_ja.md)
- [UDF gRPC クライアント](/home/kuro/git/jogasaki/docs/internal/.local/udf-grpc-client_ja.md)
- 既存実装の参考:
  - `src/jogasaki/executor/function/scalar_function_repository.{h,cpp}`
  - `src/jogasaki/executor/function/udf_functions.{h,cpp}`
  - `src/jogasaki/executor/process/impl/ops/` 配下の各演算子

## 進捗管理

| タスク | 担当 | 状態 | 備考 |
|:------|:-----|:-----|:-----|
| 1. any_sequence / any_sequence_stream | | 完了 | storage_typeのみ保持する設計に変更 |
| 2. 表値関数リポジトリ | | 完了 | |
| 3. モック表値関数 | | 完了 | builtin→mockに名前変更 |
| 4. apply 演算子 | | 完了 | |
| 5. any_sequence 統合 | | 完了 | apply.cpp 内で実装 |
| 6. UDF 関数リポジトリ統合 | | 未着手 | アダプター実装含む |
| 7. テスト | | 進行中 | any_sequence_test.cpp 完了 |
| 8. ドキュメント | | 未着手 | |
| 9. configuration統合 | | 完了 | mock_table_valued_functionsプロパティ追加 |
| 9. configuration統合 | | 完了 | mock_table_valued_functionsプロパティ追加 |
