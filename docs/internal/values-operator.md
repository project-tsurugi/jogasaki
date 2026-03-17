# VALUES 句演算子化の設計

2026-03-10 kurosawa

## 本文書について

tsurugi-issues #872 の実装設計を記述する。

## 背景・問題

### 現状

- `INSERT INTO T VALUES (expr1, expr2, ...)` のような単純な INSERT 文は、`write_statement` が VALUES 句の評価と書き込みの両方を担当している
  - VALUES 句の各行について式を評価し、`write_field` を通じてレコードを構成する処理が `write_statement::execute()` に内包されている
  - `fill_evaluated_value()` 関数（`write_statement.cpp` 内 static 関数）が各フィールドの式評価を担う

### 新たな要求

- VALUES 句に subquery を含める構文への対応が必要になる（例: `INSERT INTO T VALUES (..., (SELECT ...))`)
- subquery 部分は上流クエリとして実行し、その出力と VALUES 句の他の列とを結合して INSERT に渡す構成が必要
- このためには VALUES 句の処理を独立した演算子として切り出し、クエリパイプラインに組み込めるようにする必要がある

## 実装方針

VALUES 句の処理を `write_statement` から分離し、独立した `values` 演算子として実装することで、VALUES 句内の subquery に対応可能な構成を実現する。
値の保持にかんする共通ロジックを外部化して `value` 演算子と `write_statement` で共有するようにする

## 初期見積もり

3d-5d

## 実装詳細

### values 演算子の新設

- `src/jogasaki/executor/process/impl/ops/` 以下に `values` 演算子クラスを新設する
  - 各行の式を評価してレコードを生成し、下流の `write_create` 演算子（または既存の write 系演算子）に渡す
  - 単純な INSERT 文においても、`write_statement` の代わりに `values` -> `write_create` というパイプラインで実行される形になる
  - コンストラクタで全行・全列の `expr::evaluator` と式のソース型（`takatori::type::data const*`）を構築・保持する
  - 行数をまたいで yield/resume に対応するため、`values_context` に `current_row_` を持たせ再開後も利用可能にする

### 値保持ロジックの共通化

- `write_statement.cpp` 内の `fill_evaluated_value()` は現状 static 関数として外部から参照できない
  - この関数を `executor/common/` 配下の共通関数として外部化し、`write_statement` と新設の `values` 演算子の両方から呼び出せるようにする
  - 具体的には `fill_evaluated_value.{h, cpp}` を新設する

### 型変換（暗黙キャスト）への対応

- VALUES 句が出力する列の型はあたえられた式の型から決定されるが、複数行がある場合、SQL コンパイラが単一化変換により型を決定するため、個々の行の式評価結果の型と列変数の型が一致しないケースがある（例: `VALUES (1, 2.0), (4, 5)` の第二列は DECIMAL になる）
- apply演算子構築時に列の型を保存しておき、式の計算結果との差を検出した場合は型変換を実施する

### processor_info の拡張

- `processor_details` に `has_values_operator()` メソッドを追加し、values 演算子を含むプロセスのパーティションは強制的に 1 にする
  - 複数並列でうごくと VALUES 句のデータが重複して出力されてしまうため

## 既存コードの問題点の修正

### write_existing の修正

- INSERT ... SELECT 文において、SELECT 句の式が NULL を返す場合に、NOT NULL 制約違反の例外が発生するべきところで `not_null_constraint_violation_exception` でなく `sql_execution_exception` を戻していた 
  - VALUES 演算子が NULL を下流に渡すケースでも同様の問題があったため、`not_null_constraint_violation_exception` を返すこと処理を `write_existing.cpp` に追加

## テスト方針

- `SELECT 1` / `SELECT 1, 2` / `SELECT 1 + 2` などリテラル SELECT（FROM 句なし）が values 演算子経由で正しく結果を返すこと
- サブクエリをINSERT文のvalues句に入れたクエリを実行して、正しくINSERTが完了することを確認する
- `VALUES (...), (...)` 形式の multi-row VALUES クエリが全行正しく返ること
- 行によって列の式型が異なる場合（暗黙キャストが必要なケース）でも正しく動作すること
