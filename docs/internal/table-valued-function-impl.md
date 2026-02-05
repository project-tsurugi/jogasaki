# 表値関数

## この文書について

表値関数に関する実装上の特筆すべき内容についてメモを記載

## 設計

### UDF 実装からの抽象化

apply 演算子は UDF 特有のデータ構造（`generic_record_stream`, `generic_record`）に直接依存しない。
代わりに、以下の抽象的なインターフェースを使用する：

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

### エラーハンドリング

- UDFのエラーは基本的には `generic_record` にエラーをセットする形で伝搬される
  - 特にスカラ関数の場合は戻る `generic_record` が単独なので、式評価結果を直接確認すればよい (結果値のanyがエラーで、エラーメッセージは evaluator context にセットされる)
  - 表値関数の場合は、`generic_record` のエラーを `any_sequence` に積み替えて、その内容を使用する側 (apply 演算子など) で確認する
- さらに表値関数の場合は、`generic_record` を取得するための `generic_record_stream` の取得までに失敗する可能性がある
  - この場合は `generic_record_stream` が `nullptr` として戻され、かつエラー情報は evaluator context にセットされる
  - apply 演算子ではこのエラー情報を確認し、適切に request context に伝搬する
- API上は、`generic_record_stream` はある `generic_record` がエラーだとしても継続することが可能だが、現在の `apply` 実装では1つでもエラーが発生した場合は即座に中断する
  - jogasakiが呼出側へ複数エラーを戻す機構がないため


