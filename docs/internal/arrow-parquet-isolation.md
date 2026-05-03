# arrow/parquet 依存の分離

2026-05-01 kurosawa

## この文書について

Apache Arrow / Parquet ヘッダに直接依存するソースコードを jogasaki 本体から分離し、独立した CMake ターゲットとしてビルドするための設計メモ。

関連 issue: [tsurugi-issues#1457](https://github.com/project-tsurugi/tsurugi-issues/issues/1457)

## 背景と動機

* Apache Arrow は 23.0.0 以降、`-std=c++20` を要求する
* 現状の jogasaki は C++17 でビルドされている
* 何も対策しないと、jogasaki 全体を C++20 でビルドする必要が生じる
    * jogasaki が依存する他の Tsurugi コンポーネントや jogasaki に依存する側のコードまで C++20 統一の影響が及ぶ
    * これはリスクが大きく、段階的な移行を望ましいとする
* したがって、Arrow/Parquet を直接 include するコードのみを切り出し、別ターゲットとして将来 C++20 でビルド可能な構造にしておく

## 方針

1. ソースレベルの分離 (pimpl 化)
   * ヘッダから `<arrow/...>` / `<parquet/...>` を追い出す
   * 境界ヘッダには C++17 / C++20 で表現が変わらない型のみを残す(下記「境界ヘッダの型」セクション参照)
2. ビルドターゲットレベルの分離
   * Arrow/Parquet を直接使う `.cpp` を `arrow_objs` という OBJECT ライブラリにまとめる
   * `arrow_objs` をテストする部分も別ターゲット化する
   * `arrow_objs` とそのテストだけを C++20 でコンパイルできる

## 境界ヘッダの型

pimpl 後の public ヘッダで使われる型は、異なる C++ 標準によってコンパイルされるため、
同名の型についてサイズ・レイアウトなどABI上の差異生じた場合、ODR違反でUBになる可能性がある。これを避ける方法としては下記がある。

1. C-compatibleなAPI(extern "C" で表現される関数やPOD型)のみを限定して使用

完全に安全な境界の設計方法ではあるが、境界用のオブジェクト追加によるコード量の増加やコピーによる性能面のオーバーヘッドが懸念される

2. C++17 と C++20 で表現が同一の型に限定する

標準ライブラリ(libstdc++)は C++11 前後で ABI 互換性を崩したことがあった (dual ABI) がそれ以降は安定しており、同じコンパイラ・同じ標準ライブラリを使用していれば、同名の型はABI互換であると期待できる。また自作の型についても明示的に `#if __cplusplus >= 202002L` といった分岐をしないもののみを使用することで、C++ ABI の差異による問題を回避できると考えられる。

本設計では 2 の方針を採用する

基本的に C++17 と C++20 で表現が異なる可能性のある型を使っていないはずだが、念のため確認の上で境界ヘッダ上に使用するようにする。
具体的には下記の型について確認済みである。

* takatori: `takatori::util::maybe_shared_ptr`
* jogasaki 内部型: `accessor::record_ref`, `accessor::text`, `accessor::binary`,
  `meta::external_record_meta`, `meta::record_meta`, `runtime_t<...>`,
  `file_reader` / `file_writer` インタフェース, `reader_option`,
  `*_writer_option`, `time_unit_kind`

これらの型は jogasaki 自身および takatori で C++17/20 を意識せずに定義されており、標準切替によってサイズ・レイアウト・ABI が変動しない。したがって、
C++17 でコンパイルされた呼び出し側と C++20 でコンパイルされた `arrow_objs` 内のコードが同じプロセスに同居しても ODR 違反は発生しない。

### 設計上の留意点

* OBJECT ライブラリに対する `POSITION_INDEPENDENT_CODE ON` は必須。`${ENGINE}` が shared library であり、OBJECT ライブラリは PIC 設定を自動継承しないため。

## テスト方針 

- Arrow 23 以降 arrowやparquetのターゲットには target_compile_features で publicに cxx_std_20 が指定されているため、arrow_objs も自動的に C++20 でコンパイルされる
  - その環境で、arrow_objs 以外の箇所(jogasaki本体やテスト)が C++17 でコンパイルされ、問題なく既存のテストケースが動作することを確認する
   - check_cxx_std.h というヘッダを用意して、この中で `__cplusplus` の値をstatic_assertでチェックし、C++17 であることを確認する。
   - impl/database.cpp あたりでこのヘッダを includeする
   - テストコードもmain.cpp でこのヘッダを include して、C++17 であることを確認する
- また、Arrow 23 以降のライブラリを含んでいない環境においても動作確認のためにこれをシミュレートするビルドオプション(FORCE_CXX20_ARROW_OBJS)を用意する
   - このビルドオプションがONになった場合、arrow_objs の依存する架空の INTERFACE ライブラリのターゲットを用意して、そこに target_compile_features で public cxx_std_20 を指定する
