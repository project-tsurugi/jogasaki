# jogasaki ビルド時間分析レポート

- 調査日: 2026-10-21
- 対象: jogasaki (branch `jogasaki_build`, HEAD `856bf913`)、比較対象として shirakami (`b1dcc586`)、mizugaki

## 1. 結論

jogasaki のビルドが遅いのには、性質の違う原因が2つある。

1. **フルビルドが遅い**(268秒。shirakami は104秒、mizugaki は17秒)。
   コードベースが大きく、さらに翻訳単位(TU)あたりのコンパイルが重い。ビルド中は32コアが常に埋まっていたので、並列化の問題ではなく総CPU量の問題である。
2. **日常の差分ビルドが遅い**。src の `.cpp` を1行変えるだけで、`libjogasaki-shirakami.so` に依存するテスト実行ファイル386個がすべて再リンクされ、`gtest_discover_tests` も再実行される(77秒)。
   これは構成上の問題で、CMake の設定1行でほぼ解消できる(77秒 → 4.7秒)。

## 2. 計測環境と条件

| 項目 | 値 |
|---|---|
| マシン | 32 コア / 62 GB RAM, WSL2 (Linux 6.18) |
| コンパイラ | g++ 13.3.0 (Ubuntu 24.04) |
| ビルドツール | CMake 3.28.3, Ninja 1.11.1 (`-j32`) |
| ビルド構成 | Debug + AddressSanitizer + `BUILD_TESTS=ON`(jogasaki は `ENABLE_ALTIMETER=ON`) |
| その他 | ccache なし、毎回新規のビルドディレクトリでクリーンビルド |

## 3. 計測結果

### 3.1 コンポーネント間の比較

| | jogasaki | shirakami | mizugaki |
|---|---|---|---|
| フルビルド(テストあり) | **268 秒** | 104 秒 | 17 秒 |
| ライブラリのみ (`BUILD_TESTS=OFF`) | **69 秒** | 13 秒 | 12 秒 |
| 総CPU時間(ninja の各ステップの合計) | **8,279 秒** | 3,251 秒 | 434 秒 |
| src の規模 (.cpp/.h) | 374 ファイル / 11.9 万行 | 51 ファイル / 1.6 万行 | 147 ファイル / 2.5 万行 |
| テストの規模 | 395 ファイル / 11.3 万行 | 432 ファイル / 8.0 万行 | 58 ファイル / 3.2 万行 |
| テスト1TUのコンパイル時間(平均 / 中央値) | **13.2 / 14.6 秒** | 6.2 / 6.8 秒 | 3.9 / 4.0 秒 |
| src 1TUのコンパイル時間(平均) | 5.1 秒 | 3.8 秒 | 1.3 秒 |
| テスト実行ファイル数 | 386 | 432 | 1 |
| テスト実行ファイルの合計サイズ | **10 GB**(1個あたり約26 MB) | 1.5 GB | 1 個 |
| test ビルドディレクトリのサイズ | 13 GB | 3.5 GB | 308 MB |

### 3.2 jogasaki の総CPU時間の内訳

| 分類 | ステップ数 | CPU 時間 | 割合 |
|---|---|---|---|
| テストのコンパイル | 394 | 5,201 秒 | 62.8% |
| src のコンパイル | 396 | 2,017 秒 | 24.4% |
| テストのリンク + gtest discovery | 386 | 1,026 秒 | 12.4% |
| protobuf 生成コードのコンパイル | 10 | 22 秒 | 0.3% |

並列度の推移を見ると、ビルド開始から終了まで32並列がほぼ常に埋まっていた(0〜60秒は src のコンパイル、それ以降はテストのコンパイルとリンク)。理想的に並列化した場合の時間は 8,279 ÷ 32 ≈ 259 秒で、実測の268秒とほぼ一致する。つまり、壁時計時間を縮めるには総CPU量を減らすしかない。

### 3.3 テストTUが重い理由

代表的なTUの計測値(アイドル状態のマシンで単体コンパイル):

| TU | 前処理後の行数 | ヘッダ数 | 構文解析のみ | コンパイル全体 | .o のサイズ |
|---|---|---|---|---|---|
| test/.../sql_test.cpp | 365,589 | 3,112 | 4.1 秒 | 7.8 秒 | 15 MB |
| test/.../service_api_types_test.cpp | 461,731 | 3,722 | 5.9 秒 | 10.9 秒 | 21 MB |
| test/.../scan_test.cpp | 324,158 | 2,727 | 4.0 秒 | 9.5 秒 | 24 MB |
| test/.../binary_test.cpp(軽いテストの例) | 96,925 | 451 | 0.5 秒 | 0.6 秒 | 652 KB |
| src/.../ops/scan.cpp | 270,143 | 2,230 | 3.0 秒 | 4.8 秒 | 13 MB |
| src/.../api/impl/database.cpp | 372,111 | 3,262 | 5.9 秒 | 10.3 秒 | 20 MB |

`-ftime-report` で見た sql_test.cpp のフェーズ別の内訳:

| フェーズ | 割合 |
|---|---|
| 構文解析 (phase parsing) | 55% |
| テンプレート展開などの遅延処理 (lang. deferred) | 15% |
| 最適化とコード生成 (opt and generate) | 29% |

テンプレート展開だけを取り出すと全体の約30%になる。WSL2 環境のためか、構文解析中の sys 時間も大きい。

コンパイルフラグごとの寄与(コンパイル全体の時間):

| 変種 | sql_test | scan_test | database.cpp |
|---|---|---|---|
| 現状 (`-g` + ASan) | 7.1 秒 | 10.2 秒 | 10.7 秒 |
| `-g` なし | 6.0 秒 | 8.1 秒 | 8.5 秒 |
| `-g1` | 6.1 秒 | 8.9 秒 | 9.4 秒 |
| `-gsplit-dwarf` | 6.8 秒 | 10.5 秒 | 11.1 秒 |
| ASan なし | 6.5 秒 | 8.5 秒 | 8.9 秒 |
| `-g` なし + ASan なし | 5.0 秒 | 7.5 秒 | 7.9 秒 |

→ `-g` と ASan はそれぞれ15〜20%程度で、主因ではない。主因はヘッダの量とテンプレートの量である。

コンパイラのピークメモリは 1 TU あたり 1.1〜1.6 GB(service_api_types_test は 1.58 GB)。`-j32` だと40〜50 GB 必要になり、メモリの少ない開発機では並列度を下げざるを得ないので、差がさらに広がる。

### 3.4 ヘッダ依存の分析

sql_test.cpp が読み込むヘッダをライブラリ別に集計した結果(重複を除いたファイル数と行数):

| ライブラリ | ファイル数 | 行数 |
|---|---|---|
| **boost** | **1,043** | **162,645** |
| libstdc++ | 225 | 153,544 |
| システム / コンパイラ組み込み | 514 | 136,511 |
| protobuf | 46 | 26,884 |
| takatori | 117 | 18,893 |
| gtest | 21 | 12,847 |
| absl | 30 | 10,108 |

Boost を取り込んでいる主な入口:

| Boost ヘッダ(引き込む行数) | 取り込み元 |
|---|---|
| `boost/thread.hpp`(約11.2万行) | tateyama `task_scheduler/impl/thread_control.h` |
| `boost/thread/shared_mutex.hpp`(約7.3万行) | tateyama `api/server/session_store.h` |
| `boost/dynamic_bitset`(約2.9万行) | jogasaki `mock/jogasaki/mock/basic_record.h` |
| `boost/property_tree` + `lexical_cast` + `algorithm/string` | tateyama `api/configuration.h` |
| `boost/stacktrace`(約1.5万行) | takatori `util/stacktrace.h`(`util/exception.h` 経由) |

具体的な問題箇所:

- **`include/jogasaki/api/database.h:27`** が `tateyama/task_scheduler/scheduler.h` をインクルードしているが、ファイル内で一度も使っていない。
  経路: `create_tx.h` → `api/database.h` → `scheduler.h` → `impl/worker.h` → `impl/thread_control.h` → `boost/thread.hpp`。
  この経路で254オブジェクト(うちテスト215)に Boost.Thread が入る。
- **`include/jogasaki/request_info.h`** は `shared_ptr<request>` / `shared_ptr<response>` しか持たないのに、`tateyama/api/server/request.h` と `response.h` をインクルードしている。`response.h` の先で `diagnostics.pb.h` → protobuf が入る。前方宣言で足りる。
- **`src/jogasaki/request_context.h`** は29個のヘッダをインクルードしており(yugawara、tateyama、kvs、plan、scheduler、transaction_context など)、485オブジェクトに入る。
- テスト共通の **`api_test_base.h`**(190テストが使用)と **`mock/jogasaki/utils/create_tx.h`**(106テストが使用)が、内部実装の `api/impl/database.h` をインクルードしている。
- 上流の **takatori `util/exception.h`** が `boost/stacktrace` を引き込み、全800オブジェクト中698オブジェクトに入る。

重いヘッダのファンイン(何個のオブジェクトがそのヘッダに依存するか。全800オブジェクト中):

| ヘッダ | オブジェクト数(うちテスト) |
|---|---|
| boost/stacktrace/stacktrace.hpp | 698 (361) |
| google/protobuf/message.h | 524 (314) |
| jogasaki/executor/global.h | 518 (309) |
| yugawara/compiled_info.h | 504 (308) |
| takatori/relation/graph.h | 498 (307) |
| tbb/concurrent_hash_map.h | 496 (306) |
| jogasaki/request_context.h | 485 (305) |
| boost/thread.hpp | 254 (215) |
| include/jogasaki/api/database.h | 248 (213) |
| jogasaki/api/impl/database.h | 237 (208) |

変更したときの再ビルド範囲で見ると、プロジェクトのヘッダ654個のうち **194個が300オブジェクト以上**に波及する(例: `lob_id.h` は648、`paged_memory_resource.h` は633、`field_type.h` は614)。どれか1つを変えるとほぼフルビルドになる。

### 3.5 差分ビルド(src の .cpp を1つ変更)

テストの各実行ファイルは `libjogasaki-shirakami.so` にリンクしている。CMake + Ninja の既定動作では、共有ライブラリが更新されるとそれにリンクする実行ファイルも再リンクされ、POST_BUILD の `gtest_discover_tests`(実行ファイルを起動してテスト一覧を取得する処理)も再実行される。

| 構成 | `src/jogasaki/utils/fail.cpp` を変更 | `src/jogasaki/api/impl/database.cpp` を変更 |
|---|---|---|
| 標準リンカ (ld.bfd) | **76.9 秒**(386 実行ファイルの再リンク、CPU 1,680 秒) | — |
| mold | 17.5 秒(386 実行ファイルの再リンク、CPU 514 秒) | — |
| ld.bfd + `CMAKE_LINK_DEPENDS_NO_SHARED=ON` | **4.7 秒** | 14.4 秒 |
| mold + `CMAKE_LINK_DEPENDS_NO_SHARED=ON` | **1.0 秒** | 11.5 秒 |

(database.cpp を変更したときの時間は、ほぼ database.cpp 自体のコンパイル時間)

アイドル状態での1実行ファイルあたりの時間は、リンク(ld.bfd)が約1.0秒、mold が約0.15秒、テスト一覧の取得が約0.07秒。ビルド中は32並列で競合するため、1ステップあたり2.7〜4.4秒に延びる。

## 4. 対策案と実測した効果

### 4.1 [最優先] `CMAKE_LINK_DEPENDS_NO_SHARED=ON`

- 共有ライブラリが変わっても、それにリンクする実行ファイルを再リンクしなくなる。
- 効果: 差分ビルドが 77秒 → 4.7秒(ld.bfd)、17.5秒 → 1.0秒(mold)。
- 変更例(トップレベルの `CMakeLists.txt` か `test/CMakeLists.txt` に追加):
  ```cmake
  set(CMAKE_LINK_DEPENDS_NO_SHARED ON)
  ```
  ビルド時に `-DCMAKE_LINK_DEPENDS_NO_SHARED=ON` を渡しても同じ。
- 注意点: .so からシンボルを削除・変更した場合、リンク時ではなくテスト起動時のエラー(undefined symbol)になる。
  ヘッダ変更などで ABI が変わる場合は、テスト側のオブジェクトも再コンパイルされて再リンクされるので、問題になる場面は限られる。

### 4.2 高速リンカの利用(既存の `USE_LD` オプション)

- `-DUSE_LD=mold`(または `lld`)を指定する。
- 効果: テストのリンクのCPU時間が 1,026秒 → 232秒。
- フルビルドの壁時計時間は変わらない(268秒 → 270秒)。リンクはコンパイルと並行して走るため。効くのは差分ビルドと、4.1 を入れない場合。
- 開発者向けのドキュメントで推奨するか、mold が見つかれば既定で使う、といった形が考えられる。

### 4.3 [長期的に最も効果が大きい] ヘッダの整理

- 対象の例:
  - `include/jogasaki/api/database.h` から不要な `tateyama/task_scheduler/scheduler.h` を外す
  - `include/jogasaki/request_info.h` を前方宣言に置き換える
  - `request_context.h` を分割する / 前方宣言にする / pimpl 化する
  - テスト共通ヘッダ(`api_test_base.h`、`create_tx.h`)が `api/impl/database.h` に依存しないようにする
  - 上流への提案: takatori `exception.h` → `boost/stacktrace`、tateyama `session_store.h` → `boost/thread/shared_mutex.hpp`、`configuration.h` → `boost/property_tree` など
- 試行結果: `database.h` と `request_info.h` の2箇所を直しても、sql_test は前処理後 36.6万行 → 32.3万行、コンパイル 7.8秒 → 7.0秒(約10%減)にとどまった。同じヘッダ(Boost.Thread、protobuf)が別経路からも入ってくるため。
- 1〜2箇所の手直しでは大きな効果は出ないので、計測しながら体系的に進める必要がある。
  - clang の `-ftime-trace` + ClangBuildAnalyzer で、コストの大きいヘッダとテンプレートを特定する
  - include-what-you-use (IWYU) で不要なインクルードを洗い出す
- ヘッダのファンインを下げると、フルビルドだけでなく、ヘッダ変更時の差分ビルドの範囲も狭くなる。

### 4.4 その他(効果は小〜中)

| 対策 | 実測 / 見込みの効果 | 備考 |
|---|---|---|
| テストに PCH を導入 | テストのコンパイルCPU 5,201秒 → 4,633秒(−11%)、中央値 14.6秒 → 9.0秒。壁時計時間はほぼ変わらず (265 秒) | PCH の生成(18.7秒)が全テストの前に直列で入る。名前衝突や暗黙のインクルード依存で4つのテストに修正が必要(`reader_container_test` の `using kind`、`storage_*_test` の `configurable_provider`) |
| テストだけ `-g1` にする | コンパイル時間約15%減、.o サイズ約30%減 | デバッガでローカル変数が見えなくなる |
| ccache | 再ビルドやブランチ切り替えが速くなる | CI や、ブランチを頻繁に切り替える開発者に有効 |
| `gtest_discover_tests(... DISCOVERY_MODE PRE_TEST)` | 1実行ファイルあたり約0.1秒(負荷時はもっと大きい)をビルドから除ける | CMake 3.18 以降。テスト一覧の取得が ctest 実行時に移る |
| unity build (`CMAKE_UNITY_BUILD`) | 未計測 | `builtin_scalar_functions_id.h` の `id_11000` などのシンボル衝突でビルドが通らない。ソースの修正が必要 |
| テスト実行ファイルを集約する(mizugaki 方式) | リンクと discovery のコスト、10 GB の成果物を削減できる | テストのプロセス分離(グローバル状態、`RESOURCE_LOCK framework_server`)が崩れるので慎重な検討が必要 |

## 5. 推奨する進め方

1. **すぐできること**: `CMAKE_LINK_DEPENDS_NO_SHARED=ON` の導入と、mold/lld の推奨。日常の差分ビルドで感じる遅さはほぼ解消する。
2. **短期**: 明らかに不要なインクルードの削除(`database.h`、`request_info.h`)、テスト共通ヘッダの依存の削減、`-ftime-trace` による計測環境の整備。
3. **中長期**: `request_context.h` などの依存が多いヘッダの分割、上流(takatori / tateyama)への Boost 依存削減の提案。必要に応じてテストへの PCH 導入も検討する。

## 付録: 計測方法

- 各コンポーネントを新規のビルドディレクトリで `cmake -G Ninja -DCMAKE_BUILD_TYPE=Debug -DBUILD_TESTS=ON ...` → `ninja -j32` を実行し、壁時計時間を計測した。
- `.ninja_log` を、同じ開始・終了時刻を持つ出力をまとめて1ステップとして集計し、コンパイル / リンクに分類した。
- TU単位の計測は、`ninja -t compdb` で得たコマンドを `-E`(前処理後の行数)、`-H`(ヘッダツリー)、`-fsyntax-only`(構文解析のみ)、`-ftime-report`(フェーズ別内訳)に書き換えて実行した。
- ヘッダのファンインは `ninja -t deps` の結果から集計した。
