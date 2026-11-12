# early stop (`write_result::satisfied` と `operation_status_kind::early_stop`)

件数付きの forward exchange が件数に達したとき、上流の演算子を途中で止める仕組み (tsurugi-issues#1589 R3)。

## writer の `write_result::satisfied`

- `record_writer::write()` は、設定された上限 (forward exchange の件数) に当たる呼出で `satisfied` を戻す
  - その後も `write()` を呼び出すことは可能だが、`satisfied` が戻り続け、レコードは捨てられる
  - つまり `satisfied` は上限に到達したことを示すが、今しがた送ったレコードが使われたか捨てられたかは判定できない
- `dev_enable_early_stop=false` の場合、forward exchange の writer は `satisfied` を戻さない (上限を超えたレコードは捨てて `ok` を戻す)

## 演算子の `operation_status_kind::early_stop`

- `early_stop` は下流演算子による「途中で停止してほしい」という要請 (offer は writer から `satisfied` を受けるとこれを戻す)
  - 上流演算子はこれを受けて、ループを抜けるなどの停止処理を進める (エラーではないので abort しない)
  - さらに上流にも `early_stop` を戻す。根の演算子 (scan, take_flat など) は正常終了と同じ後始末をして終了する
  - `early_stop` が戻された後に、その下流演算子の `process_record()` を再度呼んではいけない。すでに演算子のリソース破棄が進んでいる可能性があるため
- buffer は `early_stop` を戻した子を覚えて以降は呼ばず、残りの子にレコードを送り続ける
  - その時点のレコードは残りの子にも渡す (`early_stop` からはレコードが使われたかどうかが分からないため)
  - すべての子が停止するか、`stop_with_primary` が有効で主の出力 (最初の子) が停止した場合に、上流へ `early_stop` を戻す
