# アボートメッセージにおけるストレージ名の可読化

- Issue: https://github.com/project-tsurugi/tsurugi-issues/issues/1388

## 背景・問題

Issue #1380（https://github.com/project-tsurugi/tsurugi-issues/issues/1380）の対応により、
sharksfin/shirakami に対して使用するストレージ名がテーブル名からサロゲート ID（バイナリ文字列）に変更された。

このため、競合エラー（`CCException` 等）発生時に例外メッセージに含まれる `storage_name` が
テーブル名ではなくサロゲート ID となり、アプリケーションやツールが原因テーブルを特定できなくなった。

具体的には、`src/jogasaki/utils/abort_error.cpp` の `handle_code_and_locator()` が
シリアライゼーション失敗時のアボートメッセージを組み立てる際、
`loc->storage().value()` で取得したストレージキーをそのままメッセージに出力している。

## 目的

バイナリのストレージキーを人間が読めるインデックス名（テーブル名）に置き換え、
`index:` プレフィックスを付けてアボートメッセージに出力する。

```
location={key:... index:<index_name>}
```

## 変更内容

### 1. `handle_code_and_locator` — `storage:` フィールドを `index:` フィールドに置き換える

**現在の出力フィールド:**
```
storage:<ストレージキー>
```

**変更後の出力フィールド:**
```
index:<index_name>    -- 逆引き成功時
index:<not available>      -- 逆引き失敗時
```
## 出力例

修正前:
```
serialization failed transaction:TID-... ... location={key:... storage:}
```

修正後（逆引き成功）:
```
serialization failed transaction:TID-... ... location={key:... index:MY_TABLE}
```

## 変更対象ファイル

- `src/jogasaki/utils/abort_error.cpp` — メインの変更（ロジック + インクルード）
