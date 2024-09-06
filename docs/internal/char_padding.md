# charデータタイプのパディング

2023-06-18 kurosawa

## 当文書について

SQL実行エンジンにおいてchar型データのパディングの仕様についてのメモ

## SQLのふるまい

- `CHAR(n)` 型は `n` バイトからなる文字列値を表す
- 非ヌルな `CHAR(n)` 型の値は必ず `n` バイトの長さをもつ
- INSERT文やUPDATE文で挿入・変更が指定された `CHAR(n)`列への値が `n` バイト未満である場合は `n` バイトになるように適切な個数のパディングキャラクターが付加される
- [takatori文書](https://github.com/project-tsurugi/takatori/blob/master/docs/ja/scalar-expressions-and-types.md)も参照
- パディングキャラクターは空白文字(0x20)を使用

## 内部実装、使用上のメモ

- パディングはjogasakiのkvsのレイヤ(namespace jogasaki::kvs)でデータをencodeする際およびキャストを行う際に付加される
- 比較時は特にパディング部分の処理を行わないため、使用する側がパディングを考慮する必要がある
  - 例えば`C1 CHAR(5)`に文字列`ABC`を挿入すると`ABC  `(2空白パディング)として保存される、その列で検索を行う場合は `WHERE C1='ABC  '`のようにする必要がある
    - またはtrim関数(未実装)などを適用する
- `BINARY(n)`に関しても同様のロジックが適用される(パディングキャラクターはNUL(0x00))
- `cast` 演算では`varchar` -> `char`の変換ではパディングが付加されるし、長さの変換時にパディング文字は精度を失うことなく取り除くことが可能。詳細は上のtakatori文書を参照。
