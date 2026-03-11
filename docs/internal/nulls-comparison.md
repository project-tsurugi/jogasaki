# NULLの比較セマンティクス

## 概要

SQLには「値が等しいか」を比較するセマンティクスが2つある。本文書では便宜上下記の記号で記述する。

- `==` : 通常のスカラ式内で等号 `=` として表現されるもの。評価結果は三値論理（true/false/unknown）
- `=?` : `IS NOT DISTINCT FROM` と等価なもの。評価結果は二値（true/false）

SQL標準ではグルーピングを行う場合のグルーピングキーの比較には `=?` を使用し、それ以外では `==` を使用する。

実行エンジンはこのどちらを使うべきかの判断が一貫していなく問題である。本文書では実行エンジンの各処理がどちらを使っているかをまとめる。

## tl;dr

SQL実行エンジンでは、各処理で下記のように `==` と `=?` を使い分けている。

- 式の評価 : `==`
- グルーピングキー : `=?`
- ソートキー : `==` とも `=?` とも解釈可能
- シャッフルによる結合処理 : `=?`
- インデックスジョイン(join_find/join_scan) : `==` 
- find/scan : `==` 
  - ただしscanのコーナーケースにバグあり

グルーピングキーに `=?` を使うのはSQL標準にのっとっているが、シャッフルによる結合もグルーピング処理に引きずられて `=?` を使っている点が一貫していない

---

## グルーピングキーとして使った場合の挙動 → `=?`

`GROUP BY` 時は `=?` が使われ、NULLをグルーピングキー列にもつレコードは同一グループにまとめられる。SQL標準にのっとっている。

```
tgsql> create table t (c0 int);
tgsql> insert into t values (1),(null),(null);
tgsql> select count(*) from t group by c0;
[@#0: BIGINT]
[2]
[1]
(2 rows)
```

`COUNT DISTINCT` の処理でも同じ（ただし集約関数の規則によりNULLに対するcountは0になる）。

```
tgsql> select count(distinct c0) from t group by c0 ;
[@#0: BIGINT]
[0]
[1]
(2 rows)
```

---

## ソートキーとして使った場合 → `=?` と `==` の区別はなし

`ORDER BY` では下記のようにNULLS FIRSTでソートされる。

これは大小比較なので `=?` と `==` の差ではなく、NULLをどの位置にソートするかの問題である。 Tsurugiでは NULLを最小値としてソートする。

```
tgsql> create table t (c0 int);
tgsql> insert into t values (1),(null),(null);
tgsql> select * from t order by c0;
[c0: INT]
[null]
[null]
[1]
(3 rows)
```

---

## shuffleベースでの結合処理 → `=?`

シャッフルベースで結合を処理する際にはグルーピングを行う過程で `=?` を使用している。SQL上は結合条件が `=` を含む式で書かれている（三値論理が期待される）ことと一貫していないし、PostgreSQL等の実装とも異なる。

具体的には、下記のようにTsurugiはNULLを含むレコードが余分に出現している。

**Tsurugiでの実行結果**

```
tgsql> create table t0 (c0 int, c1 int);
tgsql> create table t1 (c0 int, c1 int);
tgsql> insert into t0 values (1,10),(2,null),(3,null);
tgsql> insert into t1 values (1,10),(2,null),(3,null);
tgsql> select * from t0, t1 where t0.c1=t1.c1;
[c0: INT, c1: INT, c0: INT, c1: INT]
[2, null, 2, null]
[2, null, 3, null]
[3, null, 2, null]
[3, null, 3, null]
[1, 10, 1, 10]
(5 rows)

tgsql> explain select * from t0, t1 where t0.c1=t1.c1;
1.
  1-1. scan (scan) {source: table, table: t0, access: full-scan}
  1-2. group (group_exchange) {whole: false, sorted: false}
  1-3. >[3]
2.
  2-1. scan (scan) {source: table, table: t1, access: full-scan}
  2-2. group (group_exchange) {whole: false, sorted: false}
  2-3. >[3]
3. <[1-3, 2-3]
  3-1. join (join_group) {join-type: inner, source: flow, access: merge}
  3-2. emit (emit)
```

**同じSQLのPostgreSQLでの実行結果**

```
postgres=# create table t0(c0 int, c1 int);
postgres=# create table t1(c0 int, c1 int);
postgres=# insert into t1 values (1,10),(2, null),(3,null);
postgres=# insert into t0 values (1,10),(2, null),(3,null);

postgres=# select * from t0, t1 where t0.c1=t1.c1;
 c0 | c1 | c0 | c1
----+----+----+----
  1 | 10 |  1 | 10
(1 row)
```

---

## find/scan/join_find/join_scan → `==`

一部バグがあるものの、設計上は `==` による比較をおこなうものとしている

- インデックスに対する探索条件を作るための変数がNULLを含む場合は not found とする
- プライマリインデックスがターゲットの場合、主キー列はNULLを含むことができないので `==` と `=?` の違いはない
- セカンダリインデックスがターゲットの場合、索引のキー列はNULLを含められるが、NULLは端点が負の無限大である区間 `[-∞, x)` でスキャンした場合しか現れない
  - このケースに対するケアが十分でないため、セカンダリインデックスに対するscan系演算子（scan/join_scan）の処理でNULLが結果に含まれてしまうバグがある

例えば下記のようなシナリオで `scan` 演算子のバグが発現する（`join_scan` も同等のバグがありえるが、SQLコンパイラがそのような実行計画を作るかどうかはわからずSQLからは再現しないかもしれない）。

`where c1 < 1` という条件で `c1 = null` であるレコードが結果にあらわれてしまう。
```
tgsql> create table t (c0 int primary key, c1 int);
tgsql> create index i on t (c1);
tgsql> insert into t values (1, 10), (2, NULL);
(2 rows inserted)
tgsql> select c1 from t where c1 < 1;
[c1: INT]
[null]
(1 row)
tgsql> explain select c1 from t where c1 < 1;
1. scan (scan) {source: index, table: t, index: i, access: range-scan}
2. emit (emit)
```
