# NULLの比較セマンティクス

2026-04-28 kurosawa

## 本文書について

現在のjogasaki (tsurugi 1.10) において、NULLを含む値の比較に関するセマンティクスが一貫していない。各処理での比較がどうなっているかをまとめる。

## 概要

SQLには「値が等しいか」を比較するセマンティクスが2つある。本文書では便宜上下記の記号で記述する。

- `==` : 通常のスカラ式内で等号 `=` として表現されるもの。評価結果は三値論理（true/false/unknown）
- `=?` : `IS NOT DISTINCT FROM` と等価なもの。評価結果は二値（true/false）

SQL標準では `DISTINCT` 処理とグルーピングキーの比較には `=?` を使用し、それ以外では `==` を使用する。

## tl;dr

SQL実行エンジンでは、各処理で下記のように `==` と `=?` を使い分けている。
グルーピングキーやDISTINCTに `=?` を使うのはSQL標準にのっとっているが、シャッフルによる結合もグルーピング処理に引きずられて `=?` を使っている点が一貫していない

- SELECT DISTINCT : `=?` (SQL標準)
  - UNION DISTINCTも同じ
- スカラ式の等号による評価 : `==`
- グルーピングキー : `=?` (SQL標準)
- ソートキー : 区別なし
- シャッフルによる結合処理 : `=?` (**SQL非標準**)
- INTERSECT DISTINCT (semi join) : `=?` (SQL標準)
- EXCEPT DISTINCT (anti join) : `=?` (SQL標準)
- インデックスジョイン(join_find/join_scan) : `==` 
- find/scan : `==` 
  - ただしscanのコーナーケースにバグあり (issue #1479)
- IN演算子
  - 右辺がサブクエリの場合 `=?` (**semi join使用によるバグ: issue #1486**)
  - 右辺が値のリストの場合 `==` 

---

## 各処理の詳細

### distinct 

`DISTINCT` は、`=?` の意味と同じ (by definition)。複数のNULLは select distinct によって単一の値になる。

```
tgsql> create table t (c0 int);
tgsql> insert into t values (null), (null);
(2 rows inserted)
tgsql> select distinct c0 from t;
[c0: INT]
[null]
(1 row)
```

実行計画を見ると、groupエクスチェンジによるグルーピング と limit 1 処理が入っていることがわかる。

```
tgsql> explain select distinct c0 from t;
1. scan (scan) {source: table, table: t, access: full-scan}
2. group (group_exchange) {whole: false, sorted: false, limit: 1}
3. emit (emit)
```

Pgでの実行結果も同様(NULLが空白表示になることに注意)
```
postgres=# create table t (c0 int);
CREATE TABLE
postgres=# insert into t values (null),(null);
INSERT 0 2
postgres=# select distinct c0 from t;
 c0
----

(1 row)
```

### グルーピングキーとして使った場合の挙動 → `=?`

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

Pgでの実行結果も同様
```
postgres=# create table t (c0 int);
CREATE TABLE
postgres=# insert into t values (1),(null),(null);
INSERT 0 3
postgres=# select count(*) from t group by c0;
 count
-------
     2
     1
(2 rows)

postgres=# select count(distinct c0) from t group by c0;
 count
-------
     1
     0
(2 rows)
```

---

### ソートキーとして使った場合 → `=?` と `==` の区別はなし

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

Pgではnulls lastになる
```
postgres=# create table t (c0 int);
CREATE TABLE
postgres=# insert into t values (1),(null),(null);
INSERT 0 3
postgres=# select * from t order by c0;
 c0
----
  1


(3 rows)
```

---

### shuffleベースでの結合処理 → `=?`

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

**PostgreSQLでの実行結果**

NULLを含むレコードは出現しない

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

### find/scan/join_find/join_scan → `==`

一部バグがあるものの、設計上は `==` による比較をおこなうものとしている

- インデックスに対する探索条件を作るための変数がNULLを含む場合は not found とする
- プライマリインデックスがターゲットの場合、主キー列はNULLを含むことができないので `==` と `=?` の違いはない
- セカンダリインデックスがターゲットの場合、索引のキー列はNULLを含められるが、NULLは端点が負の無限大である区間 `[-∞, x)` でスキャンした場合しか現れない
  - このケースに対するケアが十分でないため、セカンダリインデックスに対するscan系演算子（scan/join_scan）の処理でNULLが結果に含まれてしまうバグがある

例えば下記のようなシナリオで `scan` 演算子のバグが発現する（`join_scan` も同等のバグがありえるが、SQLコンパイラがそのような実行計画を作るかどうかはわからずSQLからは再現しないかもしれない）。(issue #1479)

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

### IN演算子

IN演算子は `==` を使う想定にしているが、右辺がサブクエリの場合は semi-joinとして処理されるため、シャッフルベースの結合処理と同様に `=?` を使うことになる。これも一貫していない。
(issue #1486 でこのバグを修正する予定)
下記のように、サブクエリと一緒にINを使った場合はNULLを含むレコードがIN演算子によって拾われてしまう。

```
tgsql> create table l (c0 int primary key, c1 int);
tgsql> insert into l values (1,null);
(1 row inserted)
tgsql> create table r (c0 int primary key, c1 int);
tgsql> insert into r values (10,null);
(1 row inserted)
tgsql> select l.c0 from l where l.c1 in (select r.c1 from r);
[c0: INT]
[1]
(1 row)
```

一方、右辺が値のリストの場合は `==` を使うため、NULLを含むレコードは拾われない。
```
tgsql> select l.c0 from l where l.c1 in (null);
[c0: INT]
(0 rows)
```

Pgでの実行ではいずれもNULLを含むレコードは拾われない
```
postgres=# create table l (c0 int primary key, c1 int);
CREATE TABLE
postgres=# insert into l values (1,null);
INSERT 0 1
postgres=# create table r (c0 int primary key, c1 int);
CREATE TABLE
postgres=# insert into r values (10,null);
INSERT 0 1
postgres=# select l.c0 from l where l.c1 in (select r.c1 from r);
 c0
----
(0 rows)

postgres=# select l.c0 from l where l.c1 in (null);
 c0
----
(0 rows)
```

### INTERSECT DISTINCT

INTERSECT DISTINCTはシャッフルベースの結合処理の後に semi-joinを実行することになるため、 `=?` を使う。
下記のように、NULL同士は等しいものとみなされ、重複除去されて単一のNULLが戻される。
SQL標準にのっとっている。

```
tgsql> create table l (c0 int primary key, c1 int);
tgsql> insert into l values (1,null);
tgsql> insert into l values (2,null);
tgsql> insert into l values (3,null);
tgsql> create table r (c0 int primary key, c1 int);
tgsql> insert into r values (10,null);
tgsql> insert into r values (11,null);
tgsql> select l.c1 from l intersect distinct select r.c1 from r;
[c1: INT]
[null]
```

実行計画を見ると、シャッフルベースの結合と join-type: semi が現れていることがわかる。

```
tgsql> explain select l.c1 from l intersect distinct select r.c1 from r;
1.
  1-1. scan (scan) {source: table, table: l, access: full-scan}
  1-2. group (group_exchange) {whole: false, sorted: false, limit: 1}
  1-3. >[3]
2.
  2-1. scan (scan) {source: table, table: r, access: full-scan}
  2-2. group (group_exchange) {whole: false, sorted: false, limit: 1}
  2-3. >[3]
3. <[1-3, 2-3]
  3-1. join (join_group) {join-type: semi, source: flow, access: merge}
  3-2. emit (emit)
```

Pgでの実行結果も同様

```
postgres=# create table l (c0 int primary key, c1 int);
CREATE TABLE
postgres=# insert into l values (1,null);
INSERT 0 1
postgres=# insert into l values (2,null);
INSERT 0 1
postgres=# insert into l values (3,null);
INSERT 0 1
postgres=# create table r (c0 int primary key, c1 int);
CREATE TABLE
postgres=# insert into r values (10,null);
INSERT 0 1
postgres=# insert into r values (11,null);
INSERT 0 1
postgres=# select l.c1 from l intersect distinct select r.c1 from r;
 c1
----

(1 row)
```

### EXCEPT DISTINCT

EXCEPT DISTINCTはシャッフルベースの結合処理の後に anti-joinを実行することになるため、 `=?` を使う。
下記のように、NULL同士は等しいものとみなされ除去されてNULLが消える。
SQL標準にのっとっている。

```
tgsql> create table l (c0 int primary key, c1 int);
tgsql> insert into l values (1,null);
tgsql> insert into l values (2,null);
tgsql> insert into l values (3,null);
tgsql> create table r (c0 int primary key, c1 int);
tgsql> insert into r values (10,null);

tgsql> select l.c1 from l except distinct select r.c1 from r;
[c1: INT]
(0 rows)
```

実行計画を見ると、シャッフルベースの結合と join-type: anti が現れていることがわかる。

```
tgsql> explain select l.c1 from l except distinct select r.c1 from r;
1.
  1-1. scan (scan) {source: table, table: l, access: full-scan}
  1-2. group (group_exchange) {whole: false, sorted: false, limit: 1}
  1-3. >[3]
2.
  2-1. scan (scan) {source: table, table: r, access: full-scan}
  2-2. group (group_exchange) {whole: false, sorted: false, limit: 1}
  2-3. >[3]
3. <[1-3, 2-3]
  3-1. join (join_group) {join-type: anti, source: flow, access: merge}
  3-2. emit (emit)
```

Pgでの実行結果も同様
```
postgres=# create table l (c0 int primary key, c1 int);
CREATE TABLE
postgres=# insert into l values (1,null);
INSERT 0 1
postgres=# insert into l values (2,null);
INSERT 0 1
postgres=# insert into l values (3,null);
INSERT 0 1
postgres=# create table r (c0 int primary key, c1 int);
CREATE TABLE
postgres=# insert into r values (10,null);
INSERT 0 1
postgres=# select l.c1 from l except distinct select r.c1 from r;
 c1
----
(0 rows)
```

### UNION DISTINCT

UNION DISTINCTは group exchange でグルーピングして limit 1 で重複除去をする。これは SELECT DISTINCT と同様。

```
tgsql> create table l (c0 int primary key, c1 int);
tgsql> insert into l values (1,null);
tgsql> insert into l values (2,null);
tgsql> create table r (c0 int primary key, c1 int);
tgsql> insert into r values (10,null);

tgsql> select l.c1 from l union distinct select r.c1 from r;
[c1: INT]
[null]
```

実行計画

```
tgsql> explain select l.c1 from l union distinct select r.c1 from r;
1.
  1-1. scan (scan) {source: table, table: l, access: full-scan}
  1-2. >[3]
2.
  2-1. scan (scan) {source: table, table: r, access: full-scan}
  2-2. >[3]
3. <[1-2, 2-2]
  3-1. group (group_exchange) {whole: false, sorted: false, limit: 1}
  3-2. emit (emit)
``` 

Pgでの実行結果も同様
```
postgres=# create table l (c0 int primary key, c1 int);
CREATE TABLE
postgres=# insert into l values (1,null);
INSERT 0 1
postgres=# insert into l values (2,null);
INSERT 0 1
postgres=# create table r (c0 int primary key, c1 int);
CREATE TABLE
postgres=# insert into r values (10,null);
INSERT 0 1
postgres=# select l.c1 from l union distinct select r.c1 from r;
 c1
----

(1 row)
```

### EXCEPT ALL/INTERSECT ALL

EXCEPT と INTERSECT に対するALLは未サポート

### UNION ALL

UNION ALL は forward exchangeで単純に下流へ流すだけで、値の比較は行われない

```
tgsql> explain select l.c1 from l union all select r.c1 from r;
1.
  1-1. scan (scan) {source: table, table: l, access: full-scan}
  1-2. >[3]
2.
  2-1. scan (scan) {source: table, table: r, access: full-scan}
  2-2. >[3]
3. <[1-2, 2-2]
  3-1. forward (forward_exchange)
  3-2. emit (emit)
```
