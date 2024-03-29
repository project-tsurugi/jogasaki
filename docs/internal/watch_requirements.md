# 計測機能要求仕様

2020-05-14 kurosawa

## この文書について

開発中のデータ処理性能を改善するにあたって、コードの部分ごとに実行時間を測定する機能が求められる
本文書はその機能に求められる仕様をまとめる

## 機能の目的

- マルチスレッド/マルチコアによって何らかのタスクを実行するプログラムにおいて、処理を細分化した実行区間にかかる時間を測定して性能改善のための指標を取得する
- 実行区間の順序・タイミング・処理時間の分布を確認し、適切なタイミングで動作している事を確認・検証する

## ユーザーシナリオ

本機能は計測モジュールと集計モジュールからなる。
使用手順を下記に示す。

1. ユーザーは被計測コードを編集し、計測ポイントを規定する計測モジュール呼出しを追加する
2. ユーザーは適切な環境で被計測コードをコンパイルし実行する。計測完了後、計測ポイントの実行情報がなんらかの形式で出力できる。
4. ユーザーは上記実行情報を集計モジュールへ入力として渡し実行する。この時、同時に区間の定義を与える。
5. ユーザーは集計モジュールの結果によって下記を取得することができる
- 各区間の実行時間
- 各区間に付随する特徴値(実行回数、平均実行時間、区間充填率など)
- 直感的に実行タイミング・実行時間を把握できるビジュアルな表現

## 計測モジュール機能概要

### 計測ポイント定義

被計測コードは区間の端点の候補となる計測ポイントを下記の呼出によって指定できる。

``` 
void watch.set_point(std::size_t x, std::size_t id, ...)

x: 計測ポイントをユニークに定めるID
id: 実行主体(スレッド or タスク or ワーカーなど)を特定するID。区間の定義で使用される。
... : その他必要に応じて集計に必要な付加情報を与える

```
(関数名、引数名等は例示なので必要に応じて追加・変更があってよい)
この呼出は複数スレッドから同時に呼び出されることもあるのでthread safeに作る必要がある。

例： `f1`後半から`f2`前半までが区間[A, B]を構成する
```
void f1() {
    ...
    watch.set_point(A, 100, ...);
    ...
}

void f2() {
    ...
    watch.set_point(B, 100, ...);
    ...
}
```

### 出力機能

被計測コードを実行すると、実行IDを持つものが計測ポイントをある時刻に通過した、という内容が測定モジュール内に記録される。これを必要に応じて下記のような呼び出しでostreamへ書き出すことができる。(これはthread unsafeでよい)

```
std::ostream& operator<<(std::ostream& out, watch const& value);
```
又は
```
std::ostream& watch::print_to(std::ostream& out) const;
```

出力内容の形式についてはユーザーは関知せず、計測モジュールと集計モジュールのみが共有する知識である。

## 集計モジュール機能概要

### 用語：区間定義

2つの異なる測定ポイントA, Bにたいし、Aで開始したものが必ずBで終わる時、つまりAを実行した実行IDにたいしてBも同じ実行IDで実行されるとき、 区間[A,B]が測定可能であるという。

測定可能な区間における複数回の実行をe1, e2, ..., enとしたとき、区間の計測値を下記のように定める

開始時刻： e1, e2, ..., enの各開始時刻のうち最小のもの
終了時刻： e1, e2, ..., enの各終了時刻のうち最大もの

(現状、測定可能区間は実行は開始と終了がペアになっているもののみを考えている。複数スレッドでAが実行され、Bが特定のスレッドのみで実行される、というケースは考えていない)

### 区間定義ファイルによる区間定義

集計モジュールに入力として渡す定義ファイルにおいて、測定可能な区間名とその始点、終点をリストする。
下記はフォーマットは例であり、json等適当なフォーマットに変更してよい。
```
all,0,100
produce,10,20 
transfer,30,40
consume,50,60
```
この例ではall, produce, transfer, consumeという名前の4つの測定可能区間を定義している。

### 集計表示機能

集計モジュールの実行結果として下記のような内容を出力したい
前2者は数値によって区間の代表値として評価に使用する事を想定し、3つめは想定しているタイミングで実行されているかを確認するために使用することを想定している

- 各区間の実行時間
終了時刻と開始時刻の差

- 各区間に付随する特徴値
  - 全実行回数
  その区間が合計何回実行されたか
  - 平均実行時間 or 四分位数
  その区間の平均実行時間、または実行時間の四分位数(最小、25%ile, 中央値, 75%tile, 最大値)。
  同条件によって繰り返し測定するような限られたケース以外では、平均値よりも四分位数の方が分布を把握できて使いやすいと考えられる。
  - 区間充填率
  開始時刻から終了時刻までの間に占める実行時間の割合の平均値。100%の場合はほぼ全実行が同時に行われていることが分かる。(もっと良い指標あれば変更お願いします)

  - (同一実行IDによる複数回の実行もありえる。あるtaskがyieldして復帰した場合など。同一IDの複数回実行と異なるIDでの実行両方があった場合にどう評価するかは未定義。まずは同一実行IDによるものも異なるものも同じ1回の実行として計算する)

- 直感的に実行タイミング・実行時間を把握できるビジュアルな表現
同期して実行開始し、作業量の偏りがないような場合は上記区間充填率などでおおよそ期待したタイミングでの開始・終了となっているかが分かるが、偏りが有る場合の各スレッド終了のズレなどはビジュアルに表現されないと分からない場合があるので、各区間がどのように実行されたかを直感的に把握できる手段が求められる。
下記にイメージ案を添付する。数が少ない場合はP1のような区間による表現でも可能だが、実行数が多い場合はP2のような各時点における実行数という方が適切かもしれない。
(イメージ図)[https://drive.google.com/file/d/17OxffnpwGrck0TPRlEo5-a7qdbsWzqgb/view?usp=sharing]

