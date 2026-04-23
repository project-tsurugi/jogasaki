# セカンダリインデックスのスキャンの修正

2026-05-20 kurosawa

## 本文書について

セカンダリインデックスのスキャンに関する不具合を修正するためのコード変更点を示す。

https://github.com/project-tsurugi/tsurugi-issues/issues/1484
https://github.com/project-tsurugi/tsurugi-issues/issues/1479

## 前提

`takatori::relation::scan` の lower()/uppper() はスキャン対象のインデックス(プライマリまたはセカンダリ)のキー列のプレフィックス(キー列全てであるケースも含む)に対する値を渡し、スキャン範囲の端点を表す 

現在の重要な前提として、下記がある

- lowerとupperに含まれる列数の差は高々1である
- 範囲検索はこれらの列の末尾の列でのみ行われる。つまり lowerとupperのうち長い方の末尾以外の列と値は長くない方にも含まれる

例えば c1, c2, c3 というキー列を持つインデックスがあるとする。

あり得る lower/upper の例:
- `lower = {kind:inclusive, c1:1, c2:10}, upper = {kind:exclusive, c1:1, c2:10, c3:100}` : 列数の差が1だが、lowerの `c3` は負の無限大(-INF)として解釈することで列数を同じとして扱える
- `lower = {kind:inclusive, c1:1, c2:10, c3:100}, upper = {kind:inclusive, c1:1, c2:10, c3:200}` : 列数の差がない
- `lower = {kind:inclusive, c1:1, c2:10, c3:100}, upper = {kind:prefix_inclusive, c1:1, c2:10}` : 列数の差が1だが、upperの `c3` は正の無限大(+INF)として解釈することで列数を同じとして扱える

あり得ない lower/upper の例:
- `lower = {kind:unbound}, upper = {kind:inclusive, c1:1, c2:10, c3:100}` : 列数の差が2以上
- `lower = {kind:inclusive, c1:1, c2:10}, upper = {kind:unbound}` : 列数の差が2以上
- `lower = {kind:inclusive, c1:1, c2:10, c3:100}, upper = {kind:inclusive, c1:1, c2:20, c3:200}` : 範囲検索が末尾の列(`c3`)以外(`c2`)で行われている

 takatori::relation::scan はインデックスのキーがストレージ上でどのような構造になっているかの知識は保有せず、キー列に関する知識のみを用いて、キー(またはそのプレフィックス部分)に対して値を指定してスキャン指示を伝達する。 lower()/upper() はスキャン範囲の端点を表すが、これらはインデックスのキー列に対する値であって、ストレージ上でのインデックスエントリの表現とは異なる可能性がある。

## 問題点

プライマリインデックスのキー(つまり主キー)は常に昇順で non-nullableである。また、インデックスキーへ主キー部分を連結するといった特殊なキー構造もしていないので状況は単純であった。しかしセカンダリインデックスではこれらの状況にケアが必要であり現実装では下記の問題がある。

1. 降順のキー列で大小関係が逆転するがそれを考慮せずに begin/end を作成している

jogasakiは 上記 `lower()/upper()` でスキャン範囲の端点を取得し、そこから sharksfin/shirakami インデックスを具体的にスキャンするための端点 begin/end を作成する。

具体的には下記のコードである。
https://github.com/project-tsurugi/jogasaki/blob/ed965a1b63f92bd294c33efe27d0f5317f0f5aa8/src/jogasaki/executor/process/impl/ops/operator_builder.cpp#L475

しかし、この際単に lower() -> begin, upper() -> end という対応を行うのみで、降順のキー列を考慮していないため、ストレージ上での妥当な範囲を定義できていない。

lower/upperはコンパイラの指示であり、コンパイラはインデックスのキー構造に関する知識がないため、lower/upperは降順列に関しても昇順列と同様に値を指定するが、jogasakiはインデックスの降順列のキー構造に関する知識があり egin/end を作成する際に必要な対応を行う必要がある。

範囲検索を行う末尾のキー列が降順の場合、ストレージ上では lower() に含まれる列の値の方が大きい値を表すことになるため、lower() 由来の列で end を作成し、upper() 由来の列で begin を作成する必要がある。

2. nullableな列の場合、インデックス上では nullがその列に格納される「最小の値」となる(NULLS FIRST)。カッコ付きであるのは、あくまでストレージ上の表現として最小の位置に格納される (例えばasc列の場合、nullはnullフラグが0x80, non-nullはnullフラグが0x81となる)という意味で、SQLの範囲検索においては null を含む式評価は unknown であり全範囲以外の範囲が指定された検索においては結果に含まれるべきではない。現状ではこれをケアせずに範囲検索を行っているため、列の検索条件が下限を指定しない場合、nullが範囲に含まれてしまう。

3. インデックスのキー列がcompositeで、プレフィックスによる範囲検索を行う場合、範囲検索の端点種類を `prefixed_*` を用いるように変更している。これはセカンダリインデックスの場合、末尾に主キー部分が連結されているため、`inclusive/exclusive` そのままではその部分の比較が行われてしまうため、そこを無視して比較を行うためである。このロジックは完全キーによる範囲検索の場合は正しいが、プレフィックスによる場合は後続の列の比較を無視してしまうことになり、正しくない。

具体的には下記のコードで adjust_endpoint_kind() 関数を使用してセカンダリインデックスをターゲットに範囲検索を行う場合に `prefixed_*` な endpoint kind を用いるように変更している。
https://github.com/project-tsurugi/jogasaki/blob/dd4c18a0bb24b645b113199a579fbe4009668163/src/jogasaki/executor/process/impl/ops/operator_builder.cpp#L484

## 問題3 の修正案

下記の2案がある。

- (A) exclusive/inclusive のサポート削除

  scan/join_scan等の範囲端点を受け取る演算子において、`inclusive/exclusive` のサポートを削除し(使用された際は unsupported エラー)、 `prefixed_inclusive/prefixed_exclusive` を使用するようにする。これによって、セカンダリインデックスの際の読み替えが不要になる。

- (B) 完全キーによる範囲指定の場合のみ `prefixed_*` を使用するようにする

  インデックス末尾列に対する処理の内容を記憶しておき、完全キーであるかどうかを判定する。
  末尾列の処理がunboundでない処理であった場合、つまりその端点は完全キーである場合、端点のendpoint_kindがinclusive/exclusiveをそれぞれprefixed_inclusive/prefixed_exclusiveに変換する。これによって完全キーによる範囲指定の場合は主キー部分に関する比較の影響を受けないようになる。プレフィックスによる範囲指定の場合は、主キー部分の比較の影響を受け、そもそもこれが要求される仕様なのか不明であるが、現状維持となる。
  
現時点では(A)の案を採用し、`inclusive/exclusive` のサポートが必要になった時にあらためて案(B)を検討する。理由は以下の通り。

- 現実装ではSQLコンパイラは `inclusive/exclusive` は使用せず、 `prefixed_inclusive/prefixed_exclusive` を使用するようにしている
- プレフィックスによる範囲指定の際に `inclusive/exclusive` を使用したケースは、想定動作と要求仕様が不明瞭である。(B)によってひとまず実装は可能であるが、この仕様が正しいのかわからない上、それを単体テストで網羅的にテストするのもやりすぎ感がある。

## 問題1, 2 の修正案

問題1,2は互いに関連しており、処理の順序が重要である。下記のような順序で処理を行う。

- lower/upperの再解釈
- desc列に対するlower/upperのフリップ
- nullableな列に対するunboundの処理

## lower/upperの再解釈

lower/upperに含まれる列の数はインデックスキーの列数とは一致しないが、インデックスのキー列のうち、lower/upperに含まれない列については、それぞれ "unbound"
 という特殊値が指定されたものと解釈することによって、インデックスキー列とlower/upperの列数は一致していると解釈する。

その際の意味は下記のようになる

  - lower列がunbound: その列に対する下限がない
  - upper列がunbound: その列に対する上限がない

これはSQLよりの解釈(言及されていない列に関しては条件がないものとする)であり、キーを連接してソート済みにしたデータ構造に対する通常の解釈(upperがプレフィックスの場合はサブキーについても上限が発生する)とは異なることに注意。SQLコンパイラがこの解釈を前提としている。これはSQLコンパイラがprefixedなendpoint_kindのみを使用していることとも一致する。(prefixedを使うとサブキーに対する上限が発生しない)

unboundが指定された列がある場合、そこから後ろの列もすべてunboundであると解釈する。

以下ではこの解釈を行った lower/upperについて下記の処理を行う。
(とくにこの意味であることを明示するときには **再解釈されたlower/upper** という)

前提から、lower/upperの列数の差は高々1であるため、再解釈されたlower/upperのunboundな列数の差は高々1になる。

再解釈されたlower/upperにおいて、非unboundな列が現れる列のうち最末尾の列を **範囲検索列** と呼ぶことにする。

範囲検索列が存在しない、つまりlower/upperの両方とも全ての列がunboundである場合はフルスキャンである。この場合のみ例外的に、scan結果としてキー列にnullを含むものが戻される。

## desc列に対するlower/upperのフリップ

セカンダリインデックスのキー列が昇順(ASC)か降順(DESC)かによって、lower/upperの列とbegin/endの列の対応を逆にする。具体的には、begin/endを下記のようにlower/upperから作成する。

- セカンダリインデックスのキー列のうち、先頭から順に範囲検索列の手前までの列について下記を行う
  - キー列の値はlower/upperともに同じ値を持つ。値をエンコード(ASC/DESC, 及びnullabilityを考慮)してbegin/endの両方にアペンドする
- 範囲検索列について下記を行う
  - キー列がASCの場合
    - lowerの場合、値をエンコード(ASC/DESC, 及びnullabilityを考慮)して begin へアペンドする
    - upperの場合、同様に end へアペンドする
      - 値がunboundの場合、エンコーディングについては下記セクション「nullableな列に対するunboundの処理」を参照。またこの場合、**upperのendpoint_kindをprefixed_inclusiveとする**
    - lowerのendpoint_kindをbeginのendpoint_kindとし、upperのendpoint_kind(上で修正がある場合はその修正後のもの)をendのendpoint_kindとする
  - キー列がDESCの場合
    - 上とは逆に、upperからbeginへアペンドする
    - また、lowerからendへアペンドする。
      - 値がunboundの場合、エンコーディングについては下記セクション「nullableな列に対するunboundの処理」を参照。またこの場合、**lowerのendpoint_kindをprefixed_inclusiveとする**
    - upperのendpoint_kindをbeginのendpoint_kindとし、lowerのendpoint_kind(上で修正がある場合はその修正後のもの)をendのendpoint_kindとする
- 範囲検索列後の列については処理しない
  - 範囲検索列まででエンコーディングおよびendpoint_kindの決定が完了している

## nullableな列に対するunboundの処理

上記でbegin/endへエンコーディングした値をアペンドする際に、値としてunboundが現れた場合、それは範囲検索列であり、nullableをケアした特別な処理を行う必要がある。具体的には、下記のようにする。

- 列がnon-nullable であれば、エンコード結果は空
- 列がnullable であれば、nullフラグ (asc列なら0x81、desc列なら0x81の全ビットを反転) のみをエンコード結果とする

これによって、nullableな列に対して unboundが指定された場合(full scanを除く)、nullが範囲に含まれないようになる

## 関連情報

- jogasakiが値をどのようにエンコーディングするかについては [dataformat-ja.md](../dataformat-ja.md) を参照。

- セカンダリインデックスの構造については [secondary-index-ja.md](../secondary-index-ja.md) を参照。
