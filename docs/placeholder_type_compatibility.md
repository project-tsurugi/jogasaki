# placeholder/parameter type compatibility

## 本文書について

プリペアドステートメントのプレースホルダ（a.k.a. host variable）と実行時に渡されるパラメータ（parameter）の型互換性ルールについて記述する

## 互換性規則

原則としてプレースホルダとパラメータは同一の型であることが求められるが、以下の例外がある。

- パラメータの型が unknown の場合
  - 現状、プレースホルダは常に nullable であり、任意のプレースホルダに対して null を割り当て可能なため
- パラメータが int4 でホスト変数が int8 の場合
  - 安全かつ自明な型変換であり、利便性のため許容
- パラメータが time_of_day / time_point のいずれかで、プレースホルダの型とタイムゾーンの有無だけ異なる場合

## 参考情報

規則検査の実装箇所

https://github.com/project-tsurugi/jogasaki/blob/8543f6b546aa1dd152ce850ee014fe91216b66c4/src/jogasaki/plan/compiler.cpp#L953-L969