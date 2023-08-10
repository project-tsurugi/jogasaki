# SQLサービスのエラーの構成

2022-12-27 kurosawa

2023-08-09 kurosawa - GAリリース用に更新

## 本文書について

SQLサービスがクライアントへ返すエラーレスポンスの構造と、エラーが所属するエラーカテゴリの階層構造について記述する

## エラーの構造

- SQLサービスは処理が成功しなかった場合にその理由を説明するエラーレスポンスを戻す
- エラーレスポンスは下記の情報を含む
  - エラーコード
  - エラーメッセージ
  - メッセージID (i18n対応)
  - メッセージ引数 (i18n対応)
  - エラー付帯情報(アボート理由の詳細などデバッグ用途の情報)
  - トランザクションがエラーによってアボートされたか

- 本文書はSQL実行エンジンがサービスとしてクライアントへ戻すエラーのみを扱い、下記は対象外
  - 一般的なtateyamaサービスとしてのエラー(tateyama core diagnosticsとして、別にハンドルする)
    - サービス間通信に関するエラー(リクエストのルーティングの失敗など)
    - サービスへ渡されるべき引数が不正な場合
    - quiecescentモード等で起動されたサービスに対する許可されない操作
  - サービス開始時/停止時のエラー

## エラーカテゴリ・エラーコード

- エラーは所属するエラーカテゴリが決まっており、エラーカテゴリによって分類される
- ルートのエラーカテゴリ(SqlServiceException)以外は親エラーカテゴリに所属し、エラーカテゴリ全体は階層構造をなす
- エラーカテゴリの識別のためユニークなコードが振られる(エラーコード)
  - `SQL-nnnnn` というラベルで表示されることが多い5桁の数値
  - 先頭の2桁は大分類(ルート配下のトップレベルのエラーカテゴリ)を表し、残りの3桁がトップレベル以下の小分類を表す
  - エラーカテゴリはユーザーに求められるアクションが同じである単一の現象を表すが、エラーが起きるコード位置や状況によって付随するメッセージは異なることがある
  - メッセージやメッセージIDによってより細かい説明が追加される。下記はその例で、いずれもエラーカテゴリとエラーコードは同じだが、メッセージIDは異なる
    - 一意制約違反が検出されたのがINSERT実行時かCOMMIT実行時か
    - 一意制約違反が主キー由来の一意制約か、ユニークインデックス由来のものか

### 階層

下記にエラーカテゴリの階層およびエラーコードを記述する

(エラーコードはメンテナンスの手間を考慮して連番を避け、飛び飛びの値を使用している)

- SqlServiceException (SQL-00000)
  - SqlExecutionException (SQL-01000: SQL実行時のエラー)
    - ConstraintViolationException (SQL-01001: 制約違反)
      - UniqueConstraintViolationException (SQL-01002: 一意制約違反)
      - NotNullConstraintViolationException (SQL-01003: Not Null 制約違反)
      - ReferentialIntegrityConstraintViolationException (SQL-01004: 参照制約違反)
      - CheckConstraintViolationException (SQL-01005: 検査制約違反)

    - EvaluationException (SQL-01010: SQL文の評価に関するエラー)
      - ValueEvaluationException (SQL-01011: 式の値の評価に関するエラー)
      - ScalarSubqueryEvaluationException (SQL-01012: スカラサブクエリが期待されるステートメントにおいて評価結果がスカラでなかった)

    - TargetNotFoundException (SQL-01014: SQLステートメントの操作対象が存在しない)
    
        (例: クエリが使用している名前に対応するテーブルが存在しない)
      
        (例: WPとして指定されたテーブルが存在しない)

    - TargetAlreadyExistsException (SQL-01016: 新規作成要求の対象が既に存在する)

    - InconsistentStatementException (SQL-01018: 使用されたAPIに対して要求されたステートメントが不正)
      
        (例: クエリ/ダンプ用のAPIに結果セットを戻さないステートメントが渡された)

    - RestrictedOperationException (SQL-01020: 禁止された操作が実行された)
      - DependenciesViolationException (SQL-01021: 依存するものがある対象に対して削除操作が要求された)
        
          (例: ビュー定義が存在したままで表定義を削除しようとした)
        
      - WriteOperationByRtxException (SQL-01022: Rtxによって書き込み操作が実行された)
      - LtxWriteOperationWithoutWritePreserveException (SQL-01023: LTXがWP指定した以外の領域へ書き込み操作を要求した)
      - ReadOperationOnRestrictedReadAreaException (SQL-01024: 禁止されているread areaをreadした)
      - InactiveTransactionException (SQL-01025: commit/abort済のトランザクションに対する操作が要求された)

    - ParameterException (SQL-01027: プレースホルダーやパラメーターに関するエラー)
      - UnresolvedPlaceholderException (SQL-01028:実行要求されたステートメントが未解決のplaceholderを含む)

    - LoadFileIOException (SQL-01030: ロードファイルに関するエラー)
      - LoadFileNotFoundException (SQL-01031: ロードファイルが存在しない)
      - LoadFileFormatException (SQL-01032: 予期しないファイルフォーマット)
    - DumpFileIOException (SQL-01033: ダンプファイルに関するエラー)
      - DumpDirectoryInaccessibleException (SQL-01034: ダンプに指定されたディレクトリがアクセス可能でない)

    - SqlLimitReachedException (SQL-01036: 許可されたSQL操作の制限に達した)
      - TransactionExceededLimitException (SQL-01037: 許同時作成可能なトランザクション数の制限を越えたためトランザクション開始に失敗した)

    - SqlRequestTimedOutException (SQL-01039: SQL操作要求がタイムアウトした)

    - DataCorruptionException (SQL-01041: データ破損の検知)
      - SecondaryIndexCorruptionException (SQL-01042: セカンダリインデックスの破損) 

    - RequestFailureException (SQL-01044: リクエストの処理の開始前に前提条件違反等により失敗した)
      - TransactionNotFoundException (SQL-01045: リクエストされたトランザクションハンドルに対応するトランザクションが存在しない or 解放済み)
      - StatementNotFoundException (SQL-01046: リクエストされたステートメントハンドルに対応するトランザクションが存在しない or 解放済み)

    - InternalException (SQL-01048: 内部エラーの検知)

    - UnsupportedRuntimeFeatureException (SQL-01050: 未サポート機能の実行)

    - BlockedByHighPriorityTransactionException (SQL-01052: 高優先度のトランザクションより優先させる要求を行った)

  - CompileException (SQL-02000: コンパイル時のエラー)

    - SyntaxException (SQL-02001: 構文エラー)
     
    - AnalyzeException (SQL-02002: 解析エラー)
     
      - TypeAnalyzeException (SQL-02003: 型に関するエラー)
       
      - SymbolAnalyzeException (SQL-02004: シンボルに関するエラー)

      - ValueAnalyzeException (SQL-02005: リテラルが範囲外など)
        
    - UnsupportedCompilerFeatureException (SQL-02010: 未サポート機能/構文等のコンパイル)

  - CcException (SQL-03000: CCで直列化失敗によるエラー)

    - OccException (SQL-03001: Occ TXのアボート)

      - OccReadException (SQL-03010: Occ TXのreadを原因とするアボートが発生)
    
        (例 occがreadしたものがcommit時には上書きされていた(shirakami ERR_CC_OCC_READ_VERIFY))
        
        (例 occがreadしたmasstree nodeの状態がcommit時には変化していた(shirakami ERR_CC_OCC_PHANTOM_AVOIDANCE))
        
        (例 occがreadしたものがltxによってwrite preserveされていた(shirakami ERR_CC_OCC_WP_VERIFY))

        - ConflictOnWritePreserveException (SQL-03015: occがreadしたものがltxによってwrite preserveされていてearly abort)

      - OccWriteException (SQL-03011: Occ TXのwriteを原因とするアボートが発生 - 現状では発生ケースなし)

    - LtxException (SQL-03003: LTXのアボート)

      - LtxReadException (SQL-03013: LTXのreadを原因とするアボートが発生) 
         
        (例 ltxがreadしたものが、前置位置では読み出し可能でない(shirakami ERR_CC_LTX_READ_UPPER_BOUND_VIOLATION))

      - LtxWriteException (SQL-03014: LTXのwriteを原因とするアボートが発生)
        
        (例 ltxがwriteしたものがcommit済みのreadを壊してしまう(shirakami ERR_CC_LTX_WRITE_READ_PROTECTION))
        
        (例 ltxのwriteしたものがcommit済みのrange readを壊してしまう(shirakami ERR_CC_LTX_WRITE_PHANTOM_AVOIDANCE))

    - RtxException (SQL-03005: Read Only TXのアボートが発生)

    - BlockedByConcurrentOperationException (SQL-03007: 同時並行に実行された操作によってブロックされた)

### その他

- カテゴリ名はjavaの例外に対応させるためにErrorではなくExceptionという接尾辞で統一している
- javaの例外の継承関係の階層を上記エラーカテゴリの階層に一致させる(SqlServiceExceptionをSqlExecutionExceptionが継承し、さらにそれを ConstraintViolationExceptionが継承し、と継承関係が続く)
- 将来的にはエラー以外のレスポンスとして、警告のような「情報付き成功」を通知するレスポンスも想定しているが、当文書の範囲には含めていない
- トランザクションが関連するリクエストにおいてエラーが発生した際はトランザクションは使用不可な状態(アボート済の状態)となる
  - GA以降では一部この制限が緩和されるかもしれない
- GAでは下記の項目はエラーレスポンスには未実装、将来的に対応とする
  - メッセージID/メッセージ引数
  - トランザクションがエラーによってアボートされたか
