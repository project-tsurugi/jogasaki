# LIKWIDによるCPU HWカウンタの取得

2021-11-17 kurosawa

## この文書について

プロファイリングのために[LIKWID](https://github.com/RRZE-HPC/likwid)を使用してCPU HWカウンタを取得した際のメモ

## LIKWIDについて

- 比較的新しいCPUの性能分析用ツール(2010年ごろに論文がでている)で、マイクロベンチ、HWカウンタ、スレッドpinningなどのコマンドを提供している
- Intel VTunes, Linux perf, [Processor Counter Monitor](https://github.com/opcm/pcm)などと共通する機能も多い。特に便利そうな点は、
  - Marker APIによって、プログラム中の特定の区間のみを指定してHWカウンタを取得することができる。(そのためthreadのcore affinityの設定が重要になる)
  - LIKWIDがCPU/Numaトポロジを検出してくれるので、それを使った論理CPU IDによって簡単にCPU affinityの指定ができる
- GUIはないがプログラマブルにプロファイリングをするのに便利

## LINKWIDの提供するコマンド

### likwid-topology 

CPUトポロジを取得することができる。
likwidがどのように物理コアを認識し、ソケットやNUMAなどのドメインにマッピングするかを調べる事ができる。
この情報がcore affinityを指定するときに必要になる。

```
$ likwid-topology 
--------------------------------------------------------------------------------
CPU name:	Intel(R) Core(TM) i7-8850H CPU @ 2.60GHz
CPU type:	Intel Coffeelake processor
CPU stepping:	10
********************************************************************************
Hardware Thread Topology
********************************************************************************
Sockets:		1
Cores per socket:	6
Threads per core:	2
--------------------------------------------------------------------------------
HWThread        Thread        Core        Die        Socket        Available
0               0             0           0          0             *                
1               0             1           0          0             *                
2               0             2           0          0             *                
3               0             3           0          0             *                
4               0             4           0          0             *                
5               0             5           0          0             *                
6               1             0           0          0             *                
7               1             1           0          0             *                
8               1             2           0          0             *                
9               1             3           0          0             *                
10              1             4           0          0             *                
11              1             5           0          0             *                
--------------------------------------------------------------------------------
Socket 0:		( 0 6 1 7 2 8 3 9 4 10 5 11 )
--------------------------------------------------------------------------------
********************************************************************************
Cache Topology
********************************************************************************
Level:			1
Size:			32 kB
Cache groups:		( 0 6 ) ( 1 7 ) ( 2 8 ) ( 3 9 ) ( 4 10 ) ( 5 11 )
--------------------------------------------------------------------------------
Level:			2
Size:			256 kB
Cache groups:		( 0 6 ) ( 1 7 ) ( 2 8 ) ( 3 9 ) ( 4 10 ) ( 5 11 )
--------------------------------------------------------------------------------
Level:			3
Size:			9 MB
Cache groups:		( 0 6 1 7 2 8 3 9 4 10 5 11 )
--------------------------------------------------------------------------------
********************************************************************************
NUMA Topology
********************************************************************************
NUMA domains:		1
--------------------------------------------------------------------------------
Domain:			0
Processors:		( 0 6 1 7 2 8 3 9 4 10 5 11 )
Distances:		10
Free memory:		13525.5 MB
Total memory:		31664.3 MB
--------------------------------------------------------------------------------
```

### likwid-pin

core affinityを設定することができる。tasksetコマンドのようにコマンド起動時に使う事もできる。ここでのaffinity指定方法はlikwid-prfctrでも共通で使用される。

#### 用語

- 物理CPU ID
  hardware threadを1つの物理CPUと認識する
  cli03の環境では0-223の通し番号が付いていた

- 論理CPU ID
  下記affinity domainを使用してLIKWID独自の記法でCPUを指定するためのID。例えばS0:L:3 (0番ソケットの3番目の論理CPU)といった形式。

- affinity domain

  何らかのthread affinity基準によってCPUをグループ化したもの

  - Node domain 
    システムに存在する全てのCPUからなるドメイン
    Nodeという用語はここではOSが稼働するマシンの事を指す。いわゆるNUMA node (=numaのメモリ管理単位)と異なるので注意

  - Socket domain 
    CPUソケットにある全てのCPUからなるドメイン

  - Cache domain 
    LLCを共有するCPUからなるドメイン

  - Memory domain
    Numaメモリを共有するCPUからなるドメイン。NUMA nodeに対応するものと思われる。

#### コマンド例

- 物理CPU0,2,3,4,5でコマンドを実行する
  > likwid-pin -c 0,2-5 `<command>`

- ソケット0のsocket domainを確認する
  > likwid-pin -p -c S0

  cli03 (4 sockets, 28 cores/socket, 2 SMT/core)上での実行例
  ```
  0,112,1,113,2,114,3,115,4,116,5,117,6,118,7,119,8,120,9,121,10,122,11,123,12,124,13,125,14,126,15,127,16,128,17,129,18,130,19,131,20,132,21,133,22,134,23,135,24,136,25,137,26,138,27,139
  ```

- 上記socket domainから28個のCPUを2つおきに1つづつとりだすことによって指定した際のcpu affinityを確認する
  > likwid-pin -p -c E:S0:28:1:2

  実行結果例
  ```
  0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27
  ```

- 上記のcore affinityでコマンドを実行する
  > likwid-pin -c E:S0:28:1:2 `<command>`

### likwid-perfctr


## その他・使用上の注意

- インストールはgithubからrepositoryをcloneして、config.mkを編集し、make installする
- `modprobe msr`によってkernelモジュールを使えるようにしておく必要がある。詳しくはマニュアル参照

- directとdaemonアクセスという2方式がある。
  - 通常は後者で十分そう  
  - 前者は実行にroot権限が必要
  - 後者はdaemonコマンドにSUIDをセットすることによってroot権限を与える。インストール時のみsudoが必要になるが、それ以外は不要。ユーザーのローカルディレクトリへのインストールでOK(config.mkのprefix設定を書き換える)

- 起動時に下記のようなスタックトレースが表示されることがあるが、pmcのカウンタが使用可能かをテストして起きたsignalをキャッチしているようなので、問題なさそうに見える。
  - glogによってsignalが処理されて目立ってみえる。
  - linkwid-pfrctrの-Vオプションで開発者用ログを表示させると機能確認に失敗した、という内容が表示されていた。)

```
terminate called without an active exception
*** Aborted at 1637139858 (unix time) try "date -d @1637139858" if you are using GNU date ***
PC: @     0x7fa740869fb7 gsignal
*** SIGABRT (@0x3e800005764) received by PID 22372 (TID 0x7fa743455580) from PID 22372; stack trace: ***
    @     0x7fa74182d980 (unknown)
    @     0x7fa740869fb7 gsignal
    @     0x7fa74086b921 abort
    @     0x7fa740ec0957 (unknown)
    @     0x7fa740ec6ae6 (unknown)
    @     0x7fa740ec6b21 std::terminate()
    @     0x7fa73c337661 std::thread::~thread()
    @     0x7fa74086e735 __cxa_finalize
    @     0x7fa73c331533 (unknown)
    @     0x7fa74327cd13 (unknown)
    @     0x7fa74086e161 (unknown)
    @     0x7fa74086e25a exit
    @     0x7fa73e5a8e5e segfault_sigaction_rdpmc
    @     0x7fa74182d980 (unknown)
    @     0x7fa73e5a8fc1 test_rdpmc.constprop.1
    @     0x7fa73e5a9325 access_x86_rdpmc_init
    @     0x7fa73e5b51b0 access_client_init
    @     0x7fa73e5917cd HPMaddThread
    @     0x7fa73e589c6e perfmon_init_maps
    @     0x7fa73e58ba13 perfmon_init
    @     0x7fa73e5b27b6 likwid_markerInit
    @     0x7fa742a7457d jogasaki::api::impl::service::initialize()
    @     0x5593f8229230 (unknown)
    @     0x5593f822df4b (unknown)
    @     0x5593f8215304 (unknown)
    @     0x7fa74084cbf7 __libc_start_main
    @     0x5593f821588a (unknown)
```

- 分解能について
  - 自分の使ったDD環境(cli03)では1-2us程度の区間までは測る事ができた
  - tracy profilerと同等の分解能はありそう
  - 1us未満はあまり試していない
  - ただし毎回測るとスループットに影響があったのでカウンタを用意して100回や1000回に1回という形で測定した
  - 測定区間の合計時間が表示されるので毎回確認して想定とずれがないか確認するとよい。下記だと4570/78 = 58us秒の区間を測定した例。
  ```
  +-------------------+------------+
  |    Region Info    | HWThread 3 |
  +-------------------+------------+
  | RDTSC Runtime [s] |   0.004570 |
  |     call count    |         78 |
  +-------------------+------------+
  ```



