# Distributed Query integration checks

`distributed_query_suite.py` is the shared entry point for a private prebuilt stack and the optional integration job in the existing CI workflow. It uses ordinary DDL with persistent definition IDs, catalog RPC, and the compiler API.

The stack must contain the standard Bootstrap server and tgctl, the feature Tateyama library, and Jogasaki with normal persistent definition-ID allocation. Supply a matching tgsql Java client and its IPC JNI library. The runner requires Python 3.10+, a JDK, OpenSSL and `ss`. It writes only under its work directory and starts loopback-only DB processes with distinct PID, data and IPC names.

```sh
python3 test/integration/distributed_query_suite.py \
  --prefix "$STACK/install" \
  --server "$STACK/build-bootstrap/src/tsurugidb" \
  --tgctl "$STACK/build-bootstrap/src/tgctl" \
  --tgsql "$STACK/tgsql" \
  --client-jar "$STACK/tgsql.jar" \
  --jni-library "$STACK/libtsubakuro.so" \
  --stack-manifest "$STACK/stack-manifest.json" \
  --work-dir "$RESULTS" \
  --database-name-prefix "$USER-dq-ci"
```

On a shared development host, add `--port-base` with five consecutive ports assigned to the executing user. The first four are node endpoints and the fifth is the fault relay. Groups run sequentially. Use `--groups c1 c2 transport acceptance` to select groups; omitting the option runs all four.

| Group | Checks |
| --- | --- |
| c1 | Six process lifecycle scenarios |
| c2 | 34 SQL scenarios with ordinary SQL controls |
| transport | Multiple real RPC messages and interrupted delivery |
| acceptance | LTX, three IPC comparisons, in-flight cancellation and RTX cleanup, worker termination/recovery, connection-only interruption/recovery, a slow reader with concurrent SQL, an independently blocked worker RPC with concurrent SQL, trusted/untrusted TLS, metrics and correlated execution logs, and EXPLAIN JSON retrieval |

The acceptance tests distinguish a SQL client that pauses row consumption from a worker RPC that remains active under flow control. A paused SQL client does not imply that the worker is still running, because rows may already be buffered. The blocked-worker relay withholds only the first stream's DATA, suppresses its stream credit and returns connection credit, while forwarding unrelated streams. The control SQL must complete on another stream of the same HTTP/2 connection. Both cases run with one coordinator SQL worker; cancellation verifies the matching worker RTX cleanup.

The compiler operation-location query determines each process destination. EXPLAIN JSON must show local location ID 0 and a nonzero worker location with the configured server name.

The stack manifest contains `commits`, `jogasakiProductionFiles` (SHA-256 of every file under `src` and `include`) and `binaries` (relative paths and SHA-256 within the stack root). The suite checks the complete production-source file set, every execution input and the complete prefix shared-library set before running. Shared-library symlink names and their targets are both recorded and hashed; all supplied paths use the same absolute stack-root spelling. A base commit alone is insufficient for an uncommitted build. A transferred source snapshot can be specified with `--source-root`.

Each run preserves input hashes, group logs, SQL diagnostics, configuration, fault events and result JSON. The manual workflow uploads JSON, logs and INI files, not private TLS keys or database data files. Expected SQL codes come from the error contract; library message text is not an error identifier. The test-control timeout is a failure, never an expected SQL timeout.

Set `distributed_query_stack_root` when dispatching the existing `ci-build.yml` at the feature branch. This selects the integration job instead of the normal build and analysis jobs. Normal push and PR builds keep their existing behavior. The integration job requires its private prebuilt stack on the selected runner. A local invocation of this same entry point is not a GitHub Actions run. The feature-branch changes must be committed and pushed before this option can be dispatched. No implementation PR merge is required.


## Formal compiler API checks

`distributed_query_compiler.py` runs two standard Bootstrap processes and uses
`CREATE FOREIGN CATALOG` plus catalog RPC, the compiler's external catalogs and
process-location query.
Normal CREATE TABLE/INDEX assigns persistent IDs, and catalogs are fetched by RPC.

```sh
python3 test/integration/distributed_query_compiler.py \
  --prefix "$STACK/install" \
  --server "$STACK/install/libexec/tsurugidb" \
  --tgctl "$STACK/install/bin/tgctl" \
  --tgsql "$STACK/tgsql" \
  --client-jar "$STACK/tgsql.jar" \
  --work-dir "$RESULTS" \
  --port-base "$ASSIGNED_PORT_BASE"
```

The checks compare explicit expected rows with worker-local SQL and coordinator
SQL. They cover find/range predicates, project/filter combinations, local casts,
functions and aggregates, prepared rebind, local table precedence, remote-write
rejection without worker changes, and name-resolution failure after catalog DROP.
Compiler UTs separately assert the actual process shapes. Casts and host-variable predicates remain on the coordinator; the
kind-only compiler allow list cannot express Portable IR's type restrictions.

`distributed_query_e2e.py` retains the shared node helper and delegates its command
entry point to `distributed_query_compiler.py`. Use the same arguments, including
`--client-jar` and `--port-base`; the old `--catalog-routing` flag and environment
variable for forcing a remote destination are no longer supported.

With Phase-1 compiler settings, remote INSERT/UPDATE/DELETE fail at compilation
with `empty_operation_location` (`unsupported_compiler_feature_exception`,
`err_compiler_error`). The internal remote-target guard still reports
`restricted_operation_exception` / `err_illegal_operation` when execution metadata
preparation detects a remote write. Preserve the original compiler diagnostic
when compilation rejects the statement first.
