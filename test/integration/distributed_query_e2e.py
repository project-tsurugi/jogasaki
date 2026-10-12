#!/usr/bin/env python3
"""Run the registered Bootstrap/Tateyama/Jogasaki stack in two isolated processes."""
import argparse
from decimal import Decimal
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import tempfile
import time


def free_port():
    with socket.socket() as channel:
        channel.bind(('127.0.0.1', 0))
        return channel.getsockname()[1]


def rows(output):
    return [json.loads(line, parse_float=Decimal) for line in output.splitlines() if line.startswith('{')]


class Node:
    def __init__(self, root, name, args, worker_grpc, coordinator=False, remote=False):
        self.root = root / name
        self.root.mkdir()
        (self.root / 'pid').mkdir()
        self.name, self.args = name, args
        self.sql_port, self.grpc_port = free_port(), free_port()
        self.worker_grpc = worker_grpc or self.grpc_port
        self.coordinator, self.remote = coordinator, remote
        self.catalog_enabled = args.catalog_routing
        self.process = None
        self.log = None
        self.generation = 0

    def start(self):
        self.generation += 1
        identity = re.sub('-+', '-', re.sub('[^a-z0-9-]', '-', f'dq-e2e-{self.root.parent.name}-{self.name}')).strip('-')
        self.config = self.root / 'tsurugi.ini'
        self.config.write_text(f'''[system]
instance_id={identity}
pid_directory={self.root / 'pid'}
[sql]
thread_pool_size=2
scan_default_parallel=1
default_partitions=1
enable_hybrid_scheduler=false
[ipc_endpoint]
database_name={identity}
threads=2
datachannel_buffer_size=2
max_datachannel_buffers=4
allow_blob_privileged=false
[stream_endpoint]
enabled=true
port={self.sql_port}
threads=2
[datastore]
log_location={self.root / 'db'}
logging_max_parallelism=2
recover_max_parallelism=2
[cc]
index_restore_threads=1
[authentication]
enabled=false
[blob_relay]
enabled=false
session_store={self.root / 'blob'}
[grpc_server]
enabled=true
listen_address=127.0.0.1:{self.grpc_port}
endpoint=dns:///127.0.0.1:{self.grpc_port}
secure=false
[distributed_query_coordinator]
enabled={str(self.coordinator).lower()}
[distributed_query_remote]
enabled={str(self.remote).lower()}
[distributed_query_client]
name=worker
uri=dns:///127.0.0.1:{self.worker_grpc}
catalog_source={'fixture' if self.coordinator and self.catalog_enabled else 'disabled'}
fixture_path={self.root.parent / 'catalog.json' if self.coordinator and self.catalog_enabled else ''}
[glog]
logtostderr=true
''')
        environment = os.environ.copy()
        environment['LD_LIBRARY_PATH'] = str(self.args.prefix / 'lib')
        environment['TSURUGI_HOME'] = str(self.args.prefix)
        environment.pop('JOGASAKI_DQ_REMOTE_EXECUTION_SERVER', None)
        if not self.args.catalog_routing:
            environment['JOGASAKI_DQ_REMOTE_EXECUTION_SERVER'] = 'worker'
        self.log_path = self.root / f'server-{self.generation}.log'
        self.log = self.log_path.open('w')
        self.process = subprocess.Popen(
            [str(self.args.server), f'--conf={self.config}', f'--location={self.root / "db"}'],
            env=environment, stdout=self.log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 40
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError(f'{self.name} exited: {self.log_path.read_text()[-5000:]}')
            if 'database started' in self.log_path.read_text():
                return
            time.sleep(.1)
        raise TimeoutError(f'{self.name} startup timed out: {self.log_path}')

    def stop(self):
        if self.process is None:
            return
        if self.process.poll() is None:
            environment = os.environ.copy()
            environment['LD_LIBRARY_PATH'] = str(self.args.prefix / 'lib')
            environment['TSURUGI_HOME'] = str(self.args.prefix)
            try:
                result = subprocess.run(
                    [str(self.args.tgctl), 'shutdown', f'--conf={self.config}'],
                    env=environment, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    timeout=20, text=True)
                (self.root / f'shutdown-{self.generation}.log').write_text(result.stdout)
                self.process.wait(timeout=20)
            except (subprocess.TimeoutExpired, OSError):
                self.process.terminate()
                try:
                    self.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait(timeout=5)
        code = self.process.returncode
        self.log.close()
        self.process = None
        if code != 0:
            raise RuntimeError(f'{self.name} did not shut down cleanly: exit {code}')

    def sql(self, text, readonly=False, failure=False):
        command = [str(self.args.tgsql), '--no-auth', '--connection',
                   f'tcp://127.0.0.1:{self.sql_port}', '--transaction',
                   'readonly' if readonly else 'OCC', '--exec', text]
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                timeout=30, text=True)
        with (self.root / 'sql.log').open('a') as stream:
            stream.write(f'\nSQL: {text}\nreadonly={readonly}, exit={result.returncode}\n{result.stdout}\n')
        has_error = result.returncode != 0 or bool(re.search(r'\bERROR\b|Exception', result.stdout))
        if failure != has_error:
            raise AssertionError(f'Expected SQL failure={failure}, got exit={result.returncode}: {result.stdout}')
        return result.stdout


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--prefix', type=Path, required=True)
    parser.add_argument('--server', type=Path, required=True)
    parser.add_argument('--tgctl', type=Path, required=True)
    parser.add_argument('--tgsql', type=Path, required=True)
    parser.add_argument('--work-dir', type=Path, required=True)
    parser.add_argument('--catalog-routing', action='store_true')
    args = parser.parse_args()
    for name in ('prefix', 'server', 'tgctl', 'tgsql', 'work_dir'):
        setattr(args, name, getattr(args, name).resolve())
    args.work_dir.mkdir(parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix='run-', dir=args.work_dir))
    if args.catalog_routing:
        (root / 'catalog.json').write_text(json.dumps({'version': 1, 'catalogs': [{
            'name': 'remote_catalog', 'server': 'worker', 'schemas': [{'name': 'public', 'tables': [{
                'name': 'T', 'columns': [
                    {'name': 'C0', 'type': 'bigint', 'nullable': False},
                    {'name': 'C1', 'type': 'varchar', 'length': 40, 'nullable': True},
                    {'name': 'C2', 'type': 'decimal', 'precision': 10, 'scale': 2, 'nullable': True},
                    {'name': 'C3', 'type': 'bigint', 'nullable': True}], 'primary_key': ['C0']}]}]}]}))
    worker = Node(root, 'worker', args, None, remote=True)
    coordinator = Node(root, 'coordinator', args, worker.grpc_port, coordinator=True)
    checks = []
    completed = False
    try:
        worker.start()
        coordinator.start()
        worker.sql('CREATE TABLE T(C0 BIGINT PRIMARY KEY, C1 VARCHAR(40), C2 DECIMAL(10,2), C3 BIGINT)')
        local_table = 'LOCAL_T' if args.catalog_routing else 'T'
        coordinator.sql(f'CREATE TABLE {local_table}(C0 BIGINT PRIMARY KEY, C1 VARCHAR(40), C2 DECIMAL(10,2), C3 BIGINT)')
        worker.sql("INSERT INTO T VALUES (900,'remote-process',12.34,0),(1000,NULL,56.78,2)")
        coordinator.sql(f"INSERT INTO {local_table} VALUES (10,'coordinator-only',1.23,1),(20,NULL,4.56,2)")
        result = coordinator.sql('SELECT C0,C1,C2 FROM T WHERE C0 > 0 ORDER BY C0', readonly=True)
        assert rows(result) == [
            {'C0': 900, 'C1': 'remote-process', 'C2': Decimal('12.34')},
            {'C0': 1000, 'C1': None, 'C2': Decimal('56.78')}], result
        checks.append('readonly SQL returns worker BIGINT/VARCHAR/DECIMAL/NULL values')
        local = coordinator.sql(f'SELECT C0,C1,C2 FROM {local_table} WHERE C0 > 0 ORDER BY C0')
        assert rows(local) == [{'C0': 10, 'C1': 'coordinator-only', 'C2': Decimal('1.23')}, {'C0': 20, 'C1': None, 'C2': Decimal('4.56')}], local
        checks.append('OCC SQL retains local execution')
        filtered = coordinator.sql('SELECT C0,C1 FROM T WHERE C0 > 900 ORDER BY C0', readonly=True)
        assert rows(filtered) == [{'C0': 1000, 'C1': None}], filtered
        checks.append('remote filter uses worker data')
        found = coordinator.sql('SELECT C0 FROM T WHERE C0 = 900', readonly=True)
        assert rows(found) == [{'C0': 900}], found
        checks.append('remote find returns worker row')
        duplicate = coordinator.sql('SELECT C0 AS A, C0 AS B FROM T WHERE C0 = 900', readonly=True)
        assert rows(duplicate) == [{'A': 900, 'B': 900}], duplicate
        checks.append('inline remote output preserves duplicate fields and labels')
        grouped = coordinator.sql('SELECT SUM(C0) AS TOTAL FROM T', readonly=True)
        assert rows(grouped) == [{'TOTAL': 1900}], grouped
        checks.append('remote rows feed existing aggregate downstream')
        if args.catalog_routing:
            coordinator.sql('SELECT C0 FROM T ORDER BY C0', failure=True)
            checks.append('catalog remote table rejects OCC instead of local fallback')
            unsupported = coordinator.sql('SELECT A.C0 FROM T A JOIN T B ON A.C0 = B.C0', readonly=True, failure=True)
            assert any(code in unsupported.lower() for code in ('unsupported', 'sql-03010')), unsupported
            checks.append('unsupported remote join is rejected before remote execution')
        error = coordinator.sql('SELECT C0,10/C3 FROM T WHERE C0 > 0 ORDER BY C0', readonly=True, failure=True)
        assert any(code in error.lower() for code in ('value_evaluation_exception', 'valueevaluationexception', 'sql-02011')), error
        checks.append('worker SQL error code survives Tateyama RPC and coordinator')
        again = coordinator.sql('SELECT C0,C1 FROM T WHERE C0 > 0 ORDER BY C0', readonly=True)
        assert rows(again) == [{'C0': 900, 'C1': 'remote-process'}, {'C0': 1000, 'C1': None}], again
        checks.append('SQL succeeds after remote execution error')
        for unsupported_type in ('BIT(8)', 'INTERVAL'):
            rejected = coordinator.sql(f'SELECT CAST(NULL AS {unsupported_type}) FROM T', readonly=True, failure=True)
            assert any(code in rejected.lower() for code in ('syntax_exception', 'unsupported_compiler_feature_exception', 'sql-03001', 'sql-03010')), rejected
            healthy = coordinator.sql('SELECT C0 FROM T WHERE C0 = 900', readonly=True)
            assert rows(healthy) == [{'C0': 900}], healthy
            checks.append(f'{unsupported_type} SQL is rejected and subsequent remote SQL succeeds')
        for expression in ('(C0=900)=(C0=1000)', 'CAST(C0=900 AS VARCHAR(5))', 'CAST(C0 AS TINYINT)', 'CAST(C0 AS SMALLINT)', "CAST(DATE '2024-02-29' AS VARCHAR(10))", "CAST(DATE '2024-02-29' AS TIMESTAMP)"):
            rejected = coordinator.sql(f'SELECT {expression} FROM T', readonly=True, failure=True)
            assert any(code in rejected.lower() for code in ('unsupported', 'sql-02050', 'sql-03010', 'outside remote execution sql support')), rejected
            healthy = coordinator.sql('SELECT C0 FROM T WHERE C0=900 AND NOT C0=1000', readonly=True)
            assert rows(healthy) == [{'C0': 900}], healthy
            checks.append(f'{expression} is rejected and existing boolean predicates remain usable')
        worker.stop()
        worker.remote = False
        worker.start()
        coordinator.sql('SELECT C0,C1 FROM T WHERE C0 > 0 ORDER BY C0', readonly=True, failure=True)
        checks.append('remote role OFF rejects remote SQL without local fallback')
        coordinator.stop()
        coordinator.coordinator = False
        coordinator.start()
        disabled = coordinator.sql(f'SELECT C0,C1 FROM {local_table} WHERE C0 > 0 ORDER BY C0', readonly=True)
        assert rows(disabled) == [{'C0': 10, 'C1': 'coordinator-only'}, {'C0': 20, 'C1': None}], disabled
        checks.append('coordinator role OFF returns local values')
        if args.catalog_routing:
            coordinator.sql('SELECT C0 FROM T ORDER BY C0', readonly=True, failure=True)
            checks.append('coordinator OFF hides remote-only table')
            coordinator.stop()
            coordinator.coordinator = True
            coordinator.catalog_enabled = False
            coordinator.start()
            coordinator.sql('SELECT C0 FROM T ORDER BY C0', readonly=True, failure=True)
            local_only = coordinator.sql('SELECT C0 FROM LOCAL_T ORDER BY C0', readonly=True)
            assert rows(local_only) == [{'C0': 10}, {'C0': 20}], local_only
            checks.append('catalog dummy OFF hides remote-only table and preserves local SQL')
        assert len(checks) == (22 if args.catalog_routing else 18)
        completed = True
    finally:
        errors = []
        for node in (coordinator, worker):
            try:
                node.stop()
            except Exception as error:
                errors.append(str(error))
        (root / 'result.json').write_text(json.dumps({'success': completed and not errors, 'checks': checks, 'cleanup_errors': errors}, indent=2))
        print(json.dumps({'success': completed and not errors, 'run_directory': str(root), 'checks': checks, 'cleanup_errors': errors}, indent=2))
        if errors:
            raise RuntimeError('; '.join(errors))


if __name__ == '__main__':
    main()
