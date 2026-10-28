#!/usr/bin/env python3
"""C1 lifecycle checks using standard Bootstrap and Jogasaki test metadata."""
import argparse
from decimal import Decimal
import json
import os
import socket
import subprocess
import tempfile
import time
import traceback
from pathlib import Path

from distributed_query_e2e import Node, rows

TABLE = 'C1_DUMMY_T'
CREATE = f'CREATE TABLE {TABLE}(C0 BIGINT PRIMARY KEY,C1 VARCHAR(40),C2 DECIMAL(10,2),C3 BIGINT)'
LOCAL_SQL = f'SELECT C0,C1,C2 FROM {TABLE} WHERE C0 > 0 ORDER BY C0'
REMOTE_SQL = LOCAL_SQL.replace(f'FROM {TABLE}', f'FROM worker_schema.{TABLE}')
REMOTE_ROWS = [
    {'C0': 900, 'C1': 'remote-process', 'C2': Decimal('12.34')},
    {'C0': 1000, 'C1': None, 'C2': Decimal('56.78')},
]
LOCAL_ROWS = [
    {'C0': 10, 'C1': 'coordinator-only', 'C2': Decimal('1.23')},
    {'C0': 20, 'C1': None, 'C2': Decimal('4.56')},
]


def fixture():
    return {'version': 1, 'catalogs': [{
        'name': 'remote_catalog', 'server': 'worker', 'schemas': [{
            'name': 'worker_schema', 'tables': [{
                'name': TABLE, 'definition_id': 8101, 'index_definition_id': 9101,
                'columns': [
                    {'name': 'C0', 'type': 'bigint', 'nullable': False},
                    {'name': 'C1', 'type': 'varchar', 'length': 40, 'nullable': True},
                    {'name': 'C2', 'type': 'decimal', 'precision': 10, 'scale': 2, 'nullable': True},
                    {'name': 'C3', 'type': 'bigint', 'nullable': True},
                ], 'primary_key': ['C0'],
            }],
        }],
    }]}


def listeners(ports):
    output = subprocess.check_output(['ss', '-ltnpH'], text=True)
    return [line for line in output.splitlines()
            if line.split()[3].rsplit(':', 1)[-1] in {str(port) for port in ports}]


def healthy(node):
    assert node.process and node.process.poll() is None, node.name
    # Coordinator has no registered gRPC service in this role configuration.
    for port in [node.sql_port] + ([node.grpc_port] if node.remote else []):
        with socket.socket() as channel:
            channel.settimeout(1)
            assert channel.connect_ex(('127.0.0.1', port)) == 0, (node.name, port)
    actual = rows(node.sql('SELECT 1 AS READY'))
    assert actual == [{'READY': 1}], actual
    return {'pid': node.process.pid, 'rows': actual,
            'listeners': listeners([node.sql_port, node.grpc_port])}


def stop(node, stops):
    if node.process is None:
        return None
    process = node.process
    evidence = {'node': node.name, 'generation': node.generation,
                'pid': process.pid, 'forced': False}
    try:
        assert process.poll() is None, (node.name, process.returncode)
        # Node's wrapper and prefix handling are retained for tgctl.
        environment = os.environ.copy()
        environment['TSURUGI_HOME'] = str(node.args.prefix)
        environment['LD_LIBRARY_PATH'] = str(node.args.prefix / 'lib')
        result = subprocess.run(
            [str(node.args.tgctl), 'shutdown', f'--conf={node.config}'],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            timeout=20, env=environment)
        (node.root / f'shutdown-{node.generation}.log').write_text(result.stdout)
        evidence['tgctlExit'] = result.returncode
        process.wait(timeout=20)
        evidence['serverExit'] = process.returncode
        deadline = time.monotonic() + 5
        while listeners([node.sql_port, node.grpc_port]) and time.monotonic() < deadline:
            time.sleep(.1)
        evidence['listenersAfter'] = listeners([node.sql_port, node.grpc_port])
        evidence['pidExistsAfter'] = Path('/proc', str(process.pid)).exists()
        assert result.returncode == 0 and process.returncode == 0, evidence
        assert not evidence['listenersAfter'] and not evidence['pidExistsAfter'], evidence
    except BaseException:
        if process.poll() is None:
            evidence['forced'] = True
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        raise
    finally:
        node.log.close()
        node.process = None
        stops.append(evidence)
    return evidence


def select(coordinator):
    remote = rows(coordinator.sql(REMOTE_SQL, readonly=True))
    local = rows(coordinator.sql(LOCAL_SQL))
    assert remote == REMOTE_ROWS, remote
    assert local == LOCAL_ROWS, local
    return {'remoteSQL': REMOTE_SQL, 'readonly': remote,
            'localSQL': LOCAL_SQL, 'OCC': local}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('prefix', 'server', 'tgctl', 'tgsql', 'work-dir'):
        parser.add_argument('--' + name, type=Path, required=True)
    args = parser.parse_args()
    for name in ('prefix', 'server', 'tgctl', 'tgsql', 'work_dir'):
        setattr(args, name, getattr(args, name).resolve())
    args.catalog_routing = True  # Existing fixture path; no forced remote override.
    args.work_dir.mkdir(parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix='c1-', dir=args.work_dir))
    (root / 'catalog.json').write_text(json.dumps(fixture(), indent=2))
    worker = Node(root, 'worker', args, None, remote=True)
    coordinator = Node(root, 'coordinator', args, worker.grpc_port, coordinator=True)
    nodes = [coordinator, worker]
    ports = [port for node in nodes for port in (node.sql_port, node.grpc_port)]
    result = {'status': 'running', 'scenarios': [], 'stops': [], 'cleanupErrors': []}

    def record(case, evidence):
        result['scenarios'].append({'case': case, 'status': 'passed', 'evidence': evidence})
        print('SCENARIO_PASSED', case, flush=True)

    try:
        assert len(set(ports)) == 4 and not listeners(ports), ports
        worker.start()
        coordinator.start()
        record('C1-01', {'worker': healthy(worker), 'coordinator': healthy(coordinator)})
        for node, data in [
            (worker, "(900,'remote-process',12.34,0),(1000,NULL,56.78,2)"),
            (coordinator, "(10,'coordinator-only',1.23,1),(20,NULL,4.56,2)"),
        ]:
            node.sql(CREATE)
            node.sql(f'INSERT INTO {TABLE} VALUES {data}')
        record('C1-02', select(coordinator))
        configuration = {node.name: node.config.read_bytes() for node in nodes}
        record('C1-03', {'stop': stop(coordinator, result['stops']),
                         'workerAfter': healthy(worker)})
        record('C1-04', stop(worker, result['stops']))
        worker.start()
        coordinator.start()
        assert {node.name: node.config.read_bytes() for node in nodes} == configuration
        restart = {'sameConfiguration': True, 'dataReinserted': False,
                   'results': select(coordinator)}
        restart['coordinatorStop'] = stop(coordinator, result['stops'])
        restart['workerStop'] = stop(worker, result['stops'])
        record('C1-05', restart)
        worker.start()
        coordinator.start()
        actual = rows(coordinator.sql(REMOTE_SQL, readonly=True))
        assert actual == REMOTE_ROWS, actual
        try:
            assert len(actual) == 3, 'intentional expected row count 3; actual 2'
        except AssertionError as error:
            negative = {'status': 'failed', 'intentional': True,
                        'purpose': 'verify_cleanup_after_result_comparison_failure',
                        'originalFailure': str(error),
                        'expectedRows': 3, 'actualRows': len(actual)}
        else:
            raise AssertionError('negative control did not fail')
        negative['coordinatorStop'] = stop(coordinator, result['stops'])
        negative['workerStop'] = stop(worker, result['stops'])
        (root / 'negative-control.json').write_text(json.dumps(negative, indent=2))
        record('C1-06', negative)
        result['status'] = 'passed'
    except KeyboardInterrupt:
        result.update(status='cancelled', error='interrupted')
        raise
    except Exception as error:
        result.update(status='failed', error=str(error), traceback=traceback.format_exc())
        print(result['traceback'], flush=True)
    finally:
        for node in nodes:
            if node.process is not None:
                try:
                    stop(node, result['stops'])
                except BaseException as error:
                    result['cleanupErrors'].append(str(error))
        result['finalListeners'] = listeners(ports)
        if result['cleanupErrors'] or result['finalListeners']:
            result['status'] = 'failed'
        (root / 'result.json').write_text(json.dumps(result, default=str, indent=2))
        print('C1_RESULT=' + json.dumps({'status': result['status'], 'run_directory': str(root)}), flush=True)
    return 0 if result['status'] == 'passed' else 1


if __name__ == '__main__':
    raise SystemExit(main())
