#!/usr/bin/env python3
"""Formal compiler API acceptance on two real Bootstrap processes (no catalog fixture).

Normal DDL allocates and persists definition IDs;
no fixed-ID metadata helper is required.
"""
import argparse
from decimal import Decimal
import getpass
import json
from pathlib import Path
import re
import socket
import subprocess
import tempfile
import traceback

from distributed_query_e2e import Node, rows
from distributed_query_lifecycle import CREATE, healthy, stop_started


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('prefix', 'server', 'tgctl', 'tgsql', 'work-dir'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--port-base', type=int, required=True)
    parser.add_argument('--client-jar', type=Path, required=True)
    args = parser.parse_args()
    if not __debug__:
        raise RuntimeError('Run without Python optimization')
    if not 1024 <= args.port_base <= 65532:
        parser.error('port-base must reserve four consecutive ports')
    for port in range(args.port_base, args.port_base + 4):
        with socket.socket() as channel:
            channel.bind(('127.0.0.1', port))
    args.catalog_routing = True
    args.database_name_prefix = getpass.getuser() + '-native-compiler'
    args.work_dir.mkdir(parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix='native-compiler-', dir=args.work_dir))
    worker = Node(root, 'worker', args, None, remote=True, minimal_roles=True, ports=(args.port_base, args.port_base + 1))
    coordinator = Node(root, 'coordinator', args, worker.grpc_port, coordinator=True,
                       minimal_roles=True,
                       ports=(args.port_base + 2, args.port_base + 3))
    result = {'status': 'running', 'catalogSource': 'CREATE FOREIGN CATALOG / RPC',
              'fixture': False, 'definitionIDSource': 'ordinary-DDL', 'cases': [], 'stops': [], 'cleanupErrors': []}
    table = 'branch."public".C1_DUMMY_T'

    def query(name, template, expected):
        observations = []
        # Both paths use the same SQL expressions and worker data; expectations are explicit.
        for node, target in ((worker, 'C1_DUMMY_T'), (coordinator, table)):
            text = template.replace('$T', target)
            output = node.sql(text, readonly=True)
            actual = [list(row.values()) for row in rows(output)]
            assert actual == expected, (name, text, expected, actual, output)
            observations.append({'node': node.name, 'sql': text, 'rows': actual})
        result['cases'].append({'case': name, 'status': 'passed', 'expected': expected,
                                'observations': observations})
        print('PASS', name, flush=True)

    try:
        worker.start()
        coordinator.start()
        for node in (worker, coordinator):
            healthy(node)
            assert 'catalog_source=' not in node.config.read_text()
            assert 'fixture_path=' not in node.config.read_text()
            node.sql(CREATE)
        worker.sql("INSERT INTO C1_DUMMY_T VALUES (900,'remote-process',12.34,0),(1000,NULL,56.78,2)")
        coordinator.sql("INSERT INTO C1_DUMMY_T VALUES (10,'local-only',1.23,1)")
        coordinator.sql('CREATE FOREIGN CATALOG branch SERVER worker')
        coordinator.sql('CREATE TABLE LOCAL_COMPILER_T(C0 BIGINT PRIMARY KEY,C1 BIGINT)')
        coordinator.sql('INSERT INTO LOCAL_COMPILER_T VALUES(1,10)')
        coordinator.sql('UPDATE LOCAL_COMPILER_T SET C1=20 WHERE C0=1')
        assert [list(row.values()) for row in rows(coordinator.sql('SELECT C1 FROM LOCAL_COMPILER_T', readonly=True))] == [[20]]
        coordinator.sql('DELETE FROM LOCAL_COMPILER_T')
        coordinator.sql('DROP TABLE LOCAL_COMPILER_T')
        result['cases'].append({'case': 'local-ddl-dml-with-active-remote-catalog', 'status': 'passed'})
        query('native-read', 'SELECT C0,C1,C2 FROM $T ORDER BY C0',
              [[900, 'remote-process', Decimal('12.34')], [1000, None, Decimal('56.78')]])
        query('native-find', 'SELECT C0 FROM $T WHERE C0=900', [[900]])
        query('native-range', 'SELECT C0 FROM $T WHERE C0>900 ORDER BY C0', [[1000]])
        query('native-project-filter',
              'SELECT X FROM (SELECT C0+1 AS X FROM $T) S WHERE X>950 ORDER BY X', [[1001]])
        query('native-filter-project-filter',
              'SELECT X FROM (SELECT C0/C3 AS X FROM $T WHERE C3<>0) S WHERE X>1', [[500]])
        query('native-project-without-filter', 'SELECT C0+1 AS X FROM $T ORDER BY C0', [[901], [1001]])
        query('local-aggregate', 'SELECT COUNT(*) AS N FROM $T', [[2]])
        query('local-function', 'SELECT C0 FROM $T WHERE ABS(C0)>950 ORDER BY C0', [[1000]])
        query('local-cast', "SELECT C0 FROM $T WHERE CAST(C0 AS VARCHAR(20))='900'", [[900]])
        query('varchar-condition', "SELECT C0 FROM $T WHERE C1='remote-process'", [[900]])
        client_dir = root / 'client'
        client_dir.mkdir()
        subprocess.run(['javac', '-cp', str(args.client_jar), '-d', str(client_dir),
                        str(Path(__file__).with_name('DistributedQueryProbe.java'))], check=True)
        text = f'SELECT C0 FROM {table} WHERE C0=:key'
        prepared = subprocess.run(['java', '-cp', str(client_dir) + ':' + str(args.client_jar),
                                   'DistributedQueryProbe', 'prepared',
                                   f'tcp://127.0.0.1:{coordinator.sql_port}', text],
                                  capture_output=True, text=True, timeout=45)
        (root / 'prepared.log').write_text(prepared.stdout + prepared.stderr)
        assert prepared.returncode == 0 and 'BIND_OK 900' in prepared.stdout and 'BIND_OK 1000' in prepared.stdout, prepared
        result['cases'].append({'case': 'prepared-rebind', 'status': 'passed', 'sql': text})
        explain = subprocess.run(['java', '-cp', str(client_dir) + ':' + str(args.client_jar),
                                  'DistributedQueryProbe', 'explain',
                                  f'tcp://127.0.0.1:{coordinator.sql_port}', f'SELECT C0 FROM {table}'],
                                 capture_output=True, text=True, timeout=30)
        assert explain.returncode == 0, explain.stderr
        (root / 'explain.json').write_text(explain.stdout)
        plan = json.loads(explain.stdout)
        placements = [x for x in plan['execution_plan'] if 'location_id' in x and 'location_name' in x]
        assert any(x['location_id'] == 0 and x['location_name'] == 'local' for x in placements), plan
        assert any(x['location_id'] != 0 and x['location_name'] == 'worker' for x in placements), plan
        result['cases'].append({'case': 'explain-formal-locations', 'status': 'passed', 'placements': placements})

        assert [list(row.values()) for row in rows(coordinator.sql('SELECT C0 FROM C1_DUMMY_T', readonly=True))] == [[10]]
        result['cases'].append({'case': 'local-name-precedence', 'status': 'passed'})
        for text in ('SELECT C0 FROM "public".C1_DUMMY_T',):
            assert [list(row.values()) for row in rows(coordinator.sql(text, readonly=True))] == [[10]]
        for operation in ('INSERT INTO $T(C0) VALUES(99)', 'UPDATE $T SET C3=1 WHERE C0=900',
                          'DELETE FROM $T WHERE C0=900'):
            text = operation.replace('$T', table)
            output = coordinator.sql(text, failure=True)
            assert 'UNSUPPORTED_COMPILER_FEATURE_EXCEPTION' in output, (text, output)
            result['cases'].append({'case': 'remote-write-rejected', 'sql': text,
                                    'status': 'passed', 'output': output})
        query('worker-unchanged-after-rejection', 'SELECT C0 FROM $T ORDER BY C0', [[900], [1000]])
        coordinator.sql('DROP FOREIGN CATALOG branch')
        output = coordinator.sql(f'SELECT C0 FROM {table}', readonly=True, failure=True)
        assert 'SYMBOL_ANALYZE_EXCEPTION' in output, output
        result['cases'].append({'case': 'dropped-catalog-name-resolution', 'status': 'passed'})
        result['status'] = 'passed'
    except Exception:
        result['status'] = 'failed'
        result['failure'] = traceback.format_exc()
        raise
    finally:
        try:
            stop_started([coordinator, worker], result)
        finally:
            (root / 'results.json').write_text(json.dumps(result, indent=2, default=str))
            print('RESULT', root / 'results.json', flush=True)
    assert not result['cleanupErrors'], result['cleanupErrors']


if __name__ == '__main__':
    main()
