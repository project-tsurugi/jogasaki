#!/usr/bin/env python3
"""C2 SQL results, rejection, recovery and role checks on two real databases."""
import argparse
from decimal import Decimal
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import time
import traceback

from distributed_query_e2e import Node
from distributed_query_lifecycle import (
    CREATE, TABLE, LOCAL_SQL, REMOTE_SQL, LOCAL_ROWS, REMOTE_ROWS, fixture,
    listeners, stop,
)


def main():
    if not __debug__:
        raise RuntimeError('Run C2 without Python optimization')
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('prefix', 'server', 'tgctl', 'tgsql', 'work-dir'):
        parser.add_argument('--' + name, required=True, type=Path)
    args = parser.parse_args()
    for name in ('prefix', 'server', 'tgctl', 'tgsql', 'work_dir'):
        setattr(args, name, getattr(args, name).resolve())
    args.catalog_routing = True
    args.work_dir.mkdir(parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix='c2-', dir=args.work_dir))
    (root / 'catalog.json').write_text(json.dumps(fixture(), indent=2))
    worker = Node(root, 'worker', args, None, remote=True)
    coordinator = Node(root, 'coordinator', args, worker.grpc_port, coordinator=True)
    nodes = [coordinator, worker]
    ports = [p for n in nodes for p in (n.sql_port, n.grpc_port)]
    result = {'status': 'running', 'cases': [], 'queries': [], 'stops': [],
              'cleanupErrors': [], 'configurationRestored': False}
    initial = {}
    current_case = 'setup'

    def query(node, text, kind='RTX', error_pattern=None, expected=None,
              columns=None, types=None, readiness_probe=False, timeout=30,
              expected_code=None, unordered=False):
        command = [str(args.tgsql), '--no-auth', '--connection',
                   f'tcp://127.0.0.1:{node.sql_port}', '--transaction', kind,
                   '--exec', text]
        started = time.monotonic()
        try:
            process = subprocess.run(command, stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE, text=True, timeout=timeout)
        except subprocess.TimeoutExpired as exc:
            log = root / f'sql-{len(result["queries"]):03d}.log'
            output = exc.stdout or ''
            if isinstance(output, bytes):
                output = output.decode(errors='replace')
            diagnostic = exc.stderr or ''
            if isinstance(diagnostic, bytes):
                diagnostic = diagnostic.decode(errors='replace')
            log.write_text(output + '\n' + diagnostic)
            result['queries'].append({'case': current_case, 'node': node.name,
                                      'generation': node.generation,
                                      'transaction': kind, 'sql': text,
                                      'timedOut': True, 'timeoutSeconds': timeout,
                                      'elapsedSeconds': time.monotonic() - started,
                                      'output': log.name})
            raise
        # Metadata/rows and diagnostics are separate streams. Merging them can
        # interleave a diagnostic with the metadata JSON's terminating newline.
        output = process.stdout + '\n' + process.stderr
        log = root / f'sql-{len(result["queries"]):03d}.log'
        log.write_text(output)
        metadata = [json.loads(line[3:]) for line in process.stdout.splitlines()
                    if line.startswith('// {')]
        rows = [json.loads(line, parse_float=Decimal) for line in process.stdout.splitlines()
                if line.startswith('{')]
        diagnostic = '\n'.join(line for line in process.stderr.splitlines()
                               if re.search(r'SQL-\d{5}', line)
                               and 'processing statement' not in line
                               and 'text=' not in line)
        codes = re.findall(r'SQL-\d{5}', diagnostic)
        evidence = {'case': current_case, 'node': node.name,
                    'generation': node.generation, 'transaction': kind,
                    'sql': text, 'exitCode': process.returncode,
                    'elapsedSeconds': time.monotonic() - started,
                    'timeoutSeconds': timeout,
                    'sqlCodes': codes, 'diagnostic': diagnostic,
                    'metadata': metadata, 'rows': rows,
                    'output': log.name}
        result['queries'].append(evidence)
        failed = process.returncode != 0 or bool(re.search(r'\bERROR\b|Exception', process.stderr))
        if failed and readiness_probe:
            # A fresh query probes the reused channel after the worker restarts.
            # Never retry partial results, SQL failures, or client connection failures.
            assert codes == ['SQL-02000'] and not rows, evidence
            assert re.search(r'unavailable|failed to connect|connection refused|socket closed',
                             diagnostic, re.I), evidence
            evidence['readiness'] = 'not-ready'
            return evidence
        if error_pattern:
            assert failed and codes, evidence | {'stdout': output}
            if expected_code is not None:
                assert codes == [expected_code], evidence
                evidence['expectedCode'] = expected_code
            assert re.search(error_pattern, diagnostic, re.I), evidence | {'stdout': output}
            evidence['expectedError'] = error_pattern
        else:
            assert not failed, evidence | {'stdout': output}
            assert 'script execution was successfully completed' in output, evidence
            if expected is not None:
                if unordered:
                    from collections import Counter
                    normalize = lambda row: json.dumps(row, sort_keys=True, default=str)
                    assert Counter(map(normalize, rows)) == Counter(map(normalize, expected)), evidence
                    evidence['rowOrder'] = 'multiset'
                else:
                    assert rows == expected, evidence | {'expected': expected}
                evidence['expectedRows'] = expected
            if columns is not None:
                assert len(metadata) == 1, evidence
                fields = metadata[0]['columns']
                actual = [f.get('label') or f['name'] for f in fields]
                assert actual == columns, evidence | {'expectedColumns': columns}
                evidence['expectedColumns'] = columns
                if types is not None:
                    assert [f['type'] for f in fields] == types, evidence
                    evidence['expectedTypes'] = types
            if readiness_probe:
                evidence['readiness'] = 'ready'
        return evidence

    def baseline():
        return query(coordinator, REMOTE_SQL, expected=REMOTE_ROWS,
                     columns=['C0', 'C1', 'C2'], types=['INT8', 'CHARACTER', 'DECIMAL'])

    def case(name, operation):
        nonlocal current_case
        current_case = name
        row = {'case': name, 'status': 'running'}
        result['cases'].append(row)
        try:
            operation()
        except BaseException as exc:
            row.update(status='failed', error=str(exc))
            raise
        row['status'] = 'passed'
        print('C2_CASE_PASSED', name, flush=True)

    def restart(node):
        stop(node, result['stops'])
        node.start()
        query(node, 'SELECT 1 AS READY', expected=[{'READY': 1}])

    def worker_off():
        worker.remote = False
        restart(worker)
        failed = query(coordinator, REMOTE_SQL,
                       error_pattern=r'ExecuteRemoteProcess failed: remote server unavailable',
                       expected_code='SQL-02000')
        assert failed['rows'] == [], failed
        for node in nodes:
            evidence = query(node, 'SELECT 1')
            assert len(evidence['rows']) == 1 and list(evidence['rows'][0].values()) == [1], evidence
        worker.remote = True
        restart(worker)
        assert worker.config.read_bytes() == initial['worker']
        assert any(line.split()[3].rsplit(':', 1)[-1] == str(worker.grpc_port)
                   for line in listeners([worker.grpc_port]))
        started = time.monotonic()
        deadline = started + 30
        progress = {'limitSeconds': 30, 'attempts': 0, 'notReadyProbes': 0,
                    'workerRpcListening': True}
        result['cases'][-1]['reconnect'] = progress
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError('Worker RPC did not reconnect within 30 seconds')
            progress['attempts'] += 1
            evidence = query(coordinator,
                             f'SELECT C0 FROM worker_schema.{TABLE} WHERE C0=900',
                             expected=[{'C0': 900}], readiness_probe=True, timeout=remaining)
            progress['elapsedSeconds'] = time.monotonic() - started
            if evidence['readiness'] == 'ready':
                break
            progress['notReadyProbes'] += 1
            time.sleep(min(.2, max(0, deadline - time.monotonic())))
        baseline()

    def coordinator_off(catalog_only=False):
        coordinator.coordinator = catalog_only
        coordinator.catalog_enabled = False
        restart(coordinator)
        query(coordinator, f'SELECT C0 FROM {TABLE} ORDER BY C0',
              expected=[{'C0': 10}, {'C0': 20}])
        query(coordinator, f'SELECT C0 FROM worker_schema.{TABLE} ORDER BY C0',
              error_pattern=r'schema_not_found', expected_code='SQL-03004')
        coordinator.coordinator = True
        coordinator.catalog_enabled = True
        restart(coordinator)
        assert coordinator.config.read_bytes() == initial['coordinator']
        baseline()

    def rejected(sql, pattern, followup=REMOTE_SQL, expected=REMOTE_ROWS, code=None, no_rows=False):
        failed = query(coordinator, sql, error_pattern=pattern, expected_code=code)
        if no_rows:
            assert failed['rows'] == [], failed
        query(coordinator, followup, expected=expected)

    remote = f'worker_schema.{TABLE}'
    try:
        assert len(set(ports)) == 4 and not listeners(ports), ports
        assert not os.environ.get('JOGASAKI_DQ_REMOTE_EXECUTION_SERVER')
        assert not os.environ.get('JOGASAKI_DQ_REMOTE_EXECUTION_TIMEOUT_MS')
        worker.start()
        coordinator.start()
        initial = {n.name: n.config.read_bytes() for n in nodes}
        for node, data in [
            (worker, "(900,'remote-process',12.34,0),(1000,NULL,56.78,2)"),
            (coordinator, "(10,'coordinator-only',1.23,1),(20,NULL,4.56,2)"),
        ]:
            query(node, CREATE, kind='OCC')
            query(node, f'INSERT INTO {TABLE} VALUES {data}', kind='OCC')
        case('C2-01', baseline)
        case('C2-02', lambda: query(coordinator, LOCAL_SQL, kind='OCC',
                                  expected=LOCAL_ROWS, columns=['C0', 'C1', 'C2']))
        case('C2-03', lambda: query(coordinator,
             f'SELECT C0,C1 FROM {remote} WHERE C0 > 900 ORDER BY C0',
             expected=[{'C0': 1000, 'C1': None}], columns=['C0', 'C1']))
        case('C2-04', lambda: query(coordinator, f'SELECT C0 FROM {remote} WHERE C0=900',
                                  expected=[{'C0': 900}], columns=['C0']))
        case('C2-05', lambda: query(coordinator, f'SELECT C0 FROM {remote} WHERE C0=9999',
                                  expected=[], columns=['C0']))
        case('C2-06', lambda: query(coordinator,
             f'SELECT C1 AS LABEL,C0 AS A,C0 AS B FROM {remote} WHERE C0=900',
             expected=[{'LABEL': 'remote-process', 'A': 900, 'B': 900}],
             columns=['LABEL', 'A', 'B'], types=['CHARACTER', 'INT8', 'INT8']))
        case('C2-07', lambda: query(coordinator, f'SELECT SUM(C0) AS TOTAL FROM {remote}',
                                  expected=[{'TOTAL': 1900}], columns=['TOTAL']))
        def evaluation_error():
            template = 'SELECT C0,10/C3 FROM {t} WHERE C0 > 0 ORDER BY C0'
            for kind in ('RTX', 'OCC'):
                query(worker, template.format(t=TABLE), kind=kind,
                      error_pattern=r'arithmetic error while performing division',
                      expected_code='SQL-02011')
            rejected(template.format(t=remote),
                     r'arithmetic error while performing division', code='SQL-02011')
        case('C2-08', evaluation_error)
        def unsupported_join():
            for kind in ('RTX', 'OCC'):
                query(worker, f'SELECT A.C0 FROM {TABLE} A JOIN {TABLE} B ON A.C0=B.C0',
                      kind=kind, expected=[{'C0':900},{'C0':1000}], unordered=True)
            rejected(
             f'SELECT A.C0 FROM {remote} A JOIN {remote} B ON A.C0=B.C0',
             r'unsupported remote process shape', code='SQL-03010', no_rows=True)
        case('C2-09', unsupported_join)

        def wrong_transaction():
            text = f'SELECT C0 FROM {remote} ORDER BY C0'
            for kind in ('RTX', 'OCC'):
                query(worker, f'SELECT C0 FROM {TABLE} ORDER BY C0', kind=kind,
                      expected=[{'C0':900},{'C0':1000}])
            failed = query(coordinator, text, kind='OCC', error_pattern=r'execution requires an RTX',
                           expected_code='SQL-02020')
            assert failed['rows'] == [], failed
            query(coordinator, text, expected=[{'C0': 900}, {'C0': 1000}])

        case('C2-10', wrong_transaction)
        case('C2-11', worker_off)
        case('C2-12', coordinator_off)
        case('C2-13', lambda: coordinator_off(catalog_only=True))
        # Each SQL must succeed on ordinary Tsurugi before checking its remote result.
        samples = [
            ('CAST(C0 AS INTEGER)', 900, 'INT4'),
            ('CAST(C0 AS DOUBLE)', Decimal('900.0'), 'FLOAT8'),
            ('C0 + 100', 1000, 'INT8'),
            ('C2 + CAST(1 AS DECIMAL(10,2))', Decimal('13.34'), 'DECIMAL'),
            ('CAST(C0 AS VARCHAR(20))', '900', 'CHARACTER'),
            ('CAST(NULL AS BIGINT)', None, 'INT8'),
            ("DATE '2024-02-29'", '2024-02-29', 'DATE'),
            ("TIMESTAMP '2024-02-29 12:34:56'", '2024-02-29T12:34:56', 'TIME_POINT'),
        ]
        for i, (expression, value, field_type) in enumerate(samples, 1):
            def supported_expression(e=expression, v=value, t=field_type):
                result['cases'][-1]['scope'] = 'ordinary-and-distributed-sql'
                for node, table, kind in ((worker, TABLE, 'RTX'),
                                          (worker, TABLE, 'OCC'),
                                          (coordinator, remote, 'RTX')):
                    query(node, f'SELECT {e} AS V FROM {table} WHERE C0=900',
                          kind=kind, expected=[{'V': v}], columns=['V'], types=[t])
            case(f'C2-X{i:02d}', supported_expression)
        def equivalent(template, expected, columns, types):
            for node, table, kind in ((worker, TABLE, 'RTX'), (worker, TABLE, 'OCC'),
                                      (coordinator, remote, 'RTX')):
                query(node, template.format(t=table), kind=kind, expected=expected,
                      columns=columns, types=types)

        normal_samples = [('N01',
          'SELECT C0,C1 FROM {t} WHERE C1 IS NULL ORDER BY C0',
          [{'C0': 1000, 'C1': None}],
          ['C0', 'C1'],
          ['INT8', 'CHARACTER']),
         ('N02',
          'SELECT C0 FROM {t} WHERE NOT C0=900 OR C3=0 ORDER BY C0',
          [{'C0': 900}, {'C0': 1000}],
          ['C0'],
          ['INT8']),
         ('N03',
          'SELECT C0,C0>900 AS FLAG,C2+CAST(NULL AS DECIMAL(10,2)) AS N FROM {t} ORDER BY C0',
          [{'C0': 900, 'FLAG': False, 'N': None}, {'C0': 1000, 'FLAG': True, 'N': None}],
          ['C0', 'FLAG', 'N'],
          ['INT8', 'BOOLEAN', 'DECIMAL']),
         ('N04', 'SELECT C0 FROM {t} ORDER BY C0 DESC LIMIT 1', [{'C0': 1000}], ['C0'], ['INT8']),
         ('N05',
          'SELECT C3,SUM(C0) AS TOTAL FROM {t} GROUP BY C3 ORDER BY C3',
          [{'C3': 0, 'TOTAL': 900}, {'C3': 2, 'TOTAL': 1000}],
          ['C3', 'TOTAL'],
          ['INT8', 'INT8']),
         ('N06',
          "SELECT TIME '12:34:56' AS V,X'0041FF' AS B FROM {t} WHERE C0=900",
          [{'B': [0, 65, -1], 'V': '12:34:56'}],
          ['V', 'B'],
          ['TIME_OF_DAY', 'OCTET']),
         ('N07', 'SELECT C0 FROM {t} ORDER BY C0 LIMIT 0', [], ['C0'], ['INT8']),
         ('N08',
          'SELECT C0 FROM {t} WHERE C0>=900 AND C0<1000 ORDER BY C0',
          [{'C0': 900}],
          ['C0'],
          ['INT8'])]
        for number, template, expected, columns, types in normal_samples:
            case('C2-'+number, lambda q=template, e=expected, c=columns, t=types:
                 equivalent(q, e, c, t))
        rejection_samples = [
            ('C2-R01', 'SELECT ABS(C0) AS V FROM {t} ORDER BY C0', [{'V': 900}, {'V': 1000}], 'unsupported remote expression'),
            ('C2-R02', "SELECT COALESCE(C1,'missing') AS V FROM {t} ORDER BY C0", [{'V': 'remote-process'}, {'V': 'missing'}], 'unsupported remote expression'),
            ('C2-R03', 'SELECT CASE WHEN C0=900 THEN 1 ELSE 2 END AS V FROM {t} ORDER BY C0', [{'V': 1}, {'V': 2}], 'unsupported remote expression'),
            ('C2-R04', 'SELECT C0 FROM {t} UNION ALL SELECT C0 FROM {t}', [{'C0': 900}, {'C0': 1000}, {'C0': 900}, {'C0': 1000}], 'at most one remote execution block'),
            ('C2-R05', 'SELECT C0 FROM {t} WHERE C0 IN (SELECT C0 FROM {t}) ORDER BY C0', [{'C0': 900}, {'C0': 1000}], 'unsupported remote process shape'),
        ]
        for number, template, expected, pattern in rejection_samples:
            def distributed_rejection(q=template, e=expected, pat=pattern, unordered=(number == 'C2-R04')):
                for kind in ('RTX', 'OCC'):
                    query(worker, q.format(t=TABLE), kind=kind, expected=e, unordered=unordered)
                rejected(q.format(t=remote), pat, code='SQL-03010', no_rows=True)
                query(coordinator, LOCAL_SQL, kind='OCC', expected=LOCAL_ROWS)
            case(number, distributed_rejection)
        assert len(result['cases']) == 34
        result['status'] = 'passed'
    except KeyboardInterrupt:
        result.update(status='cancelled', error='interrupted')
    except Exception as exc:
        result.update(status='failed', error=str(exc), traceback=traceback.format_exc())
        print(result['traceback'], flush=True)
    finally:
        for node in nodes:
            try:
                stop(node, result['stops'])
            except BaseException as exc:
                result['cleanupErrors'].append(str(exc))
            if node.name in initial:
                try:
                    node.config.write_bytes(initial[node.name])
                except OSError as exc:
                    result['cleanupErrors'].append(str(exc))
        result['configurationRestored'] = bool(initial) and all(
            n.config.read_bytes() == initial[n.name] for n in nodes)
        result['finalListeners'] = listeners(ports)
        if result['cleanupErrors'] or result['finalListeners'] or not result['configurationRestored']:
            result['status'] = 'failed'
        (root / 'result.json').write_text(json.dumps(result, default=str, indent=2))
        print('C2_RESULT=' + json.dumps({'status': result['status'],
              'cases': len(result['cases']), 'run_directory': str(root)}), flush=True)
    return 0 if result['status'] == 'passed' else 1


if __name__ == '__main__':
    raise SystemExit(main())
