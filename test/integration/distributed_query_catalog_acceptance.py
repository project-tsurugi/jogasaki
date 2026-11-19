#!/usr/bin/env python3
"""Formal-SQL catalog/DDL and retained-prepared acceptance, with ordinary SQL controls."""
import argparse
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import tempfile
import time
import traceback

from distributed_query_e2e import Node, rows
from distributed_query_lifecycle import CREATE, healthy, listeners, stop, stop_started
from distributed_query_sql_proxy import Proxy


def main():
    if not __debug__:
        raise RuntimeError('Run without Python optimization')
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('prefix', 'server', 'tgctl', 'tgsql', 'work-dir', 'client-dir'):
        parser.add_argument('--' + name, type=Path, required=True)
    args = parser.parse_args()
    args.catalog_routing = True
    args.database_name_prefix = os.environ.get('DQ_TEST_DATABASE_PREFIX', 'dq-catalog')
    base = int(os.environ.get('DQ_TEST_PORT_BASE', '49660'))
    proxy_port = int(os.environ.get('DQ_TEST_PROXY_PORT', str(base + 4)))
    ports = [base, base + 1, base + 2, base + 3, proxy_port]
    if len(set(ports)) != 5 or not all(1024 <= port <= 65535 for port in ports):
        parser.error('Five distinct unprivileged ports are required')
    for port in ports:
        with socket.socket() as channel:
            channel.bind(('127.0.0.1', port))
    os.environ['DQ_TEST_PROXY_PORT'] = str(proxy_port)
    args.work_dir.mkdir(parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix='catalog-acceptance-', dir=args.work_dir))
    worker = Node(root, 'worker', args, None, remote=True, minimal_roles=True, ports=(base, base + 1))
    proxy = Proxy(worker.grpc_port)
    coordinator = Node(root, 'coordinator', args, proxy.port, coordinator=True,
                       minimal_roles=True, ports=(base + 2, base + 3))
    result = {'status': 'running', 'cases': [], 'queries': [], 'stops': [], 'cleanupErrors': []}
    table = 'remote_catalog."public".C1_DUMMY_T'
    local_read = 'SELECT C0,C1,C2,C3 FROM C1_DUMMY_T ORDER BY C0'
    remote_read = local_read.replace('C1_DUMMY_T', table)
    values = "(900,'remote-process',12.34,0),(1000,NULL,56.78,2)"
    java = ['java', '-cp', f'{args.client_dir}:{args.client_dir}/tgsql.jar', 'DistributedQueryPreparedProbe']

    def query(node, text, code=None, rtx=False):
        command = [str(args.tgsql), '--no-auth', '--connection', f'tcp://127.0.0.1:{node.sql_port}',
                   '--transaction', 'RTX' if rtx else 'OCC', '--exec', text]
        proc = subprocess.run(command, capture_output=True, text=True, timeout=30)
        log = root / f'sql-{len(result["queries"]):03d}.log'
        log.write_text(proc.stdout + '\n' + proc.stderr)
        diagnostic = '\n'.join(line for line in proc.stderr.splitlines()
                               if re.search(r'SQL-\d{5}', line) and 'processing statement' not in line and 'text=' not in line)
        codes = re.findall(r'SQL-\d{5}', diagnostic)
        data = {'node': node.name, 'sql': text, 'transaction': 'RTX' if rtx else 'OCC',
                'rows': rows(proc.stdout), 'codes': codes, 'diagnostic': diagnostic, 'log': log.name,
                'metadata': [json.loads(line[3:]) for line in proc.stdout.splitlines() if line.startswith('// {')]}
        result['queries'].append(data)
        if code:
            assert codes == [code] and not data['rows'], data
        else:
            assert proc.returncode == 0 and not codes and 'successfully completed' in proc.stderr, data
        return data

    def wait_for(condition, process=None, seconds=25):
        until = time.monotonic() + seconds
        while time.monotonic() < until:
            if value := condition():
                return value
            if process is not None and process.poll() is not None:
                raise RuntimeError('Prepared probe exited before the expected event')
            time.sleep(.01)
        raise TimeoutError('Expected prepared/RPC event did not arrive')

    def register():
        query(coordinator, 'CREATE FOREIGN CATALOG remote_catalog SERVER worker')

    def reset_catalog():
        query(coordinator, 'DROP FOREIGN CATALOG remote_catalog')
        register()

    def ordinary():
        data = query(worker, local_read, rtx=True)
        assert [row['C0'] for row in data['rows']] == [900, 1000], data
        local = query(coordinator, 'SELECT C0 FROM C1_DUMMY_T', rtx=True)
        assert [row['C0'] for row in local['rows']] == [10], local
        return data

    def fields(message):
        """Read only the protobuf field framing needed for catalog identity evidence."""
        position = 0

        def varint():
            nonlocal position
            value = 0
            for shift in range(0, 70, 7):
                assert position < len(message)
                byte = message[position]
                position += 1
                value |= (byte & 127) << shift
                if byte < 128:
                    return value
            raise AssertionError('Invalid protobuf varint')

        while position < len(message):
            tag = varint()
            number, wire = tag >> 3, tag & 7
            assert number
            if wire == 0:
                value = varint()
            else:
                length = varint() if wire == 2 else {1: 8, 5: 4}[wire]
                assert position + length <= len(message)
                value = message[position:position + length]
                position += length
            yield number, wire, value

    def definitions():
        # A separate catalog fetch observes the real worker metadata without replacing remote_catalog.
        captured = []
        proxy.response_observer = captured.append
        try:
            query(coordinator, 'CREATE FOREIGN CATALOG identity_audit SERVER worker')
        finally:
            proxy.response_observer = None
        query(coordinator, 'DROP FOREIGN CATALOG identity_audit')
        assert captured
        entries = []
        ids = {'tables': {}, 'indexes': {}}
        index_features = {}
        for response in captured:
            fragments = [value for number, wire, value in fields(response) if number == 2 and wire == 2]
            assert len(fragments) == 1
            for number, wire, value in fields(fragments[0]):
                assert wire == 2
                entries.append((number, value.hex()))
                if number in (2, 4):  # RemoteCatalog.table_list / index_list
                    item = {field: data for field, _, data in fields(value) if field in (1, 2)}
                    assert set(item) == {1, 2}  # Presence includes a valid definition ID of zero.
                    name = item[2].decode().upper()
                    target = ids['tables' if number == 2 else 'indexes']
                    assert name not in target
                    target[name] = item[1]
                    if number == 4:
                        features = []
                        for field, kind, data in fields(value):
                            if field == 4:
                                if kind == 0:
                                    features.append(data)
                                else:
                                    assert kind == 2 and all(byte < 128 for byte in data)
                                    features.extend(data)  # Current feature enum values 1..6 fit a varint byte.
                        index_features[name] = sorted(features)

        assert 'C1_DUMMY_T' in ids['tables'] and 'DQ_EXISTING_I' in ids['indexes']
        assert 6 in index_features['DQ_EXISTING_I'] and 6 in index_features['DQ_TIES_I']
        evidence = {'ids': ids, 'indexFeatures': index_features, 'metadataSha256': hashlib.sha256(json.dumps(sorted(entries)).encode()).hexdigest()}
        path = root / f'catalog-identity-{len(result.get("definitions", [])):03d}.json'
        path.write_text(json.dumps(evidence, indent=2))
        result.setdefault('definitions', []).append(path.name)
        return evidence

    def case(name, action):
        details = action()
        result['cases'].append({'case': name, 'status': 'passed', 'evidence': details})
        print('PASS', name, flush=True)

    @contextmanager
    def retained(name, mode, text, expected='900,1000'):
        directory = root / name
        directory.mkdir()
        log = directory / 'client.log'
        with log.open('w') as stream:
            process = subprocess.Popen(java + [mode, f'tcp://127.0.0.1:{coordinator.sql_port}', text,
                                               str(directory), expected], stdout=stream, stderr=subprocess.STDOUT)
            try:
                yield directory, log, process
                assert process.wait(timeout=30) == 0, log.read_text()
                assert 'PROBE_OK ' + mode in log.read_text(), log.read_text()
            finally:
                proxy.mode = 'pass'
                proxy.release.set()
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=5)

    def bindings(node):
        target = table if node is coordinator else 'C1_DUMMY_T'
        text = f'SELECT C0 FROM {target} WHERE C0=:key'
        proc = subprocess.run(java + ['bindings', f'tcp://127.0.0.1:{node.sql_port}', text],
                              capture_output=True, text=True, timeout=45)
        log = root / f'bindings-{node.name}.log'
        log.write_text(proc.stdout + proc.stderr)
        assert proc.returncode == 0 and 'PROBE_OK bindings' in proc.stdout, log.read_text()
        return {'sql': text, 'log': log.name, 'expected': ['900', 'NULL→0 rows', '1000', 'SQL-02028', 'SQL-02027']}

    def secondary_feature_boundary():
        ordinary()
        assert 'dev_enable_ties_ordered_by_primary_key' not in worker.config.read_text()
        assert '(dev_enable_ties_ordered_by_primary_key) true' in worker.log_path.read_text()
        captured = []
        proxy.response_observer = captured.append
        try:
            register()
        finally:
            proxy.response_observer = None
            for number, response in enumerate(captured):
                (root / f'initial-catalog-{number}.bin').write_bytes(response)
        first = definitions()
        assert query(coordinator, remote_read, rtx=True)['rows'] == ordinary()['rows']
        reset_catalog()
        assert definitions() == first
        return {'defaultSecondaryIndex': 'catalog registration and re-registration succeed',
                'definitions': first, 'workerTieOrdering': 'default true'}

    def tie_ordering():
        evidence = []
        for predicate, order, limit, expected in (
                ('WHERE C1=1', 'C0', 2, [101, 205]),
                ('WHERE C1=1', 'C0 DESC', 2, [308, 205]),
                ('', 'C1,C0', 3, [101, 205, 308])):
            sql = f'SELECT C0 FROM DQ_TIES_T {predicate} ORDER BY {order} LIMIT {limit}'
            control = query(worker, sql, rtx=True)
            assert [row['C0'] for row in control['rows']] == expected, control
            remote_sql = sql.replace('DQ_TIES_T', 'remote_catalog."public".DQ_TIES_T')
            actual = query(coordinator, remote_sql, rtx=True)
            assert actual['rows'] == control['rows'] and actual['metadata'] == control['metadata']
            plan = subprocess.run(['java', '-cp', f'{args.client_dir}:{args.client_dir}/tgsql.jar',
                                   'DistributedQueryProbe', 'explain', f'tcp://127.0.0.1:{coordinator.sql_port}', remote_sql],
                                  capture_output=True, text=True, timeout=30)
            assert plan.returncode == 0, plan.stdout + plan.stderr
            path = root / f'ties-plan-{len(evidence)}.json'
            path.write_text(plan.stdout)
            if predicate:
                assert 'DQ_TIES_I' in plan.stdout.upper(), plan.stdout
            evidence.append({'sql': remote_sql, 'expectedKeys': expected, 'plan': path.name})
        return evidence

    def ordered_control(name, source, selection, tail, expected, index):
        sql = f'SELECT {selection} FROM {source} {tail}'
        control = query(worker, sql, rtx=True)
        assert control['rows'] == expected, control
        remote_sql = sql.replace(source, f'remote_catalog."public".{source}')
        actual = query(coordinator, remote_sql, rtx=True)
        assert actual['rows'] == expected and actual['metadata'] == control['metadata'], actual
        plan = subprocess.run(['java', '-cp', f'{args.client_dir}:{args.client_dir}/tgsql.jar',
                               'DistributedQueryProbe', 'explain', f'tcp://127.0.0.1:{coordinator.sql_port}', remote_sql],
                              capture_output=True, text=True, timeout=30)
        assert plan.returncode == 0 and (index is None or index in plan.stdout.upper()), plan.stdout + plan.stderr
        path = root / f'{name}-plan.json'
        path.write_text(plan.stdout)
        return {'sql': remote_sql, 'expectedRows': expected, 'plan': path.name}

    def descending_primary():
        return ordered_control('descending-primary', 'DQ_DESC_T', 'C0',
                               'WHERE C1=1 ORDER BY C0 DESC LIMIT 2',
                               [{'C0': 308}, {'C0': 205}], 'DQ_DESC_I')

    def composite_primary():
        return [ordered_control('composite-forward', 'DQ_COMPOSITE_T', 'C0,C1',
                                'WHERE C2=7 ORDER BY C0,C1 DESC LIMIT 2',
                                [{'C0': 1, 'C1': 3}, {'C0': 1, 'C1': 1}], 'DQ_COMPOSITE_I'),
                ordered_control('composite-reverse', 'DQ_COMPOSITE_T', 'C0,C1',
                                'WHERE C2=7 ORDER BY C0 DESC,C1 LIMIT 2',
                                [{'C0': 2, 'C1': 1}, {'C0': 1, 'C1': 1}], 'DQ_COMPOSITE_I')]

    def nullable_secondary():
        query(worker, 'INSERT INTO DQ_TIES_T VALUES(502,NULL),(501,NULL)')
        try:
            return ordered_control('nullable-secondary', 'DQ_TIES_T', 'C0,C1',
                                   'WHERE C1 IS NULL ORDER BY C0 LIMIT 1',
                                   [{'C0': 501, 'C1': None}], None)
        finally:
            query(worker, 'DELETE FROM DQ_TIES_T WHERE C0=501 OR C0=502')

    def bounded_secondary():
        query(worker, 'INSERT INTO DQ_TIES_T VALUES(601,0)')
        try:
            return [ordered_control('bounded-secondary-upper', 'DQ_TIES_T', 'C0,C1',
                                    'WHERE C1>=1 AND C1<=1 ORDER BY C1 DESC,C0 LIMIT 2',
                                    [{'C0': 101, 'C1': 1}, {'C0': 205, 'C1': 1}], None),
                    ordered_control('bounded-secondary-lower', 'DQ_TIES_T', 'C0,C1',
                                    'WHERE C1>=1 AND C1<=1 ORDER BY C1,C0 LIMIT 4',
                                    [{'C0': 101, 'C1': 1}, {'C0': 205, 'C1': 1}, {'C0': 308, 'C1': 1}],
                                    None)]
        finally:
            query(worker, 'DELETE FROM DQ_TIES_T WHERE C0=601')

    def catalog_drop():
        with retained('catalog-drop', 'catalog-drop', f'SELECT C0 FROM {table} ORDER BY C0') as (d, log, process):
            wait_for(lambda: (d / 'ready').exists(), process)
            query(coordinator, 'DROP FOREIGN CATALOG remote_catalog')
            ordinary()
            (d / 'changed').touch()
            wait_for(lambda: (d / 'rejected').exists(), process)
            register()
            (d / 'registered').touch()
        return {'log': str(log.relative_to(root)), 'oldStatement': 'SQL-02014', 'freshStatement': 'success'}

    def admitted_drop():
        before = len(proxy.events)
        proxy.mode = 'stall-active'
        proxy.release.clear()
        with retained('admitted-drop', 'admitted-drop', f'SELECT C0 FROM {table} ORDER BY C0') as (d, log, process):
            wait_for(lambda: (d / 'ready').exists(), process)
            event = wait_for(lambda: next((e for e in proxy.events[before:] if e.get('held')), None), process)
            assert process.poll() is None
            query(coordinator, 'DROP FOREIGN CATALOG remote_catalog')
            ordinary()
            assert process.poll() is None and not proxy.release.is_set()
            result.setdefault('ordering', []).append('RPC held → catalog DROP completed → RPC released')
            proxy.mode = 'pass'
            proxy.release.set()
        register()
        return {'log': str(log.relative_to(root)), 'admittedRPC': event,
                'acceptedExecution': 'success', 'reexecution': 'SQL-02014'}

    def worker_ddl(index):
        text = f"SELECT C0 FROM {table} WHERE C1='remote-process'" if index else f'SELECT C0 FROM {table} ORDER BY C0'
        name = 'worker-index-ddl' if index else 'worker-table-ddl'
        if index:
            proc = subprocess.run(['java', '-cp', f'{args.client_dir}:{args.client_dir}/tgsql.jar',
                                   'DistributedQueryProbe', 'explain', f'tcp://127.0.0.1:{coordinator.sql_port}', text],
                                  capture_output=True, text=True, timeout=30)
            (root / 'index-plan.json').write_text(proc.stdout)
            assert proc.returncode == 0 and 'DQ_EXISTING_I' in proc.stdout.upper(), proc.stdout + proc.stderr
        with retained(name, 'worker-ddl', text, '900' if index else '900,1000') as (d, log, process):
            wait_for(lambda: (d / 'ready').exists(), process)
            before = definitions()
            offset = len(worker.log_path.read_text())
            if index:
                # Ordinary Tsurugi requires an empty table when creating a secondary index.
                query(worker, 'DELETE FROM C1_DUMMY_T')
                query(worker, 'DROP INDEX DQ_EXISTING_I')
            else:
                query(worker, 'DROP TABLE C1_DUMMY_T')
                query(worker, CREATE)
            query(worker, 'CREATE INDEX DQ_EXISTING_I ON C1_DUMMY_T(C1)')
            query(worker, 'INSERT INTO C1_DUMMY_T VALUES ' + values)
            after = definitions()
            if index:
                assert before['ids']['tables'] == after['ids']['tables']
                assert before['ids']['indexes']['DQ_EXISTING_I'] != after['ids']['indexes']['DQ_EXISTING_I']
            else:
                assert before['ids']['tables']['C1_DUMMY_T'] != after['ids']['tables']['C1_DUMMY_T']
            ordinary()
            (d / 'changed').touch()
            wait_for(lambda: (d / 'rejected').exists(), process)
            stale = query(coordinator, text, code='SQL-02000', rtx=True)
            assert 'remote precondition not satisfied' in stale['diagnostic'], stale
            calls = [line for line in worker.log_path.read_text()[offset:].splitlines()
                     if 'rpc:ExecuteRemoteProcess' in line and 'event:finish' in line]
            assert len(calls) == 2 and all('grpc_status:9' in line for line in calls), calls
            (d / 'worker-rpc.log').write_text('\n'.join(calls))
            reset_catalog()
            (d / 'registered').touch()
        return {'log': str(log.relative_to(root)), 'staleCatalog': 'SQL-02000 / FAILED_PRECONDITION',
                'oldPreparedAfterRegistration': 'SQL-02014', 'freshPrepared': 'success',
                'beforeDefinitions': before, 'afterDefinitions': after}

    def ddl(operation):
        before_definitions = definitions()
        before = ordinary()
        positive_offset = len(worker.log_path.read_text())
        assert query(coordinator, remote_read, rtx=True)['rows'] == before['rows']
        positive = worker.log_path.read_text()[positive_offset:]
        assert positive.count('rpc:ExecuteRemoteProcess event:start') == 1, positive
        rpc_offset = len(worker.log_path.read_text())
        remote = {
            'create-table': 'CREATE TABLE remote_catalog."public".DQ_NEW_T(C0 BIGINT PRIMARY KEY)',
            'drop-table': f'DROP TABLE {table}',
            'create-index': 'CREATE INDEX DQ_NEW_I ON remote_catalog."public".DQ_INDEX_CTRL_T(C1)',
            'drop-index': 'DROP INDEX remote_catalog."public".DQ_EXISTING_I',
        }[operation]
        failure = query(coordinator, remote, code='SQL-03010')
        after = ordinary()
        assert after['rows'] == before['rows'] and after['metadata'] == before['metadata']
        assert 'rpc:ExecuteRemoteProcess' not in worker.log_path.read_text()[rpc_offset:]
        after_definitions = definitions()
        assert after_definitions == before_definitions
        assert query(coordinator, remote_read, rtx=True)['rows'] == before['rows']
        if operation == 'create-table':
            query(worker, 'CREATE TABLE DQ_NEW_T(C0 BIGINT PRIMARY KEY)')
            query(worker, 'INSERT INTO DQ_NEW_T VALUES(1)')
            assert query(worker, 'SELECT C0 FROM DQ_NEW_T', rtx=True)['rows'] == [{'C0': 1}]
            query(worker, 'DROP TABLE DQ_NEW_T')
        elif operation == 'drop-table':
            query(worker, 'CREATE TABLE DQ_DROP_T(C0 BIGINT PRIMARY KEY)')
            query(worker, 'DROP TABLE DQ_DROP_T')
        elif operation == 'create-index':
            query(worker, 'CREATE INDEX DQ_NEW_I ON DQ_INDEX_CTRL_T(C1)')
            query(worker, 'DROP INDEX DQ_NEW_I')
        else:
            query(worker, 'CREATE INDEX DQ_DROP_I ON DQ_INDEX_CTRL_T(C1)')
            query(worker, 'DROP INDEX DQ_DROP_I')
        return {'sql': remote, 'failureLog': failure['log'], 'expected': 'SQL-03010',
                'workerRowsAndTypesUnchanged': True, 'executionRPCSent': False,
                'beforeDefinitions': before_definitions, 'afterDefinitions': after_definitions}

    try:
        worker.start()
        coordinator.start()
        for node in (worker, coordinator):
            healthy(node)
            query(node, CREATE)
        query(worker, 'CREATE INDEX DQ_EXISTING_I ON C1_DUMMY_T(C1)')
        query(worker, 'CREATE TABLE DQ_TIES_T(C0 BIGINT PRIMARY KEY,C1 BIGINT)')
        query(worker, 'CREATE INDEX DQ_TIES_I ON DQ_TIES_T(C1)')
        query(worker, 'INSERT INTO DQ_TIES_T VALUES(308,1),(101,1),(205,1),(401,2)')
        query(worker, 'CREATE TABLE DQ_DESC_T(C0 BIGINT,C1 BIGINT,PRIMARY KEY(C0 DESC))')
        query(worker, 'CREATE INDEX DQ_DESC_I ON DQ_DESC_T(C1)')
        query(worker, 'INSERT INTO DQ_DESC_T VALUES(308,1),(101,1),(205,1)')
        query(worker, 'CREATE TABLE DQ_COMPOSITE_T(C0 BIGINT,C1 BIGINT,C2 BIGINT,PRIMARY KEY(C0,C1 DESC))')
        query(worker, 'CREATE INDEX DQ_COMPOSITE_I ON DQ_COMPOSITE_T(C2)')
        query(worker, 'INSERT INTO DQ_COMPOSITE_T VALUES(2,1,7),(1,1,7),(1,3,7),(1,2,8)')
        query(worker, 'INSERT INTO C1_DUMMY_T VALUES ' + values)
        query(worker, 'CREATE TABLE DQ_INDEX_CTRL_T(C0 BIGINT PRIMARY KEY,C1 VARCHAR(40))')
        query(coordinator, "INSERT INTO C1_DUMMY_T VALUES(10,'coordinator-only',1.23,1)")
        case('P00-secondary-index-feature-boundary', secondary_feature_boundary)
        case('P01-ordinary-prepared-bindings', lambda: bindings(worker))
        case('P02-remote-prepared-bindings', lambda: bindings(coordinator))
        case('P07-secondary-tie-ordering', tie_ordering)
        case('P08-descending-primary', descending_primary)
        case('P09-composite-primary', composite_primary)
        case('P10-nullable-secondary', nullable_secondary)
        case('P11-bounded-secondary', bounded_secondary)
        case('P03-prepared-catalog-drop-and-register', catalog_drop)
        case('P04-admitted-prepared-survives-drop', admitted_drop)
        for name in ('create-table', 'drop-table', 'create-index', 'drop-index'):
            case('D-' + name, lambda name=name: ddl(name))
        case('P05-worker-table-ddl-stale-catalog', lambda: worker_ddl(False))
        case('P06-worker-index-ddl-stale-catalog', lambda: worker_ddl(True))
        result['status'] = 'passed'
    except BaseException:
        result['status'] = 'failed'
        result['failure'] = traceback.format_exc()
        raise
    finally:
        proxy.mode = 'pass'
        proxy.release.set()
        try:
            stop_started([coordinator, worker], result)
        finally:
            proxy.close()
            result['remainingListeners'] = listeners(ports)
            (root / 'proxy-events.json').write_text(json.dumps(proxy.events, indent=2))
            (root / 'result.json').write_text(json.dumps(result, indent=2, default=str))
            print('RESULT', root / 'result.json', flush=True)
    assert not result['cleanupErrors'] and not result['remainingListeners'], result


if __name__ == '__main__':
    main()
