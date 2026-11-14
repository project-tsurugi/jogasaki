#!/usr/bin/env python3
"""Exercise formal foreign catalog DDL through SQL on two Bootstrap processes."""
import argparse
import configparser
import getpass
import json
import re
import socket
import subprocess
import tempfile
import time
import traceback
from pathlib import Path

from distributed_query_e2e import Node, rows
from distributed_query_lifecycle import CREATE, LOCAL_SQL, REMOTE_ROWS, healthy, stop, stop_started


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('prefix', 'server', 'tgctl', 'tgsql', 'work-dir'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--port-base', type=int, help='First of four consecutive unused ports')
    args = parser.parse_args()
    for name in ('prefix', 'server', 'tgctl', 'tgsql', 'work_dir'):
        setattr(args, name, getattr(args, name).resolve())
    if args.port_base is not None:
        if not 1024 <= args.port_base <= 65532:
            parser.error('--port-base must be between 1024 and 65532')
        for port in range(args.port_base, args.port_base + 4):
            with socket.socket() as channel:
                channel.bind(('127.0.0.1', port))
    args.catalog_routing = True  # Never force local plans to a worker via an environment override.
    args.database_name_prefix = getpass.getuser().lower() + '-dq-e1'
    args.work_dir.mkdir(parents=True, exist_ok=True)
    root = Path(tempfile.mkdtemp(prefix='e1-', dir=args.work_dir))
    worker_ports = None if args.port_base is None else (args.port_base, args.port_base + 1)
    coordinator_ports = None if args.port_base is None else (args.port_base + 2, args.port_base + 3)
    worker = Node(root, 'worker', args, None, remote=True, minimal_roles=True,
                  formal_catalog=True, ports=worker_ports)
    coordinator = Node(root, 'coordinator', args, worker.grpc_port, coordinator=True,
                       minimal_roles=True, formal_catalog=True, ports=coordinator_ports)
    result = {'status': 'running', 'scenarios': [], 'stops': [], 'cleanupErrors': [],
              'catalogSource': 'formal-DDL/RPC', 'sql': [], 'configurationChecks': []}

    def start_node(node):
        node.start()
        snapshot = node.root / f'tsurugi-{node.generation}.ini'
        snapshot.write_text(node.config.read_text())
        result.setdefault('configurations', []).append(str(snapshot.relative_to(root)))

    def sql(text, code=None, error_name=None):
        output = coordinator.sql(text, failure=code is not None or error_name is not None)
        codes = re.findall(r'\bSQL-\d{5}\b', output)
        if code is not None:
            assert code in codes, (text, code, output)
        if error_name is not None:
            assert re.search(r'\b' + re.escape(error_name) + r'\b', output), (text, error_name, output)
        result['sql'].append({'text': text, 'expectedCode': code, 'expectedErrorName': error_name, 'codes': codes, 'output': output})

    def case(name, action):
        begin = len(result['sql'])
        action()
        assert rows(worker.sql(LOCAL_SQL, readonly=True)) == REMOTE_ROWS
        result['scenarios'].append({'case': name, 'status': 'passed',
                                    'sqlBegin': begin, 'sqlEnd': len(result['sql'])})
        print('PASS', name, flush=True)

    try:
        start_node(worker)
        start_node(coordinator)
        for node in (worker, coordinator):
            healthy(node)
            ini = node.config.read_text()
            check = {'node': node.name, 'catalogSourceKeyPresent': 'catalog_source=' in ini,
                     'fixturePathKeyPresent': 'fixture_path=' in ini}
            result['configurationChecks'].append(check)
            assert not check['catalogSourceKeyPresent'] and not check['fixturePathKeyPresent']
        result['fixtureFilePresent'] = (root / 'catalog.json').exists()
        assert not result['fixtureFilePresent']
        worker.sql(CREATE)
        worker.sql("INSERT INTO C1_DUMMY_T VALUES (900,'remote-process',12.34,0),(1000,NULL,56.78,2)")

        def create_drop():
            sql('CREATE FOREIGN CATALOG c1_e1 SERVER worker')
            sql('DROP FOREIGN CATALOG c1_e1')
            sql('DROP FOREIGN CATALOG IF EXISTS c1_e1')
        case('E1-01-create-drop', create_drop)

        def conditions():
            sql('CREATE FOREIGN CATALOG IF NOT EXISTS c1_e1 SERVER worker')
            sql('CREATE FOREIGN CATALOG IF NOT EXISTS c1_e1 SERVER worker')
            sql('CREATE FOREIGN CATALOG IF NOT EXISTS c1_e1 SERVER IF EXISTS missing')
            sql('CREATE FOREIGN CATALOG skipped SERVER IF EXISTS missing')
            sql('DROP FOREIGN CATALOG skipped', 'SQL-02014')
            sql('DROP FOREIGN CATALOG c1_e1')
        case('E1-02-conditions', conditions)

        def runtime_errors():
            sql('CREATE FOREIGN CATALOG c1_e1 SERVER missing', 'SQL-02014')
            sql('DROP FOREIGN CATALOG c1_e1', 'SQL-02014')
            sql('CREATE FOREIGN CATALOG c1_e1 SERVER worker')
            # Tsubakuro currently prints SQL-00100 for this protocol error; assert its stable name.
            sql('CREATE FOREIGN CATALOG c1_e1 SERVER worker',
                error_name='TARGET_ALREADY_EXISTS_EXCEPTION')
            sql('DROP FOREIGN CATALOG c1_e1')
        case('E1-03-runtime-errors', runtime_errors)

        def identifiers():
            sql('CREATE FOREIGN CATALOG "C1.E1" SERVER "worker"')
            sql('DROP FOREIGN CATALOG "C1.E1"')
            sql('CREATE FOREIGN CATALOG c1_e1 SERVER "WORKER"', 'SQL-02014')
            for text in ('CREATE FOREIGN CATALOG a.b SERVER worker',
                         'CREATE FOREIGN CATALOG c1_e1 SERVER a.b',
                         'DROP FOREIGN CATALOG a.b'):
                sql(text, 'SQL-03010')
            for behavior in ('CASCADE', 'RESTRICT'):
                sql('DROP FOREIGN CATALOG c1_e1 ' + behavior, 'SQL-03001')
        case('E1-04-identifiers-and-syntax', identifiers)

        def restart():
            sql('CREATE FOREIGN CATALOG c1_e1 SERVER worker')
            stop(coordinator, result['stops'])
            start_node(coordinator)
            healthy(coordinator)
            sql('DROP FOREIGN CATALOG c1_e1', 'SQL-02014')
            sql('CREATE FOREIGN CATALOG c1_e1 SERVER worker')
            sql('DROP FOREIGN CATALOG c1_e1')
        case('E1-05-nonpersistent-registration', restart)

        def fetch_failure():
            stop(worker, result['stops'])
            sql('CREATE FOREIGN CATALOG c1_e1 SERVER worker', 'SQL-02000')
            start_node(worker)
            healthy(worker)
            # Each attempt is a new SQL request while the cached channel reconnects.
            # The failed request itself is never retried inside the product.
            deadline = time.monotonic() + 15
            attempts = result.setdefault('recoveryAttempts', [])
            while True:
                text = 'CREATE FOREIGN CATALOG c1_e1 SERVER worker'
                remaining = deadline - time.monotonic()
                assert remaining > 0, attempts
                output = coordinator.sql(text, failure=None, timeout=remaining)
                codes = re.findall(r'\bSQL-\d{5}\b', output)
                attempts.append({'text': text, 'codes': codes, 'output': output})
                if not coordinator.last_sql_failed:
                    break
                assert 'SQL-02000' in codes, output
                assert time.monotonic() < deadline, attempts
                time.sleep(.25)
            sql('DROP FOREIGN CATALOG c1_e1')
        case('E1-06-fetch-failure-and-retry', fetch_failure)

        def rollback():
            sql('CREATE TABLE E1_ROLLBACK_PROBE(C0 BIGINT PRIMARY KEY)')

            def transaction(name, ddl, value):
                script = coordinator.root / f'{name}.sql'
                script.write_text(f'BEGIN TRANSACTION;\n{ddl};\n'
                                  f'INSERT INTO E1_ROLLBACK_PROBE VALUES ({value});\n'
                                  f'SELECT C0 FROM E1_ROLLBACK_PROBE WHERE C0={value};\n'
                                  'ROLLBACK;\n')
                output = subprocess.run(
                    [str(args.tgsql), '--no-auth', '--connection',
                     f'tcp://127.0.0.1:{coordinator.sql_port}', '--transaction', 'manual',
                     '--no-auto-commit', '--script', str(script)],
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=30, text=True)
                (coordinator.root / f'{name}.log').write_text(output.stdout)
                assert output.returncode == 0, output.stdout
                assert not re.search(r'\bERROR\b|Exception', output.stdout), output.stdout
                assert output.stdout.count('transaction started. option=') == 1, output.stdout
                assert output.stdout.count('transaction rollback finished.') == 1, output.stdout
                assert rows(output.stdout) == [{'C0': value}], output.stdout
                result['sql'].append({'script': script.read_text(), 'output': output.stdout,
                                      'transaction': 'OCC', 'completion': 'ROLLBACK'})
                assert rows(coordinator.sql('SELECT C0 FROM E1_ROLLBACK_PROBE')) == []

            transaction('create-rollback', 'CREATE FOREIGN CATALOG rb_e1 SERVER worker', 1)
            sql('CREATE FOREIGN CATALOG rb_e1 SERVER worker',
                error_name='TARGET_ALREADY_EXISTS_EXCEPTION')
            transaction('drop-rollback', 'DROP FOREIGN CATALOG rb_e1', 2)
            sql('DROP FOREIGN CATALOG rb_e1', 'SQL-02014')
            sql('INSERT INTO E1_ROLLBACK_PROBE VALUES (3)')
            assert rows(coordinator.sql('SELECT C0 FROM E1_ROLLBACK_PROBE')) == [{'C0': 3}]
            sql('DROP TABLE E1_ROLLBACK_PROBE')
        case('E1-08-rollback-independent-registration', rollback)

        def identifier_setting(enabled):
            stop(coordinator, result['stops'])
            coordinator.sql_settings = {'lowercase_regular_identifiers': str(enabled).lower()}
            start_node(coordinator)
            healthy(coordinator)
            ini = coordinator.config.read_text()
            settings = configparser.ConfigParser(interpolation=None)
            settings.read_string(ini)
            assert settings.getboolean('sql', 'lowercase_regular_identifiers') is enabled
            assert re.search(r'\(dev_lowercase_regular_identifiers\)\s+' + str(enabled).lower() + r'\b',
                             coordinator.log_path.read_text()), coordinator.log_path
            result['configurationChecks'].append({'node': coordinator.name,
                'generation': coordinator.generation, 'lowercaseRegularIdentifiers': enabled})
            if enabled:
                sql('CREATE FOREIGN CATALOG MiXeD SERVER WORKER')
                sql('DROP FOREIGN CATALOG "MiXeD"', 'SQL-02014')
                sql('DROP FOREIGN CATALOG mixed')
            else:
                sql('CREATE FOREIGN CATALOG MiXeD SERVER WORKER', 'SQL-02014')
                sql('CREATE FOREIGN CATALOG MiXeD SERVER worker')
                sql('DROP FOREIGN CATALOG mixed', 'SQL-02014')
                sql('DROP FOREIGN CATALOG "MiXeD"')
            sql('CREATE FOREIGN CATALOG "Keep.Mixed" SERVER "worker"')
            sql('DROP FOREIGN CATALOG "keep.mixed"', 'SQL-02014')
            sql('DROP FOREIGN CATALOG "Keep.Mixed"')
            sql('CREATE FOREIGN CATALOG quoted_server SERVER "WORKER"', 'SQL-02014')
        case('E1-09-identifier-normalization-off', lambda: identifier_setting(False))
        case('E1-10-identifier-normalization-on', lambda: identifier_setting(True))

        def disabled():
            stop(coordinator, result['stops'])
            coordinator.coordinator = False
            coordinator.sql_settings = {}
            start_node(coordinator)
            healthy(coordinator)
            sql('CREATE FOREIGN CATALOG c1_e1 SERVER worker', 'SQL-03010')
            sql('DROP FOREIGN CATALOG c1_e1', 'SQL-03010')
            healthy(coordinator)
        case('E1-07-coordinator-disabled', disabled)
        result['status'] = 'passed'
    except BaseException:
        result['status'] = 'failed'
        result['error'] = traceback.format_exc()
    finally:
        stop_started([coordinator, worker], result)
        if result['cleanupErrors']:
            result['status'] = 'failed'
        (root / 'result.json').write_text(json.dumps(result, indent=2, default=str))
        print('RESULT', root / 'result.json', flush=True)
    if result['status'] != 'passed':
        raise SystemExit(result.get('error', str(result['cleanupErrors'])))


if __name__ == '__main__':
    main()
