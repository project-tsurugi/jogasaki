#!/usr/bin/env python3
"""Run the registered Bootstrap/Tateyama/Jogasaki stack in two isolated processes."""
from decimal import Decimal
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import time


_next_test_port = 0


def free_port():
    global _next_test_port
    base = os.environ.get('DQ_TEST_PORT_BASE')
    port = int(base) + _next_test_port if base else 0
    if base:
        if _next_test_port >= 4:
            raise RuntimeError('fixture groups reserve four node ports and one proxy port')
        _next_test_port += 1
    with socket.socket() as channel:
        channel.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        channel.bind(('127.0.0.1', port))
        return channel.getsockname()[1]


def rows(output):
    return [json.loads(line, parse_float=Decimal) for line in output.splitlines() if line.startswith('{')]


class Node:
    def __init__(self, root, name, args, worker_grpc, coordinator=False, remote=False, minimal_roles=False,
                 formal_catalog=False, ports=None):
        self.root = root / name
        self.root.mkdir()
        (self.root / 'pid').mkdir()
        self.name, self.args = name, args
        self.sql_port, self.grpc_port = ports if ports is not None else (free_port(), free_port())
        self.worker_grpc = worker_grpc or self.grpc_port
        self.coordinator, self.remote = coordinator, remote
        self.catalog_enabled = args.catalog_routing
        self.minimal_roles = minimal_roles
        self.formal_catalog = formal_catalog
        self.process = None
        self.log = None
        self.generation = 0

    def start(self):
        self.generation += 1
        identity = re.sub('-+', '-', re.sub('[^a-z0-9-]', '-', f"{getattr(self.args, 'database_name_prefix', os.environ.get('DQ_TEST_DATABASE_PREFIX', 'dq-e2e'))}-{self.root.parent.name}-{self.name}")).strip('-')
        self.config = self.root / 'tsurugi.ini'
        client_config = f"""[distributed_query_client]
name=worker
uri=dns:///127.0.0.1:{self.worker_grpc}
"""
        if not self.formal_catalog:
            client_config += f"""catalog_source={'fixture' if self.coordinator and self.catalog_enabled else 'disabled'}
fixture_path={self.root.parent / 'catalog.json' if self.coordinator and self.catalog_enabled else ''}
"""
        if self.minimal_roles and not self.coordinator:
            client_config = ''
        sql_settings = ''.join(f'{key}={value}\n' for key, value in getattr(self, 'sql_settings', {}).items())
        self.config.write_text(f'''[system]
instance_id={identity}
pid_directory={self.root / 'pid'}
[sql]
thread_pool_size=2
scan_default_parallel=1
default_partitions=1
enable_hybrid_scheduler=false
{sql_settings}[ipc_endpoint]
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
enabled={str(self.remote or not self.minimal_roles).lower()}
listen_address=127.0.0.1:{self.grpc_port}
endpoint=dns:///127.0.0.1:{self.grpc_port}
secure=false
[distributed_query_coordinator]
enabled={str(self.coordinator).lower()}
[distributed_query_remote]
enabled={str(self.remote).lower()}
{client_config}[glog]
logtostderr=true
''')
        # Test-specific TLS and flow-control settings use the same configuration writer.
        configuration = self.config.read_text()
        for section, values in getattr(self, 'config_overrides', {}).items():
            pattern = re.compile(r'(?ms)^\[' + re.escape(section) + r'\]\n(.*?)(?=^\[|\Z)')
            match = pattern.search(configuration)
            if not match:
                raise ValueError(f'unknown test configuration section: {section}')
            body = match[1]
            for key, value in values.items():
                body = re.sub(r'(?m)^' + re.escape(key) + r'=.*\n?', '', body)
                body += f'{key}={value}\n'
            configuration = configuration[:match.start(1)] + body + configuration[match.end(1):]
        self.config.write_text(configuration)
        environment = os.environ.copy()
        environment['LD_LIBRARY_PATH'] = str(self.args.prefix / 'lib')
        environment['TSURUGI_HOME'] = str(self.args.prefix)
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

    def sql(self, text, readonly=False, failure=False, timeout=30):
        command = [str(self.args.tgsql), '--no-auth', '--connection',
                   f'tcp://127.0.0.1:{self.sql_port}', '--transaction',
                   'readonly' if readonly else 'OCC', '--exec', text]
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                timeout=timeout, text=True)
        with (self.root / 'sql.log').open('a') as stream:
            stream.write(f'\nSQL: {text}\nreadonly={readonly}, exit={result.returncode}\n{result.stdout}\n')
        has_error = result.returncode != 0 or bool(re.search(r'\bERROR\b|Exception', result.stdout))
        self.last_sql_failed = has_error
        if failure is not None and failure != has_error:
            raise AssertionError(f'Expected SQL failure={failure}, got exit={result.returncode}: {result.stdout}')
        return result.stdout


def main():
    # Keep the historical entry point, using the formal compiler acceptance runner.
    from distributed_query_compiler import main as run_formal_compiler_checks
    run_formal_compiler_checks()


if __name__ == '__main__':
    main()
