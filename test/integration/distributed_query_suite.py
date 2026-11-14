#!/usr/bin/env python3
"""CI entry point for an isolated, prebuilt fixture-enabled distributed-query stack."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time


GROUPS = {
    'c1': ('distributed_query_lifecycle.py', 6),
    'c2': ('distributed_query_sql.py', 34),
    'transport': ('distributed_query_sql_transport.py', 3),
    'acceptance': ('distributed_query_acceptance.py', 11),
}


def digest(path):
    with path.open('rb') as stream:
        result = hashlib.sha256()
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            result.update(chunk)
        return result.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ('prefix', 'server', 'tgctl', 'tgsql', 'client-jar', 'jni-library', 'work-dir'):
        parser.add_argument('--' + name, type=Path, required=True)
    parser.add_argument('--stack-manifest', type=Path)
    parser.add_argument('--source-root', type=Path)
    parser.add_argument('--port-base', type=int)
    parser.add_argument('--database-name-prefix', default='dq-ci')
    parser.add_argument('--groups', nargs='+', choices=GROUPS, default=list(GROUPS))
    args = parser.parse_args()
    if not __debug__:
        parser.error('Python optimization disables test assertions')
    args.work_dir.mkdir(parents=True, exist_ok=True)
    out = Path(tempfile.mkdtemp(prefix='dq-suite-', dir=args.work_dir)).resolve()
    source = Path(__file__).resolve().parent
    if args.stack_manifest:
        manifest = json.loads(args.stack_manifest.read_text())
        repository = args.source_root.resolve() if args.source_root else source.parents[1]
        production = {str(f.relative_to(repository)) for area in ('src', 'include')
                      for f in (repository/area).rglob('*') if f.is_file()}
        if not production or production != set(manifest['jogasakiProductionFiles']):
            raise RuntimeError('stack production-source file set mismatch')
        for relative, expected in manifest['jogasakiProductionFiles'].items():
            if digest(repository/relative) != expected:
                raise RuntimeError(f'stack source mismatch: {relative}')
        stack_root = args.stack_manifest.absolute().parent
        for relative, expected in manifest['binaries'].items():
            if digest(stack_root/relative) != expected:
                raise RuntimeError(f'stack binary mismatch: {relative}')
        inputs = [args.server, args.tgctl, args.tgsql, args.client_jar, args.jni_library,
                  *sorted((args.prefix/'lib').glob('*.so*'))]
        for executable in inputs:
            relative = str(executable.absolute().relative_to(stack_root))
            if relative not in manifest['binaries']:
                raise RuntimeError(f'execution input absent from stack manifest: {relative}')
        recorded_libraries = {name for name in manifest['binaries'] if name.startswith('install/lib/')}
        actual_libraries = {str(f.absolute().relative_to(stack_root))
                            for f in (args.prefix/'lib').glob('*.so*') if f.is_file()}
        if recorded_libraries != actual_libraries:
            raise RuntimeError('stack library set mismatch')
        if not all(manifest['buildOptions'].get(key) for key in ('FIXTURES', 'TEST_METADATA')):
            raise RuntimeError('fixture and test-metadata options must both be ON')
    client = out / 'client'; client.mkdir()
    shutil.copyfile(args.client_jar, client / 'tgsql.jar')
    shutil.copyfile(args.jni_library, client / 'libtsubakuro.so')
    subprocess.run(['javac', '-cp', str(client/'tgsql.jar'), '-d', str(client),
                    str(source/'DistributedQueryProbe.java')], check=True)
    env = os.environ.copy()
    for key in ('JAVA_TOOL_OPTIONS', 'JDK_JAVA_OPTIONS', '_JAVA_OPTIONS',
                'JOGASAKI_DQ_REMOTE_EXECUTION_TIMEOUT_MS', 'JOGASAKI_DQ_REMOTE_EXECUTION_SERVER'):
        env.pop(key, None)
    env['LD_LIBRARY_PATH'] = str(args.prefix.resolve()/'lib')
    env['DQ_TEST_DATABASE_PREFIX'] = args.database_name_prefix
    if args.port_base is not None:
        if not 1024 <= args.port_base <= 65531:
            parser.error('port-base must leave room for five unprivileged ports')
        env['DQ_TEST_PORT_BASE'] = str(args.port_base)
        env['DQ_TEST_PROXY_PORT'] = str(args.port_base + 4)
    tracked = [*source.glob('distributed_query*.py'), source/'DistributedQueryProbe.java',
               args.server.resolve(), args.tgctl.resolve(), args.tgsql.resolve(), args.client_jar.resolve(), args.jni_library.resolve(),
               *sorted((args.prefix.resolve()/'lib').glob('*.so*'))]
    result = {'groups': {}, 'inputsSha256': {str(p): digest(p) for p in tracked if p.is_file()},
              'portBase':args.port_base, 'databaseNamePrefix':args.database_name_prefix,
              'startedUnix':time.time(), 'status':'running'}
    (out/'result.json').write_text(json.dumps(result, indent=2))
    active = None
    def interrupt(_signum, _frame):
        raise KeyboardInterrupt()
    signal.signal(signal.SIGTERM, interrupt)
    try:
        for group in args.groups:
            script, count = GROUPS[group]
            dest = out/group; dest.mkdir()
            command = [sys.executable, '-u', '-c',
                'import runpy,signal,sys; '
                'signal.signal(signal.SIGTERM,lambda *_:(_ for _ in ()).throw(KeyboardInterrupt())); '
                'sys.path.insert(0,sys.argv[1]);sys.argv=sys.argv[2:]; '
                'runpy.run_path(sys.argv[0],run_name="__main__")', str(source), str(source/script),
                '--prefix', str(args.prefix.resolve()), '--server', str(args.server.resolve()),
                '--tgctl', str(args.tgctl.resolve()), '--tgsql', str(args.tgsql.resolve()), '--work-dir', str(dest)]
            if group == 'acceptance':command += ['--client-dir', str(client)]
            print('BEGIN', group, dest, flush=True)
            with (dest/'console.log').open('w') as log:
                active = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT,
                                          start_new_session=True)
                rc = active.wait(timeout=1200)
                active = None
            reports = list(dest.glob('*/result.json'))
            if rc != 0 or len(reports) != 1:
                raise RuntimeError(f'{group} failed, exit {rc}; see {dest}/console.log')
            data = json.loads(reports[0].read_text())
            cases = data.get('cases', data.get('scenarios', []))
            if len(cases) != count or data.get('cleanupErrors') or data.get('remainingListeners', data.get('finalListeners')):
                raise RuntimeError(f'{group} incomplete or cleanup failed: {reports[0]}')
            result['groups'][group] = {'exitCode':rc, 'cases':len(cases), 'report':str(reports[0])}
            print('PASS', group, len(cases), flush=True)
        result['status'] = 'passed'
    except BaseException as exc:
        result.update(status='failed', error=str(exc))
        raise
    finally:
        if active is not None and active.poll() is None:
            os.killpg(active.pid, signal.SIGTERM)
            try:active.wait(timeout=30)
            except subprocess.TimeoutExpired:
                os.killpg(active.pid, signal.SIGKILL); active.wait(timeout=5)
        result['finishedUnix'] = time.time()
        (out/'result.json').write_text(json.dumps(result, indent=2))
        print('RESULT', out/'result.json', flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
