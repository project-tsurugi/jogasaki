#!/usr/bin/env python3
"""Fixture-path acceptance: IPC, cancellation, recovery, concurrency, TLS and observation."""
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
from distributed_query_lifecycle import CREATE, TABLE, LOCAL_SQL, LOCAL_ROWS, REMOTE_SQL, REMOTE_ROWS, fixture, listeners, stop, healthy
from distributed_query_sql_proxy import Proxy


def main():
    parser=argparse.ArgumentParser()
    for name in ('prefix','server','tgctl','tgsql','work-dir'):
        parser.add_argument('--'+name,type=Path,required=True)
    parser.add_argument('--client-dir', type=Path, required=True)
    args=parser.parse_args()
    if not __debug__:
        raise RuntimeError('Run acceptance checks without Python optimization')
    args.catalog_routing=True
    root=Path(tempfile.mkdtemp(prefix='dq-extra-',dir=args.work_dir))
    (root/'catalog.json').write_text(json.dumps(fixture()))
    worker=Node(root,'worker',args,None,remote=True,minimal_roles=True)
    proxy=Proxy(worker.grpc_port)
    coordinator=Node(root,'coordinator',args,worker.grpc_port,coordinator=True,minimal_roles=True)
    def connections(primary):
        return {'distributed_query_client': {'name':'worker', 'uri':f'dns:///127.0.0.1:{primary}'}}
    coordinator.config_overrides = connections(worker.grpc_port)
    nodes=[coordinator,worker]
    ports=[p for node in nodes for p in (node.sql_port,node.grpc_port)]+[proxy.port]
    result={'cases':[],'queries':[],'stops':[],'cleanupErrors':[]}

    def endpoint(node, ipc=False):
        if not ipc: return f'tcp://127.0.0.1:{node.sql_port}'
        return 'ipc:'+re.search(r'^database_name=(\S+)',node.config.read_text(),re.M)[1]

    def command(node,text,kind='RTX',ipc=False):
        return [str(args.tgsql),'--no-auth','--connection',endpoint(node,ipc),
                '--transaction',kind,'--exec',text]

    def read(node,text,kind,ipc,proc):
        log=root/f'sql-{len(result["queries"]):03d}.log'
        log.write_text(proc.stdout+'\n'+proc.stderr)
        rows=[json.loads(s,parse_float=Decimal) for s in proc.stdout.splitlines() if s.startswith('{')]
        metadata=[json.loads(s[3:]) for s in proc.stdout.splitlines() if s.startswith('// {')]
        diagnostics=[s for s in proc.stderr.splitlines() if re.search(r'(?:SQL|SCD)-\d{5}',s) and 'processing statement' not in s and 'text=' not in s]
        codes=re.findall(r'(?:SQL|SCD)-\d{5}','\n'.join(diagnostics))
        data={'node':node.name,'transaction':kind,'endpoint':endpoint(node,ipc),'SQL':text,'rc':proc.returncode,
              'rows':rows,'metadata':metadata,'codes':codes,'diagnostics':diagnostics,'log':log.name}
        result['queries'].append(data)
        return data

    def query(node,text,kind='RTX',ipc=False,expected=None,timeout=22):
        proc=subprocess.run(command(node,text,kind,ipc),capture_output=True,text=True,timeout=timeout)
        data=read(node,text,kind,ipc,proc)
        if expected is not None:
            assert proc.returncode==0 and not data['codes'] and data['rows']==expected and 'successfully completed' in proc.stderr,data
        return data

    def case(name,run):
        try:
            detail=run()
            result['cases'].append({'case':name,'status':'passed','evidence':detail})
            print(name,'PASS',flush=True)
        except Exception:
            result['cases'].append({'case':name,'status':'failed','failure':traceback.format_exc()})
            print(name,'FAIL',traceback.format_exc(),flush=True)

    def wait_for(predicate, seconds=22):
        deadline=time.monotonic()+seconds
        while time.monotonic()<deadline:
            if (found:=predicate()):return found
            time.sleep(.01)
        raise TimeoutError('expected in-flight event did not arrive')

    def fields(line):
        return dict(re.findall(r'([a-z_]+):([^ ]+)', line))

    def worker_calls(event, after=0):
        return [fields(line) for line in worker.log_path.read_text()[after:].splitlines()
                if 'rpc:ExecuteRemoteProcess' in line and f'event:{event}' in line]

    def worker_cleanup(execution, after=0):
        item = wait_for(lambda: next((c for c in worker_calls('finish', after)
                                    if c.get('execution_id') == execution), None))
        assert item['rtx_close_status'] == '0', item
        assert item.get('worker_job_id') not in (None, 'unavailable'), item
        return item

    def stats(node, verb='show'):
        proc = subprocess.run([str(args.tgctl), 'dbstats', verb, '--format=json',
                               '--conf=' + str(node.config)], capture_output=True,
                              text=True, timeout=10)
        name = root / f'metrics-{node.name}-{len(result.get("metrics", []))}-{verb}.json'
        name.write_text(proc.stdout)
        assert proc.returncode == 0, proc.stderr
        data = json.loads(proc.stdout)
        result.setdefault('metrics', []).append(str(name.name))
        return data

    def metric(node, key, rpc='ExecuteRemoteProcess'):
        value = stats(node)[key]
        if isinstance(value, list):
            return next(x['value'] for x in value if x.get('rpc') == rpc)
        return value

    def ltx():
        worker_read=query(worker,LOCAL_SQL,'LTX',expected=REMOTE_ROWS)
        rejection=query(coordinator,REMOTE_SQL,'LTX')
        assert rejection['codes']==['SQL-02020'] and not rejection['rows'],rejection
        assert 'RTX' in ' '.join(rejection['diagnostics']),rejection
        query(coordinator,REMOTE_SQL,expected=REMOTE_ROWS)
        return {'worker':worker_read['log'],'coordinator':rejection['log'],'sqlCode':'SQL-02020'}

    def ipc(node,sql,rows,kind):
        control=query(node,sql,kind,expected=rows)
        tested=query(node,sql,kind,True,expected=rows)
        assert control['metadata'] and tested['metadata']==control['metadata'],tested
        return {'tcp':control['log'],'ipc':tested['log']}

    def kill_and_recover():
        # Observe request admission and first real response on the worker side before killing.
        stop(coordinator, result['stops'])
        proxy.mode='stall-active'
        proxy.release.clear()
        coordinator.config_overrides = connections(proxy.port)
        coordinator.start(); healthy(coordinator)
        start=len(proxy.events)
        worker_log_offset = len(worker.log_path.read_text())
        worker_pid=worker.process.pid
        coordinator_pid=coordinator.process.pid
        fault_sql=f'SELECT C0,C1,C2,C3 FROM worker_schema.{TABLE} WHERE C0>=10000'
        process=subprocess.Popen(command(coordinator,fault_sql),stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        try:
            event=wait_for(lambda: next((e for e in proxy.events[start:] if e.get('held')),None))
            assert process.poll() is None and worker.process.poll() is None
            assert any(e.get('direction')=='request' and e.get('stream')==event['stream'] and e.get('grpcMessages') for e in proxy.events[start:])
            execution = wait_for(lambda: next((c for c in worker_calls('start', worker_log_offset)), None))
            active_before_kill = metric(worker, 'distributed_query_requests_active')
            assert active_before_kill >= 1
            worker.process.kill()
            assert worker.process.wait(timeout=10)==-9
            worker.log.close()
            worker.process=None
            # Close the relay's worker connection after SIGKILL. This also releases held DATA.
            proxy.release.set()
            proxy.mode='pass'
            stdout,stderr=process.communicate(timeout=22)
            failure=read(coordinator,fault_sql,'RTX',False,subprocess.CompletedProcess([],process.returncode,stdout,stderr))
            assert failure['codes']==['SQL-02000'],failure
            assert 'successfully completed' not in stderr
            worker.start()
            healthy(worker)
            ready_at=time.monotonic()
            last=None
            attempts=0
            while time.monotonic()-ready_at<30:
                attempts+=1
                last=query(coordinator,REMOTE_SQL.replace('C0 > 0','C0 < 10000'))
                if last['rc']==0 and not last['codes'] and last['rows']==REMOTE_ROWS:
                    break
                assert last['codes']==['SQL-02000'] and not last['rows'],last
                time.sleep(.2)
            else:raise AssertionError(last)
            assert coordinator.process.pid==coordinator_pid and coordinator.process.poll() is None
            return {'workerKilled':worker_pid,'workerNewPid':worker.process.pid,'coordinatorPid':coordinator_pid,
                    'responseStarted':event,'workerActiveBeforeKill':active_before_kill,'workerStart':execution,'failureLog':failure['log'],'recoveryLog':last['log'],'recoveryAttempts':attempts,
                    'recoverySeconds':time.monotonic()-ready_at}
        finally:
            proxy.mode='pass'
            proxy.release.set()
            if process.poll() is None:
                process.kill()
                process.communicate(timeout=5)
            (root/'kill-relay.json').write_text(json.dumps(proxy.events[start:],indent=2))

    def cancellation():
        stop(coordinator, result['stops'])
        proxy.mode='stall-active'
        proxy.release.clear()
        coordinator.config_overrides = connections(proxy.port)
        coordinator.start(); healthy(coordinator)
        start=len(proxy.events)
        worker_log_offset = len(worker.log_path.read_text())
        go=root/'cancel-now'
        java=args.client_dir
        log=root/'cancel-client.log'
        sql=f'SELECT C0,C1,C2,C3 FROM worker_schema.{TABLE} WHERE C0>=10000'
        cmd=['java','-cp',f'{java}:{java}/tgsql.jar','DistributedQueryProbe','cancel',endpoint(coordinator),sql,str(go)]
        with log.open('w') as stream:
            process=subprocess.Popen(cmd,stdout=stream,stderr=subprocess.STDOUT,text=True)
            try:
                event=wait_for(lambda: next((e for e in proxy.events[start:] if e.get('held')),None))
                assert process.poll() is None and 'SQL_SUBMITTED' in log.read_text()
                execution = wait_for(lambda: next((c['execution_id'] for c in worker_calls('start', worker_log_offset)), None))
                # Absence of a development RPC deadline must not end this held call.
                time.sleep(2)
                assert process.poll() is None
                assert metric(worker, 'distributed_query_requests_active') >= 1
                assert any(e.get('limitedResponseWindow')==65535 for e in proxy.events[start:])
                go.touch()
                reset=wait_for(lambda: next((e for e in proxy.events[start:] if e.get('direction')=='request'
                   and e.get('type')==3 and e.get('stream')==event['stream'] and e.get('forwarded')),None),8)
                assert 'CANCEL_BEGIN' in log.read_text()
                proxy.mode='pass'
                proxy.release.set()
                assert process.wait(timeout=15)==0,log.read_text()
                assert 'CANCEL_SENT' in log.read_text()
                query(coordinator,REMOTE_SQL.replace('C0 > 0','C0 < 10000'),expected=REMOTE_ROWS)
                assert not any(e.get('holdExpired') for e in proxy.events[start:]), proxy.events[start:]
                cleanup = worker_cleanup(execution, worker_log_offset)
                assert cleanup['grpc_status'] == '1' and cleanup['rtx_rollback_requested'] == '1', cleanup
                wait_for(lambda: metric(worker, 'distributed_query_requests_active') == 0)
                return {'clientLog':log.name,'resetForwardedToWorker':reset,'workerResponseStarted':event,
                        'workerCleanup':cleanup, 'noDefaultDeadlineObservedSeconds':2}
            finally:
                proxy.mode='pass'
                proxy.release.set()
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=5)
                (root/'cancel-relay.json').write_text(json.dumps(proxy.events[start:],indent=2))

    def cut_and_recover():
        before = len(proxy.events)
        pid = coordinator.process.pid
        text = f'SELECT C0,C1,C2,C3 FROM worker_schema.{TABLE} WHERE C0>=10000'
        proxy.mode = 'cut-after-message'
        try:
            failure = query(coordinator, text)
            assert failure['codes'] == ['SQL-02000'], failure
            assert any(e.get('fault') == 'cut-after-complete-response-message'
                       for e in proxy.events[before:]), proxy.events[before:]
        finally:
            proxy.mode = 'pass'; proxy.release.set()
        until = time.monotonic() + 22
        recovery = None
        while time.monotonic() < until:
            assert coordinator.process.pid == pid and coordinator.process.poll() is None
            recovery = query(coordinator, small_sql, timeout=max(.1, until-time.monotonic()))
            if recovery['rc'] == 0 and not recovery['codes'] and recovery['rows'] == REMOTE_ROWS:
                break
            assert recovery['codes'] == ['SQL-02000'], recovery
            time.sleep(.1)
        else:
            raise AssertionError(recovery)
        return {'coordinatorPid':pid, 'failure':failure['log'], 'recovery':recovery['log']}

    def slow_receiver():
        proxy.mode = 'pass'; proxy.release.set()
        stop(coordinator, result['stops'])
        coordinator.config_overrides = connections(proxy.port)
        coordinator.config_overrides['sql'] = {'thread_pool_size':'1'}
        coordinator.start(); healthy(coordinator)
        trigger = root / 'release-slow-reader'
        log = root / 'slow-reader.log'
        text = f'SELECT C0,C1,C2,C3 FROM worker_schema.{TABLE} WHERE C0>=10000'
        cmd = ['java', '-cp', f'{args.client_dir}:{args.client_dir}/tgsql.jar',
               'DistributedQueryProbe', 'slow', endpoint(coordinator), text, str(trigger)]
        with log.open('w') as stream:
            process = subprocess.Popen(cmd, stdout=stream, stderr=subprocess.STDOUT, text=True)
            try:
                wait_for(lambda: 'ROW_READY' in log.read_text())
                assert process.poll() is None
                local = query(coordinator, 'SELECT 123 AS "VALUE"', 'OCC', expected=[{'VALUE':123}])
                remote = query(coordinator, small_sql, expected=REMOTE_ROWS)
                assert process.poll() is None, log.read_text()
                trigger.touch()
                assert process.wait(timeout=20) == 0, log.read_text()
                again = query(coordinator, small_sql, expected=REMOTE_ROWS)
                return {'clientLog':log.name, 'concurrentLocal':local['log'],
                        'concurrentRemote':remote['log'], 'afterClose':again['log'],
                        'clientPaused':True, 'workerMayHaveFinished':True}
            finally:
                trigger.touch()
                if process.poll() is None:
                    process.kill(); process.wait(timeout=5)

    def blocked_worker():
        stop(coordinator, result['stops'])
        proxy.mode = 'stall-active-multiplexed'; proxy.release.clear()
        coordinator.config_overrides = connections(proxy.port)
        coordinator.config_overrides['sql'] = {'thread_pool_size':'1'}
        coordinator.start(); healthy(coordinator)
        worker_log_offset = len(worker.log_path.read_text())
        before = len(proxy.events)
        trigger = root/'cancel-blocked-worker'
        log = root/'blocked-worker-client.log'
        text = f'SELECT C0,C1,C2,C3 FROM worker_schema.{TABLE} WHERE C0>=10000'
        cmd = ['java', '-cp', f'{args.client_dir}:{args.client_dir}/tgsql.jar',
               'DistributedQueryProbe', 'cancel', endpoint(coordinator), text, str(trigger)]
        with log.open('w') as stream:
            process = subprocess.Popen(cmd, stdout=stream, stderr=subprocess.STDOUT, text=True)
            try:
                held = wait_for(lambda: next((e for e in proxy.events[before:] if e.get('held')),None))
                execution = wait_for(lambda: next((c['execution_id'] for c in worker_calls('start', worker_log_offset)), None))
                assert metric(worker, 'distributed_query_requests_active') >= 1
                local = query(coordinator, 'SELECT 123 AS "VALUE"', 'OCC', expected=[{'VALUE':123}])
                remote = query(coordinator, small_sql, expected=REMOTE_ROWS)
                assert process.poll() is None and metric(worker,'distributed_query_requests_active') >= 1
                control = [e for e in proxy.events[before:] if e.get('direction')=='response'
                           and e.get('connection')==held['connection'] and e.get('stream')!=held['stream']
                           and e.get('forwarded') and e.get('grpcMessages')]
                assert control, proxy.events[before:]
                trigger.touch()
                wait_for(lambda: any(e.get('direction')=='request' and e.get('type')==3
                                     and e.get('stream')==held['stream'] for e in proxy.events[before:]))
                proxy.mode='pass'; proxy.release.set()
                assert process.wait(timeout=20)==0, log.read_text()
                cleanup = worker_cleanup(execution, worker_log_offset)
                assert cleanup['grpc_status']=='1' and cleanup['rtx_rollback_requested']=='1',cleanup
                assert not any(e.get('holdExpired') for e in proxy.events[before:])
                return {'heldResponse':held,'workerCleanup':cleanup,'concurrentLocal':local['log'],
                        'concurrentRemote':remote['log'],'clientLog':log.name,'sharedSQLWorkers':1,'sameHTTP2Connection':held['connection'],'controlResponses':control}
            finally:
                proxy.mode='pass';proxy.release.set();trigger.touch()
                if process.poll() is None:process.kill();process.wait(timeout=5)

    def tls():
        certs = root / 'tls'; certs.mkdir()
        for name in ('trusted', 'untrusted'):
            generated = subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
                '-keyout', str(certs/(name+'.key')), '-out', str(certs/(name+'.pem')), '-days', '1',
                '-subj', '/CN=localhost', '-addext', 'subjectAltName=IP:127.0.0.1,DNS:localhost'],
                capture_output=True, text=True, timeout=20)
            assert generated.returncode == 0, generated.stderr
        try:
            stop(coordinator,result['stops']); stop(worker,result['stops'])
            worker.config_overrides = {'grpc_server':{'secure':'true', 'fullchain_crt':str(certs/'trusted.pem'), 'server_key':str(certs/'trusted.key')}}
            worker.start(); healthy(worker)
            coordinator.worker_grpc = worker.grpc_port
            coordinator.config_overrides = connections(worker.grpc_port)
            coordinator.config_overrides['distributed_query_client'].update(secure='true', root_cert=str(certs/'trusted.pem'))
            coordinator.start(); healthy(coordinator)
            good = query(coordinator, small_sql, expected=REMOTE_ROWS)
            stop(coordinator,result['stops'])
            coordinator.config_overrides['distributed_query_client']['root_cert'] = str(certs/'untrusted.pem')
            coordinator.start(); healthy(coordinator)
            failed = query(coordinator, small_sql)
            assert failed['codes'] == ['SQL-02000'] and not failed['rows'], failed
            events = coordinator.log_path.read_text()
            assert 'grpc_status:14' in events, events[-4000:]
        finally:
            stop(coordinator,result['stops']); stop(worker,result['stops'])
            worker.config_overrides = {}; coordinator.config_overrides = connections(proxy.port)
            worker.start(); healthy(worker)
            coordinator.worker_grpc = proxy.port
            coordinator.start(); healthy(coordinator)
            query(coordinator, small_sql, expected=REMOTE_ROWS)
        return {'trustedSQL':good['log'], 'untrustedSQL':failed['log'], 'grpcStatus':14}

    def observation():
        listed = stats(worker, 'list')
        for key in ['distributed_query_requests_total', 'distributed_query_requests_active',
                    'distributed_query_errors_total', 'distributed_query_elapsed_microseconds_total']:
            assert key in listed, (key, listed)
        before = metric(worker, 'distributed_query_requests_total')
        acquired = metric(coordinator, 'distributed_query_channel_acquisitions_total')
        worker_log_offset = len(worker.log_path.read_text())
        read = query(coordinator, small_sql, expected=REMOTE_ROWS)
        wait_for(lambda: metric(worker, 'distributed_query_requests_active') == 0)
        assert metric(worker, 'distributed_query_requests_total') > before
        assert metric(coordinator, 'distributed_query_channel_acquisitions_total') > acquired
        assert metric(coordinator, 'distributed_query_channels') == 1
        assert metric(worker, 'distributed_query_elapsed_microseconds_total') > 0
        proc = subprocess.run(['java', '-cp', f'{args.client_dir}:{args.client_dir}/tgsql.jar',
            'DistributedQueryProbe', 'explain', endpoint(coordinator), small_sql],
            capture_output=True, text=True, timeout=15)
        (root/'explain.json').write_text(proc.stdout)
        assert proc.returncode == 0, proc.stderr
        explanation = json.loads(proc.stdout)
        def entries(x):
            if isinstance(x, dict):
                yield x
                for value in x.values():yield from entries(value)
            elif isinstance(x, list):
                for value in x:yield from entries(value)
        placed = [x for x in entries(explanation) if 'location_id' in x and 'location_name' in x]
        # The fixture adapter does not capture the formal operation-location query.
        # Do not invent local/remote placement fields for an uncaptured plan.
        assert not placed, explanation
        completion = wait_for(lambda: next((c for c in worker_calls('finish', worker_log_offset)), None))
        cleanup = worker_cleanup(completion['execution_id'], worker_log_offset)
        assert cleanup['grpc_status'] == '0' and int(cleanup['row_count']) > 0, cleanup
        matching = [fields(line) for line in coordinator.log_path.read_text().splitlines()
                    if 'event:finish' in line and 'execution_id:'+completion['execution_id']+' ' in line]
        assert matching, completion
        for field in ('job_id', 'execution_id', 'attempt', 'request_bytes', 'response_bytes',
                      'row_count', 'response_count', 'elapsed_us', 'grpc_status'):
            assert field in completion and field in matching[-1], (field,completion,matching)
        for field in ('job_id', 'execution_id', 'attempt', 'request_bytes', 'response_bytes', 'row_count', 'response_count'):
            assert completion[field] == matching[-1][field], (field, completion, matching[-1])
        assert int(completion['row_count']) == len(REMOTE_ROWS), completion
        return {'SQL':read['log'], 'workerFinish':completion, 'coordinatorFinish':matching[-1],
                'explain':'explain.json', 'locations':placed,
                'placementStatus':'not-captured-by-fixture; formal-location-SQL-check-pending'}

    small_sql = REMOTE_SQL.replace('C0 > 0','C0 < 10000')

    try:
        worker.start()
        healthy(worker)
        coordinator.start()
        healthy(coordinator)
        for node,data in [(worker,"(900,'remote-process',12.34,0),(1000,NULL,56.78,2)"),
                          (coordinator,"(10,'coordinator-only',1.23,1),(20,NULL,4.56,2)")]:
            query(node,CREATE,'OCC',expected=[])
            query(node,f'INSERT INTO {TABLE} VALUES {data}','OCC',expected=[])
        case('LTX',ltx)
        case('IPC-worker-local',lambda:ipc(worker,LOCAL_SQL,REMOTE_ROWS,'RTX'))
        case('IPC-coordinator-local',lambda:ipc(coordinator,LOCAL_SQL,LOCAL_ROWS,'OCC'))
        case('IPC-coordinator-distributed',lambda:ipc(coordinator,REMOTE_SQL,REMOTE_ROWS,'RTX'))
        for first in range(10000,50000,1000):
            values=','.join(f"({i},'{'a'*40}',12.34,2)" for i in range(first,first+1000))
            query(worker,f'INSERT INTO {TABLE} VALUES {values}','OCC',expected=[])
        stop(coordinator,result['stops'])
        coordinator.worker_grpc=proxy.port
        coordinator.config_overrides = connections(proxy.port)
        coordinator.start(); healthy(coordinator)
        case('SQL-cancel-propagation',cancellation)
        case('worker-SIGKILL-and-recover',kill_and_recover)
        case('transport-cut-and-recover-same-coordinator',cut_and_recover)
        case('slow-receiver-and-concurrent-SQL',slow_receiver)
        case('blocked-worker-and-concurrent-SQL',blocked_worker)
        case('TLS-trust-and-rejection',tls)
        case('metrics-explain-and-correlated-logs',observation)
    except Exception:
        result['setupFailure']=traceback.format_exc()
    finally:
        for node in nodes:
            try:stop(node,result['stops'])
            except Exception:result['cleanupErrors'].append(traceback.format_exc())
        try:proxy.close()
        except Exception:result['cleanupErrors'].append(traceback.format_exc())
        result['remainingListeners']=listeners(ports)
        (root/'result.json').write_text(json.dumps(result,indent=2,default=str))
        print(root/'result.json',flush=True)
    return 0 if len(result['cases'])==11 and all(c['status']=='passed' for c in result['cases']) and not any(result.get(k) for k in ('setupFailure','cleanupErrors','remainingListeners')) else 1


if __name__=='__main__':
    raise SystemExit(main())
