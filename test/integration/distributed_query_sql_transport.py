#!/usr/bin/env python3
"""C2 real SQL streaming, interrupted delivery and RPC deadline checks."""
import argparse
from decimal import Decimal
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile
import traceback
import time
from distributed_query_sql_proxy import Proxy
from distributed_query_e2e import Node
from distributed_query_lifecycle import CREATE, TABLE, REMOTE_SQL, REMOTE_ROWS, fixture, listeners, stop


def main():
    if not __debug__:
        raise RuntimeError('Run C2 without Python optimization')
    parser = argparse.ArgumentParser()
    for n in ['prefix', 'server', 'tgctl', 'tgsql', 'work-dir']:
        parser.add_argument('--'+n, type=Path, required=True)
    args = parser.parse_args()
    for n in ['prefix', 'server', 'tgctl', 'tgsql', 'work_dir']:
        setattr(args,n,getattr(args,n).resolve())
    args.catalog_routing = True
    root = Path(tempfile.mkdtemp(prefix='c2-validity-',dir=args.work_dir))
    (root/'catalog.json').write_text(json.dumps(fixture(),indent=2))
    worker = Node(root,'worker',args,None,remote=True)
    proxy = Proxy(worker.grpc_port)
    coordinator = Node(root,'coordinator',args,proxy.port,coordinator=True)
    nodes=[coordinator,worker];ports=[p for n in nodes for p in [n.sql_port,n.grpc_port]]
    result={'status':'running','cases':[],'queries':[],'stops':[],'cleanupErrors':[]}
    initial={}
    def query(node,sql,kind='RTX'):
        cmd=[str(args.tgsql),'--no-auth','--connection',f'tcp://127.0.0.1:{node.sql_port}',
             '--transaction',kind,'--exec',sql]
        proc=subprocess.run(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,timeout=30)
        log=root/f'sql-{len(result["queries"]):03d}.log';log.write_text(proc.stdout+'\n'+proc.stderr)
        diag='\n'.join(l for l in proc.stderr.splitlines() if re.search(r'SQL-\d{5}',l)
                       and 'processing statement'not in l and 'text='not in l)
        codes=re.findall(r'SQL-\d{5}',diag)
        failed=proc.returncode!=0 or bool(re.search(r'\bERROR\b|Exception',proc.stderr))
        metadata=[json.loads(l[3:])for l in proc.stdout.splitlines()if l.startswith('// {')]
        rows=[json.loads(l,parse_float=Decimal)for l in proc.stdout.splitlines()if l.startswith('{')]
        if failed:assert codes,proc.stderr
        else:assert 'script execution was successfully completed'in proc.stderr,proc.stderr
        row={'node':node.name,'transaction':kind,'sql':sql,'status':'sql-error'if failed else 'success',
             'sqlCodes':codes,'diagnostic':diag,'rows':rows,'metadata':metadata,'log':log.name}
        result['queries'].append(row)
        return row
    small_sql=REMOTE_SQL.replace('C0 > 0','C0 < 10000')
    try:
        assert len(set(ports))==4 and not listeners(ports)
        assert not os.environ.get('JOGASAKI_DQ_REMOTE_EXECUTION_SERVER')
        assert not os.environ.get('JOGASAKI_DQ_REMOTE_EXECUTION_TIMEOUT_MS')
        worker.start();coordinator.start();initial={n.name:n.config.read_bytes()for n in nodes}
        for node,data in [(worker,"(900,'remote-process',12.34,0),(1000,NULL,56.78,2)"),
                          (coordinator,"(10,'coordinator-only',1.23,1),(20,NULL,4.56,2)")]:
            assert query(node,CREATE,'OCC')['status']=='success'
            assert query(node,f'INSERT INTO {TABLE} VALUES {data}','OCC')['status']=='success'
        assert query(worker,f'SELECT C0,C1,C2 FROM {TABLE} ORDER BY C0')['rows']==REMOTE_ROWS
        assert query(coordinator,small_sql)['rows']==REMOTE_ROWS
        # Send enough actual SQL rows for more than one gRPC result message.
        for start in range(10000,50000,1000):
            values=','.join(f"({i},'{'a'*40}',12.34,2)" for i in range(start,start+1000))
            assert query(worker,f'INSERT INTO {TABLE} VALUES {values}','OCC')['status']=='success'
        sql=f'SELECT C0,C1,C2,C3 FROM {{t}} WHERE C0>=10000 ORDER BY C0'
        begin=len(proxy.events)
        ordinary=query(worker,sql.format(t=TABLE)); ordinary_occ=query(worker,sql.format(t=TABLE),'OCC')
        distributed=query(coordinator,sql.format(t='worker_schema.'+TABLE))
        expected=[{'C0':i,'C1':'a'*40,'C2':Decimal('12.34'),'C3':2}for i in range(10000,50000)]
        for row in [ordinary,ordinary_occ,distributed]:
            assert row['status']=='success' and row['rows']==expected,row['diagnostic']
            assert row['metadata']==ordinary['metadata']
        frames=proxy.events[begin:]
        counts=[count for event in frames for count in event.get('recordCounts',[])]
        assert len(counts)>=2 and sum(counts)==40000,counts
        result['cases'].append({'case':'C2-T01','status':'passed','rows':40000,'rpcRecordCounts':counts})
        print('TRANSPORT multi-message',counts,flush=True)
        small_sql=REMOTE_SQL.replace('C0 > 0','C0 < 10000')
        for case, mode in [('C2-T02','cut-after-message'),('C2-T03','stall')]:
            stop(coordinator,result['stops'])
            if mode=='stall':os.environ['JOGASAKI_DQ_REMOTE_EXECUTION_TIMEOUT_MS']='1000'
            try:coordinator.start()
            finally:os.environ.pop('JOGASAKI_DQ_REMOTE_EXECUTION_TIMEOUT_MS',None)
            assert query(coordinator,small_sql)['rows']==REMOTE_ROWS
            fault_sql=(sql.format(t='worker_schema.'+TABLE).replace(' ORDER BY C0','')
                       if mode=='cut-after-message' else small_sql)
            local_sql=fault_sql.replace('worker_schema.'+TABLE,TABLE)
            for kind in ('RTX','OCC'):
                control=query(worker,local_sql,kind)
                assert control['status']=='success',control['diagnostic']
                expected_control=expected if mode=='cut-after-message' else REMOTE_ROWS
                assert sorted(control['rows'],key=lambda v:v['C0'])==expected_control
            begin=len(proxy.events);proxy.mode=mode;proxy.release.clear()
            started=time.monotonic()
            row=query(coordinator,fault_sql)
            elapsed=time.monotonic()-started
            time.sleep(.1)
            events=proxy.events[begin:]
            assert sum(e.get('grpcMessages',0)for e in events if e.get('direction')=='request')==1,events
            proxy.mode='pass';proxy.release.set()
            expected_code = 'SQL-02039' if mode == 'stall' else 'SQL-02000'
            assert row['status'] == 'sql-error' and row['sqlCodes'] == [expected_code], row
            if mode=='stall':
                assert any(e.get('held')for e in events),events
                assert 'ExecuteRemoteProcess timed out' in row['diagnostic'], row
                assert elapsed<10,elapsed
            else:
                assert any(e.get('fault')=='cut-after-complete-response-message'for e in events),events
                assert sum(e.get('grpcMessages',0)for e in events if e.get('direction')=='response')==1,events
                assert any(sum(e.get('recordCounts',[]))>0 and e.get('forwarded')for e in events),events
                assert len(row['rows'])<40000
                assert len({v['C0']for v in row['rows']})==len(row['rows'])
                assert all(10000<=v['C0']<50000 for v in row['rows'])
                assert re.search('unavailable|socket closed|connection|transport',row['diagnostic'],re.I),row
            result['cases'].append({'case':case,'status':'passed','error':row['diagnostic'],
                'sqlRowsBeforeFailure':row['rows'],'elapsedSeconds':elapsed,'proxyEvents':events})
            print('TRANSPORT fault',case,row['sqlCodes'],row['diagnostic'],'rows',len(row['rows']),flush=True)
            stop(coordinator,result['stops']);coordinator.start()
            assert query(coordinator,small_sql)['rows']==REMOTE_ROWS
        result['status']='completed'
    except BaseException as exc:
        result.update(status='failed',error=str(exc),traceback=traceback.format_exc());print(result['traceback'],flush=True)
    finally:
        proxy.mode='pass';proxy.release.set()
        for n in nodes:
            try:stop(n,result['stops'])
            except BaseException as exc:result['cleanupErrors'].append(str(exc))
            if n.name in initial:
                try:n.config.write_bytes(initial[n.name])
                except BaseException as exc:result['cleanupErrors'].append('restore '+n.name+': '+str(exc))
        try:proxy.close()
        except BaseException as exc:result['cleanupErrors'].append('proxy close: '+str(exc))
        (root/'proxy-events.json').write_text(json.dumps(proxy.events,indent=2))
        try:result['configurationRestored']=bool(initial)and all(n.config.read_bytes()==initial[n.name]for n in nodes)
        except BaseException as exc:
            result['configurationRestored']=False;result['cleanupErrors'].append('restore verification: '+str(exc))
        result['finalListeners']=listeners(ports+[proxy.port])
        if result['cleanupErrors']or result['finalListeners']or not result['configurationRestored']:result['status']='failed'
        (root/'result.json').write_text(json.dumps(result,default=str,indent=2))
        print('VALIDITY_RESULT='+json.dumps({'status':result['status'],'run_directory':str(root)}),flush=True)
    return 0 if result['status']=='completed'else 1
if __name__=='__main__':raise SystemExit(main())
