"""Offline behavioral tests; no real provider calls or credentials."""
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from unittest.mock import patch
import unittest

CLIENT=Path(__file__).with_name('external_agents.py')
spec=importlib.util.spec_from_file_location('external_agents',CLIENT)
ea=importlib.util.module_from_spec(spec); spec.loader.exec_module(ea)

@contextlib.contextmanager
def server(responder):
    seen=[]
    class Handler(BaseHTTPRequestHandler):
        def log_message(self,*args): pass
        def handle_request(self):
            raw=self.rfile.read(int(self.headers.get('Content-Length','0')))
            body=json.loads(raw) if raw else None
            seen.append((self.path,body,self.headers.get('Authorization')))
            status,result,headers=responder(self.path,body)
            self.send_response(status)
            for k,v in headers.items(): self.send_header(k,v)
            self.end_headers()
            try: self.wfile.write((result if isinstance(result,str) else json.dumps(result)).encode())
            except BrokenPipeError: pass
        do_GET=handle_request
        do_POST=handle_request
    srv=ThreadingHTTPServer(('127.0.0.1',0),Handler)
    thread=threading.Thread(target=srv.serve_forever,daemon=True); thread.start()
    try: yield f'http://127.0.0.1:{srv.server_port}/v1',seen
    finally: srv.shutdown(); srv.server_close(); thread.join()

def completed(text='done',reason='stop'):
    return {'choices':[{'message':{'role':'assistant','content':text},'finish_reason':reason}],
            'usage':{'prompt_tokens':10,'completion_tokens':5}}

class Tests(unittest.TestCase):
    def opts(self,**kw):
        return SimpleNamespace(**dict({'no_tools':False,'max_turns':3,'max_tokens':100,'timeout':3,'max_continuations':0},**kw))
    def cfg(self,url,api='chat-completions'):
        return {'base_url':url,'api':api,'model':'a','key_env':'TEST_AGENT_KEY'}
    def task(self): return {'id':'test','prompt':'Read file and answer.'}

    def test_chat_tool_loop_and_isolation(self):
        def reply(path,body):
            if body['messages'][-1]['role']=='tool':
                data=json.loads(body['messages'][-1]['content'])
                self.assertEqual(data['text'],'1: hello')
                return 200,completed(),{}
            return 200,{'choices':[{'message':{'role':'assistant','content':None,'tool_calls':[
                {'id':'c1','type':'function','function':{'name':'read_context','arguments':json.dumps(
                    {'path':'x.py','start_line':1,'end_line':1})}}]},'finish_reason':'tool_calls'}]},{}
        with server(reply) as (url,seen):
            r=ea.execute(self.task(),{'x.py':['hello']},self.cfg(url),'test-secret',self.opts())
        self.assertEqual(r['status'],'completed'); self.assertEqual(r['calls'],2)
        self.assertEqual(seen[0][2],'Bearer test-secret')
        self.assertIn('error',ea.tool('read_context','{"path":"/etc/passwd"}',{'x.py':['hello']}))
        self.assertIn('error',ea.tool('shell','{}',{}))

    def test_responses_tool_loop(self):
        def reply(path,body):
            self.assertEqual(path,'/v1/responses'); self.assertFalse(body['store'])
            if body['input'][-1].get('type')=='function_call_output':
                return 200,{'status':'completed','output':[{'type':'message','content':[{'type':'output_text','text':'done'}]}]},{}
            return 200,{'status':'completed','output':[{'type':'function_call','name':'list_context','arguments':'{}','call_id':'c1'}]},{}
        with server(reply) as (url,seen):
            r=ea.execute(self.task(),{},self.cfg(url,'responses'),'secret',self.opts())
        self.assertEqual(r['status'],'completed'); self.assertEqual(len(seen),2)

    def test_errors_not_retried_and_redacted(self):
        with server(lambda p,b:(401,{'error':'secret'},{})) as (url,seen):
            r=ea.execute(self.task(),{},self.cfg(url),'secret',self.opts())
        self.assertEqual(r['status'],'failed'); self.assertEqual(len(seen),1)
        self.assertNotIn('secret',json.dumps(r))
        with server(lambda p,b:(200,completed('secret'),{})) as (url,seen):
            r=ea.execute(self.task(),{},self.cfg(url),'secret',self.opts())
        self.assertEqual(r['answer'],'[REDACTED]')

    def test_redirect_refused(self):
        with server(lambda p,b:(302,{}, {'Location':'http://127.0.0.1:1/stolen'})) as (url,seen):
            r=ea.execute(self.task(),{},self.cfg(url),'secret',self.opts())
        self.assertEqual(r['status'],'failed'); self.assertEqual(len(seen),1)

    def test_truncation_and_turn_limit(self):
        with server(lambda p,b:(200,completed('partial','length'),{})) as (url,_):
            r=ea.execute(self.task(),{},self.cfg(url),'secret',self.opts())
        self.assertEqual(r['status'],'failed'); self.assertEqual(r['answer'],'partial')
        tool_call={'choices':[{'message':{'role':'assistant','content':None,'tool_calls':[
            {'id':'c1','function':{'name':'list_context','arguments':'{}'}}]}}]}
        with server(lambda p,b:(200,tool_call,{})) as (url,seen):
            r=ea.execute(self.task(),{},self.cfg(url),'secret',self.opts(max_turns=2))
        self.assertEqual(r['status'],'turn_limit'); self.assertEqual(len(seen),2)

    def test_continuation_keeps_history_and_joins_answer(self):
        def reply(path,body):
            if len(body['messages'])==2:
                return 200,completed('First part. ','length'),{}
            self.assertEqual(body['messages'][-2]['content'],'First part. ')
            self.assertIn('Continue',body['messages'][-1]['content'])
            self.assertEqual(body['messages'][1]['content'].split('\n')[0],self.task()['prompt'])
            return 200,completed('Final part.'),{}
        with server(reply) as (url,seen):
            r=ea.execute(self.task(),{},self.cfg(url),'secret',self.opts(max_continuations=2))
        self.assertEqual(r['status'],'completed'); self.assertEqual(r['continuations'],1)
        self.assertEqual(r['answer'],'First part. Final part.'); self.assertEqual(len(seen),2)

    def test_continuation_limit_is_bounded(self):
        with server(lambda p,b:(200,completed('partial','length'),{})) as (url,seen):
            r=ea.execute(self.task(),{},self.cfg(url),'secret',self.opts(max_turns=8,max_continuations=2))
        self.assertEqual(r['status'],'failed'); self.assertEqual(len(seen),3)
        self.assertEqual(r['continuations'],2)

    def test_incomplete_tool_arguments_are_never_executed(self):
        broken={'choices':[{'message':{'role':'assistant','content':None,'tool_calls':[
            {'id':'bad','type':'function','function':{'name':'read_context','arguments':'{"path":'}}]},'finish_reason':'length'}]}
        def reply(path,body):
            if len(body['messages'])==2: return 200,broken,{}
            self.assertNotIn('tool_calls',body['messages'][-2])
            self.assertNotEqual(body['messages'][-1]['role'],'tool')
            return 200,completed('Recovered.'),{}
        with server(reply) as (url,seen):
            r=ea.execute(self.task(),{},self.cfg(url),'secret',self.opts(max_continuations=1))
        self.assertEqual(r['status'],'completed'); self.assertEqual(r['tool_history'],[])

    def test_discovered_output_cap_and_explicit_clamping(self):
        def reply(path,body):
            self.assertEqual(body['max_tokens'],200)
            return 200,completed(),{}
        with server(reply) as (url,seen):
            cfg=self.cfg(url); cfg['model_cards']={'a':{'top_provider':{'max_completion_tokens':200},'context_length':10000}}
            auto=ea.execute(self.task(),{},cfg,'secret',self.opts(max_tokens=0))
            clamped=ea.execute(self.task(),{},cfg,'secret',self.opts(max_tokens=1000))
        self.assertEqual(auto['output_tokens_per_call'],200); self.assertEqual(clamped['output_tokens_per_call'],200)
        self.assertEqual(auto['context_window'],10000)

    def test_sse_aggregates_tool_fragments_and_usage(self):
        chunks=[{'choices':[{'index':0,'delta':{'tool_calls':[{'index':0,'id':'c1','function':{'name':'read_context','arguments':'{"path":'}}]},'finish_reason':None}]},
            {'choices':[{'index':0,'delta':{'tool_calls':[{'index':0,'function':{'arguments':'"x.py","start_line":1,"end_line":1}'}}]},'finish_reason':'tool_calls'}]},
            {'choices':[],'usage':{'total_tokens':15}}]
        def reply(path,body):
            self.assertTrue(body['stream'])
            if body['messages'][-1]['role']=='tool':
                data=json.loads(body['messages'][-1]['content']); self.assertEqual(data['text'],'1: hello')
                events=[{'choices':[{'index':0,'delta':{'content':'done'},'finish_reason':'stop'}]}]
            else: events=chunks
            stream=': keep-alive\n\n'+''.join('data: '+json.dumps(e)+'\n\n' for e in events)+'data: [DONE]\n\n'
            return 200,stream,{'Content-Type':'text/event-stream'}
        with server(reply) as (url,seen):
            cfg=self.cfg(url); cfg['stream']=True
            r=ea.execute(self.task(),{'x.py':['hello']},cfg,'secret',self.opts())
        self.assertEqual(r['status'],'completed'); self.assertEqual(r['answer'],'done')
        self.assertEqual(r['usage'][0]['total_tokens'],15); self.assertEqual(len(seen),2)

    def test_sse_stall_and_missing_finish_are_failures(self):
        for stream in ('data: {"id":"joingonka-stream-stalled","choices":[{"delta":{"content":"partial"},"finish_reason":"stop"}]}\n\ndata: [DONE]\n\n',
                       'data: {"choices":[{"delta":{"content":"partial"},"finish_reason":null}]}\n\n'):
            with server(lambda p,b:(200,stream,{'Content-Type':'text/event-stream'})) as (url,seen):
                cfg=self.cfg(url); cfg['stream']=True
                r=ea.execute(self.task(),{},cfg,'secret',self.opts())
            self.assertEqual(r['status'],'failed')

    def test_context_credentials_blocked(self):
        with tempfile.TemporaryDirectory() as tmp:
            p=Path(tmp)/'.env'; p.write_text('secret')
            with self.assertRaises(ea.AgentError):
                ea.snapshots({'context_files':{'env':str(p)}},{})
        with self.assertRaises(ea.AgentError): ea.endpoint('http://example.com','models')

    def test_request_has_wall_clock_deadline(self):
        def slow(path,body):
            time.sleep(2)
            return 200,completed(),{}
        with server(slow) as (url,seen):
            began=time.monotonic()
            r=ea.execute(self.task(),{},self.cfg(url),'secret',self.opts(timeout=0.5))
            elapsed=time.monotonic()-began
        self.assertEqual(r['status'],'failed')
        self.assertIn('deadline',r['error'])
        self.assertLess(elapsed,1.5)
        self.assertEqual(len(seen),1)

    def test_hedged_out_transport_stops_local_waiter(self):
        def slow(path,body):
            time.sleep(2)
            return 200,completed(),{}
        with server(slow) as (url,seen):
            cancel=threading.Event(); result={}
            worker=threading.Thread(target=lambda:result.update(
                ea.execute(self.task(),{},self.cfg(url),'secret',self.opts(timeout=3),cancel)))
            began=time.monotonic(); worker.start()
            limit=time.monotonic()+1
            while not seen and time.monotonic()<limit: time.sleep(0.01)
            cancel.set(); worker.join(1.2)
            elapsed=time.monotonic()-began
        self.assertFalse(worker.is_alive())
        self.assertEqual(result['status'],'cancelled')
        self.assertLess(elapsed,1.5)

    def test_transport_streams_large_private_stdin_without_deadlock(self):
        content='x'*900_000
        def reply(path,body):
            self.assertEqual(len(body['messages'][0]['content']),len(content))
            return 200,completed(),{}
        with server(reply) as (url,seen):
            result=ea.http(self.cfg(url),'secret','chat/completions',
                           {'model':'a','messages':[{'role':'user','content':content}]},timeout=3)
        self.assertEqual(result['choices'][0]['message']['content'],'done')

    def test_private_artifacts_are_0600_at_creation_or_replacement(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); fresh=root/'result.json'; existing=root/'config.json'
            ea.write(fresh,{'ok':True})
            existing.write_text('{"old":true}'); existing.chmod(0o644)
            ea.replace_private(existing,{'new':True})
            self.assertEqual(stat.S_IMODE(fresh.stat().st_mode),0o600)
            self.assertEqual(stat.S_IMODE(existing.stat().st_mode),0o600)
            self.assertEqual(json.loads(existing.read_text()),{'new':True})

    def test_batch_hedges_slow_or_failed_primary_but_not_fast_success(self):
        with tempfile.TemporaryDirectory() as tmp:
            out=Path(tmp); out.mkdir(exist_ok=True)
            opts=SimpleNamespace(workers=4,max_replicas=3,hedge_after=0.05,
                                 output_dir=out,write_reports=True)
            clone_lock=threading.Lock(); clone_count=[0]; clones_started=threading.Event()
            def slow_execute(task,context,cfg,key,opts,cancel_event=None):
                replica=task['replica_index']
                if replica==1:
                    self.assertTrue(clones_started.wait(1), 'replicas should launch while primary is pending')
                else:
                    with clone_lock:
                        clone_count[0]+=1
                        if clone_count[0]==2: clones_started.set()
                    self.assertTrue(clones_started.wait(1), 'backup replicas should launch together')
                return {'id':task['id'],'model':'a','status':'completed','answer':f'answer-{replica}',
                        'calls':1,'elapsed_seconds':0}
            with patch.object(ea,'execute',side_effect=slow_execute):
                reports=ea.batch([(dict(self.task(),model='a'),{})],{},'secret',opts,first=True)
            self.assertEqual(len(reports),3)
            self.assertEqual({r['replica_index'] for r in reports},{1,2,3})
            self.assertTrue(all(r['status']=='completed' for r in reports))
            self.assertEqual(sum(r['selected_for_judge'] for r in reports),1)
            self.assertEqual(json.loads((out/'first.json').read_text())['status'],'completed')
            self.assertTrue((out/'replica-1-r2.json').exists())
            self.assertTrue((out/'replica-1-r3.json').exists())

        with tempfile.TemporaryDirectory() as tmp:
            out=Path(tmp); out.mkdir(exist_ok=True)
            opts=SimpleNamespace(workers=4,max_replicas=3,hedge_after=10,
                                 output_dir=out,write_reports=True)
            calls=[]
            def fail_primary(task,context,cfg,key,opts,cancel_event=None):
                calls.append(task['replica_index'])
                status='failed' if task['replica_index']==1 else 'completed'
                return {'id':task['id'],'model':'a','status':status,
                        'answer':'backup' if status=='completed' else '', 'retryable':True, 'calls':1,'elapsed_seconds':0}
            with patch.object(ea,'execute',side_effect=fail_primary):
                reports=ea.batch([(dict(self.task(),model='a'),{})],{},'secret',opts)
            self.assertIn(1,calls)
            self.assertGreaterEqual(len(calls),2)
            self.assertEqual(len(reports),3)
            self.assertEqual(sum(r['status']=='completed' for r in reports),2)
            self.assertEqual(sum(r['selected_for_judge'] for r in reports),1)
            self.assertEqual({r['hedge_reason'] for r in reports if r['replica_index']>1},{'primary_failed'})

        with tempfile.TemporaryDirectory() as tmp:
            out=Path(tmp); out.mkdir(exist_ok=True)
            opts=SimpleNamespace(workers=4,max_replicas=3,hedge_after=0.2,
                                 output_dir=out,write_reports=True)
            calls=[]
            def fast_success(task,context,cfg,key,opts,cancel_event=None):
                calls.append(task['replica_index'])
                return {'id':task['id'],'model':'a','status':'completed','answer':'fast',
                        'calls':1,'elapsed_seconds':0}
            with patch.object(ea,'execute',side_effect=fast_success):
                reports=ea.batch([(dict(self.task(),model='a'),{})],{},'secret',opts)
            self.assertEqual(len(reports),1)
            self.assertEqual(calls,[1])
            self.assertTrue(reports[0]['selected_for_judge'])

    def test_hydra_retryable_failure_reuses_original_prompt_and_context_once(self):
        # All three fail: children must not recursively multiply.
        for status in (408, 429, 503):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as tmp:
                with server(lambda p,b:(status,{},{})) as (url,seen):
                    opts=self.opts(workers=3,output_dir=Path(tmp),no_tools=True)
                    reports=ea.batch([(self.task(),{'x.py':['original snapshot']})],
                                     self.cfg(url),'secret',opts)
                n=ea.DEFAULT_REPLICAS
                self.assertEqual(len(seen),n)
                self.assertEqual(len(reports),n)
                self.assertEqual({r['replica_index'] for r in reports},set(range(1,n+1)))
                self.assertTrue(all(r['retryable'] for r in reports))
                self.assertEqual(len({r['prompt_sha256'] for r in reports}),1)
                self.assertEqual(len({json.dumps(r['context_manifest']) for r in reports}),1)
                self.assertEqual(seen[0][1],seen[1][1])
                self.assertEqual(seen[0][1],seen[2][1])
                self.assertFalse(any(r['selected_for_judge'] for r in reports))

    def test_hydra_does_not_multiply_permanent_http_errors(self):
        for status in (400, 401, 403, 404, 422):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as tmp:
                with server(lambda p,b:(status,{},{})) as (url,seen):
                    reports=ea.batch([(self.task(),{})],self.cfg(url),'secret',
                                     self.opts(workers=3,output_dir=Path(tmp)))
                self.assertEqual(len(seen),1)
                self.assertEqual(len(reports),1)
                self.assertFalse(reports[0]['retryable'])

    def test_no_timed_hedging_by_default_and_explicit_single_attempt(self):
        with tempfile.TemporaryDirectory() as tmp:
            calls=[]
            def slow(task,context,cfg,key,opts,cancel_event=None):
                calls.append(task['replica_index'])
                time.sleep(0.05)
                return {'id':task['id'],'model':'a','status':'completed','answer':'ok','calls':1}
            with patch.object(ea,'execute',side_effect=slow):
                reports=ea.batch([(self.task(),{})],{},'secret',
                                 self.opts(workers=3,output_dir=Path(tmp)))
            self.assertEqual(calls,[1])
            self.assertEqual(ea.DEFAULT_HEDGE_AFTER,0)
        with tempfile.TemporaryDirectory() as tmp, server(lambda p,b:(503,{},{})) as (url,seen):
            # Regression: max_replicas=1 + expired hedge deadline used to spin.
            reports=ea.batch([(self.task(),{})],self.cfg(url),'secret',
                             self.opts(workers=1,output_dir=Path(tmp),max_replicas=1,hedge_after=0.001))
            self.assertEqual(len(seen),1)
            self.assertEqual(len(reports),1)

    def test_council_failed_judge_is_not_a_judged_success(self):
        def reply(path,body):
            if path.endswith('/models'):
                return 200,{'data':[{'id':'a'}]},{}
            if 'Act as an independent judge' in body['messages'][-1]['content']:
                return 503,{},{}
            return 200,completed('candidate'),{}
        with tempfile.TemporaryDirectory() as tmp, server(reply) as (url,seen):
            root=Path(tmp); key=root/'key'; key.write_text('test-secret'); key.chmod(0o600)
            cfg=self.cfg(url); cfg.pop('key_env'); cfg['key_file']=str(key)
            (root/'cfg.json').write_text(json.dumps(cfg)); (root/'task.json').write_text(json.dumps(self.task()))
            proc=subprocess.run([sys.executable,str(CLIENT),'--config',str(root/'cfg.json'),'council',
                '--task',str(root/'task.json'),'--output-dir',str(root/'out'),'--no-tools','--max-replicas','3',
                '--health-file',str(root/'health.json')],capture_output=True,text=True)
            self.assertEqual(proc.returncode,1,proc.stderr)
            self.assertEqual(json.loads((root/'out'/'first.json').read_text())['status'],'completed')
            self.assertEqual(json.loads((root/'out'/'judge.json').read_text())['status'],'failed')
            self.assertEqual(len(seen),6)  # discovery + preflight + candidate + three judge attempts
            self.assertEqual(json.loads((root/'out'/'council.json').read_text())['status'],'judge_failed')
            judges=[body for path,body,_ in seen if body and 'Act as an independent judge' in body['messages'][-1]['content']]
            self.assertEqual(len(judges),3)
            self.assertEqual(judges[0],judges[1])
            self.assertEqual(judges[0],judges[2])

    def test_council_judge_and_failure_accounting(self):
        def reply(path,body):
            if path.endswith('/models'):
                return 200,{'data':[{'id':'a'},{'id':'b'},{'id':'broken'}]},{}
            if body['model']=='broken': return 503,{},{}
            prompt=body['messages'][-1]['content']
            if 'Act as an independent judge' in prompt:
                self.assertIn('Candidates:',prompt); self.assertIn('answer-a',prompt); self.assertIn('answer-b',prompt)
                return 200,completed('judged'),{}
            return 200,completed('answer-'+body['model']),{}
        with tempfile.TemporaryDirectory() as tmp, server(reply) as (url,seen):
            root=Path(tmp); key=root/'key'; key.write_text('test-secret'); key.chmod(0o600)
            cfg=self.cfg(url); cfg.pop('key_env'); cfg['key_file']=str(key)
            (root/'cfg.json').write_text(json.dumps(cfg)); (root/'task.json').write_text(json.dumps(self.task()))
            proc=subprocess.run([sys.executable,str(CLIENT),'--config',str(root/'cfg.json'),'council',
                '--task',str(root/'task.json'),'--output-dir',str(root/'out'),'--no-tools','--no-preflight',
                '--max-replicas','3','--health-file',str(root/'health.json')],capture_output=True,text=True)
            self.assertEqual(proc.returncode,1,proc.stderr)
            self.assertEqual(json.loads((root/'out'/'judge.json').read_text())['answer'],'judged')
            self.assertEqual(json.loads((root/'out'/'first.json').read_text())['status'],'completed')
            self.assertEqual(len(seen),7)  # discovery + a/b + three broken attempts + judge
            summary=json.loads((root/'out'/'summary.json').read_text())
            self.assertEqual(len(summary),7)  # six attempts + council row
            self.assertEqual(summary[-1]['status'],'partial')
            failed=[x for x in summary if x.get('task_id')=='candidate-3']
            self.assertEqual(len(failed),3)
            self.assertEqual(len(json.loads((root/'out'/'candidate-map.json').read_text())),2)

    # ---- resilience features -------------------------------------------------
    def run_council(self,reply,*extra,task=None,health=None,timeout=60):
        tmp=tempfile.TemporaryDirectory(); self.addCleanup(tmp.cleanup)
        root=Path(tmp.name); key=root/'key'; key.write_text('test-secret'); key.chmod(0o600)
        if health is not None: (root/'health.json').write_text(json.dumps(health))
        with server(reply) as (url,seen):
            cfg=self.cfg(url); cfg.pop('key_env'); cfg['key_file']=str(key)
            (root/'cfg.json').write_text(json.dumps(cfg)); (root/'task.json').write_text(json.dumps(task or self.task()))
            began=time.monotonic()
            proc=subprocess.run([sys.executable,str(CLIENT),'--config',str(root/'cfg.json'),'council',
                '--task',str(root/'task.json'),'--output-dir',str(root/'out'),'--no-tools',
                '--health-file',str(root/'health.json'),*extra],capture_output=True,text=True,timeout=timeout)
            elapsed=time.monotonic()-began
        out=root/'out'
        read=lambda name: json.loads((out/name).read_text()) if (out/name).exists() else None
        return SimpleNamespace(proc=proc,seen=seen,root=root,read=read,elapsed=elapsed)

    @staticmethod
    def models_of(seen,judge=None):
        result=[]
        for path,body,_ in seen:
            if not body or 'messages' not in body or len(body['messages'])<2: continue
            is_judge='Act as an independent judge' in body['messages'][-1]['content']
            if judge is None or judge==is_judge: result.append(body['model'])
        return result

    def test_failed_model_is_substituted_from_fallback_pool(self):
        def reply(path,body):
            if path.endswith('/models'): return 200,{'data':[{'id':'a'},{'id':'dead'},{'id':'c'}]},{}
            if body['model']=='dead': return 404,{},{}
            if 'Act as an independent judge' in body['messages'][-1]['content']:
                self.assertIn('answer-c',body['messages'][-1]['content']); return 200,completed('judged'),{}
            return 200,completed('answer-'+body['model']),{}
        r=self.run_council(reply,'--models','a','dead','--fallback-models','c','--no-preflight','--min-candidates','2')
        self.assertEqual(r.proc.returncode,0,r.proc.stderr)
        council=r.read('council.json')
        self.assertEqual(council['substitutions'],[{'model':'c','replaces':'dead','reason':council['substitutions'][0]['reason']}])
        self.assertIn('404',council['substitutions'][0]['reason'])
        summary=r.read('summary.json')
        sub=[x for x in summary if x.get('model')=='c' and x.get('role')=='candidate']
        self.assertEqual(sub[0]['substitutes_for'],'dead'); self.assertEqual(sub[0]['hedge_reason'],'substitute')
        self.assertEqual(self.models_of(r.seen,judge=False).count('dead'),1)  # 404 is not retried
        self.assertEqual(council['good_candidates'],2)

    def test_retryable_exhaustion_also_triggers_substitution_and_is_bounded(self):
        def reply(path,body):
            if path.endswith('/models'): return 200,{'data':[{'id':'a'},{'id':'flaky'},{'id':'b'}]},{}
            if body['model']=='flaky': return 503,{},{}
            return 200,completed('answer-'+body['model']),{}
        r=self.run_council(reply,'--models','a','flaky','--no-preflight','--max-replicas','2','--min-candidates','2')
        self.assertEqual(r.proc.returncode,0,r.proc.stderr)
        self.assertEqual(self.models_of(r.seen,judge=False).count('flaky'),2)
        self.assertEqual(r.read('council.json')['substitutions'][0]['model'],'b')

    def test_auth_failure_stops_council_without_substitution(self):
        def reply(path,body):
            if path.endswith('/models'): return 200,{'data':[{'id':'a'},{'id':'b'},{'id':'c'}]},{}
            return 401,{'error':'bad key'},{}
        r=self.run_council(reply,'--no-preflight','--min-candidates','3')
        self.assertEqual(r.proc.returncode,1)
        self.assertIn('401',r.proc.stderr)
        self.assertLessEqual(len(self.models_of(r.seen)),3)  # at most one attempt per model, no replicas
        self.assertEqual(r.read('council.json')['substitutions'],[])
        self.assertEqual(r.read('council.json')['status'],'auth_failed')
        self.assertIsNone(r.read('judge.json'))
        self.assertNotIn('test-secret',(r.root/'out'/'summary.json').read_text())

    def test_preflight_auth_failure_stops_before_candidates(self):
        def reply(path,body):
            if path.endswith('/models'): return 200,{'data':[{'id':'a'},{'id':'b'}]},{}
            return 403,{},{}
        r=self.run_council(reply)
        self.assertEqual(r.proc.returncode,1); self.assertIn('preflight',r.proc.stderr)
        self.assertEqual(len(r.seen),3)  # discovery + two probes
        self.assertEqual(r.read('council.json')['status'],'auth_failed')

    def test_preflight_drops_dead_models_and_records_health(self):
        def reply(path,body):
            if path.endswith('/models'): return 200,{'data':[{'id':'a'},{'id':'b'},{'id':'dead'}]},{}
            if body['model']=='dead': return 503,{},{}
            if len(body['messages'])==1:
                self.assertEqual(body['max_tokens'],1); return 200,completed('O','length'),{}
            return 200,completed('answer-'+body['model']),{}
        r=self.run_council(reply)
        self.assertEqual(r.proc.returncode,0,r.proc.stderr)
        self.assertEqual(self.models_of(r.seen).count('dead'),0)  # only the probe reached it
        council=r.read('council.json')
        self.assertFalse(council['preflight']['dead']['ok']); self.assertTrue(council['preflight']['a']['ok'])
        self.assertIn('preflight_failed',[x['status'] for x in r.read('summary.json')])
        health=json.loads((r.root/'health.json').read_text())
        self.assertEqual(health['models']['dead']['last_failure_source'],'preflight')
        self.assertGreaterEqual(health['models']['a']['ok'],1)
        self.assertEqual(stat.S_IMODE((r.root/'health.json').stat().st_mode),0o600)

    def test_preflight_inconclusive_keeps_models(self):
        def reply(path,body):
            if path.endswith('/models'): return 200,{'data':[{'id':'a'}]},{}
            if len(body['messages'])==1: return 400,{},{}
            return 200,completed('answer'),{}
        r=self.run_council(reply,'--min-candidates','1')
        self.assertEqual(r.proc.returncode,0,r.proc.stderr)
        self.assertTrue(r.read('council.json')['preflight_inconclusive'])

    def test_cooldown_skips_model_unless_ignored(self):
        future=time.time()+600
        health={'version':1,'models':{'b':{'consecutive_failures':3,'cooldown_until':future,'last_error':'timeout'}}}
        def reply(path,body):
            if path.endswith('/models'): return 200,{'data':[{'id':'a'},{'id':'b'}]},{}
            return 200,completed('answer-'+body['model']),{}
        r=self.run_council(reply,'--no-preflight','--min-candidates','1',health=health)
        self.assertEqual(r.proc.returncode,0,r.proc.stderr)
        self.assertNotIn('b',self.models_of(r.seen,judge=False))
        self.assertEqual(r.read('council.json')['cooldown_skipped'][0]['model'],'b')
        self.assertIn('skipped_cooldown',[x['status'] for x in r.read('summary.json')])
        r=self.run_council(reply,'--no-preflight','--min-candidates','1','--quorum','0','--ignore-health',health=health)
        self.assertIn('b',self.models_of(r.seen,judge=False))

    def test_cooled_models_remain_last_resort_substitutes(self):
        health={'version':1,'models':{'b':{'consecutive_failures':2,'cooldown_until':time.time()+600}}}
        def reply(path,body):
            if path.endswith('/models'): return 200,{'data':[{'id':'a'},{'id':'b'}]},{}
            if body['model']=='a': return 400,{},{}
            return 200,completed('answer-'+body['model']),{}
        r=self.run_council(reply,'--no-preflight','--min-candidates','1',health=health)
        self.assertEqual(r.proc.returncode,0,r.proc.stderr)
        self.assertEqual(r.read('council.json')['substitutions'][0]['model'],'b')

    def test_judge_fallback_chain(self):
        def reply(path,body):
            if path.endswith('/models'): return 200,{'data':[{'id':'a'},{'id':'j1'},{'id':'j2'}]},{}
            if 'Act as an independent judge' in body['messages'][-1]['content']:
                return (503,{},{}) if body['model']=='j1' else (200,completed('verdict-'+body['model']),{})
            return 200,completed('answer-'+body['model']),{}
        r=self.run_council(reply,'--models','a','--min-candidates','1','--no-preflight','--max-replicas','2',
                           '--judge-models','j1','j2')
        self.assertEqual(r.proc.returncode,0,r.proc.stderr)
        judge=r.read('judge.json'); council=r.read('council.json')
        self.assertEqual(judge['model'],'j2'); self.assertEqual(judge['answer'],'verdict-j2')
        self.assertEqual(council['judge_chain'][:2],['j1','j2']); self.assertEqual(council['judge_model'],'j2')
        self.assertEqual(council['judge_substitutions'][0]['replaces'],'j1')
        self.assertEqual(self.models_of(r.seen,judge=True).count('j1'),2)

    def test_multiple_judges_in_parallel(self):
        def reply(path,body):
            if path.endswith('/models'): return 200,{'data':[{'id':'a'},{'id':'b'},{'id':'c'}]},{}
            if 'Act as an independent judge' in body['messages'][-1]['content']:
                return 200,completed('verdict-'+body['model']),{}
            return 200,completed('answer-'+body['model']),{}
        r=self.run_council(reply,'--no-preflight','--judges','2')
        self.assertEqual(r.proc.returncode,0,r.proc.stderr)
        self.assertEqual(len(r.read('judges.json')),2)
        self.assertEqual(len(r.read('council.json')['judges_completed']),2)

    def test_judge_chain_prefers_non_candidates_and_skips_dead(self):
        h=ea.Health(None,enabled=False)
        a=SimpleNamespace(judge_models=None,judge_model=None)
        good=[{'model':'a'},{'model':'b'}]
        chain=ea.judge_chain(a,{'judge_models':['a','x']},good,['a','b','x','y','dead'],h,{'dead'})
        self.assertEqual(chain[0],'x'); self.assertEqual(chain[-1],'dead')
        self.assertLess(chain.index('y'),chain.index('a'))
        self.assertLess(chain.index('a'),chain.index('b'))  # configured candidate before auto candidate
        a=SimpleNamespace(judge_models=['b'],judge_model=None)
        self.assertEqual(ea.judge_chain(a,{},good,['a','b','x'],h,set())[0],'b')  # CLI choice is strict
        self.assertLessEqual(len(ea.judge_chain(a,{},good,[f'm{i}' for i in range(20)],h,set())),ea.MAX_JUDGE_CHAIN)

    def test_quorum_starts_judge_without_waiting_for_stragglers(self):
        def reply(path,body):
            if path.endswith('/models'): return 200,{'data':[{'id':'a'},{'id':'b'},{'id':'slow'}]},{}
            if body['model']=='slow' and 'Act as an independent judge' not in body['messages'][-1]['content']:
                time.sleep(4)
            return 200,completed('answer-'+body['model']),{}
        r=self.run_council(reply,'--no-preflight','--quorum','2','--judge-models','a')
        self.assertEqual(r.proc.returncode,0,r.proc.stderr)
        self.assertLess(r.elapsed,3.5)
        summary=r.read('summary.json')
        slow=[x for x in summary if x.get('model')=='slow' and x.get('role')=='candidate']
        self.assertEqual(slow[0]['status'],'cancelled'); self.assertEqual(slow[0]['cancel_reason'],'quorum_reached')
        self.assertTrue(r.read('council.json')['quorum_reached'])
        self.assertEqual(len(r.read('candidate-map.json')),2)

    def test_health_cache_records_cooldown_order_and_adaptive_limits(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'sub'/'health.json'
            h=ea.Health(path)
            for _ in range(3):
                h.record({'model':'fast','status':'completed','calls':1,'elapsed_seconds':2,'first_byte_seconds':1,
                          'usage':[{'completion_tokens':400}]})
            h.record({'model':'slow','status':'completed','calls':1,'elapsed_seconds':40})
            h.record({'model':'bad','status':'failed','error':'Request deadline exceeded'})
            self.assertFalse(h.in_cooldown('bad'))
            h.record({'model':'bad','status':'failed','error':'Request deadline exceeded'})
            self.assertTrue(h.in_cooldown('bad'))
            h.record({'model':'x','status':'failed','auth_failed':True,'error':'401'})
            h.record({'model':'y','status':'cancelled'})
            data=json.loads(path.read_text())
            self.assertEqual(data['models']['bad']['timeouts'],2)
            self.assertNotIn('x',data['models']); self.assertNotIn('y',data['models'])
            self.assertEqual(stat.S_IMODE(path.stat().st_mode),0o600)
            self.assertEqual(stat.S_IMODE(path.parent.stat().st_mode),0o700)
            h2=ea.Health(path)
            self.assertEqual(h2.order(['bad','slow','unknown','fast']),['fast','unknown','slow','bad'])
            self.assertEqual(ea.Health(path,ignore=True).order(['bad','fast']),['bad','fast'])
            self.assertFalse(ea.Health(path,ignore=True).in_cooldown('bad'))
            # 200 tok/s, ttft 1s, 8192 tokens -> ~93s; unknown model keeps the base timeout.
            self.assertEqual(h2.timeout_for('fast',180,8192),int(30+2+1.5*8192/200))
            self.assertEqual(h2.timeout_for('unknown',180,8192),180)
            self.assertEqual(h2.timeout_for('fast',180,100000),360)  # slow budget may extend up to 2x base
            self.assertEqual(h2.timeout_for('fast',180,100000,explicit=True),180)
            self.assertEqual(h2.ttft_hedge_for('fast',25),8)
            self.assertEqual(h2.ttft_hedge_for('unknown',25),25)
            h3=ea.Health(path,enabled=False); h3.record({'model':'z','status':'completed'})
            self.assertNotIn('z',json.loads(path.read_text())['models'])
            path.write_text('not json'); self.assertEqual(ea.Health(path).data['models'],{})

    def test_ttft_hedge_launches_two_replicas_only_without_first_token(self):
        def run(first_byte):
            calls=[]
            def fake(task,context,cfg,key,opts,cancel_event=None):
                calls.append((task['replica_index'],time.monotonic()))
                if task['replica_index']==1:
                    if first_byte: cancel_event.first_byte_at=time.monotonic()
                    cancel_event.wait(0.6)
                    if cancel_event.is_set(): return {'id':task['id'],'model':'a','status':'cancelled','answer':'','calls':1}
                time.sleep(0.05)
                return {'id':task['id'],'model':'a','status':'completed','answer':'ok','calls':1}
            with tempfile.TemporaryDirectory() as tmp, patch.object(ea,'execute',side_effect=fake):
                opts=SimpleNamespace(workers=4,max_replicas=5,output_dir=Path(tmp),write_reports=True)
                reports=ea.batch([(self.task(),{},{'ttft_hedge':0.1,'timeout':7})],{},'secret',opts)
            return calls,reports
        calls,reports=run(False)
        self.assertEqual(sorted(c[0] for c in calls),[1,2,3])
        self.assertEqual({r['hedge_reason'] for r in reports if r['replica_index']>1},{'ttft_hedge'})
        self.assertIn('sibling_won',{r.get('cancel_reason') for r in reports if r['status']=='cancelled'})
        self.assertEqual(sum(r['selected_for_judge'] for r in reports),1)
        calls,reports=run(True)
        self.assertEqual([c[0] for c in calls],[1])

    def test_per_task_timeout_reaches_transport_and_first_byte_is_measured(self):
        def slow(path,body):
            time.sleep(1.5); return 200,completed(),{}
        with server(slow) as (url,seen):
            r=ea.execute(dict(self.task(),timeout_seconds=0.4),{},self.cfg(url),'secret',self.opts(timeout=5))
        self.assertEqual(r['status'],'failed'); self.assertIn('deadline',r['error']); self.assertEqual(r['timeout_seconds'],0.4)
        events=[{'choices':[{'index':0,'delta':{'content':'hi'},'finish_reason':'stop'}]}]
        stream=''.join('data: '+json.dumps(e)+'\n\n' for e in events)+'data: [DONE]\n\n'
        with server(lambda p,b:(200,stream,{'Content-Type':'text/event-stream'})) as (url,seen):
            cfg=self.cfg(url); cfg['stream']=True; attempt=ea.Attempt()
            r=ea.execute(self.task(),{},cfg,'secret',self.opts(),attempt)
        self.assertEqual(r['status'],'completed'); self.assertIsNotNone(attempt.first_byte_at)
        self.assertIn('first_byte_seconds',r)

    def test_auth_error_flag_crosses_transport_and_stops_batch(self):
        with server(lambda p,b:(401,{},{})) as (url,seen):
            with self.assertRaises(ea.AgentError) as ctx:
                ea.http(self.cfg(url),'secret','chat/completions',{'model':'a','messages':[]},timeout=3)
        self.assertTrue(ctx.exception.auth); self.assertFalse(ctx.exception.retryable)
        def fake(task,context,cfg,key,opts,cancel_event=None):
            if task['id']=='bad':
                return {'id':task['id'],'model':'a','status':'failed','auth_failed':True,'answer':'','calls':1}
            cancel_event.wait(3)
            return {'id':task['id'],'model':'a','status':'cancelled','answer':'','calls':1}
        with tempfile.TemporaryDirectory() as tmp, patch.object(ea,'execute',side_effect=fake):
            began=time.monotonic()
            reports=ea.batch([({'id':'bad','prompt':'x'},{}),({'id':'other','prompt':'x'},{})],{},'secret',
                             SimpleNamespace(workers=2,output_dir=Path(tmp)))
        self.assertLess(time.monotonic()-began,2)
        other=[r for r in reports if r['task_id']=='other'][0]
        self.assertEqual(other['cancel_reason'],'provider_auth_failed')

    def test_new_cli_defaults_are_aggressive_but_capped(self):
        self.assertEqual(ea.MAX_REPLICAS,6); self.assertEqual(ea.DEFAULT_REPLICAS,5)
        self.assertEqual(ea.MAX_INFLIGHT,48)
        with self.assertRaises(SystemExit), contextlib.redirect_stderr(io.StringIO()):
            with patch.object(sys,'argv',['x','council','--task','t','--output-dir','o','--max-replicas','7']):
                ea.main()

if __name__=='__main__': unittest.main()
