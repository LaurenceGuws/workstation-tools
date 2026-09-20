#!/usr/bin/env python3
"""Independent adapter review. Only workstream-owned fixtures and our child processes."""
import ctypes, json, os, shutil, signal, subprocess, sys, tempfile, time, unittest
from pathlib import Path
HERE=Path(__file__).resolve().parent
WS=Path(os.environ["WALKER_TEST_ROOT"])
WS.mkdir(parents=True,exist_ok=True,mode=0o700)
TERMINAL={'exited','timed_out','cancelled','failed'}
WALKER_TERMINAL={'exited','stopped','timed_out','failed'}
class AdapterReview(unittest.TestCase):
 @classmethod
 def setUpClass(cls):
  libc=ctypes.CDLL(None,use_errno=True)
  if libc.prctl(36,1,0,0,0)!=0: raise OSError(ctypes.get_errno(),'subreaper failed')
  cls.host=HERE.parent/'zig-out/bin/walker-contract-driver'
  cls.walker=Path(os.environ['WALKER_BINARY']).resolve()
  cls.whome=Path(os.environ['WALKER_HOME']).resolve()
  for f in [cls.host,cls.walker]:
   if not f.is_file(): raise RuntimeError('missing binary '+str(f))
  ping=json.loads(subprocess.check_output([str(cls.walker),'ping'],env=dict(os.environ,WALKER_HOME=str(cls.whome))))
  if ping.get('schema')!='walker/v5' or not ping.get('durable_workloads_v1') or ping.get('delegated_cgroup_v2_admission')!='ready':
   raise RuntimeError('Walker fixture is not v5 durable/ready')
 def setUp(self):
  self.root=Path(tempfile.mkdtemp(prefix='rv-',dir=WS)); self.state=self.root/'tools'; self.whome=self.__class__.whome
  self.env=dict(os.environ,HOME=str(self.root),TEST_STATE=str(self.state),TEST_BACKEND='walker',WALKER_BINARY=str(self.walker),WALKER_HOME=str(self.whome),PATH='/usr/bin:/bin',REVIEW_ENV='caller-owned')
  self.calls=0
  self.run_ids=[]
 def tearDown(self):
  for run_id in reversed(self.run_ids):
   deadline=time.monotonic()+5
   while time.monotonic()<deadline:
    seen=subprocess.run([str(self.walker),'inspect',run_id],env=self.env,capture_output=True)
    if seen.returncode: break
    try: meta=json.loads(seen.stdout)['animal']
    except (json.JSONDecodeError,KeyError): break
    if meta['state'] not in WALKER_TERMINAL:
     subprocess.run([str(self.walker),'stop',run_id],env=self.env,capture_output=True)
     time.sleep(.04); continue
    removed=subprocess.run([str(self.walker),'rm',run_id],env=self.env,capture_output=True)
    if removed.returncode==0: break
    time.sleep(.04)
  for _ in range(18):
   children=[]
   for path in Path('/proc').glob('[0-9]*/stat'):
    try:
     raw=path.read_text(); f=raw[raw.rfind(') ')+2:].split()
     if int(f[1])==os.getpid(): children.append(int(path.parent.name))
    except (FileNotFoundError,ProcessLookupError): pass
   if not children: break
   for pid in children:
    try:
     fd=os.pidfd_open(pid)
     try: signal.pidfd_send_signal(fd,signal.SIGKILL)
     finally: os.close(fd)
    except ProcessLookupError: pass
   time.sleep(.025)
   while True:
    try:
     pid,_=os.waitpid(-1,os.WNOHANG)
     if not pid: break
    except ChildProcessError: break
  shutil.rmtree(self.root)
 def tool(self,name,data,success=True,env=None,timeout=8):
  self.calls+=1; req=self.root/f'request-{self.calls}.json'; req.write_text(json.dumps(data))
  selected=env or self.env
  p=subprocess.run([str(self.host),selected['WALKER_BINARY'],selected['WALKER_HOME'],selected['TEST_STATE'],name,str(req)],env=selected,capture_output=True,timeout=timeout)
  self.assertEqual(p.returncode==0,success,f'{name}: rc={p.returncode}, out={p.stdout[:250]!r}, err={p.stderr[:450]!r}')
  self.assertEqual(p.stderr if success else p.stdout,b'')
  out=p.stdout if success else p.stderr; self.assertTrue(out.endswith(b'\n')); reply=json.loads(out)
  if success and name=='job_start' and 'job_id' in reply and reply['job_id'] not in self.run_ids: self.run_ids.append(reply['job_id'])
  return reply
 def job(self,code,options=None,args=(),env=None):
  data=dict(argv=[sys.executable,'-c',code,*args],cwd=str(self.root),timeout_seconds=8)
  data.update(options or {}); return self.tool('job_start',data,env=env)
 def read(self,j,**kw): return self.tool('job_read',dict(job_id=j,**kw))
 def finish(self,j,timeout=7):
  end=time.monotonic()+timeout
  while time.monotonic()<end:
   r=self.read(j)
   if r['state'] in TERMINAL: return r
   self.assertNotEqual(r['state'],'indeterminate',r); time.sleep(.025)
  self.fail('no terminal receipt '+j)
 def walker_cli(self,*args):
  p=subprocess.run([str(self.walker),*args],env=self.env,capture_output=True,timeout=6)
  self.assertEqual(p.returncode,0,p.stderr); return json.loads(p.stdout)
 def wrapper(self,mode):
  path=self.root/('walker-'+mode); actual=repr(str(self.walker)); once=repr(str(self.root/'once'))
  body='#!'+sys.executable+'\nimport os,sys,json,subprocess\n'
  if mode=='lost-run':
   body+=f"if sys.argv[1]=='run':\n r=subprocess.run([{actual},*sys.argv[1:]],capture_output=True)\n if r.returncode: sys.stdout.buffer.write(r.stdout);sys.stderr.buffer.write(r.stderr);sys.exit(r.returncode)\n print(json.dumps({{'schema':'walker/v5','ok':False,'error':'SubmissionUncertain'}}),file=sys.stderr);sys.exit(1)\n"
  elif mode=='known-reject':
   body+="if sys.argv[1]=='run':\n print(json.dumps({'schema':'walker/v5','ok':False,'error':'DelegatedContainmentUnavailable'}),file=sys.stderr);sys.exit(1)\n"
  elif mode=='unversioned-reject':
   body+="if sys.argv[1]=='run':\n print(json.dumps({'ok':False,'error':'DelegatedContainmentUnavailable'}),file=sys.stderr);sys.exit(1)\n"
  elif mode=='old-ping':
   body+="if sys.argv[1]=='ping':\n print(json.dumps({'schema':'walker/v4','ok':True,'version':4}));sys.exit(0)\n"
  else:
   body+=f"if sys.argv[1]=='inspect' and not os.path.exists({once}):\n open({once},'w').write('yes');print('broken-json');sys.exit(0)\n"
  body+=f'os.execv({actual},[{actual},*sys.argv[1:]])\n'; path.write_text(body); path.chmod(0o700); return path
 def test_short_command_deadline_after_closed_streams(self):
  start=time.monotonic(); r=self.tool('command',dict(argv=['/bin/sh','-c','exec 1>&- 2>&-; sleep 20'],cwd=str(self.root),timeout_seconds=1),timeout=4)
  self.assertTrue(r['timed_out'],r); self.assertLess(time.monotonic()-start,3)
 def test_short_shell_deadline_after_closed_streams(self):
  r=self.tool('shell',dict(command='exec 1>&- 2>&-; sleep 20',cwd=str(self.root),timeout_seconds=1),timeout=4)
  self.assertTrue(r['timed_out'],r)
 def test_short_command_escaped_pipe_holder_is_bounded(self):
  code='import os,time\np=os.fork()\nif p==0: os.setsid()\ntime.sleep(20)'
  start=time.monotonic(); r=self.tool('command',dict(argv=[sys.executable,'-c',code],cwd=str(self.root),timeout_seconds=1),timeout=5)
  self.assertTrue(r['timed_out'],r); self.assertLess(time.monotonic()-start,4)
 def test_identity_exit_and_shell_visibility(self):
  j=self.job('import os,sys;print(os.environ["REVIEW_ENV"]);print("ERR",file=sys.stderr);sys.exit(7)')
  r=self.finish(j['job_id']); self.assertEqual(r['exit_code'],7); self.assertEqual(r['stdout'],'caller-owned\n'); self.assertEqual(r['stderr'],'ERR\n')
  meta=self.walker_cli('inspect',j['job_id'])['animal']; self.assertEqual(meta['run_id'],j['job_id']); self.assertEqual(meta['state'],'exited')
  self.assertEqual(r['ended_at'],meta['terminalized_at_ms']//1000); self.assertIsNone(meta['reconciled_at_ms'])
  self.assertIsNone(r.get('unit')); self.assertNotIn('systemd_properties',r)
 def test_new_environment_reaches_existing_walker(self):
  keep=self.job('import time;time.sleep(10)')
  second=self.job('import os;print(os.environ["REVIEW_ENV"])',env=dict(self.env,REVIEW_ENV='second-shell'))
  self.assertEqual(self.finish(second['job_id'])['stdout'],'second-shell\n')
  a=self.walker_cli('inspect',keep['job_id'])['animal']; b=self.walker_cli('inspect',second['job_id'])['animal']
  self.assertEqual(a['walker_pid'],b['walker_pid']); self.tool('job_cancel',dict(job_id=keep['job_id'])); self.finish(keep['job_id'])
 def test_missing_walker_cannot_start_legacy_job(self):
  r=self.tool('job_start',dict(argv=['/usr/bin/true'],cwd=str(self.root)),success=False,env=dict(self.env,WALKER_BINARY=str(self.root/'missing')))
  self.assertIn('Walker',r['error']); self.assertFalse((self.state/'jobs').exists())
 def test_nondurable_walker_rejected_before_state(self):
  other=self.root/'nondurable-home'; other_env=dict(self.env,WALKER_HOME=str(other))
  direct=json.loads(subprocess.check_output([
   str(self.walker),'run','--name','nondurable-anchor','--cwd',str(self.root),'--','/usr/bin/sleep','2'
  ],env=other_env))
  try:
   ping=json.loads(subprocess.check_output([str(self.walker),'ping'],env=other_env))
   self.assertFalse(ping['durable_workloads_v1'])
   r=self.tool('job_start',dict(argv=['/usr/bin/true'],cwd=str(self.root)),success=False,env=other_env)
   self.assertEqual(r['error'],'WalkerDurabilityUnavailable')
   self.assertFalse((self.state/'jobs').exists())
  finally:
   subprocess.run([str(self.walker),'stop',direct['run_id']],env=other_env,capture_output=True)
 def test_systemd_properties_rejected_before_launch(self):
  self.tool('job_start',dict(argv=['/usr/bin/true'],cwd=str(self.root),systemd_properties=['MemoryMax=1G']),success=False)
  self.assertFalse((self.state/'jobs').exists())
 def test_maximum_argument_count(self):
  j=self.job('import sys;print(len(sys.argv))',args=['x']*253); self.assertEqual(self.finish(j['job_id'])['stdout'],'254\n')
 def test_exact_payload_byte_limit(self):
  code='import sys;print(len(sys.argv[1]))'; n=32768-sum(map(len,[sys.executable,'-c',code]))
  j=self.job(code,args=['x'*n]); self.assertEqual(self.finish(j['job_id'])['stdout'],str(n)+'\n')
 def test_maximum_retention_option(self):
  j=self.job('print("small")',options=dict(output_limit_bytes=512*1024*1024)); r=self.finish(j['job_id'])
  self.assertEqual(r['stdout'],'small\n'); self.assertEqual(r['output_limit_bytes'],512*1024*1024)
 def test_full_read_budget_and_offsets(self):
  j=self.job('import os;os.write(1,b"x"*24000);os.write(2,b"y"*24000)'); self.finish(j['job_id'])
  r=self.read(j['job_id'],max_bytes=32768); self.assertEqual(len(r['stdout'])+len(r['stderr']),32768)
  s=self.read(j['job_id'],max_bytes=32768,stdout_offset=r['next_stdout_offset'],stderr_offset=r['next_stderr_offset'])
  self.assertEqual(r['stdout']+s['stdout'],'x'*24000); self.assertEqual(r['stderr']+s['stderr'],'y'*24000); self.assertTrue(s['stdout_eof'] and s['stderr_eof'])
 def test_binary_log_slices_stay_json_strings(self):
  j=self.job('import os;os.write(1,bytes([255,0,195,169]));os.write(2,b"z")'); self.finish(j['job_id'])
  r=self.read(j['job_id'],max_bytes=3); self.assertIsInstance(r['stdout'],str); self.assertIsInstance(r['stderr'],str); self.assertGreater(r['next_stdout_offset'],0)
 def test_stdin_exact_bound_and_cleanup(self):
  j=self.tool('job_start',dict(argv=[sys.executable,'-c','import sys;print(len(sys.stdin.buffer.read()))'],cwd=str(self.root),stdin='x'*131072))
  self.assertEqual(self.finish(j['job_id'])['stdout'],'131072\n'); self.assertFalse((self.state/'jobs'/j['job_id']/'stdin').exists()); self.assertFalse((self.whome/'runs'/j['job_id']/'stdin').exists())
  self.tool('job_start',dict(argv=['/usr/bin/true'],cwd=str(self.root),stdin='x'*131073),success=False)
 def test_prefix_retention_keeps_draining(self):
  j=self.job('import os;os.write(1,b"x"*90000);os.write(2,b"y"*90000)',options=dict(output_limit_bytes=4096)); r=self.finish(j['job_id'])
  self.assertEqual(r['exit_code'],0); self.assertEqual(len(r['stdout']),4096); self.assertEqual(len(r['stderr']),4096); self.assertTrue(r['stdout_truncated'] and r['stderr_truncated'])
 def test_cancel_and_terminal_confirmation(self):
  j=self.job('import time;print("ready",flush=True);time.sleep(20)'); c=self.tool('job_cancel',dict(job_id=j['job_id']))
  self.assertEqual(c['reason'],'stop_requested'); r=self.finish(j['job_id']); self.assertEqual(r['state'],'cancelled'); self.assertTrue(r['stdout_eof'] and r['stderr_eof'])
  self.assertEqual(self.tool('job_cancel',dict(job_id=j['job_id']))['reason'],'already_finished')
 def test_job_timeout(self):
  j=self.job('import time;time.sleep(20)',options=dict(timeout_seconds=1)); self.assertEqual(self.finish(j['job_id'])['state'],'timed_out')
 def test_lost_run_reply_keeps_identity_without_replay(self):
  wrapper=self.wrapper('lost-run'); mark=self.root/'count'
  j=self.job(f'open({str(mark)!r},"a").write("one\\n")',env=dict(self.env,WALKER_BINARY=str(wrapper)))
  self.assertEqual(j['state'],'indeterminate'); self.assertRegex(j['job_id'],r'^[0-9a-f]{32}$'); time.sleep(.15); self.assertEqual(mark.read_text(),'one\n')
  self.assertEqual(self.walker_cli('inspect',j['job_id'])['animal']['state'],'exited')
 def test_known_not_run_failure_removes_local_binding(self):
  wrapper=self.wrapper('known-reject')
  r=self.tool('job_start',dict(argv=['/usr/bin/true'],cwd=str(self.root)),success=False,env=dict(self.env,WALKER_BINARY=str(wrapper)))
  self.assertEqual(r['error'],'WalkerDurabilityUnavailable')
  jobs=self.state/'jobs'; self.assertFalse(jobs.exists() and any(jobs.iterdir()))
 def test_unversioned_mutation_error_is_uncertain_not_authority(self):
  wrapper=self.wrapper('unversioned-reject'); mark=self.root/'must-not-run'
  j=self.job(f'open({str(mark)!r},"w").write("bad")',env=dict(self.env,WALKER_BINARY=str(wrapper)))
  self.assertEqual(j['state'],'indeterminate')
  self.assertTrue((self.state/'jobs'/j['job_id']/'request.json').is_file())
  self.assertFalse(mark.exists())
 def test_valid_old_ping_is_nondurable(self):
  wrapper=self.wrapper('old-ping')
  r=self.tool('job_start',dict(argv=['/usr/bin/true'],cwd=str(self.root)),success=False,env=dict(self.env,WALKER_BINARY=str(wrapper)))
  self.assertEqual(r['error'],'WalkerDurabilityUnavailable')
  jobs=self.state/'jobs'; self.assertFalse(jobs.exists() and any(jobs.iterdir()))
 def test_acknowledged_start_does_not_depend_on_followup_inspection(self):
  j=self.job('print("started")',env=dict(self.env,WALKER_BINARY=str(self.wrapper('lost-inspect'))))
  self.assertEqual(j['state'],'starting'); self.assertRegex(j['job_id'],r'^[0-9a-f]{32}$'); self.assertFalse((self.root/'once').exists())
 def test_saved_binding_cannot_override_new_configuration(self):
  j=self.job('print("complete")'); self.finish(j['job_id'])
  self.tool('job_read',dict(job_id=j['job_id']),success=False,env=dict(self.env,WALKER_BINARY=str(self.root/'missing-new-walker')))
 def test_metadata_cannot_select_an_executable_for_read(self):
  j=self.job('print("complete")'); self.finish(j['job_id']); marker=self.root/'executed'; rogue=self.root/'unselected'
  rogue.write_text('#!'+sys.executable+'\nfrom pathlib import Path\nPath('+repr(str(marker))+').write_text("unsafe")\n'); rogue.chmod(0o700)
  path=self.state/'jobs'/j['job_id']/'request.json'; data=json.loads(path.read_text()); data['walker_ref']['config']['executable']=str(rogue); path.write_text(json.dumps(data))
  self.tool('job_read',dict(job_id=j['job_id']),success=False); self.assertFalse(marker.exists(),'metadata selected executable authority')
 def test_binary_short_command_stays_json_string(self):
  r=self.tool('command',dict(argv=[sys.executable,'-c','import os;os.write(1,bytes([255,0]));os.write(2,bytes([254]))'],cwd=str(self.root)))
  self.assertIsInstance(r['stdout'],str); self.assertIsInstance(r['stderr'],str)
 def test_live_log_snapshot_is_not_final_eof(self):
  j=self.job('import time;print("first",flush=True);time.sleep(10)')
  time.sleep(.05); r=self.read(j['job_id']); self.assertFalse(r['stdout_eof']); self.assertFalse(r['stderr_eof'])
  self.tool('job_cancel',dict(job_id=j['job_id'])); self.finish(j['job_id'])
 def test_namespace_change_does_not_follow_stored_namespace(self):
  j=self.job('print("complete")'); self.finish(j['job_id'])
  self.tool('job_read',dict(job_id=j['job_id']),success=False,env=dict(self.env,WALKER_HOME=str(self.root/'new-namespace')))
 def test_crashed_platform_owner_reconciles_without_stale_pid_control(self):
  j=self.job('import time;time.sleep(20)'); meta=self.walker_cli('inspect',j['job_id'])['animal']
  fd=os.pidfd_open(meta['walker_pid'])
  try: signal.pidfd_send_signal(fd,signal.SIGKILL)
  finally: os.close(fd)
  end=time.monotonic()+6; r=None
  while time.monotonic()<end:
   try: r=self.read(j['job_id'])
   except AssertionError:
    time.sleep(.04); continue
   if r['state'] in TERMINAL: break
   time.sleep(.04)
  self.assertIsNotNone(r); self.assertEqual(r['state'],'failed')
  receipt=self.walker_cli('inspect',j['job_id'])['animal']
  self.assertEqual(receipt['failure'],'SupervisorLost')
  self.assertEqual(receipt['cleanup_phase'],'sealed')
  self.assertEqual(receipt['reconciled_at_ms'],receipt['terminalized_at_ms'])
  self.assertEqual(r['ended_at'],receipt['terminalized_at_ms']//1000)
 def test_login_profile_selects_sdk_in_exact_command_and_job(self):
  sdk=self.root/'sdk'; (sdk/'bin').mkdir(parents=True)
  java=sdk/'bin/java'; java.write_text('#!/usr/bin/bash\nprintf "sdk=%s\\n" "$JAVA_HOME"\n'); java.chmod(0o700)
  (self.root/'.bash_profile').write_text('export JAVA_HOME="$HOME/sdk"\nexport PATH="$JAVA_HOME/bin:/usr/bin:/bin"\n')
  expected='sdk='+str(sdk)+'\n'
  c=self.tool('command',dict(argv=['java'],cwd=str(self.root))); self.assertEqual(c['stdout'],expected)
  j=self.tool('job_start',dict(argv=['java'],cwd=str(self.root),timeout_seconds=5)); self.assertEqual(self.finish(j['job_id'])['stdout'],expected)
 def test_large_invalid_text_remains_bounded_valid_json(self):
  c=self.tool('command',dict(argv=[sys.executable,'-c','import os;os.write(1,b"\\xff"*262145)'],cwd=str(self.root)),timeout=6)
  self.assertIsInstance(c['stdout'],str); self.assertTrue(c['truncated']); self.assertEqual(len(c['stdout']),262144)
 def test_short_shell_does_not_write_human_history(self):
  history=self.root/'.bash_history'; history.write_text('preserve-human-history\n')
  c=self.tool('shell',dict(command='printf test',cwd=str(self.root)),env=dict(self.env,HISTFILE=str(history)))
  self.assertEqual(c['stdout'],'test'); self.assertEqual(history.read_text(),'preserve-human-history\n')
if __name__=='__main__': unittest.main(verbosity=2)
