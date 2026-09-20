#!/usr/bin/env python3
"""Live public-contract tests. Only explicitly selected Walker and private owned fixtures are used."""
import base64, concurrent.futures, ctypes, json, os, pathlib, shutil, signal, subprocess, sys, tempfile, time, unittest
P = pathlib.Path
ROOT = P(__file__).resolve().parents[1]
class Contract(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.walker = P(os.environ["WALKER_BINARY"]).resolve()
        cls.home = P(os.environ["WALKER_HOME"]).resolve()
        cls.driver = ROOT / "zig-out/bin/walker-contract-driver"
        cls.observer = ROOT / "zig-out/bin/walker-observer-driver"
        cls.parent = P(os.environ["WALKER_TEST_ROOT"])
        cls.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0):
            raise RuntimeError("Cannot scope test cleanup to owned descendants")
        if not cls.observer.is_file():
            raise RuntimeError("missing observer driver")
        ping=json.loads(subprocess.check_output([str(cls.walker),"ping"],env=dict(os.environ,WALKER_HOME=str(cls.home))))
        if ping.get("schema")!="walker/v5" or not ping.get("durable_workloads_v1") or ping.get("delegated_cgroup_v2_admission")!="ready":
            raise RuntimeError("Walker fixture is not v5 durable/ready")
    def setUp(self):
        self.root = P(tempfile.mkdtemp(prefix="case-", dir=self.parent))
        self.home = self.__class__.home
        self.state = self.root / "s"
        self.env = dict(os.environ, WALKER_HOME=str(self.home))
        self.n = 0
        self.run_ids = []
    def tearDown(self):
        # Remove only exact receipts created by this case from the shared
        # platform-owned Walker store, keeping finite history independent of
        # test order/repetition.
        for run_id in reversed(self.run_ids):
            deadline=time.monotonic()+5
            while time.monotonic()<deadline:
                seen=subprocess.run([str(self.walker),"inspect",run_id],env=self.env,capture_output=True)
                if seen.returncode:
                    break
                try: meta=json.loads(seen.stdout)["animal"]
                except (json.JSONDecodeError,KeyError): break
                if meta["state"] not in ("exited","stopped","timed_out","failed"):
                    subprocess.run([str(self.walker),"stop",run_id],env=self.env,capture_output=True)
                    time.sleep(.04); continue
                removed=subprocess.run([str(self.walker),"rm",run_id],env=self.env,capture_output=True)
                if removed.returncode==0: break
                time.sleep(.04)
        # Reap only children acquired by this test process; never broad executable-name cleanup.
        for _ in range(10):
            children=[]
            for path in P("/proc").glob("[0-9]*/stat"):
                try:
                    fields=path.read_text().rsplit(") ",1)[1].split()
                    if int(fields[1]) == os.getpid(): children.append(int(path.parent.name))
                except (FileNotFoundError, ProcessLookupError): pass
            if not children: break
            for pid in children:
                try:
                    fd=os.pidfd_open(pid)
                    try: signal.pidfd_send_signal(fd, signal.SIGKILL)
                    finally: os.close(fd)
                except ProcessLookupError: pass
            time.sleep(.03)
            while True:
                try:
                    if os.waitpid(-1,os.WNOHANG)[0] == 0: break
                except ChildProcessError: break
        shutil.rmtree(self.root)
    def call(self, tool, value, ok=True, walker=None, home=None, trace=None, env=None):
        self.n += 1
        path=self.root/f"request-{self.n}.json"; path.write_text(json.dumps(value)); path.chmod(0o600)
        argv=[str(self.driver),str(walker or self.walker),str(home or self.home),str(self.state),tool,str(path)]
        if trace: argv=["strace","-f","-e","trace=execve","-o",str(trace),*argv]
        result=subprocess.run(argv,env=env or self.env,capture_output=True,timeout=25)
        self.assertEqual(result.returncode == 0,ok,(result.stdout[:500],result.stderr[:500]))
        self.assertFalse(result.stderr if ok else result.stdout)
        reply=json.loads(result.stdout if ok else result.stderr)
        if ok and tool=="job_start" and "job_id" in reply and reply["job_id"] not in self.run_ids:
            self.run_ids.append(reply["job_id"])
        return reply
    def start(self, code, **kwargs):
        return self.call("job_start",dict(argv=[sys.executable,"-c",code],cwd=str(self.root),timeout_seconds=5,**kwargs))
    def observe(self, op, run_id=None):
        argv=[str(self.observer),str(self.walker),str(self.home),op]
        if run_id: argv.append(run_id)
        result=subprocess.run(argv,env=self.env,capture_output=True,timeout=10)
        self.assertEqual(result.returncode,0,(result.stdout[:500],result.stderr[:500]))
        self.assertFalse(result.stderr)
        return json.loads(result.stdout)
    def done(self, id, timeout=10):
        end=time.monotonic()+timeout
        while time.monotonic()<end:
            r=self.call("job_read",dict(job_id=id))
            if r["state"] in ("exited","failed","timed_out","cancelled"): return r
            time.sleep(.04)
        self.fail(f"no terminal receipt: {id}")
    def test_launch_read_shared_identity_and_offline_logs(self):
        r=self.start("print('hello',flush=True); import sys; print('error',file=sys.stderr); sys.exit(7)")
        end=self.done(r["job_id"]); self.assertEqual(end["exit_code"],7)
        time.sleep(.5)
        view=json.loads(subprocess.check_output([str(self.walker),"inspect",r["job_id"]],env=self.env))["animal"]
        self.assertEqual(view["name"],"canary-"+r["job_id"])
        self.assertEqual(end["ended_at"],view["terminalized_at_ms"]//1000)
        self.assertIsNone(view["reconciled_at_ms"])
        r=self.call("job_read",dict(job_id=r["job_id"])); self.assertEqual(r["stdout"],"hello\n")
        self.assertTrue(r["stdout_eof"] and r["stderr_eof"])
        self.assertNotIn("unit",r); self.assertNotIn("systemd_properties",r)
    def test_operator_observation_surface_is_v5_typed(self):
        r=self.start("import time; print('observe',flush=True); time.sleep(2)")
        run_id=r["job_id"]
        rows=self.observe("inventory")
        row=next((item for item in rows if item["run_id"]==run_id),None)
        self.assertIsNotNone(row)
        self.assertEqual(row["name"],"canary-"+run_id)
        detail=self.observe("inspect",run_id)
        self.assertEqual(detail["schema"],"walker.run/v5")
        self.assertEqual(detail["run_id"],run_id)
        self.assertIn("terminalized_at_ms",detail)
        self.assertIn("reconciled_at_ms",detail)
        logs=self.observe("logs",run_id)
        self.assertEqual(logs["schema"],"walker/v5")
        self.assertEqual(logs["run_id"],run_id)
        self.assertIn("observe",logs["stdout"]["data"])
        stats=self.observe("stats",run_id)
        self.assertEqual(stats["run_id"],run_id)
        self.assertIn(stats["sampling"],("observed","unavailable","budget_exhausted"))
        self.call("job_cancel",dict(job_id=run_id))
        self.done(run_id)
        terminal=self.observe("inspect",run_id)
        self.assertIsInstance(terminal["terminalized_at_ms"],int)
        self.assertIsNone(terminal["reconciled_at_ms"])
    def test_no_systemd_or_container_exec_and_local_shell(self):
        trace=self.root/"exec.log"
        r=self.call("job_start",dict(argv=["/usr/bin/printf","host-local"],cwd=str(self.root)),trace=trace)
        self.assertEqual(self.done(r["job_id"])["stdout"],"host-local")
        executions=trace.read_text()
        for executable in ("/usr/bin/systemctl", "/usr/bin/systemd-run", "/usr/bin/podman", "/usr/bin/docker"):
            self.assertNotIn('execve("'+executable+'"',executions)
        r=self.call("shell",dict(command="printf local-shell",cwd=str(self.root)))
        self.assertEqual(r["stdout"],"local-shell")
    def test_missing_walker_fails_without_fallback(self):
        r=self.call("job_start",dict(argv=["/usr/bin/true"],cwd=str(self.root)),ok=False,walker=self.root/"missing")
        self.assertEqual(r["error"],"WalkerUnavailable"); self.assertFalse((self.state/"jobs").exists())
    def test_nondurable_walker_rejected_before_job_state(self):
        other=self.root/"nondurable-home"
        other_env=dict(self.env,WALKER_HOME=str(other))
        direct=json.loads(subprocess.check_output([
            str(self.walker),"run","--name","nondurable-anchor","--cwd",str(self.root),"--","/usr/bin/sleep","2"
        ],env=other_env))
        try:
            ping=json.loads(subprocess.check_output([str(self.walker),"ping"],env=other_env))
            self.assertFalse(ping["durable_workloads_v1"])
            r=self.call(
                "job_start",
                dict(argv=["/usr/bin/true"],cwd=str(self.root)),
                ok=False,
                walker=self.walker,
                home=other,
            )
            self.assertEqual(r["error"],"WalkerDurabilityUnavailable")
            self.assertFalse((self.state/"jobs").exists())
        finally:
            subprocess.run([str(self.walker),"stop",direct["run_id"]],env=other_env,capture_output=True)
    def test_timeout_after_closed_output(self):
        r=self.call("job_start",dict(argv=["/bin/sh","-c","exec 1>&- 2>&-; sleep 20"],cwd=str(self.root),timeout_seconds=1))
        self.assertEqual(self.done(r["job_id"])["state"],"timed_out")
    def test_cancel_then_read_and_repeat_cancel(self):
        r=self.start("import time; print('ready',flush=True); time.sleep(20)")
        c=self.call("job_cancel",dict(job_id=r["job_id"])); self.assertTrue(c["cancelled"])
        self.assertEqual(self.done(r["job_id"])["state"],"cancelled")
        self.assertEqual(self.call("job_cancel",dict(job_id=r["job_id"]))["reason"],"already_finished")
    def test_prefix_limit_and_two_byte_read_budget(self):
        r=self.start("import os; os.write(1,b'o'*10000); os.write(2,b'e'*10000)",output_limit_bytes=4096)
        d=self.done(r["job_id"]); self.assertTrue(d["stdout_truncated"] and d["stderr_truncated"])
        d=self.call("job_read",dict(job_id=r["job_id"],max_bytes=2))
        self.assertEqual((d["stdout"],d["stderr"],d["next_stdout_offset"],d["next_stderr_offset"]),("o","e",1,1))
        self.call("job_read",dict(job_id=r["job_id"],stdout_offset=4097),ok=False)
    def test_maximum_output_cap_is_admitted_without_preallocation(self):
        r=self.start("print('small')",output_limit_bytes=512*1024*1024)
        self.assertEqual(self.done(r["job_id"])["output_limit_bytes"],512*1024*1024)
    def test_full_stdin_and_environment_not_retained(self):
        r=self.start("import sys; print(len(sys.stdin.buffer.read()))",stdin="z"*(128*1024))
        self.assertEqual(self.done(r["job_id"])["stdout"],"131072\n")
        self.assertFalse((self.state/"jobs"/r["job_id"]/"stdin").exists())
        self.assertFalse((self.home/"runs"/r["job_id"]/"stdin").exists())
    def test_full_argument_count_and_wrapper_overhead(self):
        args=["/usr/bin/printf","%s",*("x" for _ in range(254))]
        r=self.call("job_start",dict(argv=args,cwd=str(self.root)))
        self.assertEqual(self.done(r["job_id"])["stdout"],"x"*254)
    def test_systemd_options_are_not_admitted(self):
        r=self.call("job_start",dict(argv=["/usr/bin/true"],cwd=str(self.root),systemd_properties=[]),ok=False)
        self.assertEqual(r["error"],"InvalidArguments")
    def test_lost_launch_ack_retains_id_and_does_not_replay(self):
        shim=self.root/"lost-ack"
        shim.write_text(f"""#!/usr/bin/python3
import json,subprocess,sys
r=subprocess.run([{str(self.walker)!r},*sys.argv[1:]],capture_output=True)
if sys.argv[1]=="run":
 if r.returncode:
  sys.stdout.buffer.write(r.stdout); sys.stderr.buffer.write(r.stderr); sys.exit(r.returncode)
 print(json.dumps(dict(schema="walker/v5",ok=False,error="SubmissionUncertain")),file=sys.stderr)
 sys.exit(1)
sys.stdout.buffer.write(r.stdout); sys.stderr.buffer.write(r.stderr)
sys.exit(r.returncode)
""")
        shim.chmod(0o700)
        r=self.call("job_start",dict(argv=["/bin/sh","-c","printf x >> count"],cwd=str(self.root)),walker=shim)
        self.assertEqual(r["state"],"indeterminate")
        id=r["job_id"]
        self.assertTrue((self.state/"jobs"/id/"request.json").exists())
        time.sleep(.2)
        self.assertEqual((self.root/"count").read_text(),"x")
        view=json.loads(subprocess.check_output([str(self.walker),"inspect",id],env=self.env))["animal"]
        self.assertEqual(view["run_id"],id)
    def test_known_not_run_readiness_race_drops_local_binding(self):
        shim=self.root/"known-not-run"
        shim.write_text(f"""#!/usr/bin/python3
import json,os,sys
if sys.argv[1]=="run":
 print(json.dumps(dict(schema="walker/v5",ok=False,error="DelegatedContainmentUnavailable")),file=sys.stderr)
 sys.exit(1)
os.execv({str(self.walker)!r},[{str(self.walker)!r},*sys.argv[1:]])
""")
        shim.chmod(0o700)
        r=self.call("job_start",dict(argv=["/usr/bin/true"],cwd=str(self.root)),ok=False,walker=shim)
        self.assertEqual(r["error"],"WalkerDurabilityUnavailable")
        jobs=self.state/"jobs"
        self.assertFalse(jobs.exists() and any(jobs.iterdir()),"known-not-run failure retained unreachable job binding")
    def test_unversioned_mutation_failure_is_submission_uncertain(self):
        shim=self.root/"unversioned-run-error"
        shim.write_text(f"""#!/usr/bin/python3
import json,os,sys
if sys.argv[1]=="run":
 print(json.dumps(dict(ok=False,error="DelegatedContainmentUnavailable")),file=sys.stderr)
 sys.exit(1)
os.execv({str(self.walker)!r},[{str(self.walker)!r},*sys.argv[1:]])
""")
        shim.chmod(0o700)
        r=self.call("job_start",dict(argv=["/bin/sh","-c","printf SHOULD_NOT_RUN >> marker"],cwd=str(self.root)),walker=shim)
        self.assertEqual(r["state"],"indeterminate")
        self.assertTrue((self.state/"jobs"/r["job_id"]/"request.json").is_file())
        self.assertFalse((self.root/"marker").exists())
    def test_valid_older_walker_ping_is_nondurable_not_malformed(self):
        shim=self.root/"walker-v4-ping"
        shim.write_text(f"""#!/usr/bin/python3
import json,os,sys
if sys.argv[1]=="ping":
 print(json.dumps(dict(schema="walker/v4",ok=True,version=4)))
 sys.exit(0)
os.execv({str(self.walker)!r},[{str(self.walker)!r},*sys.argv[1:]])
""")
        shim.chmod(0o700)
        r=self.call("job_start",dict(argv=["/usr/bin/true"],cwd=str(self.root)),ok=False,walker=shim)
        self.assertEqual(r["error"],"WalkerDurabilityUnavailable")
        jobs=self.state/"jobs"
        self.assertFalse(jobs.exists() and any(jobs.iterdir()))
    def test_platform_owner_crash_reconciles_without_fallback(self):
        r=self.start("import time; time.sleep(20)")
        v=json.loads(subprocess.check_output([str(self.walker),"inspect",r["job_id"]],env=self.env))["animal"]
        os.kill(v["walker_pid"],signal.SIGKILL); time.sleep(.1)
        end=time.monotonic()+6
        while time.monotonic()<end:
            try: view=self.call("job_read",dict(job_id=r["job_id"]))
            except AssertionError:
                time.sleep(.04); continue
            if view["state"] in ("failed","exited","cancelled","timed_out"): break
            time.sleep(.04)
        else: self.fail("successor did not reconcile crashed Walker job")
        self.assertEqual(view["state"],"failed")
        receipt=json.loads(subprocess.check_output([str(self.walker),"inspect",r["job_id"]],env=self.env))["animal"]
        self.assertEqual(receipt["failure"],"SupervisorLost")
        self.assertEqual(receipt["cleanup_phase"],"sealed")
        self.assertEqual(receipt["reconciled_at_ms"],receipt["terminalized_at_ms"])
        self.assertEqual(view["ended_at"],receipt["terminalized_at_ms"]//1000)
        detail=self.observe("inspect",r["job_id"])
        self.assertEqual(detail["terminalized_at_ms"],receipt["terminalized_at_ms"])
        self.assertEqual(detail["reconciled_at_ms"],receipt["reconciled_at_ms"])
    def test_reconciled_prefix_unknown_truncation_stays_null(self):
        r=self.start("import os,time; os.write(1,b'x'*10000); time.sleep(20)",output_limit_bytes=4096)
        end=time.monotonic()+3
        while time.monotonic()<end:
            v=json.loads(subprocess.check_output([str(self.walker),"inspect",r["job_id"]],env=self.env))["animal"]
            if v["stdout_discarded_bytes"] and v["stdout_discarded_bytes"]>0: break
            time.sleep(.02)
        else: self.fail("prefix fixture never exceeded retention cap")
        os.kill(v["walker_pid"],signal.SIGKILL)
        end=time.monotonic()+6
        while time.monotonic()<end:
            try: view=self.call("job_read",dict(job_id=r["job_id"]))
            except AssertionError:
                time.sleep(.04); continue
            if view["state"]=="failed": break
            time.sleep(.04)
        else: self.fail("prefix fixture was not reconciled")
        self.assertIsNone(view["stdout_truncated"])
        self.assertFalse(view["stderr_truncated"])
if __name__ == "__main__": unittest.main(verbosity=2)
