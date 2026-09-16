#!/usr/bin/env python3
"""Live public-contract tests. Only explicitly selected Walker and private owned fixtures are used."""
import base64, concurrent.futures, ctypes, json, os, pathlib, shutil, signal, subprocess, sys, tempfile, time, unittest
P = pathlib.Path
ROOT = P(__file__).resolve().parents[1]
class Contract(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.walker = P(os.environ["WALKER_BINARY"]).resolve()
        cls.driver = ROOT / "zig-out/bin/walker-contract-driver"
        cls.parent = P(os.environ["WALKER_TEST_ROOT"])
        cls.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if ctypes.CDLL(None, use_errno=True).prctl(36, 1, 0, 0, 0):
            raise RuntimeError("Cannot scope test cleanup to owned descendants")
    def setUp(self):
        self.root = P(tempfile.mkdtemp(prefix="case-", dir=self.parent))
        self.home = self.root / "w"
        self.state = self.root / "s"
        self.env = dict(os.environ, WALKER_HOME=str(self.home))
        self.n = 0
    def tearDown(self):
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
    def call(self, tool, value, ok=True, walker=None, trace=None):
        self.n += 1
        path=self.root/f"request-{self.n}.json"; path.write_text(json.dumps(value)); path.chmod(0o600)
        argv=[str(self.driver),str(walker or self.walker),str(self.home),str(self.state),tool,str(path)]
        if trace: argv=["strace","-f","-e","trace=execve","-o",str(trace),*argv]
        result=subprocess.run(argv,env=self.env,capture_output=True,timeout=25)
        self.assertEqual(result.returncode == 0,ok,(result.stdout[:500],result.stderr[:500]))
        self.assertFalse(result.stderr if ok else result.stdout)
        return json.loads(result.stdout if ok else result.stderr)
    def start(self, code, **kwargs):
        return self.call("job_start",dict(argv=[sys.executable,"-c",code],cwd=str(self.root),timeout_seconds=5,**kwargs))
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
        r=self.call("job_read",dict(job_id=r["job_id"])); self.assertEqual(r["stdout"],"hello\n")
        self.assertTrue(r["stdout_eof"] and r["stderr_eof"])
        self.assertNotIn("unit",r); self.assertNotIn("systemd_properties",r)
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
        self.assertEqual(r["error"],"WalkerUnavailable"); self.assertFalse(self.home.exists())
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
r=subprocess.run([{str(self.walker)!r},*sys.argv[1:]],stdout=subprocess.DEVNULL)
if sys.argv[1]=="run":
 print(json.dumps(dict(ok=False,error="SubmissionUncertain")),file=sys.stderr)
 sys.exit(1)
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
    def test_crash_ownership_never_falls_back(self):
        r=self.start("import time; time.sleep(20)")
        v=json.loads(subprocess.check_output([str(self.walker),"inspect",r["job_id"]],env=self.env))["animal"]
        os.kill(v["walker_pid"],signal.SIGKILL); time.sleep(.1)
        self.assertEqual(self.call("job_read",dict(job_id=r["job_id"]))["state"],"indeterminate")
        self.assertEqual(self.call("job_cancel",dict(job_id=r["job_id"]),ok=False)["error"],"WalkerOwnershipUnavailable")
if __name__ == "__main__": unittest.main(verbosity=2)
