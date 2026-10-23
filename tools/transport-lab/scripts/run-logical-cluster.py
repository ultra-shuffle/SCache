#!/usr/bin/env python3
"""Isolated two-logical-node simulation on 223. Never connects real UB hardware."""
import argparse, concurrent.futures, fcntl, hashlib, json, os, pathlib, platform, shutil, signal, socket
import subprocess, sys, tempfile, time, traceback

R = pathlib.Path(__file__).resolve().parents[1]
PKG = R if (R/"runtime/spark").is_dir() else R/"runtime/package"
UMQ_LIB = PKG/"dependencies/umq/lib"
NATIVE = UMQ_LIB/"libScacheUmq.so"
MKDEV = PKG/"tools/umq-sim/mkdev.sh"
SHIM = UMQ_LIB/"openurma_shim.so"
JAVA = PKG / "dependencies/jdk/bin/java"
SPARK = PKG / "runtime/spark"
SCACHE = PKG / "runtime/scache/SCache-assembly-0.1.0-SNAPSHOT.jar"
TEST = PKG / "runtime/scache/SCache-ub-integration-test.jar"
IPS = {"A": "127.0.0.1", "B": "127.0.0.2"}
MAIN = "org.scache.deploy.NettyBlockTransferIntegrationNode"
LOGGING = PKG/"tools/umq-sim/log4j.properties"

def require(ok, message):
    if not ok: raise RuntimeError(message)

def clean_env():
    env = dict(os.environ)
    for key in list(env):
        if key.startswith(("OPENURMA_", "SCACHE_UMQ_", "SCACHE_URPC_")) or key in {
            "JAVA_TOOL_OPTIONS", "_JAVA_OPTIONS", "JDK_JAVA_OPTIONS", "CLASSPATH", "LD_PRELOAD",
            "LD_LIBRARY_PATH", "SPARK_CONF_DIR", "SPARK_ENV_LOADED", "SPARK_SUBMIT_OPTS",
            "SPARK_DAEMON_JAVA_OPTS", "SPARK_LOCAL_IP", "SCACHE_HOME", "SPARK_SCACHE_HOME",
            "SCACHE_CLIENT_HOST", "SCACHE_CLIENT_PORT"}:
            env.pop(key, None)
    env.update(JAVA_HOME=str(JAVA.parents[1]), SPARK_HOME=str(SPARK),
               PATH=str(JAVA.parent)+":/usr/bin:/bin")
    return env

class Lab:
    def __init__(self, stage):
        self.stage = stage
        (R/"logs").mkdir(exist_ok=True)
        self.run = pathlib.Path(tempfile.mkdtemp(prefix="csm-", dir=R/"logs"))
        self.socket_root=pathlib.Path(tempfile.mkdtemp(prefix="scache-umq-"))
        self.processes = {}
        self.env = clean_env()
        self.record = {"stage": stage, "topology": "two-logical-nodes-one-Linux-kernel",
                       "hardware_verified": False, "official_umq": True, "mode": "rm-ctp", "umdk_reference": "099343116b63aad37e2f60f99b71e772bf6bf009",
                       "run": str(self.run), "socket_root":str(self.socket_root), "applications": [], "result": "RUNNING"}
        print("RUN="+str(self.run), flush=True)
        self.save()
    def save(self):
        (self.run/"REPORT.json").write_text(json.dumps(self.record, indent=2))
    def logpath(self, name): return self.run/(name+".log")
    def start(self, name, cmd, env=None):
        out = self.logpath(name).open("w")
        p = subprocess.Popen(list(map(str, cmd)), env=env or self.env, stdout=out,
                             stderr=subprocess.STDOUT, start_new_session=True)
        out.close()
        self.processes[name] = p
        (self.run/(name+".command.json")).write_text(json.dumps(list(map(str,cmd))))
        (self.run/(name+".pid")).write_text(str(p.pid))
        return p
    def ready(self, name, marker, seconds=90):
        end=time.monotonic()+seconds
        while time.monotonic()<end:
            p=self.processes[name]
            require(p.poll() is None, name+" exited: "+self.logpath(name).read_text()[-6000:])
            if marker in self.logpath(name).read_text(errors="replace"): return
            time.sleep(.2)
        raise RuntimeError("startup deadline: "+name)
    def command(self, ip, text):
        with socket.create_connection((ip,29072),timeout=60) as s:
            s.sendall((text+"\n").encode())
            f=s.makefile("rb")
            result=f.readline(100000).decode().strip()
        require(result.startswith("OK "), "control rejected: "+result)
        return result
    def metrics(self, label):
        data={}
        for node, ip in IPS.items():
            reply=self.command(ip,"METRICS")
            data[node]={k:int(v) for k,v in (x.split("=",1) for x in reply[3:].split())}
        (self.run/(label+"-metrics.json")).write_text(json.dumps(data,indent=2))
        return data
    def configure(self, home, text):
        (home/"conf").mkdir(parents=True,exist_ok=True)
        (home/"tmp").mkdir(exist_ok=True)
        (home/"conf/scache.conf").write_text(text)
        shutil.copy2(LOGGING,home/"conf/log4j.properties")
    def install(self):
        for p in [JAVA,NATIVE,SHIM,MKDEV,LOGGING]:require(p.is_file(),"missing precompiled/staged dependency: "+str(p))
        if not (R/"repos/SCache").is_dir():
            manifest=json.loads((PKG/"metadata/umq-manifest.json").read_text())
            require(platform.machine() in {"aarch64","arm64"},"This is an aarch64 simulation package")
            libc=os.confstr("CS_GNU_LIBC_VERSION").split()[-1]
            require(tuple(map(int,libc.split("."))) >= tuple(map(int,manifest["minimum_glibc"].split("."))),
                    "glibc "+manifest["minimum_glibc"]+" or newer required; do not overwrite system glibc")
            require(shutil.disk_usage(R).free>=10*1024**3,"At least 10 GiB free workspace recommended/required")
            print("UMQ_PACKAGE_PREFLIGHT glibc="+libc+" minimum="+manifest["minimum_glibc"],flush=True)
            for name,digest in manifest["artifacts"].items():
                require(hashlib.sha256((PKG/name).read_bytes()).hexdigest()==digest,"artifact mismatch "+name)
            self.record["artifacts"]=manifest["artifacts"];self.save();return
        for src,dst in [
            (R/"repos/SCache/target/scala-2.13/SCache-assembly-0.1.0-SNAPSHOT.jar",SCACHE),
            (R/"repos/SCache/target/scala-2.13/SCache-integration-0.1.0-SNAPSHOT.jar",TEST)]:
            require(src.stat().st_size>0,"empty artifact "+str(src)); shutil.copy2(src,dst)
        self.record["artifacts"]={str(p.relative_to(R)):hashlib.sha256(p.read_bytes()).hexdigest()
                                 for p in [SCACHE,TEST,NATIVE]}
        self.save()
    def launch(self):
        for ip in IPS.values():
            for port in [29071,29072,29073]:
                with socket.socket() as s:
                    s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
                    s.bind((ip,port))
        for port in [29070,29777,29880,29181,29182,29881,29882]:
            with socket.socket() as s:
                s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
                s.bind(("127.0.0.1",port))
        master=self.run/"master"
        self.configure(master,'scache.master.ip="127.0.0.1"\nscache.master.port=29070\nscache.ub.pool.enabled=true\n')
        env=dict(self.env,SCACHE_HOME=str(master))
        self.start("scache-master",[JAVA,"-Xmx1g","-cp",SCACHE,
            "org.scache.deploy.ScacheMaster","--ip",IPS["A"],"--port",29070,"--local","false"],env)
        self.ready("scache-master","Registering MapOutputTracker")
        libs=str(UMQ_LIB)
        for i,(node,ip) in enumerate(IPS.items()):
            home=self.run/node/"scache"
            self.configure(home, f"""
scache.master.ip="127.0.0.1"
scache.master.port=29070
scache.client.host="{ip}"
scache.client.port=29071
scache.storage.network.enabled=true
scache.storage.cxl.shared.enabled=false
scache.blockTransfer.backend="ub"
scache.ub.transport="umq"
scache.ub.mode="rm-ctp"
scache.ub.device="openurma{i}"
scache.ub.eidIndex=0
scache.ub.priority=6
scache.ub.pool.enabled=true
scache.ub.pool.path="{home}/ub-pool"
scache.ub.pool.forceOnCommit=false
scache.ub.checksum.enabled=true
scache.ub.diagnostics.level="summary"
scache.daemon.ipc.backend="pool"
scache.daemon.ipc.pool.path="{home}/ipc.pool"
scache.daemon.ipc.pool.size="128m"
scache.daemon.ipc.pool.mapChunk="64m"
scache.daemon.ipc.pool.align=4096
scache.daemon.ipc.pool.zeroCopy.put=true
""")
            root=self.run/("root-"+node)
            subprocess.run(["bash",str(MKDEV),
                            "openurma"+str(i),"fe80::"+str(i+1),str(root)],check=True,env=self.env)
            native_dir=self.socket_root/("u"+node);native_dir.mkdir()
            env=dict(self.env,SCACHE_HOME=str(home),LD_LIBRARY_PATH=libs,
                LD_PRELOAD=str(SHIM),
                OPENURMA_FAKE_ROOT=str(root),OPENURMA_WIRE_PATH=str(self.socket_root/"wire"),
                OPENURMA_WIRE_ROLE="listen" if i==0 else "connect",OPENURMA_PROVIDER_LOG="0",
                SCACHE_UMQ_LIBRARY=str(NATIVE))
            self.start("scache-"+node,[JAVA,"-Xmx1g","--add-opens=java.base/java.nio=ALL-UNNAMED",
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED","-cp",str(SCACHE)+":"+str(TEST),MAIN,
                "--name","umq-"+node,"--host",ip,"--rpc-port",29071,"--control-port",29072,
                "--master-host",IPS["A"],"--master-port",29070,"--backend","ub","--ub-transport","umq",
                "--ub-mode","rm-ctp","--ub-device","openurma"+str(i),"--eid-index",0,"--priority",6,
                "--ub-control-port",29073,"--ub-pool-path",home/"ub-pool","--ipc-pool-path",home/"ipc.pool",
                "--arena-bytes",67108864,"--arena-count",8,"--queue-depth",64,
                "--chunk-size",10485760,"--timeout-ms",60000],env)
            self.ready("scache-"+node,"SCACHE_TEST_NODE_READY")
            maps=pathlib.Path("/proc")/str(self.processes["scache-"+node].pid)/"maps"
            mapped=maps.read_text();(self.run/("scache-"+node+"-maps.txt")).write_text(mapped)
            for library in ["libScacheUmq.so","libumq.so", "libumq_ub.so", "liburma.so","liburma_openurma.so"]:
                require(library in mapped,"library not loaded: "+library)
        self.start("spark-master",[SPARK/"bin/spark-class","org.apache.spark.deploy.master.Master",
            "--host",IPS["A"],"--port",29777,"--webui-port",29880],dict(self.env,SPARK_LOCAL_IP=IPS["A"]))
        self.ready("spark-master","Starting Spark master")
        for i,(node,ip) in enumerate(IPS.items()):
            home=self.run/node/"scache";(home/"jars").mkdir()
            shutil.copy2(SCACHE,home/"jars"/SCACHE.name)
            env=dict(self.env,SPARK_LOCAL_IP=ip,SCACHE_HOME=str(home),SPARK_SCACHE_HOME=str(home),
                     SCACHE_CLIENT_HOST=ip,SCACHE_CLIENT_PORT="29071")
            self.start("worker-"+node,[SPARK/"bin/spark-class","org.apache.spark.deploy.worker.Worker",
                "--host",ip,"--port",29181+i,"--webui-port",29881+i,"--cores",2,"--memory","3g",
                "--work-dir",self.run/node/"worker","spark://127.0.0.1:29777"],env)
            self.ready("worker-"+node,"Successfully registered with master")
        print("UMQ_LOGICAL_CLUSTER_READY",flush=True)
    def application(self, kind, name):
        label=kind+"-"+name; events=self.run/"events"/label;events.mkdir(parents=True)
        bench=self.run/"data"/kind
        env=dict(self.env,SPARK_LOCAL_IP=IPS["A"],SCACHE_HOME=str(self.run/"A/scache"),
            SPARK_SCACHE_HOME=str(self.run/"A/scache"),SCACHE_CLIENT_HOST=IPS["A"],SCACHE_CLIENT_PORT="29071",
            SPARKBENCH_PROPERTIES_FILES=str(bench/"hibench.properties"))
        conf={
            "spark.cores.max":"4","spark.executor.cores":"2","spark.executor.memory":"2g",
            "spark.driver.host":IPS["A"],"spark.driver.bindAddress":IPS["A"],
            "spark.driver.port":"29778","spark.driver.blockManager.port":"29779","spark.blockManager.port":"29780",
            "spark.port.maxRetries":"0","spark.default.parallelism":"4","spark.sql.shuffle.partitions":"4",
            "spark.sql.adaptive.enabled":"false","spark.scheduler.minRegisteredResourcesRatio":"1.0",
            "spark.scheduler.maxRegisteredResourcesWaitingTime":"60s","spark.serializer":"org.apache.spark.serializer.JavaSerializer",
            "spark.eventLog.enabled":"true","spark.eventLog.compress":"false","spark.eventLog.rolling.enabled":"false",
            "spark.eventLog.dir":"file://"+str(events),
            "spark.driver.extraJavaOptions":"--add-opens=java.base/java.nio=ALL-UNNAMED",
            "spark.executor.extraJavaOptions":"--add-opens=java.base/java.nio=ALL-UNNAMED",
            "spark.executorEnv.SPARKBENCH_PROPERTIES_FILES":str(bench/"hibench.properties")}
        jars=str(PKG/"benchmarks/hibench/hadoop-mapreduce-examples-3.3.4.jar")
        if kind=="umq":
            jars+=","+str(SCACHE)
            conf.update({"spark.scache.enable":"true","spark.scache.strict":"true",
                "spark.scache.productionFallback.enabled":"false","spark.scache.shuffle.noLocalFiles":"true",
                "spark.scache.blockTransfer.backend":"ub","spark.scache.ub.pool.enabled":"true",
                "spark.shuffle.useOldFetchProtocol":"true"})
        else:conf.update({"spark.scache.enable":"false","spark.shuffle.manager":"sort"})
        cmd=[SPARK/"bin/spark-submit","--master","spark://127.0.0.1:29777",
             "--deploy-mode","client","--driver-memory","3g","--jars",jars]
        for key,value in conf.items():cmd+=["--conf",key+"="+value]
        if name=="smoke":cmd+=["--class","StandardSparkShuffleSmoke",PKG/"benchmarks/tools/standard-spark-smoke.jar"]
        elif name=="tpcds":
            (bench/"tpcds").mkdir()
            cmd+=["--class","BenchmarkChecks",PKG/"benchmarks/tools/benchmark-checks.jar",
                  "tpcds",PKG,bench/"tpcds"]
        else:
            classes={"wordcount":"ScalaWordCount","sort":"ScalaSort","terasort":"ScalaTeraSort","repartition":"ScalaRepartition"}
            inp=bench/("input-tera" if name in {"terasort","repartition"} else "input")
            if name in {"terasort","repartition"}:
                cmd+=["--conf","spark.hadoop.mapreduce.input.fileinputformat.split.maxsize=65536"]
            cmd+=["--class","com.intel.hibench.sparkbench.micro."+classes[name],
                  PKG/"benchmarks/hibench/sparkbench-assembly-7.1.1-dist.jar","file://"+str(inp),"file://"+str(bench/name)]
            if name=="repartition":cmd+=["false","false"]
        before=self.metrics(label+"-before") if kind=="umq" else None
        start=time.time();print("APP_START "+label,flush=True)
        p=self.start(label,cmd,env);rc=p.wait(timeout=3600)
        text=self.logpath(label).read_text(errors="replace")
        require(rc==0,"application failed: "+label+"\n"+text[-6000:])
        require("SCACHE_FALLBACK" not in text,"fallback in driver")
        if name=="smoke":require("STANDARD_SPARK_357_SMOKE_PASS" in text,"smoke result missing")
        if name=="tpcds":require("TPCDS_PASS files=99 statements=103" in text,"TPCDS not complete")
        if kind=="umq":
            after=self.metrics(label+"-after")
            delta=sum(after[n]["umq.readBytes"]-before[n]["umq.readBytes"] for n in IPS)
            require(delta>0,"no remote UMQ payload for "+label)
            for n,m in after.items():
                for key in ["umq.rpcFailed","urma.fallbacks","urma.tcpPayloadBytes","urma.nettyPayloadBytes","urma.checksumErrors"]:
                    require(m[key]==0,n+" bad metric "+key+"="+str(m[key]))
            print("APP_UMQ_EVIDENCE "+label+" remote_read_bytes="+str(delta),flush=True)
        else:delta=0
        self.record["applications"].append({"name":label,"exit":rc,"seconds":round(time.time()-start,3),
                                            "remote_umq_bytes":delta})
        self.save();print("APP_PASS "+label,flush=True)
    def workloads(self):
        tool=PKG/"benchmarks/tools/benchmark-checks.jar"
        cp=str(tool)+":"+str(SPARK/"jars/*")
        for kind in ["standard","umq"]:
            bench=self.run/"data"/kind;bench.mkdir(parents=True)
            subprocess.run([JAVA,"-cp",cp,"BenchmarkChecks","prepare",bench],check=True,env=self.env,
                           stdout=self.logpath("prepare-"+kind).open("w"),stderr=subprocess.STDOUT)
        self.application("umq","smoke")
        if self.stage=="smoke":return
        for kind in ["standard","umq"]:
            for name in ["wordcount","sort","terasort","repartition","tpcds"]:self.application(kind,name)
            subprocess.run([JAVA,"-cp",cp,"BenchmarkChecks","check",self.run/"data"/kind],
                           check=True,env=self.env,stdout=self.logpath("check-"+kind).open("w"),stderr=subprocess.STDOUT)
        ref=self.run/"data/standard/tpcds";actual=self.run/"data/umq/tpcds"
        files=list(ref.glob("*-results.jsonl"))
        require(len(files)==103,"reference results missing")
        for f in files:
            for name in [f.name,f.name.replace("-results.jsonl","-schema.json")]:
                require((ref/name).read_bytes()==(actual/name).read_bytes(),"TPCDS mismatch "+name)
        self.record["tpcds_reference_matches"]=103
        print("UMQ_HIBENCH_4_TPCDS_103_REFERENCE_MATCH_PASS",flush=True)
    def stop(self):
        errors=[]
        active_apps=[n for n in self.processes if n.startswith(("umq-","standard-"))]
        for name in active_apps+["worker-A","worker-B","spark-master"]:
            p=self.processes.get(name)
            if p and p.poll() is None:
                os.killpg(p.pid,signal.SIGTERM)
                try:p.wait(timeout=20)
                except subprocess.TimeoutExpired:errors.append("application group retained "+name)
        def stop_node(node):
            p=self.processes.get("scache-"+node)
            if not p:return
            if p.poll() is None:self.command(IPS[node],"STOP")
            rc=p.wait(timeout=30)
            require(rc==0,"SCache exit "+node+" rc="+str(rc))
            require("SCACHE_UMQ_CLOSE_PASS" in self.logpath("scache-"+node).read_text(),"native close missing "+node)
        with concurrent.futures.ThreadPoolExecutor(2) as pool:
            futures=[pool.submit(stop_node,n) for n in IPS]
            for f in futures:
                try:f.result()
                except Exception as e:errors.append(str(e))
        p=self.processes.get("scache-master")
        if p and p.poll() is None:
            p.terminate()
            try:p.wait(timeout=15)
            except subprocess.TimeoutExpired:errors.append("master retained")
        self.record["cleanup_errors"]=errors;self.save()
        require(not errors,"cleanup failed: "+repr(errors))

def main():
    parser=argparse.ArgumentParser();parser.add_argument("--stage",choices=["smoke","bench"],default="smoke")
    args=parser.parse_args()
    (R/"build").mkdir(exist_ok=True)
    lock=(R/"build/logical-cluster.lock").open("a+")
    try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    except BlockingIOError:
        raise SystemExit("Another isolated logical cluster is active; inspect its REPORT.json first")
    lab=Lab(args.stage)
    try:
        lab.install();lab.launch();lab.workloads();lab.record["result"]="DATA_PASS"
    except BaseException as e:
        lab.record["result"]="FAIL";lab.record["error"]=repr(e);traceback.print_exc()
    finally:
        try:lab.stop()
        except BaseException as e:
            lab.record["result"]="FAIL";lab.record["cleanup_error"]=repr(e);traceback.print_exc()
        if lab.record["result"]=="DATA_PASS":lab.record["result"]="PASS"
        lab.save()
        concise={key:value for key,value in lab.record.items() if key!="artifacts"}
        concise["verified_artifact_count"]=len(lab.record.get("artifacts",{}))
        summary=json.dumps(concise,indent=2)+"\n"
        if lab.record["result"]!="PASS":
            for name in ["scache-A","scache-B"]:
                p=lab.logpath(name)
                if p.exists():summary+="\n=== "+name+" ===\n"+"\n".join(p.read_text(errors="replace").splitlines()[-50:])+"\n"
        (lab.run/"REPORT.txt").write_text(summary)
        if lab.record["result"]!="PASS":print(summary,flush=True)
    print("UMQ_CLUSTER_RESULT="+lab.record["result"]+" REPORT="+str(lab.run/"REPORT.json"),flush=True)
    return 0 if lab.record["result"]=="PASS" else 1
if __name__=="__main__":sys.exit(main())
