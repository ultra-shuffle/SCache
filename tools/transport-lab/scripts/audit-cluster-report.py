#!/usr/bin/env python3
"""Read-only, independent evidence audit. Prints JSON; never starts a workload."""
import argparse, collections, hashlib, json, pathlib

def require(ok, message):
    if not ok: raise RuntimeError(message)

def main():
    parser=argparse.ArgumentParser();parser.add_argument("report",type=pathlib.Path)
    args=parser.parse_args();report=json.loads(args.report.read_text());run=args.report.parent
    require(report["result"]=="PASS" and report["cleanup_errors"]==[],"cluster not completely PASS")
    require(report.get("mode")=="rm-ctp" and report.get("umdk_reference")=="099343116b63aad37e2f60f99b71e772bf6bf009","wrong mode or SDK baseline")
    require(report.get("tpcds_reference_matches")==103,"not a complete benchmark report")
    expected={"umq-smoke"}|{kind+"-"+name for kind in ["standard","umq"]
        for name in ["wordcount","sort","terasort","repartition","tpcds"]}
    require({a["name"] for a in report["applications"]}==expected,"application coverage differs")
    evidence={"report":str(args.report),"report_sha256":hashlib.sha256(args.report.read_bytes()).hexdigest(),
              "result":"PASS","hardware_verified":False,"applications":{},"nodes":{}}
    for app in report["applications"]:
        label=app["name"];require(app["exit"]==0,"failed application "+label)
        executors=set();hosts=collections.Counter()
        for eventfile in (run/"events"/label).iterdir():
            if not eventfile.is_file():continue
            with eventfile.open() as stream:
                for line in stream:
                    e=json.loads(line)
                    if e["Event"]=="SparkListenerExecutorAdded":executors.add(e["Executor Info"]["Host"])
                    if e["Event"]=="SparkListenerTaskEnd":hosts[e["Task Info"]["Host"]]+=1
        require(executors=={"127.0.0.1","127.0.0.2"},"both executors not present: "+label)
        require(all(hosts[h]>0 for h in executors),"both nodes must execute tasks: "+label)
        evidence["applications"][label]={"executor_hosts":sorted(executors),"task_counts":dict(hosts),
            "remote_umq_bytes":app["remote_umq_bytes"],"seconds":app["seconds"]}
        if label.startswith("umq-"):
            require(app["remote_umq_bytes"]>0,"no actual UMQ bytes: "+label)
    reference=run/"data/standard/tpcds";actual=run/"data/umq/tpcds"
    files=sorted(reference.glob("*-results.jsonl"));require(len(files)==103,"reference count")
    digests={}
    for f in files:
        for name in [f.name,f.name.replace("-results.jsonl","-schema.json")]:
            data=(reference/name).read_bytes();require(data==(actual/name).read_bytes(),"mismatch "+name)
            digests[name]=hashlib.sha256(data).hexdigest()
    evidence["tpcds_statements"]=103;evidence["matched_files"]=digests
    for kind in ["standard","umq"]:
        require("HIBENCH_PASS workloads=4" in (run/("check-"+kind+".log")).read_text(),"HiBench check "+kind)
        require("TPCDS_PASS files=99 statements=103" in (run/(kind+"-tpcds.log")).read_text(),"TPCDS gate "+kind)
    metrics=json.loads((run/"umq-tpcds-after-metrics.json").read_text())
    for node,m in metrics.items():
        for key in ["umq.rpcFailed","urma.fallbacks","urma.tcpPayloadBytes","urma.nettyPayloadBytes",
                    "urma.checksumErrors","urma.leaseErrors","urma.generationErrors",
                    "urma.activeLeases","urma.activeControlConnections","urma.providerOutstanding",
                    "urma.transportInflightOperations","urma.activeSlots","urma.sharedPoolAborts"]:
            require(m[key]==0,node+" nonzero "+key+"="+str(m[key]))
        require(m["umq.rpcCalls"]==m["umq.rpcCompleted"] and m["umq.rpcCalls"]>0,node+" RPC reconciliation")
        text=(run/("scache-"+node+".log")).read_text()
        require("SCACHE_UMQ_CLOSE_PASS arena_grants=0 mode=RM tp=CTP" in text,node+" close gate")
        require("SCACHE_FALLBACK" not in text,node+" fallback log")
        for marker in ["SIM_UMQ_CREATE_JETTY mode=RM tp=CTP priority=6", "SIM_UMQ_IMPORT_JETTY mode=RM tp=CTP"]:
            require(marker in text,node+" missing RM/CTP evidence "+marker)
        require("FORBIDDEN_RC_BIND" not in text and "SCACHE_UMQ_FATAL" not in text,node+" wrong path/fatal")
        maps=(run/("scache-"+node+"-maps.txt")).read_text()
        libraries={name:sorted({line.split()[-1] for line in maps.splitlines() if name in line})
            for name in ["libScacheUmq.so","libumq.so","libumq_ub.so","liburma.so","liburma_openurma.so"]}
        require(all(libraries.values()),node+" library evidence")
        require(all("/dependencies/umq/lib/" in path for paths in libraries.values() for path in paths),
                node+" loaded libraries outside the isolated UMQ bundle")
        require("liburpc_framework.so" not in maps and "libScacheUrpc.so" not in maps,node+" old backend loaded")
        evidence["nodes"][node]={"rpc_calls":m["umq.rpcCalls"],"read_bytes":m["umq.readBytes"],
            "rpc_failures":m["umq.rpcFailed"],"libraries":libraries,"close":"PASS"}
    print(json.dumps(evidence,indent=2))
if __name__=="__main__":main()
