#!/usr/bin/env python3
"""Read-only, independent evidence audit. Prints JSON; never starts a workload."""
import argparse, collections, hashlib, json, pathlib

def require(ok, message):
    if not ok: raise RuntimeError(message)

def main():
    parser=argparse.ArgumentParser();parser.add_argument("report",type=pathlib.Path)
    args=parser.parse_args();report=json.loads(args.report.read_text());run=args.report.parent
    require(report["result"]=="PASS" and report["cleanup_errors"]==[],"cluster not completely PASS")
    require(report.get("tpcds_reference_matches")==103,"not a complete benchmark report")
    expected={"urpc-smoke"}|{kind+"-"+name for kind in ["standard","urpc"]
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
            "remote_urpc_bytes":app["remote_urpc_bytes"],"seconds":app["seconds"]}
        if label.startswith("urpc-"):
            require(app["remote_urpc_bytes"]>0,"no actual URPC bytes: "+label)
    reference=run/"data/standard/tpcds";actual=run/"data/urpc/tpcds"
    files=sorted(reference.glob("*-results.jsonl"));require(len(files)==103,"reference count")
    digests={}
    for f in files:
        for name in [f.name,f.name.replace("-results.jsonl","-schema.json")]:
            data=(reference/name).read_bytes();require(data==(actual/name).read_bytes(),"mismatch "+name)
            digests[name]=hashlib.sha256(data).hexdigest()
    evidence["tpcds_statements"]=103;evidence["matched_files"]=digests
    for kind in ["standard","urpc"]:
        require("HIBENCH_PASS workloads=4" in (run/("check-"+kind+".log")).read_text(),"HiBench check "+kind)
        require("TPCDS_PASS files=99 statements=103" in (run/(kind+"-tpcds.log")).read_text(),"TPCDS gate "+kind)
    metrics=json.loads((run/"urpc-tpcds-after-metrics.json").read_text())
    for node,m in metrics.items():
        for key in ["urpc.rpcFailed","urma.fallbacks","urma.tcpPayloadBytes","urma.nettyPayloadBytes",
                    "urma.checksumErrors","urma.leaseErrors","urma.generationErrors",
                    "urma.activeLeases","urma.activeControlConnections","urma.providerOutstanding",
                    "urma.transportInflightOperations","urma.activeSlots","urma.sharedPoolAborts"]:
            require(m[key]==0,node+" nonzero "+key+"="+str(m[key]))
        require(m["urpc.rpcCalls"]==m["urpc.rpcCompleted"] and m["urpc.rpcCalls"]>0,node+" RPC reconciliation")
        text=(run/("scache-"+node+".log")).read_text()
        require("SCACHE_URPC_CLOSE_PASS staging_live=0 arena_grants=0" in text,node+" close gate")
        require("SCACHE_FALLBACK" not in text,node+" fallback log")
        maps=(run/("scache-"+node+"-maps.txt")).read_text()
        libraries={name:sorted({line.split()[-1] for line in maps.splitlines() if name in line})
            for name in ["libScacheUrpc.so","liburpc_framework.so","liburma_openurma.so"]}
        require(all(libraries.values()),node+" library evidence")
        evidence["nodes"][node]={"rpc_calls":m["urpc.rpcCalls"],"read_bytes":m["urpc.readBytes"],
            "rpc_failures":m["urpc.rpcFailed"],"libraries":libraries,"close":"PASS"}
    print(json.dumps(evidence,indent=2))
if __name__=="__main__":main()
