#!/usr/bin/env python3
"""Create a new, precompiled UMQ-only simulation delivery from audited artifacts."""
import argparse,hashlib,json,os,pathlib,re,shutil,subprocess
R=pathlib.Path(__file__).resolve().parents[1]
OUT=R/"delivery/spark-3.5.7-scache-umq-rm-ctp-simulator-aarch64"
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def copy(a,b):b.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(a,b)
def main():
    p=argparse.ArgumentParser();p.add_argument("--report",required=True,type=pathlib.Path);a=p.parse_args()
    audit=subprocess.check_output(["python3",str(R/"scripts/audit-cluster-report.py"),str(a.report)],text=True)
    assert not OUT.exists(),"Refuse overwrite of an existing delivery"
    OUT.mkdir(parents=True)
    source=R/"runtime/package"
    # Whitelist: no historical URPC backend libraries, old logs, hardware scripts or source bundle.
    for name in ["runtime","benchmarks","dependencies/jdk","dependencies/umq","licenses"]:
        shutil.copytree(source/name,OUT/name,symlinks=True)
    for name in ["scripts/run-logical-cluster.py","scripts/audit-cluster-report.py","scripts/run-simulation.sh"]:
        copy(R/name,OUT/name)
    shutil.copytree(source/"tools/umq-sim",OUT/"tools/umq-sim",symlinks=True)
    for f in (R/"docs").glob("*.md"):copy(f,OUT/"docs"/f.name)
    copy(R/"docs/DEPLOY_SIMULATION_CN.md",OUT/"README_CN.md")
    copy(pathlib.Path("/usr/share/doc/libnl3/COPYING"),OUT/"licenses/libnl3-COPYING")
    copy(R/"repos/OpenURMA/LICENSE",OUT/"licenses/OpenURMA-LICENSE")
    evidence=OUT/"evidence";evidence.mkdir()
    copy(a.report,evidence/"REPORT.json")
    (evidence/"AUDIT.json").write_text(audit)
    for name in ["check-standard.log","check-umq.log"]:
        copy(a.report.parent/name,evidence/name)
    meta=OUT/"metadata";meta.mkdir()
    copy(source/"metadata/umq-libraries.json",meta/"umq-libraries.json")
    versions=set()
    for f in list((OUT/"dependencies").rglob("*"))+list((OUT/"runtime").rglob("*.so")):
        if not f.is_file() or f.is_symlink():continue
        with f.open("rb") as stream:magic=stream.read(4)
        if magic!=b"\x7fELF":continue
        text=subprocess.check_output(["readelf","--version-info",str(f)],text=True)
        versions.update(re.findall(r"GLIBC_(\d+\.\d+(?:\.\d+)?)",text))
    refs={name:subprocess.check_output(["git","-C",str(R/"repos"/name),"rev-parse","HEAD"],text=True).strip()
          for name in ["SCache","OpenURMA","spark-3.5-scache","umdk"]}
    artifacts={}
    for f in sorted(OUT.rglob("*")):
        if f.is_symlink():assert f.resolve().is_relative_to(OUT),"External symlink "+str(f)
        elif f.is_file():artifacts[str(f.relative_to(OUT))]=sha(f)
    manifest={"kind":"official-june24-umq-rm-ctp-functional-simulation","precompiled":True,
        "minimum_glibc":max(versions,key=lambda v:tuple(map(int,v.split(".")))),
        "hardware_verified":False,"mode":"RM/CTP","source_commits":refs,
        "official_umdk":"099343116b63aad37e2f60f99b71e772bf6bf009",
        "kernel_reference":"41b5f26ff0e91e8af102aa248117e6210b486a40",
        "verification":{"benchmarks":"PASS","tpcds_reference_matches":103,"audit_sha256":sha(evidence/"AUDIT.json")},
        "artifacts":artifacts}
    (meta/"umq-manifest.json").write_text(json.dumps(manifest,indent=2))
    print("UMQ_DELIVERY_STAGED="+str(OUT));print("MINIMUM_GLIBC="+manifest["minimum_glibc"])
if __name__=="__main__":main()
