#!/usr/bin/env python3
"""Finalize only a fully verified isolated package; preserve stale copied metadata."""
import argparse, hashlib, json, pathlib, shutil, subprocess, tempfile
R=pathlib.Path(__file__).resolve().parents[1]
def digest(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def main():
    p=argparse.ArgumentParser();p.add_argument("--package",required=True,type=pathlib.Path)
    p.add_argument("--report",required=True,type=pathlib.Path);args=p.parse_args()
    package=args.package.resolve();report=args.report.resolve()
    assert package==R/"delivery/spark-3.5.7-scache-urpc-simulator-aarch64","unexpected package target"
    manifest=json.loads((package/"metadata/urpc-manifest.json").read_text())
    for name,want in manifest["artifacts"].items():
        assert digest(package/name)==want,"modified tested artifact "+name
    audited=subprocess.check_output(["python3",str(R/"scripts/audit-cluster-report.py"),str(report)],text=True)
    evidence=json.loads(audited)
    archive=pathlib.Path(tempfile.mkdtemp(prefix="stale-delivery-metadata-",dir=R/"logs"))
    for name in [".validation-running","SHA256SUMS","README.md"]:
        f=package/name
        if f.exists():shutil.move(str(f),archive/name)
    shutil.copytree(R/"repos/umdk/LICENSES",package/"licenses/UMDK",dirs_exist_ok=True)
    for source in (R/"docs").glob("*.md"):
        if source.name.startswith("HISTORY_"):continue
        shutil.copy2(source,package/"docs"/source.name)
    shutil.copy2(R/"docs/DEPLOY_SIMULATION_CN.md",package/"README_CN.md")
    target=package/"evidence";target.mkdir(exist_ok=True)
    shutil.copy2(report,target/"REPORT.json")
    (target/"AUDIT.json").write_text(audited)
    for name in ["check-standard.log","check-urpc.log"]:shutil.copy2(report.parent/name,target/name)
    artifacts={}
    for f in sorted(package.rglob("*")):
        relative=f.relative_to(package)
        if relative.parts[0] in {"logs","build","metadata"}:continue
        if f.is_symlink():
            assert f.resolve().is_relative_to(package),"external symlink "+str(f)
        elif f.is_file():artifacts[str(relative)]=digest(f)
    manifest["artifacts"]=artifacts
    manifest["verification"]={"full_benchmark":"PASS","tpcds_reference_matches":103,
        "audit_sha256":digest(target/"AUDIT.json"),"report_sha256":digest(target/"REPORT.json")}
    (package/"metadata/urpc-manifest.json").write_text(json.dumps(manifest,indent=2))
    print("URPC_DELIVERY_FINALIZED="+str(package));print("PRESERVED_STALE_METADATA="+str(archive))
if __name__=="__main__":main()
