#!/usr/bin/env python3
"""Assemble a precompiled simulation runtime; do not include old hardware scripts or source bundles."""
import hashlib, json, os, pathlib, re, shutil, subprocess, sys
R=pathlib.Path(__file__).resolve().parents[1]
OUT=R/"delivery/spark-3.5.7-scache-urpc-simulator-aarch64"
def copy(src,dst):
    dst.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(src,dst)
def library(src,libdir):
    src=pathlib.Path(src);real=src.resolve()
    copy(real,libdir/real.name)
    for name in {src.name,real.name}:
        if name!=real.name:
            link=libdir/name
            if link.is_symlink():link.unlink()
            if not link.exists():link.symlink_to(real.name)
def family(directory,pattern,libdir):
    for src in pathlib.Path(directory).glob(pattern):library(src,libdir)
def main():
    require=lambda x,m: x or (_ for _ in ()).throw(RuntimeError(m))
    require(not OUT.exists(),"Use a new delivery directory or inspect existing staging first")
    OUT.mkdir(parents=True)
    subprocess.run(["rsync","-a","--exclude=/.validation-running","--exclude=/SHA256SUMS",
        "--exclude=/README.md","--exclude=/sources/","--exclude=/metadata/","--exclude=/tools/",
        "--exclude=/bin/","--exclude=/dependencies/native-toolchain/","--exclude=/dependencies/source-rpms/",
        "--exclude=/dependencies/umdk-reference/",str(R/"runtime/package")+"/",str(OUT)+"/"],check=True)
    copy(R/"repos/SCache/target/scala-2.13/SCache-assembly-0.1.0-SNAPSHOT.jar",
         OUT/"runtime/scache/SCache-assembly-0.1.0-SNAPSHOT.jar")
    copy(R/"repos/SCache/target/scala-2.13/SCache-integration-0.1.0-SNAPSHOT.jar",
         OUT/"runtime/scache/SCache-ub-integration-test.jar")
    lib=OUT/"dependencies/urpc/lib";lib.mkdir(parents=True)
    family(R/"build/umdk/urpc/framework","liburpc_framework.so*",lib)
    family(R/"build/umdk/urma/lib/urma/core","liburma.so*",lib)
    family(R/"build/umdk/urma/common","liburma_common.so*",lib)
    family(R/"build/systemc-install/lib64","libsystemc.so*",lib)
    for f in ["libcrypto.so.3","libssl.so.3","libstdc++.so.6","libgcc_s.so.1"]:
        library("/usr/lib64/"+f,lib)
    copy(R/"build/libScacheUrpc.so",lib/"libScacheUrpc.so")
    copy(R/"repos/OpenURMA/integration/umdk/build/tier_s/openurma_shim.so",lib/"openurma_shim.so")
    copy(R/"repos/OpenURMA/integration/umdk/build/tier_s/liburma_openurma.so",lib/"urma/liburma_openurma.so")
    copy(R/"scripts/run-logical-cluster.py",OUT/"scripts/run-logical-cluster.py")
    copy(R/"repos/OpenURMA/integration/umdk/tools/openurma_mkdev.sh",OUT/"tools/urpc-sim/mkdev.sh")
    copy(R/"runtime/package/bin/real-ub/log4j.properties",OUT/"tools/urpc-sim/log4j.properties")
    for source in (R/"docs").glob("*.md"):
        if source.name.startswith("HISTORY_"):continue
        copy(source,OUT/"docs"/source.name)
    copy(R/"docs/DEPLOY_SIMULATION_CN.md",OUT/"README_CN.md")
    for name in ["libgcc","gcc","openssl-libs"]:
        source=pathlib.Path("/usr/share/licenses")/name
        if source.is_dir():shutil.copytree(source,OUT/"licenses"/name)
    versions=set()
    for f in lib.rglob("*"):
        if f.is_file() and not f.is_symlink():
            output=subprocess.check_output(["readelf","--version-info",str(f)],text=True)
            versions.update(re.findall(r"GLIBC_(\d+\.\d+(?:\.\d+)?)",output))
    shutil.copytree(R/"repos/umdk/LICENSES",OUT/"licenses/UMDK")
    for repo in ["OpenURMA","systemc","OpenClickNP"]:
        candidates=[R/"repos"/repo/f for f in ["LICENSE","LICENSE.txt","LICENSE.md","LICENSE-Apache-2.0"]]
        for f in candidates:
            if f.is_file():copy(f,OUT/"licenses"/(repo+"-"+f.name))
    (OUT/"metadata").mkdir()
    refs={repo:subprocess.check_output(["git","-C",str(R/"repos"/repo),"rev-parse","HEAD"],text=True).strip()
          for repo in ["SCache","spark-3.5-scache","umdk","OpenURMA","systemc","OpenClickNP"]}
    artifacts={}
    for f in sorted(OUT.rglob("*")):
        if f.is_file() and not f.is_symlink():
            artifacts[str(f.relative_to(OUT))]=hashlib.sha256(f.read_bytes()).hexdigest()
    manifest={"kind":"official-urpc-two-logical-node-simulation","precompiled":True,
              "minimum_glibc":max(versions,key=lambda s:tuple(map(int,s.split(".")))),
              "hardware_verified":False,"transport":"official framework RC over OpenURMA simulator",
              "source_commits":refs,"artifacts":artifacts}
    (OUT/"metadata/urpc-manifest.json").write_text(json.dumps(manifest,indent=2))
    env=dict(os.environ,LD_LIBRARY_PATH=str(lib))
    for f in [lib/"libScacheUrpc.so",lib/"urma/liburma_openurma.so"]:
        p=subprocess.run(["ldd",str(f)],env=env,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
        require(p.returncode==0 and "not found" not in p.stdout,"Unresolved runtime library: "+p.stdout)
        (OUT/"metadata"/(f.name+".ldd.txt")).write_text(p.stdout)
    print("URPC_RUNTIME_STAGED="+str(OUT),flush=True)
if __name__=="__main__":main()
