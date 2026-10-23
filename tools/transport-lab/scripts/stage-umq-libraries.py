#!/usr/bin/env python3
"""Stage the paired June-24 UMQ runtime in the isolated development package."""
import hashlib,json,pathlib,shutil,subprocess,os,fcntl
R=pathlib.Path(__file__).resolve().parents[1]
def digest(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def main():
    lock=(R/"build/logical-cluster.lock").open("a+")
    fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
    package=R/"runtime/package";lib=package/"dependencies/umq/lib";lib.mkdir(parents=True,exist_ok=True)
    def copy(src,dst):dst.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(src,dst)
    def library(src):
        real=src.resolve();copy(real,lib/real.name)
        if src.name!=real.name:
            link=lib/src.name
            if link.is_symlink():link.unlink()
            if not link.exists():link.symlink_to(real.name)
    for pattern in ["libumq.so*","libumq_buf.so*","libumq_ub.so*","liburma.so*","liburma_common.so*","libtpsa.so*"]:
        files=list((R/"build/umdk").rglob(pattern));assert files,pattern
        for f in files:library(f)
    for name in ["libcrypto.so.3","libssl.so.3","libstdc++.so.6","libgcc_s.so.1","libnl-3.so.200","libnl-genl-3.so.200"]:
        library(pathlib.Path("/usr/lib64")/name)
    copy(R/"build/libScacheUmq.so",lib/"libScacheUmq.so")
    simulator=R/"repos/OpenURMA/integration/umdk"
    copy(simulator/"build/umq-functional/liburma_openurma.so",lib/"urma/liburma_openurma.so")
    copy(simulator/"build/umq-functional/openurma_shim.so",lib/"openurma_shim.so")
    tool=package/"tools/umq-sim";tool.mkdir(parents=True,exist_ok=True)
    copy(simulator/"tools/openurma_mkdev.sh",tool/"mkdev.sh")
    copy(package/"tools/urpc-sim/log4j.properties",tool/"log4j.properties")
    env=dict(os.environ,LD_LIBRARY_PATH=str(lib))
    for f in [lib/"libScacheUmq.so",lib/"libumq_ub.so",lib/"libtpsa.so",lib/"urma/liburma_openurma.so"]:
        p=subprocess.run(["ldd",str(f)],env=env,text=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
        assert p.returncode==0 and "not found" not in p.stdout,p.stdout
    api=R/"repos/umdk/src/urma/lib/urma/core/include/urma_api.h"
    assert digest(api)=="c334280559eb312982c8a8a3e6250e9413161878a6245be96e928b239bd23d54","June-24/site API hash differs"
    record={"official_umdk":"099343116b63aad37e2f60f99b71e772bf6bf009",
        "kernel_reference":"41b5f26ff0e91e8af102aa248117e6210b486a40",
        "api_sha256":digest(api),"mode":"RM/CTP","simulation":"verbs-functional-not-hardware-timing",
        "libraries":{str(f.relative_to(lib)):digest(f) for f in sorted(lib.rglob("*")) if f.is_file() and not f.is_symlink()}}
    (package/"metadata/umq-libraries.json").write_text(json.dumps(record,indent=2))
    print("JUNE24_UMQ_RUNTIME_STAGED="+str(lib))
if __name__=="__main__":main()
