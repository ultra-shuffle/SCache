#!/usr/bin/env python3
"""Single invocation per machine, original single-node package as toolchain.

Only discovery/connect-before-DMA is retried. An actual UB test is never retried
automatically. Product sources and original package files are not changed.
"""
import argparse
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import shlex
import socket
import subprocess
import sys
import tempfile
import threading
import time
import fcntl

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent


def source_id():
    # Same GNU sha256sum/sort contract as prepare-tests.sh and run-package.sh.
    p = subprocess.run(['bash', '-c',
        'set -euo pipefail; { find src/main src/integration -type f -exec sha256sum {} +; sha256sum build.sbt; } | sort | sha256sum'],
        cwd=REPO, text=True, capture_output=True, check=True)
    return p.stdout.split()[0]


def native_source_id():
    # Runtime classpath identity alone does not cover the JNI implementation.
    # Hash relative names and contents so different install directories agree.
    p = subprocess.run(['bash', '-c',
        'set -euo pipefail; { find native/spark_urma/src native/spark_urma/include -type f -exec sha256sum {} +; sha256sum native/spark_urma/CMakeLists.txt; } | sort | sha256sum'],
        cwd=REPO, text=True, capture_output=True, check=True)
    return p.stdout.split()[0]


def runner_source_id(root=HERE):
    """Require both hosts to run the same adapter, not merely the same SCache classes."""
    digest = hashlib.sha256()
    for name in ('auto-run.py', 'run-all.sh', 'run-package.sh', 'run-node.sh',
                 'select-endpoint.py', 'package-env.sh'):
        digest.update(name.encode('utf-8') + b'\0')
        digest.update((root / name).read_bytes())
    return digest.hexdigest()


def candidates(roots, limit=20, depth=7):
    """Bounded search, no symlink recursion, with an audit of skipped roots."""
    seen = set()
    deadline = time.monotonic() + limit
    for root in roots:
        root = Path(root).expanduser().resolve()
        if not root.is_dir():
            print('SEARCH_SKIP missing=' + str(root), flush=True)
            continue
        print('SEARCH_ROOT=' + str(root), flush=True)
        for current, dirs, files in os.walk(root):
            p = Path(current)
            if time.monotonic() > deadline:
                print('SEARCH_LIMIT_REACHED (use --search-root for an extra location)', flush=True)
                return
            if p in seen:
                dirs[:] = []
                continue
            seen.add(p)
            if len(p.relative_to(root).parts) >= depth:
                dirs[:] = []
            else:
                dirs[:] = sorted(d for d in dirs if d not in
                    {'.git', 'logs', 'node_modules', 'proc', 'sys', 'target', 'dependency-cache'})
            yield p


def package_ok(p):
    return all((p / rel).is_file() for rel in (
        'dependencies/jdk/bin/java', 'dependencies/jdk/bin/javac',
        'dependencies/native-toolchain/SHA256SUMS',
        'bin/build-lib-spark-urma.sh', 'bin/real-ub/00-detect-vendor-umdk.sh'))


def runtime_ok(p, identity):
    try:
        if (p / 'source-id.txt').read_text().strip() != identity:
            return False
        entries = (p / 'classpath.relative').read_text().splitlines()
        if not entries or not all(e.startswith('entry-') and e[6:].isdigit()
                                  and (p / e).exists() for e in entries):
            return False
        return subprocess.run(['sha256sum', '-c', 'SHA256SUMS'], cwd=p,
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode == 0
    except OSError:
        return False


def discover(args, work, identity):
    state = work / 'state' / 'auto-paths.json'
    try:
        saved = json.loads(state.read_text())
    except (OSError, ValueError):
        saved = {}
    preferred = [args.package_home, os.environ.get('RM_CTP_PACKAGE'),
                 os.environ.get('PACKAGE_HOME'), saved.get('package'),
                 str(Path.home() / 'ub-source/ultrashuffle/spark-3.5.7-benchmark-runtime-aarch64')]
    package = next((Path(p).expanduser().resolve() for p in preferred if p and package_ok(Path(p).expanduser())), None)
    runtime = next((Path(p).expanduser().resolve() for p in
                    [args.classes, saved.get('classes'), str(REPO / 'runtime')]
                    if p and runtime_ok(Path(p).expanduser(), identity)), None)
    if not package or not runtime:
        roots = args.search_root + [str(REPO), str(REPO.parent), str(Path.cwd()),
                                   str(Path.home() / 'ub-source'), '/opt/ultrashuffle']
        for p in candidates(roots):
            if package is None and package_ok(p):
                package = p
            if runtime is None and (p / 'classpath.relative').is_file():
                if runtime_ok(p, identity):
                    runtime = p
                else:
                    print('RUNTIME_REJECTED incompatible/incomplete=' + str(p), flush=True)
            if package and runtime:
                break
    if not package:
        raise RuntimeError('PACKAGE_NOT_FOUND: need the extracted original single-node package containing dependencies/jdk, native-toolchain and bin/real-ub. Use --search-root DIR; do not install another system Java.')
    if not runtime:
        raise RuntimeError('MATCHING_RUNTIME_NOT_FOUND: delivery must include the exported RM/CTP runtime for source=' + identity + '; the old SCache JAR cannot replace it. No target-side sbt download attempted.')
    state.write_text(json.dumps({'package': str(package), 'classes': str(runtime)}, indent=2))
    print('PACKAGE_HOME=' + str(package), flush=True)
    print('RM_CTP_RUNTIME=' + str(runtime), flush=True)
    return package, runtime


def free_ports(local, first):
    sockets = []
    try:
        for port in (first, first + 1, first + 2):
            s = socket.socket()
            sockets.append(s)
            s.bind((local, port))
        return True
    except OSError:
        return False
    finally:
        for s in sockets:
            s.close()


def send(sock, value):
    sock.sendall((json.dumps(value) + '\n').encode())


def receive(sock):
    raw = bytearray()
    while len(raw) < 16384:
        b = sock.recv(1)
        if not b:
            raise RuntimeError('PEER_DISCONNECTED: see peer summary.log')
        if b == b'\n':
            return json.loads(raw)
        raw.extend(b)
    raise RuntimeError('INVALID_RENDEZVOUS_MESSAGE')


def rendezvous(args, local):
    deadline = time.monotonic() + args.wait_seconds
    if args.role == 'A':
        with socket.socket() as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind((local, args.sync_port))
            listener.listen(4)
            listener.settimeout(15)
            while time.monotonic() < deadline:
                try:
                    conn, addr = listener.accept()
                except socket.timeout:
                    print('WAITING_FOR_B: peer preparation may still be compiling', flush=True)
                    continue
                if addr[0] == args.peer_ip:
                    return conn
                conn.close()
            raise RuntimeError('PEER_WAIT_TIMEOUT: B must run run-all.sh; allow TCP sync port ' + str(args.sync_port))
    while time.monotonic() < deadline:
        try:
            return socket.create_connection((args.peer_ip, args.sync_port), timeout=10,
                                            source_address=(local, 0))
        except OSError as e:
            print('WAITING_FOR_A: ' + str(e), flush=True)
            time.sleep(5)
    raise RuntimeError('PEER_WAIT_TIMEOUT: A not ready or management TCP blocked')


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--peer-ip', help='other machine management IPv4, required for actual test')
    p.add_argument('--role', choices=['A', 'B'], help='automatic: smaller management IPv4 is A')
    p.add_argument('--local-ip')
    p.add_argument('--workspace', default=str(Path.home() / 'ub-source/ultrashulfe-work/scache-rm-ctp'))
    p.add_argument('--package-home')
    p.add_argument('--classes')
    p.add_argument('--search-root', action='append', default=[])
    p.add_argument('--device', default='auto')
    p.add_argument('--eid', default='auto')
    p.add_argument('--priority', default='auto')
    p.add_argument('--provider-lib')
    p.add_argument('--umdk-root')
    p.add_argument('--sync-port', type=int, default=19173)
    p.add_argument('--wait-seconds', type=int, default=1800)
    p.add_argument('--prepare-only', action='store_true', help='build/check JNI without initializing a UB device')
    a = p.parse_args()
    if not a.prepare_only and not a.peer_ip:
        p.error('--peer-ip is required: the intended remote host cannot safely be guessed')
    if a.wait_seconds <= 0:
        p.error('--wait-seconds must be positive')
    work = Path(a.workspace).expanduser().resolve()
    (work / 'state').mkdir(parents=True, exist_ok=True)
    (work / 'logs').mkdir(exist_ok=True)
    run = Path(tempfile.mkdtemp(prefix='auto-', dir=work / 'logs'))
    # Entire run, including all child stdout/stderr, is both printed and saved.
    terminal = open(run / 'summary.log', 'w', buffering=1)
    original = sys.stdout
    class Tee:
        def write(self, text):
            terminal.write(text)
            original.write(text)
        def flush(self):
            terminal.flush()
            original.flush()
    sys.stdout = sys.stderr = Tee()
    lock = open(work / 'state/auto.lock', 'w')
    child = None
    stage = 'discover'
    rc = 1
    def command(cmd, ready=None):
        nonlocal child
        print('COMMAND=' + shlex.join(cmd), flush=True)
        child = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
        process = child
        def pump():
            for line in process.stdout:
                print(line, end='', flush=True)
                if ready is not None and 'RM_CTP_SERVICE_READY ' in line:
                    ready.set()
                if line.startswith('RM_CTP_SERVICE_PASS '):
                    thread.service_pass = True
        thread = threading.Thread(target=pump, daemon=True)
        thread.service_pass = False
        thread.start()
        return child, thread
    try:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        print('RUN=' + str(run), flush=True)
        identity = source_id()
        native_identity = native_source_id()
        runner_identity = runner_source_id()
        package, runtime = discover(a, work, identity)
        base = ['bash', str(HERE / 'run-package.sh'), '--package-home', str(package),
                '--workspace', str(work), '--classes', str(runtime), '--device', a.device,
                '--eid', a.eid, '--priority', a.priority]
        for opt in ('provider_lib', 'umdk_root'):
            if getattr(a, opt):
                base += ['--' + opt.replace('_', '-'), getattr(a, opt)]
        for root in a.search_root:
            base += ['--search-root', root]
        for stage in (['check'] if a.prepare_only else ['detect', 'check']):
            proc, thread = command(base + ['--phase', stage])
            result = proc.wait(); thread.join()
            if result:
                raise RuntimeError('PREPARATION_FAILED phase=' + stage + ' exit=' + str(result) + '; original detailed log printed above; rerun reuses successful build')
        if a.prepare_only:
            print('RM_CTP_PREPARE_PASS (no UB device initialized)', flush=True)
            rc = 0
            return
        stage = 'rendezvous'
        ipaddress.IPv4Address(a.peer_ip)
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as route:
            route.connect((a.peer_ip, 9))
            local = a.local_ip or route.getsockname()[0]
        if local == a.peer_ip or ipaddress.ip_address(a.peer_ip).is_loopback:
            raise RuntimeError('TWO_DISTINCT_HOSTS_REQUIRED: RM/CTP is not this package single-host loopback test')
        a.role = a.role or ('A' if int(ipaddress.IPv4Address(local)) < int(ipaddress.IPv4Address(a.peer_ip)) else 'B')
        if not 1024 < a.sync_port < 65536:
            raise RuntimeError('INVALID_SYNC_PORT')
        # Auto-choose a free data-control pair; sync port is stable for discovery.
        port = next((n for n in range(19171, 19272, 4) if n + 1 != a.sync_port and n != a.sync_port and free_ports(local, n)), None)
        if port is None:
            raise RuntimeError('NO_FREE_CONTROL_PORT_PAIR')
        print(f'ENDPOINT role={a.role} local={local} peer={a.peer_ip} control={port} sync={a.sync_port}', flush=True)
        with rendezvous(a, local) as conn:
            conn.settimeout(a.wait_seconds)
            send(conn, {'role': a.role, 'source': identity, 'nativeSource': native_identity,
                        'runnerSource': runner_identity,
                        'port': port, 'local': local})
            other = receive(conn)
            if other.get('runnerSource') != runner_identity:
                raise RuntimeError('PEER_RUNNER_VERSION_MISMATCH: both hosts need the same ' +
                                   'RM/CTP test scripts; local=' + runner_identity +
                                   ' peer=' + str(other.get('runnerSource')))
            if (other.get('source') != identity or other.get('nativeSource') != native_identity or
                    other.get('role') == a.role or other.get('local') != a.peer_ip):
                raise RuntimeError('PEER_SOURCE_ROLE_OR_IP_MISMATCH: ' + repr(other))
            peer_port = other.get('port')
            if not isinstance(peer_port, int) or not 1024 < peer_port < 65535:
                raise RuntimeError('PEER_PORT_INVALID')
            node = base + ['--phase', 'launch', '--role', a.role, '--peer-ip', a.peer_ip,
                '--local-ip', local, '--port', str(port), '--peer-port', str(peer_port)]
            stage = 'scache-rm-ctp'
            ready = threading.Event()
            if a.role == 'A':
                proc, thread = command(node, ready)
                deadline = time.monotonic() + 120
                while not ready.wait(0.2):
                    if proc.poll() is not None or time.monotonic() > deadline:
                        raise RuntimeError('A_NOT_READY: detailed node log above; no automatic DMA retry')
                send(conn, {'start': True})
            else:
                if receive(conn) != {'start': True}:
                    raise RuntimeError('PEER_NOT_READY')
                proc, thread = command(node)
            result = proc.wait(); thread.join()
            if result == 0 and not thread.service_pass:
                print('SERVICE_PASS_MARKER_MISSING: exit zero alone is not a data-path success', flush=True)
                result = 1
            send(conn, {'exit': result})
            other_result = receive(conn)
            if result or other_result.get('exit') != 0:
                raise RuntimeError(f'UB_TEST_FAILED local={result} peer={other_result}; no automatic retry after DMA submission')
        print('SCACHE_RM_CTP_TWO_NODE_PASS (Spark not started)', flush=True)
        rc = 0
    except Exception as e:
        print(f'FAILURE stage={stage} reason={type(e).__name__}: {e}', flush=True)
        if child and child.poll() is None:
            print(f'CHILD_STILL_RUNNING pid={child.pid}; do not reuse its arena or rerun while outstanding DMA is unresolved', flush=True)
            # Keep consuming output: do not SIGKILL a provider-owned DMA request.
            print('Waiting for the existing child to exit; no new requests or retries.', flush=True)
            child.wait()
    finally:
        report = {'result': 'PASS' if rc == 0 else 'FAIL', 'stage': stage, 'log': str(run / 'summary.log')}
        (run / 'status.json').write_text(json.dumps(report, indent=2))
        print('FINAL_REPORT=' + json.dumps(report), flush=True)
    if rc:
        raise SystemExit(rc)


if __name__ == '__main__':
    main()
