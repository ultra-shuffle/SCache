#!/usr/bin/env python3
"""Assert one actual SCache node and Spark Worker per kernel; inspect Master too."""
import argparse
import json
from pathlib import Path
import time
import urllib.request

p = argparse.ArgumentParser()
for key in ('run', 'node', 'ip', 'master', 'peer'):
    p.add_argument('--' + key, required=True)
a = p.parse_args()
run = Path(a.run)
classes = {'spark-worker': 'org.apache.spark.deploy.worker.Worker',
           'scache-node': 'org.scache.deploy.NettyBlockTransferIntegrationNode'}
report = {'node': a.node, 'ip': a.ip, 'boot_id': Path('/proc/sys/kernel/random/boot_id').read_text().strip()}
for name, cls in classes.items():
    matches = []
    for proc in Path('/proc').iterdir():
        if not proc.name.isdigit():
            continue
        try:
            args = (proc / 'cmdline').read_bytes().split(b'\0')
        except (OSError, PermissionError):
            continue
        if cls.encode() in args:
            matches.append(int(proc.name))
    expected = int((run / (name + '.pid')).read_text())
    report[name] = {'pids': matches, 'expected_pid': expected}
    if matches != [expected]:
        raise SystemExit('TOPOLOGY_PROCESS_MISMATCH ' + json.dumps(report))

if a.node == 'A':
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    workers = []
    for attempt in range(30):
        with opener.open('http://' + a.master + ':19880/json/', timeout=5) as response:
            master = json.load(response)
        workers = [w for w in master.get('workers', []) if w.get('state') == 'ALIVE']
        if len(workers) == 2 and sorted(w['host'] for w in workers) == sorted([a.ip, a.peer]):
            break
        time.sleep(1)
    else:
        raise SystemExit('SPARK_MASTER_WORKERS_MISMATCH ' + json.dumps(workers))
    report['registered_workers'] = workers
(run / 'topology.json').write_text(json.dumps(report, indent=2))
print('TWO_NODE_TOPOLOGY_PASS ' + json.dumps(report))
