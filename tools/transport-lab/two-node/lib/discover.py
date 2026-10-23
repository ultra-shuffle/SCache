#!/usr/bin/env python3
"""Read-only hardware discovery and explicit, persisted endpoint selection.

Only the small diagnostic probe is compiled. No application source is changed.
The presence of a capability is a precondition, not proof of connectivity.
"""
import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shlex
import shutil
import stat
import subprocess
import sys


class DiscoveryError(Exception):
    pass


def require(condition, message):
    if not condition:
        raise DiscoveryError(message)


def digest(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def run_command(command, logfile, env=None, timeout=30, accepted=(0,)):
    with open(str(logfile) + '.command', 'w') as f:
        f.write(' '.join(shlex.quote(str(x)) for x in command) + '\n')
    try:
        p = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                           env=env, timeout=timeout, text=True, errors='replace')
        text, rc = p.stdout, p.returncode
    except subprocess.TimeoutExpired as e:
        partial = e.stdout or ''
        if isinstance(partial, bytes):
            partial = partial.decode('utf-8', errors='replace')
        text, rc = partial + '\nCOMMAND_TIMEOUT\n', 124
    except OSError as e:
        text, rc = str(e), 127
    Path(logfile).write_text(text)
    require(rc in accepted, 'COMMAND_FAILED rc={} log={}\n{}'.format(rc, logfile, text[-2500:]))
    return text


def parse_admin(text):
    """Parse per-device priority-to-TP tables, never the coarse mode bitmask."""
    result = {}
    current = None
    priorities = None
    for line in text.splitlines():
        m = re.match(r'\s*name\s*:\s*(\S+)', line)
        if m:
            current = m.group(1)
            result[current] = {}
            priorities = None
        m = re.match(r'\s*priority\s*:\s*(.*)', line)
        if m:
            priorities = [int(x) for x in m.group(1).split() if x.isdigit()]
        m = re.match(r'\s*tp_type\s*:\s*(.*)', line)
        if m and current and priorities is not None:
            types = m.group(1).split()
            require(len(types) == len(priorities) and len(set(priorities)) == len(priorities),
                    'PRIORITY_TABLE_INVALID device=' + current)
            result[current] = dict(zip(priorities, types))
    return result


def parse_visible(text):
    result = {}
    for line in text.splitlines():
        fields = line.split()
        if len(fields) >= 4 and fields[0].isdigit():
            result[fields[1]] = fields[-1].upper()
    return result


def parse_probe(text):
    result = {}
    for line in text.splitlines():
        fields = line.split('\t')
        if len(fields) < 3 or fields[0] not in ('CAP', 'EID', 'PORT', 'WRITE', 'MSG', 'FAIL'):
            continue
        kind, name = fields[:2]
        item = result.setdefault(name, {'eids': {}, 'ports': [], 'errors': []})
        if kind == 'CAP':
            require(len(fields) == 8, 'CAP_OUTPUT_INVALID ' + line)
            item.update(zip(('modes', 'rc_rtp', 'rc_ctp', 'rm_rtp', 'rm_ctp', 'max_read'),
                            map(int, fields[2:])))
        elif kind == 'WRITE':
            item['max_write'] = int(fields[2])
        elif kind == 'MSG':
            item['max_msg'] = int(fields[2])
        elif kind == 'PORT':
            item['ports'].append([int(fields[2]), int(fields[3])])
        elif kind == 'EID':
            index, address = int(fields[2]), fields[3].lower()
            require(re.fullmatch(r'[0-9a-f]{32}', address) is not None, 'EID_OUTPUT_INVALID ' + line)
            if int(address, 16):
                require(index not in item['eids'], 'DUPLICATE_EID_INDEX ' + name)
                item['eids'][index] = address
        else:
            item['errors'].append(fields[2])
    return result


def choose_value(label, candidates, requested, previous=None):
    candidates = sorted(set(candidates))
    if requested != 'auto':
        value = int(requested) if candidates and isinstance(candidates[0], int) else requested
        require(value in candidates, '{}_INVALID requested={} available={}'.format(label, value, candidates))
        return value
    if previous is not None:
        require(previous in candidates, '{}_PREVIOUS_NO_LONGER_VALID previous={} available={}; rerun 10 with an explicit choice'.format(label, previous, candidates))
        return previous
    require(len(candidates) == 1, '{}_SELECTION_REQUIRED candidates={}; set only this field and rerun 10'.format(label, candidates))
    return candidates[0]


def provider_kind(driver):
    return {'udma': 'udma', 'ub_agg': 'ubagg', 'ubagg': 'ubagg'}.get(driver)


def provider_matches(driver, path):
    kind = provider_kind(driver)
    name = Path(path).name.lower().replace('-', '_')
    return kind is not None and re.match(r'liburma_' + kind + r'\.so(?:\.|$)', name) is not None


def candidate_config_lines(node, devices, caps, priorities, visible, providers, mode):
    """Show all selectable fields at once, before the first ambiguity aborts 10."""
    needed = 'rm_ctp' if mode == 'rm-ctp' else 'rc_rtp'
    mode_bit = 1 if mode == 'rm-ctp' else 2
    tp = 'CTP' if mode == 'rm-ctp' else 'RTP'
    lines = []
    for name, meta in sorted(devices.items()):
        cap = caps.get(name, {})
        if not meta.get('read_write') or cap.get('errors') or cap.get(needed) != 1:
            continue
        if not cap.get('modes', 0) & mode_bit or not cap.get('eids'):
            continue
        active = visible.get(name) == 'ACTIVE' or any(p[1] == 4 for p in cap.get('ports', []))
        if not active:
            continue
        matches = sorted(set(p['path'] for p in providers if provider_matches(meta['driver'], p['path'])))
        eids = sorted(cap['eids'])
        choices = sorted(i for i, kind in priorities.get(name, {}).items() if kind == tp and 0 <= i <= 15)
        lines.append('CONFIG_CANDIDATE node={} {}_DEVICE={} {}_PROVIDER={} {}_EID={} {}_PRIORITY={} visible={}'.format(
            node, node, name, node, ','.join(matches) or 'NONE', node,
            ','.join(map(str, eids)), node, ','.join(map(str, choices)) or 'NONE',
            visible.get(name, 'port-active-only')))
    return lines


def select_endpoint(devices, caps, priorities, visible, providers, mode,
                    device='auto', provider='auto', eid='auto', priority='auto', previous=None):
    previous = previous or {}
    # EID indices and priority/provider selections are local to a device and
    # mode. A confirmed aggregate endpoint must not silently choose eid0 (or
    # retain the UBAGG provider) after an explicit switch to physical UDMA.
    if previous.get('mode') != mode:
        previous = {}
    candidates = []
    rejected = []
    needed = 'rm_ctp' if mode == 'rm-ctp' else 'rc_rtp'
    mode_bit = 1 if mode == 'rm-ctp' else 2
    for name, meta in sorted(devices.items()):
        cap = caps.get(name, {})
        reasons = []
        if not meta.get('read_write'):
            reasons.append('device-node-missing-or-permission-denied')
        if cap.get('errors') or needed not in cap:
            reasons.append('capability-query-incomplete')
        elif cap[needed] != 1 or not cap.get('modes', 0) & mode_bit:
            reasons.append(needed + '=0')
        if not cap.get('eids'):
            reasons.append('no-valid-eid')
        active = visible.get(name) == 'ACTIVE' or any(p[1] == 4 for p in cap.get('ports', []))
        if not active:
            reasons.append('link-not-confirmed-active')
        if reasons:
            rejected.append('{}: {}'.format(name, ','.join(reasons)))
        else:
            candidates.append(name)
    # The normal application-visible list is preferred over --all physical
    # devices. Hidden devices remain available for explicit, validated choices.
    if device == 'auto' and not previous.get('device'):
        candidates = [n for n in candidates if visible.get(n) == 'ACTIVE']
    try:
        name = choose_value('DEVICE', candidates, device, previous.get('device'))
    except DiscoveryError as e:
        raise DiscoveryError(str(e) + '\nRejected: ' + '; '.join(rejected))
    driver = devices[name]['driver']
    prior_device = previous if previous.get('device') == name else {}
    matches = [p for p in providers if provider_matches(driver, p['path'])]
    require(matches, 'PROVIDER_NOT_FOUND device={} driver={}; see candidates-raw.tsv'.format(name, driver))
    if provider != 'auto':
        path = str(Path(provider).resolve())
        require(any(p['path'] == path for p in matches), 'PROVIDER_DEVICE_MISMATCH device={} driver={} provider={}'.format(name, driver, path))
    elif prior_device.get('provider'):
        path = prior_device['provider']
        require(any(p['path'] == path for p in matches), 'PROVIDER_PREVIOUS_NO_LONGER_VALID ' + path)
    else:
        rank = min(p['rank'] for p in matches)
        paths = sorted(set(p['path'] for p in matches if p['rank'] == rank))
        by_hash = {}
        for p in paths:
            by_hash.setdefault(digest(p), p)
        require(len(by_hash) == 1, 'PROVIDER_SELECTION_REQUIRED device={} candidates={}'.format(name, paths))
        path = next(iter(by_hash.values()))
    cap = caps[name]
    require(cap.get('max_msg', 0) > 0, 'MAX_MSG_MISSING_OR_ZERO device=' + name)
    e = choose_value('EID', cap['eids'], eid, prior_device.get('eid_index'))
    tp = 'CTP' if mode == 'rm-ctp' else 'RTP'
    table = priorities.get(name, {})
    require(table, 'PRIORITY_TABLE_MISSING device={}; see urma-admin-all.log'.format(name))
    options = [i for i, value in table.items() if value == tp and 0 <= i <= 15]
    pr = choose_value('PRIORITY', options, priority, prior_device.get('priority'))
    return {'device': name, 'driver': driver, 'provider': path, 'mode': mode,
            'eid_index': e, 'eid_address': cap['eids'][e], 'priority': pr,
            'max_read': cap['max_read'], 'max_write': cap.get('max_write', 0),
            'max_msg': cap['max_msg']}


def atomic_json(path, value):
    tmp = path.with_suffix('.tmp')
    tmp.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')
    tmp.replace(path)


def inspect_sdk_link_interfaces(files, out, compiler, flags, env):
    """Record optional public API evidence without executing a probe.

    Header+core linkage does NOT prove provider implementation or connectivity.
    Legacy SDKs can legitimately lack these newer APIs: this is not a gate and
    must not hide the actual device-selection or native import error.
    """
    evidence = {'hardware_exercised': False, 'probes_executed': False,
                'provider_implementation': 'not-verified', 'interfaces': {}}
    for feature, source in (
            ('context_export', 'sdk-context-export-probe.c'),
            ('explicit_bond_context', 'sdk-bond-context-probe.c')):
        log = out / ('sdk-' + feature + '.log')
        command = [str(compiler), '-std=c11', '-O0', '-Werror=implicit-function-declaration'] + list(flags)
        command += ['-I', str(Path(files['api']).parent),
                    str(Path(__file__).with_name(source)), files['core'], files['common'],
                    '-o', str(out / ('sdk-' + feature + '-probe'))]
        try:
            run_command(command, log, env, timeout=60)
            evidence['interfaces'][feature] = {'compile_link': True, 'log': str(log)}
        except DiscoveryError:
            evidence['interfaces'][feature] = {'compile_link': False, 'log': str(log),
                'meaning': 'optional API unavailable or probe build failed; inspect log, not a hardware failure'}
    atomic_json(out / 'sdk-link-interfaces.json', evidence)
    print('SDK_LINK_INTERFACES context_export_compile_link={} explicit_bond_context_compile_link={} '
          'provider=not-verified probes_executed=false report={}'.format(
              evidence['interfaces']['context_export']['compile_link'],
              evidence['interfaces']['explicit_bond_context']['compile_link'],
              out / 'sdk-link-interfaces.json'))
    return evidence


def collect_optional_link_evidence(admin, out, env, feature_path=Path('/sys/bus/ub/ub_feature')):
    """Retain topology/platform context; never infer an interoperable EID pair."""
    topology_log = out / 'urma-admin-topology.log'
    try:
        # Official June-24 admin help: omitted NODE_ID means the current node.
        run_command([str(admin), 'show', 'topo'], topology_log, env, timeout=15)
        topology = 'captured'
    except DiscoveryError:
        topology = 'unavailable-see-log'
    try:
        text = Path(feature_path).read_text(errors='replace')
        features = 'captured'
    except OSError as e:
        text = 'UB_FEATURE_UNAVAILABLE: {}\n'.format(e)
        features = 'unavailable-see-log'
    (out / 'platform-ub-feature.log').write_text(text)
    feature_match = re.search(r'^\s*URMA\s+UTP-UNO\s*([+-])\s*$', text, re.MULTILINE)
    disabled = feature_match.group(1) == '-' if feature_match else None
    print('LINK_CONTEXT topology={} platform_features={} utp_uno_disabled={} '
          'connectivity=not-proven evidence={}'.format(
              topology, features, disabled if disabled is not None else 'unknown', out))
    return {'topology': topology, 'platform_features': features, 'utp_uno_disabled': disabled}


def check_state(old, endpoint, identity, policy):
    if policy == 'reuse':
        require(old is not None, 'NO_CONFIRMED_SELECTION: run 10 first; 30 does not silently select a device')
        require(old.get('endpoint') == endpoint and old.get('identity') == identity,
                'DISCOVERY_CHANGED: saved selection or environment changed; inspect previous-selection.json and proposed-selection.json, then rerun 10 to confirm')


def main():
    parser = argparse.ArgumentParser()
    for name in ('package', 'work', 'run', 'node', 'mode', 'device', 'provider', 'eid', 'priority'):
        parser.add_argument('--' + name, required=True)
    parser.add_argument('--policy', choices=('confirm', 'reuse'), required=True)
    a = parser.parse_args()
    out = Path(a.run) / 'discovery'
    state = Path(a.work) / 'state'
    out.mkdir(parents=True, exist_ok=True)
    state.mkdir(parents=True, exist_ok=True)
    try:
        saved = state / 'device-selection.json'
        old = json.loads(saved.read_text()) if saved.exists() else None
        if old:
            atomic_json(out / 'previous-selection.json', old)
        require(a.policy != 'reuse' or old is not None, 'NO_CONFIRMED_SELECTION: run 10 first; 30 does not silently select a device')
        files = dict(csv.reader((out / 'files.tsv').open(), delimiter='\t'))
        env = os.environ.copy()
        env.pop('LD_PRELOAD', None)
        for k in ('CPATH', 'C_INCLUDE_PATH', 'CPLUS_INCLUDE_PATH', 'LIBRARY_PATH'):
            env.pop(k, None)
        env['LC_ALL'] = 'C'
        env['LD_LIBRARY_PATH'] = ':'.join(sorted(set(str(Path(files[k]).parent) for k in ('core', 'common'))))
        providers = []
        for row in csv.reader((out / 'candidates-raw.tsv').open(), delimiter='\t'):
            if row[0] == 'provider':
                providers.append({'rank': int(row[1]), 'path': row[2]})
        # Verify core architecture and dependencies before invoking vendor tools.
        for kind in ('core', 'common', 'admin', 'perftest'):
            path = Path(files[kind])
            header = path.read_bytes()[:20]
            require(header[:4] == b'\x7fELF' and len(header) == 20 and
                    int.from_bytes(header[18:20], 'little' if header[5] == 1 else 'big') == 183,
                    'WRONG_ARCH_OR_NON_ELF ' + str(path))
            text = run_command(['ldd', str(path)], out / (kind + '-ldd.log'), env)
            require('not found' not in text, 'UNRESOLVED_DEPENDENCY ' + kind)
        versions = {}
        if shutil.which('rpm'):
            for kind in ('core', 'common', 'api'):
                versions[kind] = run_command(['rpm', '-qf', '--qf', '%{VERSION}-%{RELEASE}\n', files[kind]], out / (kind + '-rpm.log'), env).strip()
            require(len(set(versions.values())) == 1, 'SDK_VERSION_MIX ' + str(versions))
        link_evidence = collect_optional_link_evidence(files['admin'], out, env)
        devices = {}
        for p in sorted(Path('/sys/class/ubcore').glob('*')):
            if not (p / 'ubdev').is_file() or not (p / 'driver_name').is_file():
                continue
            name = (p / 'ubdev').read_text().strip()
            driver = (p / 'driver_name').read_text().strip()
            if not re.fullmatch(r'[A-Za-z0-9_.-]+', name) or name == 'ubcore' or any(x in (name + driver).lower() for x in ('vdev', 'openurma')):
                continue
            devnode = Path('/dev/uburma') / name
            devices[name] = {'driver': driver, 'sysfs': str(p), 'read_write':
                             devnode.exists() and stat.S_ISCHR(devnode.stat().st_mode) and os.access(devnode, os.R_OK | os.W_OK)}
        atomic_json(out / 'devices.json', devices)
        require(devices, 'NO_REAL_UB_DEVICE: no valid ubdev + driver_name entries; inspect driver registration and /dev/uburma')
        show = run_command([files['admin'], 'show'], out / 'urma-admin-show.log', env)
        all_text = run_command([files['admin'], 'show', '--all', '--whole'], out / 'urma-admin-all.log', env)
        help_text = run_command([files['perftest'], 'read_bw', '-h'], out / 'perftest-help.log', env, accepted=(0, 1, 255))
        for flag in ('--eid_idx', '--bind_ip', '--priority', '--share_jfr'):
            require(flag in help_text, 'PERFTEST_OPTION_MISSING ' + flag)
        if a.mode == 'rm-ctp':
            require('--ctp' in help_text, 'PERFTEST_OPTION_MISSING --ctp')
        tc = Path(a.package) / 'dependencies/native-toolchain'
        cc = tc / 'usr/bin/gcc'
        compile_env = env.copy()
        flags = []
        if cc.is_file():
            libs = ':'.join(str(tc / p) for p in ('usr/lib64', 'usr/lib', 'lib64', 'lib'))
            compile_env['LD_LIBRARY_PATH'] = libs
            compile_env['LIBRARY_PATH'] = libs
            inc = run_command([str(cc), '-print-file-name=include'], out / 'compiler-include.log', compile_env).strip()
            require(Path(inc).is_dir(), 'COMPILER_INTERNAL_HEADERS_MISSING ' + inc)
            flags = ['-nostdinc', '-isystem', inc, '-isystem', str(tc / 'usr/include')]
        else:
            cc = shutil.which('gcc') or shutil.which('cc')
            require(cc, 'CAP_PROBE_COMPILER_MISSING: bundled native-toolchain and system C compiler both absent')
        inspect_sdk_link_interfaces(files, out, cc, flags, compile_env)
        probe = out / 'device-probe'
        run_command([str(cc), '-std=c11', '-O0'] + flags + ['-I', str(Path(files['api']).parent),
                    str(Path(__file__).with_name('device-probe.c')), files['core'], files['common'], '-o', str(probe)],
                    out / 'cap-build.log', compile_env, timeout=60)
        cap_text = run_command([str(probe)] + sorted(devices), out / 'cap-query.log', env, accepted=(0, 1))
        caps, priorities, visible = parse_probe(cap_text), parse_admin(all_text), parse_visible(show)
        inventory = {'devices': devices, 'caps': caps, 'priorities': priorities, 'visible': visible, 'providers': providers}
        atomic_json(out / 'inventory.json', inventory)
        print('DEVICE_CANDIDATES node=' + a.node)
        for name, meta in devices.items():
            cap = caps.get(name, {})
            print('{} driver={} visible={} rc_rtp={} rm_ctp={} eids={} priorities={}'.format(
                name, meta['driver'], visible.get(name, 'not-in-default-list'), cap.get('rc_rtp', 'unknown'),
                cap.get('rm_ctp', 'unknown'), cap.get('eids', {}), priorities.get(name, {})))
        for line in candidate_config_lines(a.node, devices, caps, priorities, visible, providers, a.mode):
            print(line)
        endpoint = select_endpoint(devices, caps, priorities, visible, providers, a.mode, a.device,
                                   a.provider, a.eid, a.priority, old.get('endpoint') if old else None)
        if endpoint['driver'] in ('ub_agg', 'ubagg') and link_evidence['utp_uno_disabled']:
            print('WARN UBAGG_INTERNAL_UTP_UNO_DISABLED: application mode is {}, but the fixed '
                  'June-24 basic aggregate metadata exchange uses internal UM/UTP. '
                  'This is a compatibility clue, not an interoperability result; '
                  'inspect platform-ub-feature.log, topology and actual native errors. '
                  'No feature bit or driver was modified.'.format(a.mode))
        if endpoint['driver'] in ('ub_agg', 'ubagg') and a.mode == 'rc-rtp':
            require('--single_path' in help_text, 'PERFTEST_OPTION_MISSING --single_path; --bond_level is not an equivalent substitute')
        provider = endpoint['provider']
        loaded = {str(Path(line.split('\t', 1)[1]).resolve())
                  for line in cap_text.splitlines() if line.startswith('MAP\t')}
        require(str(Path(provider).resolve()) in loaded,
                'PROVIDER_NOT_LOADED selected={} loaded={}; public_v25_12_loader_directory={}; '
                'LD_LIBRARY_PATH resolves dependencies, not the public loader scan directory. '
                'Inspect cap-query.log and vendor installation layout; the B103 vendor loader may differ. '
                'No provider files or system symlinks were changed.'.format(
                    provider, sorted(loaded), Path(files['core']).parent / 'urma'))
        for kind in ('core', 'common'):
            require(str(Path(files[kind]).resolve()) in loaded,
                    'SDK_LOADER_MISMATCH kind={} selected={} loaded={}'.format(kind, files[kind], sorted(loaded)))
        env['LD_LIBRARY_PATH'] += ':' + str(Path(provider).parent)
        text = run_command(['ldd', provider], out / 'provider-ldd.log', env)
        require('not found' not in text, 'UNRESOLVED_PROVIDER_DEPENDENCY ' + provider)
        name = endpoint['device']
        identity = {'kernel': platform.release(), 'files': {k: {'path': p, 'sha256': digest(p)} for k, p in files.items()},
                    'provider_sha256': digest(provider), 'sdk_headers': {str(p): digest(p) for p in sorted(Path(files['api']).parent.rglob('*.h'))},
                    'capabilities': caps[name], 'priorities': priorities[name], 'device': devices[name],
                    'visible': visible.get(name), 'versions': versions}
        # Normalize integer dict keys to JSON keys before comparisons with disk.
        proposed = json.loads(json.dumps({'schema': 1, 'endpoint': endpoint, 'identity': identity}))
        atomic_json(out / 'proposed-selection.json', proposed)
        check_state(old, proposed['endpoint'], proposed['identity'], a.policy)
        if a.policy == 'confirm':
            atomic_json(saved, proposed)
        values = {'DEVICE': name, 'PROVIDER_LIB': provider, 'PROVIDER_DIR': str(Path(provider).parent),
                  'URMA_LIB': files['core'], 'URMA_COMMON_LIB': files['common'], 'URMA_INCLUDE': str(Path(files['api']).parent),
                  'URMA_COMMON_INCLUDE': str(Path(files['api']).parent), 'URMA_ADMIN': files['admin'], 'URMA_PERFTEST': files['perftest'],
                  'UMDK_LABEL': str(versions), 'EID': endpoint['eid_index'], 'PRIORITY': endpoint['priority'],
                  'MAX_READ': endpoint['max_read'], 'MAX_WRITE': endpoint['max_write'],
                  'MAX_MSG': endpoint['max_msg']}
        content = ''.join(k + '=' + shlex.quote(str(v)) + '\n' for k, v in values.items())
        tmp = state / 'detected-umdk.env.tmp'
        tmp.write_text(content)
        tmp.replace(state / 'detected-umdk.env')
        (out / 'selected.env').write_text(content)
        message = 'DEVICE_SELECTION_{} node={} device={} provider={} mode={} eid_index={} eid={} priority={}'.format(
            'CONFIRMED' if a.policy == 'confirm' else 'REVALIDATED', a.node, name, provider, a.mode,
            endpoint['eid_index'], endpoint['eid_address'], endpoint['priority'])
        print(message)
        (out / 'selection.log').write_text(message + '\n')
    except (DiscoveryError, OSError, ValueError, KeyError) as e:
        message = 'DISCOVERY_FAIL node={} {}\nEvidence={}\nOnly ambiguous fields need config overrides; original package and drivers were not modified.'.format(a.node, e, out)
        (out / 'failure.log').write_text(message + '\n')
        print(message, file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
