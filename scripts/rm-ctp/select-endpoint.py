#!/usr/bin/env python3
"""Parse the installed urma_admin output; never assume EID 0 or accept any TP."""
import argparse
import json
import re
from pathlib import Path


def devices(text):
    return sorted({f[1] for line in text.splitlines()
                   if len(f := line.split()) >= 4 and f[0].isdigit()
                   and f[-1].upper() == 'ACTIVE'})


def provider_for_device(device_table, candidate_table, device):
    """Select a provider using only the chosen device, not all sysfs devices."""
    rows = [line.split('\t') for line in device_table.splitlines()[1:]]
    drivers = {row[2] for row in rows if len(row) >= 3 and row[1] == device}
    if len(drivers) != 1:
        raise ValueError(f'DEVICE_DRIVER_SELECTION_REQUIRED device={device} drivers={sorted(drivers)}')
    driver = drivers.pop().lower()
    if driver in ('ub_agg', 'ubagg'):
        provider_kind = 'ubagg'
    elif driver == 'udma':
        provider_kind = 'udma'
    else:
        raise ValueError(f'PROVIDER_DRIVER_UNKNOWN device={device} driver={driver}; use --provider-lib')
    candidates = []
    for line in candidate_table.splitlines():
        parts = line.split('\t')
        if len(parts) >= 3 and parts[0] == 'provider':
            try:
                candidates.append((int(parts[1]), parts[2]))
            except ValueError:
                raise ValueError(f'INVALID_PROVIDER_RANK: {parts[1]}') from None
    if not candidates:
        raise ValueError('PROVIDER_CANDIDATES_MISSING')
    matching = [(rank, path) for rank, path in candidates
                if re.match(r'liburma_' + provider_kind + r'\.so(?:\.|$)',
                            Path(path).name.lower().replace('-', '_'))]
    if not matching:
        raise ValueError(f'PROVIDER_SELECTION_REQUIRED device={device} driver={driver} '
                         'candidates=[]; use --provider-lib')
    best_rank = min(rank for rank, _ in matching)
    matches = sorted({path for rank, path in matching if rank == best_rank})
    if len(matches) != 1:
        raise ValueError(f'PROVIDER_SELECTION_REQUIRED device={device} driver={driver} '
                         f'candidates={matches}; use --provider-lib')
    return matches[0]


def parameters(text, device, eid='auto', priority='auto'):
    selected = False
    transport_ub = False
    rm_supported = False
    eids = []
    columns = []
    ctp = []
    for line in text.splitlines():
        match = re.match(r'\s*name\s*:\s*(\S+)', line)
        if match:
            selected = match[1] == device
        if not selected:
            continue
        match = re.match(r'\s*transport_type\s*:\s*(.*)', line)
        if match:
            transport_ub = bool(re.search(r'\bUB\b', match[1]))
        match = re.match(r'\s*trans_mode\s*:\s*(.*)', line)
        if match:
            rm_supported = bool(re.search(r'\bRM\b', match[1]))
        match = re.match(r'\s*eid(\d+)\s*:\s*([0-9a-fA-F:]+)', line)
        if match and int(match[2].replace(':', ''), 16):
            eids.append(int(match[1]))
        match = re.match(r'\s*priority\s*:\s*(.*)', line)
        if match:
            columns = [int(x) for x in match[1].split()]
        match = re.match(r'\s*tp_type\s*:\s*(.*)', line)
        if match:
            values = match[1].split()
            if len(values) != len(columns) or len(set(columns)) != len(columns):
                raise ValueError('PRIORITY_TABLE_INVALID')
            ctp = [p for p, tp in zip(columns, values) if tp == 'CTP' and 0 <= p <= 15]
    if not transport_ub:
        raise ValueError(f'UB_TRANSPORT_REQUIRED device={device}')
    if not rm_supported:
        raise ValueError(f'RM_MODE_REQUIRED device={device}')
    def choose(label, options, requested):
        options = sorted(set(options))
        if requested != 'auto':
            value = int(requested)
            if value not in options:
                raise ValueError(f'{label}_INVALID requested={value} candidates={options}')
            return value
        # Priority is a device/site policy, not a universal URMA constant.
        # Match the current dual-node delivery: do not guess between 6 and 7.
        if len(options) != 1:
            raise ValueError(f'{label}_SELECTION_REQUIRED candidates={options}; specify once, then saved')
        return options[0]
    return choose('EID', eids, eid), choose('CTP_PRIORITY', ctp, priority)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--show')
    p.add_argument('--all')
    p.add_argument('--devices-table')
    p.add_argument('--candidates-table')
    p.add_argument('--provider-for-device', action='store_true')
    p.add_argument('--device', default='auto')
    p.add_argument('--eid', default='auto')
    p.add_argument('--priority', default='auto')
    a = p.parse_args()
    if a.provider_for_device:
        if not (a.devices_table and a.candidates_table and a.device != 'auto'):
            p.error('--provider-for-device requires --devices-table, --candidates-table and --device')
        print(provider_for_device(Path(a.devices_table).read_text(),
                                  Path(a.candidates_table).read_text(), a.device))
        return
    if not a.show:
        p.error('--show is required for endpoint selection')
    available = devices(Path(a.show).read_text())
    if a.device == 'auto':
        if len(available) != 1:
            raise ValueError(f'DEVICE_SELECTION_REQUIRED active={available}; use --device')
        device = available[0]
    else:
        device = a.device
    if not re.fullmatch(r'[A-Za-z0-9_.-]+', device):
        raise ValueError('INVALID_DEVICE_NAME')
    if not a.all:
        print(device)
    else:
        eid, priority = parameters(Path(a.all).read_text(), device, a.eid, a.priority)
        print(json.dumps({'device': device, 'eid': eid, 'priority': priority}))


if __name__ == '__main__':
    try:
        main()
    except (ValueError, OSError) as e:
        raise SystemExit(str(e))
