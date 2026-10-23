#!/usr/bin/env bash
# Guest-only lab configuration. Never invoke on a production host.
set -Eeuo pipefail
ip=$1 mac=$2 node=$3
iface=
for f in /sys/class/net/*/address; do
  [[ $(< "$f") != "$mac" ]] || iface=${f%/address}
done
[[ -n $iface ]]; iface=${iface##*/}
if command -v nmcli >/dev/null && systemctl is-active NetworkManager >/dev/null; then
  if ! nmcli connection show standard-lab >/dev/null 2>&1; then
    nmcli connection add type ethernet ifname "$iface" con-name standard-lab
  fi
  nmcli connection modify standard-lab ipv4.method manual ipv4.addresses "$ip/24" \
    ipv4.never-default yes ipv6.method disabled connection.autoconnect yes connection.autoconnect-priority 999
  nmcli connection up standard-lab
else
  ip link set "$iface" up
  ip addr replace "$ip/24" dev "$iface"
fi
if command -v firewall-cmd >/dev/null && firewall-cmd --state >/dev/null 2>&1; then
  firewall-cmd --zone=trusted --add-interface="$iface"
fi
hostnamectl set-hostname "standard-vm-$node"
mkdir -p /opt/benchmark-package /mnt/ub-benchmark-shared
mountpoint -q /opt/benchmark-package || mount -t 9p -o trans=virtio,version=9p2000.L,ro,cache=none runtime /opt/benchmark-package
mountpoint -q /mnt/ub-benchmark-shared || mount -t 9p -o trans=virtio,version=9p2000.L,cache=none benchmark /mnt/ub-benchmark-shared
id; uname -a; cat /proc/sys/kernel/random/boot_id; ip -brief addr
