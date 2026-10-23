#!/usr/bin/env bash
# Isolated VM lab. New qcow2 overlays; no host bridge/firewall changes.
set -Eeuo pipefail
HERE=$(cd "$(dirname "$0")" && pwd -P)
LAB=${1:?usage: start-two-vm-lab.sh NEW_LAB_DIR PREPARED_IMAGE PACKAGE_DIR}
IMAGE=${2:?}; PACKAGE=${3:?}
KEY=${VM_SSH_KEY:-/home/xinzhang/vm/keys/codex_vm_ed25519}
SEED=${VM_SEED:-/home/xinzhang/vm/seed/seed.iso}
[[ $LAB == /* && ! -e $LAB ]] || { echo 'Use a new absolute lab directory'; exit 1; }
[[ -f $IMAGE && -f $KEY && -d $PACKAGE/runtime/spark ]] || exit 1
mkdir -p "$LAB"/{A,B,shared}
chmod 1777 "$LAB/shared"
exec > >(tee "$LAB/start.log") 2>&1
for port in 2235 2236 24100; do
  [[ -z $(ss -H -ltn "sport = :$port") ]] || { echo "Port occupied: $port"; exit 1; }
done
for node in A B; do
  dir=$LAB/$node
  qemu-img create -f qcow2 -F qcow2 -b "$IMAGE" "$dir/overlay.qcow2"
  cp /usr/share/edk2/aarch64/vars-template-pflash.raw "$dir/vars.raw"
  port=2235; link='listen=127.0.0.1:24100'; mac=52:54:00:76:00:11
  [[ $node == A ]] || { port=2236; link='connect=127.0.0.1:24100'; mac=52:54:00:76:00:12; }
  /usr/local/bin/qemu-system-aarch64 -name "standard-two-$node" \
    -machine virt,accel=kvm,gic-version=3 -cpu host -smp 8 -m 16384 \
    -drive if=pflash,format=raw,readonly=on,file=/usr/share/edk2/aarch64/QEMU_EFI-pflash.raw \
    -drive if=pflash,format=raw,file="$dir/vars.raw" \
    -drive if=virtio,format=qcow2,file="$dir/overlay.qcow2",cache=none,aio=native \
    -drive if=virtio,format=raw,file="$SEED",readonly=on \
    -netdev user,id=management,restrict=on,hostfwd=tcp:127.0.0.1:$port-:22 \
    -device virtio-net-pci,netdev=management \
    -netdev socket,id=cluster,"$link" -device virtio-net-pci,netdev=cluster,mac="$mac" \
    -virtfs local,path="$PACKAGE",mount_tag=runtime,security_model=none,readonly=on,id=runtime \
    -virtfs local,path="$LAB/shared",mount_tag=benchmark,security_model=mapped-xattr,id=benchmark \
    -device virtio-rng-pci -display none -serial file:"$dir/console.log" \
    -monitor unix:"$dir/monitor.sock",server=on,wait=off -pidfile "$dir/qemu.pid" -daemonize
done
ssh_vm() { local port=$1; shift; ssh -i "$KEY" -p "$port" -o BatchMode=yes -o ConnectTimeout=3 -o StrictHostKeyChecking=accept-new -o UserKnownHostsFile="$LAB/known_hosts" xinzhang@127.0.0.1 "$@"; }
for node in A B; do
  port=2235; ip=192.168.76.11; mac=52:54:00:76:00:11
  [[ $node == A ]] || { port=2236; ip=192.168.76.12; mac=52:54:00:76:00:12; }
  for ((i=0;i<120;i++)); do ssh_vm "$port" true && break; sleep 2; done
  ssh_vm "$port" sudo bash -s -- "$ip" "$mac" "$node" < "$HERE/prepare-vm-node.sh"
done
ssh_vm 2235 ping -c 2 192.168.76.12
# The guest-to-guest key is created for this lab only, not copied from host credentials.
ssh_vm 2235 'mkdir -p /home/xinzhang/.ssh; ssh-keygen -q -t ed25519 -N "" -f /home/xinzhang/.ssh/standard-lab-key'
ssh_vm 2235 'cat /home/xinzhang/.ssh/standard-lab-key.pub' | ssh_vm 2236 'umask 077; mkdir -p /home/xinzhang/.ssh; tee -a /home/xinzhang/.ssh/authorized_keys >/dev/null'
ssh_vm 2235 'ssh -i /home/xinzhang/.ssh/standard-lab-key -o StrictHostKeyChecking=accept-new xinzhang@192.168.76.12 hostname'
echo "TWO_VM_LAB_READY lab=$LAB SSH_A=2235 SSH_B=2236"
