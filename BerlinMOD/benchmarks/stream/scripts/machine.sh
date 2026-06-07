#!/usr/bin/env bash
# Emit the markdown "Machine" block describing the host the benchmark runs on, so
# the timings docs reflect whoever ran them. Re-run on your machine and paste the
# output into the doc's Machine block:
#   bash scripts/machine.sh
set -euo pipefail

cpu=$(grep -m1 'model name' /proc/cpuinfo 2>/dev/null | cut -d: -f2- | sed 's/^ *//' || echo unknown)
phys=$(grep -m1 'cpu cores' /proc/cpuinfo 2>/dev/null | awk '{print $4}')
log=$(grep -c ^processor /proc/cpuinfo 2>/dev/null || nproc)
cores="${phys:+${phys} cores / }${log} threads"
ram=$(free -h 2>/dev/null | awk 'NR==2{print $2}' || echo unknown)
os=$( . /etc/os-release 2>/dev/null && echo "$PRETTY_NAME" || uname -s)
kern=$(uname -r)
virt="bare metal"
grep -qi microsoft /proc/version 2>/dev/null && virt="WSL2 (memory-capped VM, shared host)"
jver=$(java -version 2>&1 | head -1 | sed 's/ version//; s/"//g')

printf -- '- **CPU** — %s (%s)\n' "$cpu" "$cores"
printf -- '- **Memory** — %s\n' "$ram"
printf -- '- **OS** — %s, kernel `%s`, %s\n' "$os" "$kern" "$virt"
printf -- '- **Runtime** — %s\n' "$jver"
printf -- '- **libmeos** — built `-DMEOS=ON -DCBUFFER=ON -DNPOINT=ON -DPOSE=ON -DRGEO=ON`\n'
