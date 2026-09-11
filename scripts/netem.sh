#!/usr/bin/env bash
set -euo pipefail

# Apply or remove an explicitly supplied traffic-control profile. Record every
# invocation alongside benchmark raw data; this script never invents WAN values.
# Usage:
#   sudo scripts/netem.sh apply eth0 25ms 3ms 0.1%
#   sudo scripts/netem.sh clear eth0

action=${1:?apply or clear required}
device=${2:?network device required}

case "$action" in
  apply)
    delay=${3:?delay required, e.g. 25ms}
    jitter=${4:?jitter required, e.g. 3ms}
    loss=${5:?loss required, e.g. 0.1%}
    tc qdisc replace dev "$device" root netem delay "$delay" "$jitter" distribution normal loss "$loss"
    ;;
  clear)
    tc qdisc del dev "$device" root 2>/dev/null || true
    ;;
  *)
    echo "unknown action: $action" >&2
    exit 2
    ;;
esac
