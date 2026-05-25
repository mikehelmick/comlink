#!/bin/bash
# Local-process comlink-kvd cluster orchestrator.
#
# Spins up N kvd processes on 127.0.0.1 ports for soak / chaos
# / migration demos without the kind+Docker overhead. Each replica
# runs as a background process with its own DataDir; identities
# are deterministic SHA-256 of "kvd-<ordinal>" so the substrate
# membership matches across restarts.
#
# Subcommands:
#   up [N]              — start N replicas (default 3). Founder
#                         is ordinal 0; joiners are 1..N-1.
#   down                — kill every replica + cleanup.
#   status              — print per-replica health + node owner.
#   migrate ORDINAL     — kill replica ORDINAL, restart on a
#                         NEW transport port against the same
#                         DataDir. Survivors get UpdatePeerAddr
#                         via the kvd binary's restart picking up
#                         the new port (or via curl in this
#                         script if/when /admin/peers is added).
#   logs ORDINAL        — tail -f the replica's log.
#
# Layout under STATE_DIR (default /tmp/comlink-cluster/):
#   data/<ordinal>/                  — COMLINK_DATA_DIR per replica
#   logs/<ordinal>.log               — combined stdout+stderr
#   state.env                        — cluster-wide env (CONVID, BASE_PORTS)
#   pids/<ordinal>.pid               — PID for kill
#   ports/<ordinal>.transport        — current listen port
#   ports/<ordinal>.http             — current HTTP port

set -euo pipefail

STATE_DIR="${COMLINK_CLUSTER_DIR:-/tmp/comlink-cluster}"
KVD_BIN="${COMLINK_KVD:-$(go env GOPATH)/bin/comlink-kvd}"
DEFAULT_REPLICAS=3

# Deterministic 32-hex-char ReplicaID from "kvd-<ordinal>".
self_for() {
    printf '%s' "kvd-$1" | shasum -a 256 | cut -c1-32
}

ensure_dirs() {
    mkdir -p "$STATE_DIR"/{data,logs,pids,ports}
}

ensure_binary() {
    if [ ! -x "$KVD_BIN" ]; then
        echo "comlink-kvd not found at $KVD_BIN — building..." >&2
        ( cd "$(dirname "$0")/../.." && go install ./examples/kvstore/cmd/comlink-kvd )
    fi
}

# Compute the COMLINK_KV_MEMBERS env value: comma-separated list
# of every replica's deterministic ReplicaID, for the substrate
# membership list. Must match across all replicas.
kv_members_list() {
    local n=$1
    local out=""
    local i=0
    while [ "$i" -lt "$n" ]; do
        local mem
        mem="$(self_for "$i")"
        if [ -z "$out" ]; then
            out="$mem"
        else
            out="$out,$mem"
        fi
        i=$((i + 1))
    done
    printf '%s' "$out"
}

# Start a single replica. Args:
#   ordinal              — integer index
#   transport_port       — TCP port for cluster transport
#   http_port            — TCP port for HTTP front-end
#   n_replicas           — total replicas (drives KV_MEMBERS)
#   sponsor_addr         — empty for founder; "<self>@<host>:<port>" for joiners
start_one() {
    local ord=$1 tp=$2 hp=$3 n=$4 sponsor=$5
    local self="$(self_for "$ord")"
    local data="$STATE_DIR/data/$ord"
    local log="$STATE_DIR/logs/$ord.log"
    local pidfile="$STATE_DIR/pids/$ord.pid"

    mkdir -p "$data"

    # Founder: force-bootstrap on FIRST start, recover otherwise.
    local bootstrap_env=""
    local members_env=""
    local sponsor_env=""
    if [ -z "$sponsor" ]; then
        members_env="COMLINK_MEMBERS=$self"
        if [ ! -f "$data/cluster_state/comlink.cluster_id" ]; then
            bootstrap_env="COMLINK_BOOTSTRAP_FORCE=true"
        fi
    else
        sponsor_env="COMLINK_TRANSPORT_SPONSORS=$sponsor"
    fi

    # KV substrate membership — every replica must agree on the
    # same list. Deterministic per ordinal so restart preserves it.
    local kv_members
    kv_members="$(kv_members_list "$n")"

    # Hardcoded ConversationID for the substrate so all replicas
    # use the same substrate ID across restarts.
    local conv_id="0000000000000000000000000000abcd"

    env \
        COMLINK_SELF="$self" \
        COMLINK_DATA_DIR="$data" \
        COMLINK_TRANSPORT_LISTEN="127.0.0.1:$tp" \
        COMLINK_KV_HTTP="127.0.0.1:$hp" \
        COMLINK_KV_CONVID="$conv_id" \
        COMLINK_KV_MEMBERS="$kv_members" \
        $members_env \
        $bootstrap_env \
        $sponsor_env \
        "$KVD_BIN" >>"$log" 2>&1 &

    local pid=$!
    echo "$pid" >"$pidfile"
    echo "$tp" >"$STATE_DIR/ports/$ord.transport"
    echo "$hp" >"$STATE_DIR/ports/$ord.http"
    echo "[start] ordinal=$ord pid=$pid transport=$tp http=$hp self=${self:0:8}..."
}

# Wait for a replica's HTTP endpoint to respond on /cluster/info.
wait_http_ready() {
    local hp=$1 timeout=${2:-15}
    local deadline=$(($(date +%s) + timeout))
    while [ "$(date +%s)" -lt "$deadline" ]; do
        if curl -sf -o /dev/null --max-time 1 "http://127.0.0.1:$hp/cluster/info"; then
            return 0
        fi
        sleep 0.2
    done
    return 1
}

cmd_up() {
    local n="${1:-$DEFAULT_REPLICAS}"
    ensure_binary
    ensure_dirs

    if [ -n "$(ls "$STATE_DIR/pids/" 2>/dev/null)" ]; then
        echo "cluster appears to be running; run 'down' first" >&2
        exit 1
    fi

    local base_transport=7100
    local base_http=8100
    local founder_addr="127.0.0.1:$base_transport"
    local founder_self="$(self_for 0)"

    # Founder first.
    start_one 0 "$base_transport" "$base_http" "$n" ""
    if ! wait_http_ready "$base_http" 20; then
        echo "founder HTTP never came up — check $STATE_DIR/logs/0.log" >&2
        exit 1
    fi
    echo "[up] founder ready at http://127.0.0.1:$base_http"

    # Joiners.
    local i=1
    while [ "$i" -lt "$n" ]; do
        local tp=$((base_transport + i))
        local hp=$((base_http + i))
        local sponsor="$founder_self@$founder_addr"
        start_one "$i" "$tp" "$hp" "$n" "$sponsor"
        if ! wait_http_ready "$hp" 30; then
            echo "joiner $i HTTP never came up — check $STATE_DIR/logs/$i.log" >&2
            exit 1
        fi
        echo "[up] joiner $i ready at http://127.0.0.1:$hp"
        i=$((i + 1))
    done

    echo
    echo "cluster up — $n replicas. Soak target: http://127.0.0.1:$base_http"
    echo "logs:    tail -F $STATE_DIR/logs/*.log"
    echo "status:  $0 status"
    echo "down:    $0 down"
}

cmd_down() {
    if [ ! -d "$STATE_DIR" ]; then
        echo "no cluster state at $STATE_DIR"
        return 0
    fi
    if [ -d "$STATE_DIR/pids" ]; then
        for pf in "$STATE_DIR"/pids/*.pid; do
            [ -f "$pf" ] || continue
            local pid
            pid="$(cat "$pf")"
            if kill -0 "$pid" 2>/dev/null; then
                kill "$pid" 2>/dev/null || true
                # Wait briefly for clean shutdown.
                for _ in 1 2 3 4 5; do
                    kill -0 "$pid" 2>/dev/null || break
                    sleep 0.2
                done
                kill -9 "$pid" 2>/dev/null || true
            fi
            rm -f "$pf"
        done
    fi
    rm -rf "$STATE_DIR"
    echo "cluster torn down; state dir $STATE_DIR removed"
}

cmd_status() {
    if [ ! -d "$STATE_DIR/pids" ]; then
        echo "no cluster running"
        return 0
    fi
    printf "%-4s %-8s %-10s %-10s %-6s %-12s %s\n" "ORD" "PID" "TRANSPORT" "HTTP" "ALIVE" "MEMBERS" "SELF"
    for pf in "$STATE_DIR"/pids/*.pid; do
        [ -f "$pf" ] || continue
        local ord
        ord="$(basename "$pf" .pid)"
        local pid tp hp
        pid="$(cat "$pf")"
        tp="$(cat "$STATE_DIR/ports/$ord.transport" 2>/dev/null || echo ?)"
        hp="$(cat "$STATE_DIR/ports/$ord.http" 2>/dev/null || echo ?)"
        local alive=no
        kill -0 "$pid" 2>/dev/null && alive=yes
        local members="?"
        local self="?"
        if [ "$alive" = "yes" ]; then
            local info
            info="$(curl -sf --max-time 2 "http://127.0.0.1:$hp/cluster/info" 2>/dev/null || echo '')"
            if [ -n "$info" ]; then
                members="$(printf '%s' "$info" | sed -E 's/.*"membership_n":([0-9]+).*/\1/')"
                self="$(printf '%s' "$info" | sed -E 's/.*"self":"([^"]+)".*/\1/' | cut -c1-8)..."
            fi
        fi
        printf "%-4s %-8s %-10s %-10s %-6s %-12s %s\n" "$ord" "$pid" "$tp" "$hp" "$alive" "$members" "$self"
    done
}

# migrate ORDINAL: simulates "drain node, reschedule pod" by
# killing the replica's process and restarting it on a NEW
# transport port against the SAME DataDir. The HTTP port is
# also bumped (mostly to keep the demo legible — apps could
# reuse it).
cmd_migrate() {
    local ord="${1:?usage: migrate ORDINAL}"
    if [ ! -f "$STATE_DIR/pids/$ord.pid" ]; then
        echo "no replica $ord running" >&2
        exit 1
    fi
    local pid old_tp old_hp
    pid="$(cat "$STATE_DIR/pids/$ord.pid")"
    old_tp="$(cat "$STATE_DIR/ports/$ord.transport")"
    old_hp="$(cat "$STATE_DIR/ports/$ord.http")"
    echo "[migrate] killing ordinal=$ord pid=$pid (transport=$old_tp, http=$old_hp)"
    kill "$pid" 2>/dev/null || true
    for _ in 1 2 3 4 5 6 7 8 9 10; do
        kill -0 "$pid" 2>/dev/null || break
        sleep 0.2
    done
    kill -9 "$pid" 2>/dev/null || true
    rm -f "$STATE_DIR/pids/$ord.pid"

    # NEW ports — bump by 1000 to clearly distinguish from
    # the original allocations.
    local new_tp=$((old_tp + 1000))
    local new_hp=$((old_hp + 1000))

    # Total replicas: count pid files PLUS this one (we just removed ours).
    local n
    n=$(($(ls "$STATE_DIR/pids/" 2>/dev/null | wc -l | tr -d ' ') + 1))

    # Sponsor for the restart: pick the lowest surviving ordinal
    # as the sponsor. (Founder may have moved too; this picks
    # whoever's lowest.)
    local sponsor=""
    if [ "$ord" != "0" ]; then
        # Use ordinal 0 as sponsor if alive.
        if [ -f "$STATE_DIR/pids/0.pid" ]; then
            local s0_self s0_tp
            s0_self="$(self_for 0)"
            s0_tp="$(cat "$STATE_DIR/ports/0.transport")"
            sponsor="$s0_self@127.0.0.1:$s0_tp"
        fi
    fi
    # If migrating the founder, leave sponsor empty — the founder
    # recovers from disk + persisted ClusterID; no fresh bootstrap.

    echo "[migrate] restarting ordinal=$ord on NEW ports transport=$new_tp http=$new_hp"
    start_one "$ord" "$new_tp" "$new_hp" "$n" "$sponsor"
    if ! wait_http_ready "$new_hp" 20; then
        echo "ordinal $ord HTTP never came up after migration — check $STATE_DIR/logs/$ord.log" >&2
        exit 1
    fi

    echo
    echo "[migrate] ordinal=$ord restarted: http://127.0.0.1:$new_hp"
    echo "[migrate] NOTE: survivors' persisted membership still points at $old_tp."
    echo "[migrate] In a production K8s deployment, headless-service DNS"
    echo "[migrate] would re-resolve. For this local demo, restart-on-same-port"
    echo "[migrate] avoids the address-change handshake — see developer guide."
}

cmd_logs() {
    local ord="${1:?usage: logs ORDINAL}"
    local log="$STATE_DIR/logs/$ord.log"
    if [ ! -f "$log" ]; then
        echo "no log at $log" >&2
        exit 1
    fi
    tail -F "$log"
}

case "${1:-}" in
    up)       shift; cmd_up "$@" ;;
    down)     shift; cmd_down "$@" ;;
    status)   shift; cmd_status "$@" ;;
    migrate)  shift; cmd_migrate "$@" ;;
    logs)     shift; cmd_logs "$@" ;;
    *)
        cat >&2 <<EOF
usage: $0 {up [N] | down | status | migrate ORDINAL | logs ORDINAL}

  up [N]            start N replicas (default $DEFAULT_REPLICAS) on
                    127.0.0.1:710X (transport) and :810X (HTTP).
                    State under $STATE_DIR.
  down              kill all replicas and remove state.
  status            per-replica health + members view.
  migrate ORDINAL   kill replica ORDINAL, restart on NEW ports
                    against the same DataDir (simulates a K8s pod
                    reschedule to a different node).
  logs ORDINAL      tail -F the replica's log.

env:
  COMLINK_CLUSTER_DIR  state dir (default /tmp/comlink-cluster)
  COMLINK_KVD          path to the kvd binary
                       (default \$GOPATH/bin/comlink-kvd)
EOF
        exit 2
        ;;
esac
