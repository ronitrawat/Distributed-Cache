#!/usr/bin/env bash
set -euo pipefail

build_dir=${1:-"$(cd "$(dirname "$0")/../build" && pwd)"}
operations_per_client=${OPS_PER_CLIENT:-2000}
get_percent=${GET_PERCENT:-80}
key_count=${KEY_COUNT:-512}
base_port=${BASE_PORT:-16420}
seed=${SEED:-17}

server="$build_dir/cache_server"
benchmark="$build_dir/cache_bench"

if [[ ! -x "$server" || ! -x "$benchmark" ]]; then
    echo "Build cache_server and cache_bench first (expected in $build_dir)." >&2
    exit 1
fi

header_printed=0
for shard_offset in 0 1; do
    if [[ "$shard_offset" -eq 0 ]]; then
        shards=1
    else
        shards=16
    fi
    port=$((base_port + shard_offset))
    log_file=$(mktemp)

    "$server" "$port" "$shards" >"$log_file" 2>&1 &
    server_pid=$!
    cleanup_server() {
        kill -INT "$server_pid" 2>/dev/null || true
        wait "$server_pid" 2>/dev/null || true
        rm -f "$log_file"
    }
    trap cleanup_server EXIT

    ready=0
    for attempt in $(seq 1 100); do
        if (echo >"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
            ready=1
            break
        fi
        sleep 0.05
    done
    if [[ "$ready" -ne 1 ]]; then
        cat "$log_file" >&2
        echo "Server did not start on port $port." >&2
        exit 1
    fi

    for clients in 1 8 32 128; do
        result=$("$benchmark" \
            127.0.0.1 \
            "$port" \
            "$clients" \
            "$operations_per_client" \
            "$get_percent" \
            "$key_count" \
            "$shards" \
            "$seed")
        if [[ "$header_printed" -eq 0 ]]; then
            printf '%s\n' "${result%%$'\n'*}"
            header_printed=1
        fi
        printf '%s\n' "${result#*$'\n'}"
    done

    cleanup_server
    trap - EXIT
done