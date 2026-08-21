# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Runs a single systest against real worker processes on loopback, so a query is split
# across separate OS processes instead of the in-process workers systest uses by default.
#
# The topology files under nes-systests/configs/topologies/ are written for Docker Compose:
# workers are named `sink-node:8080`, `source-node:8080`, ... Compose gives every container
# its own DNS name and network namespace, so they can all share port 8080. Neither holds on
# a single host, so this script rewrites the topology onto free loopback ports before
# starting anything.
#
# Runs in two phases. Phase 1 needs yq (a runtimeInput, present without the dev shell);
# phase 2 needs the dev shell, because the built binaries resolve their libraries from it.

DEFAULT_TOPOLOGY="nes-systests/configs/topologies/two-node.yaml"
DEFAULT_GRPC_BASE_PORT=8080
DEFAULT_DATA_BASE_PORT=9090
READINESS_TIMEOUT_SECONDS=60

COLOR_YELLOW_BOLD="\033[1;33m"
COLOR_RED_BOLD="\033[1;31m"
COLOR_RESET="\033[0m"

log_info() { printf '==> %b\n' "$*"; }
log_warn() { printf '%bWarning%b: %b\n' "$COLOR_YELLOW_BOLD" "$COLOR_RESET" "$*" >&2; }
log_fatal() {
    printf '%bFatal%b: %b\n' "$COLOR_RED_BOLD" "$COLOR_RESET" "$*" >&2
    exit 1
}

usage() {
    cat <<EOF
Run one systest across real worker processes on localhost.

Usage:
  nix run .#systest-distributed -- [options] [systest args...]

Options:
  -c, --clusterConfig <path>  topology to run       (default: $DEFAULT_TOPOLOGY)
      --base-port <n>         first gRPC port       (default: $DEFAULT_GRPC_BASE_PORT)
      --data-base-port <n>    first data-plane port (default: $DEFAULT_DATA_BASE_PORT)
      --run-dir <path>        artifact directory    (default: \$NES_BUILD_DIR/nes-systests/systest-distributed-run)
      --keep                  leave the workers running after the test
  -h, --help                  show this help

Ports are probed before use, so anything already bound is skipped. All remaining arguments
are forwarded verbatim to systest, including a trailing '--' worker-config block.

Environment:
  NES_BUILD_DIR   CMake build directory (default: cmake-build-debug)

Examples:
  nix run .#systest-distributed -- -t nes-systests/function/arithmetical/FunctionAdd.test:1
  nix run .#systest-distributed -- -c nes-systests/configs/topologies/8-node.yaml -t Filter.test
EOF
}

# Ask the kernel whether anything already answers on a port. A stale worker (or an
# unrelated process holding 8080) otherwise surfaces much later as an opaque "Address
# already in use" panic from inside the worker.
port_in_use() {
    timeout 1 bash -c "exec 3<>/dev/tcp/127.0.0.1/$1" 2>/dev/null
}

# Echo the next free port at or above $1.
claim_port() {
    candidate=$1
    while [ "$candidate" -lt 65536 ]; do
        if ! port_in_use "$candidate"; then
            printf '%s' "$candidate"
            return 0
        fi
        candidate=$((candidate + 1))
    done
    log_fatal "ran out of free ports at or above $1"
}

# Replace every string scalar in $1 equal to $2 with $3. Matching on value rather than key
# covers workers[].host, workers[].data_address, workers[].downstream[],
# allow_source_placement[] and allow_sink_placement[] in one expression, so the rewrite
# keeps working if the topology schema grows another host-bearing field.
replace_scalar() {
    yq -i "(.. | select(tag == \"!!str\" and . == \"$2\")) = \"$3\"" "$1"
}

# Pick the devShell variant matching the build directory. mkVariantAttrset names shells
# <sanitizer>-<stdlib>; running an asan build against the default shell would link it
# against the wrong runtime.
dev_shell_attr() {
    cache="$1/CMakeCache.txt"
    sanitizer="none"
    stdlib="libstdcxx"

    if [ -f "$cache" ]; then
        case "$(sed -n 's/^USE_SANITIZER:STRING=\(.*\)$/\1/p' "$cache" | head -n 1)" in
            address | asan) sanitizer="asan" ;;
            thread | tsan) sanitizer="tsan" ;;
            undefined | ubsan) sanitizer="ubsan" ;;
            *) ;;
        esac
        case "$(sed -n 's/^USE_LIBCXX_IF_AVAILABLE:BOOL=\(.*\)$/\1/p' "$cache" | head -n 1)" in
            ON | on | TRUE | true | 1) stdlib="libcxx" ;;
            *) ;;
        esac
    fi

    if [ "$sanitizer" = "none" ] && [ "$stdlib" = "libstdcxx" ]; then
        printf 'default'
    else
        printf '%s-%s' "$sanitizer" "$stdlib"
    fi
}

#######################################
# Phase 1: rewrite the topology, then hand off to the dev shell.
#######################################

phase_one() {
    topology="$DEFAULT_TOPOLOGY"
    grpc_port="$DEFAULT_GRPC_BASE_PORT"
    data_port="$DEFAULT_DATA_BASE_PORT"
    build_dir="${NES_BUILD_DIR:-cmake-build-debug}"
    run_dir=""
    keep=0
    systest_args=()

    while [ "$#" -gt 0 ]; do
        case "$1" in
            -c | --clusterConfig)
                [ "$#" -ge 2 ] || log_fatal "$1 requires a path"
                topology="$2"
                shift 2
                ;;
            --base-port)
                [ "$#" -ge 2 ] || log_fatal "$1 requires a port number"
                grpc_port="$2"
                shift 2
                ;;
            --data-base-port)
                [ "$#" -ge 2 ] || log_fatal "$1 requires a port number"
                data_port="$2"
                shift 2
                ;;
            --run-dir)
                [ "$#" -ge 2 ] || log_fatal "$1 requires a path"
                run_dir="$2"
                shift 2
                ;;
            --keep)
                keep=1
                shift
                ;;
            -h | --help)
                usage
                exit 0
                ;;
            *)
                # Everything else belongs to systest. Stop claiming arguments at the first
                # unknown one so a trailing '-- --worker.x=y' survives intact.
                systest_args=("$@")
                break
                ;;
        esac
    done

    if [ ! -f flake.nix ] || [ ! -d nes-systests ]; then
        log_fatal "run this command from the NebulaStream repository root"
    fi

    # Without a selection systest would run every discovered test against the cluster. That
    # is what the Docker suite is for; this app exists to run one thing on demand, so make
    # the omission an error rather than a very long surprise.
    if [ "${#systest_args[@]}" -eq 0 ]; then
        log_warn "no systest arguments given; select a test with -t or a group with -g"
        printf '\n' >&2
        usage >&2
        exit 1
    fi

    # Option parsing stops at the first argument we do not own, so a -c placed after the
    # test selection would reach systest and point it at the original topology while the
    # workers listen on the rewritten one. Nothing about that failure would be obvious.
    for arg in "${systest_args[@]}"; do
        case "$arg" in
            -c | --clusterConfig)
                log_fatal "pass $arg before the systest arguments, e.g.\n    nix run .#systest-distributed -- $arg <topology> -t <test>"
                ;;
            *) ;;
        esac
    done

    worker_bin="$build_dir/nes-single-node-worker/nes-single-node-worker"
    systest_bin="$build_dir/nes-systests/systest/systest"
    for binary in "$worker_bin" "$systest_bin"; do
        if [ ! -x "$binary" ]; then
            log_fatal "$binary not found or not executable\n  Build it with:\n    cmake --build $build_dir -j --target systest nes-single-node-worker"
        fi
    done

    [ -f "$topology" ] || log_fatal "topology file not found: $topology"

    # Alongside systest's own default working-dir, which CMake pins to
    # ${CMAKE_BINARY_DIR}/nes-systests, so all systest artifacts stay in one place.
    [ -n "$run_dir" ] || run_dir="$build_dir/nes-systests/systest-distributed-run"

    # The run directory is wiped on every invocation, and --run-dir makes it arbitrary, so
    # only clear one we plausibly created: empty, or carrying our own topology.yaml.
    if [ -e "$run_dir" ]; then
        if [ ! -f "$run_dir/topology.yaml" ] && [ -n "$(ls -A "$run_dir" 2>/dev/null)" ]; then
            log_fatal "refusing to clear $run_dir: it is not empty and not a previous run directory"
        fi
        rm -rf "$run_dir"
    fi
    mkdir -p "$run_dir/configs"

    # Absolute from here on: phase 2 starts each worker in its own working directory, and
    # `nix develop` is free to land somewhere else entirely.
    run_dir=$(realpath "$run_dir")
    build_dir=$(realpath "$build_dir")

    rewritten="$run_dir/topology.yaml"
    cp "$topology" "$rewritten"

    worker_count=$(yq '.workers | length' "$topology")
    [ "$worker_count" -gt 0 ] || log_fatal "$topology defines no workers"

    # Remap in two passes through a placeholder. A single pass clobbers whenever a worker's
    # new address equals another worker's not-yet-rewritten old one, which a localhost
    # topology can trigger just by permuting ports: given hosts 8081 and 8080 in that
    # order, rewriting the first to 8080 aliases it onto the second, and rewriting the
    # second then drags both to 8081.
    old_hosts=()
    old_data=()
    new_hosts=()
    new_data=()
    for i in $(seq 0 $((worker_count - 1))); do
        old_host=$(yq -r ".workers[$i].host" "$topology")
        old_datum=$(yq -r ".workers[$i].data_address" "$topology")
        [ "$old_host" != "null" ] || log_fatal "workers[$i] in $topology has no 'host'"
        [ "$old_datum" != "null" ] || log_fatal "workers[$i] in $topology has no 'data_address'"
        old_hosts+=("$old_host")
        old_data+=("$old_datum")

        grpc_port=$(claim_port "$grpc_port")
        new_hosts+=("localhost:$grpc_port")
        grpc_port=$((grpc_port + 1))

        data_port=$(claim_port "$data_port")
        new_data+=("localhost:$data_port")
        data_port=$((data_port + 1))

        replace_scalar "$rewritten" "$old_host" "@@nes-remap-$i-grpc@@"
        replace_scalar "$rewritten" "$old_datum" "@@nes-remap-$i-data@@"
    done

    for i in $(seq 0 $((worker_count - 1))); do
        replace_scalar "$rewritten" "@@nes-remap-$i-grpc@@" "${new_hosts[i]}"
        replace_scalar "$rewritten" "@@nes-remap-$i-data@@" "${new_data[i]}"

        # Per-worker overrides live inline in the topology; the worker only reads them from
        # a file, via --configPath.
        if [ "$(yq ".workers[$i] | has(\"config\")" "$topology")" = "true" ]; then
            yq ".workers[$i].config" "$topology" > "$run_dir/configs/worker-$i.yaml"
        fi
    done

    if yq -r '.workers[].host' "$rewritten" | grep -qv '^localhost:'; then
        log_fatal "topology rewrite left a non-localhost host in $rewritten"
    fi

    log_info "topology $topology -> $rewritten"
    for i in $(seq 0 $((worker_count - 1))); do
        printf '      worker-%-2s %s -> %s   (data %s -> %s)\n' \
            "$i" "${old_hosts[i]}" "${new_hosts[i]}" "${old_data[i]}" "${new_data[i]}"
    done

    # The built binaries cannot run bare: outside the dev shell they report over a hundred
    # unresolved libraries. NES_USE_SYSTEM_DEPS is set by mkDevShell and by nothing else,
    # so it tells us whether the caller is already inside one.
    if [ "${NES_USE_SYSTEM_DEPS:-}" = "ON" ]; then
        phase_two "$run_dir" "$build_dir" "$keep" "${systest_args[@]}"
    else
        shell_attr=$(dev_shell_attr "$build_dir")
        log_info "entering dev shell .#$shell_attr"
        exec nix develop ".#$shell_attr" --command bash "$0" \
            --phase2 "$run_dir" "$build_dir" "$keep" "${systest_args[@]}"
    fi
}

#######################################
# Phase 2: start the workers, run the test, tear everything down.
#######################################

WORKER_PIDS=()
KEEP_WORKERS=0

cleanup() {
    if [ "$KEEP_WORKERS" = "1" ] && [ "${#WORKER_PIDS[@]}" -gt 0 ]; then
        log_info "leaving workers running: ${WORKER_PIDS[*]}"
        return
    fi
    for pid in "${WORKER_PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done

    # A worker wedged past SIGTERM would hang `wait` forever and keep holding its ports, so
    # give the graceful path a few seconds and then insist.
    for _ in $(seq 1 10); do
        still_running=0
        for pid in "${WORKER_PIDS[@]}"; do
            if kill -0 "$pid" 2>/dev/null; then
                still_running=1
            fi
        done
        if [ "$still_running" -eq 0 ]; then
            break
        fi
        sleep 0.5
    done

    for pid in "${WORKER_PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then
            log_warn "worker pid $pid ignored SIGTERM; sending SIGKILL"
            kill -9 "$pid" 2>/dev/null || true
        fi
    done
    wait 2>/dev/null || true
}

phase_two() {
    run_dir="$1"
    build_dir="$2"
    KEEP_WORKERS="$3"
    shift 3

    topology="$run_dir/topology.yaml"
    worker_bin="$build_dir/nes-single-node-worker/nes-single-node-worker"
    systest_bin="$build_dir/nes-systests/systest/systest"
    worker_count=$(yq '.workers | length' "$topology")

    trap cleanup EXIT
    # Without explicit handlers a signal terminates the shell without running the EXIT trap,
    # which would strand the workers on their ports. Exiting from the handler runs it.
    trap 'exit 130' INT
    trap 'exit 143' TERM
    trap 'exit 129' HUP

    for i in $(seq 0 $((worker_count - 1))); do
        host=$(yq -r ".workers[$i].host" "$topology")
        data=$(yq -r ".workers[$i].data_address" "$topology")

        # Each worker gets its own working directory, so a relative path in a query cannot
        # have two of them writing the same file. This mirrors the per-service working_dir
        # that the Docker Compose variant sets up.
        mkdir -p "$run_dir/worker-$i"

        config_args=()
        if [ -f "$run_dir/configs/worker-$i.yaml" ]; then
            config_args=("--configPath=$run_dir/configs/worker-$i.yaml")
        fi

        (
            cd "$run_dir/worker-$i" || exit 1
            exec "$worker_bin" \
                --grpc="$host" \
                --data_address="$data" \
                "${config_args[@]}"
        ) > "$run_dir/worker-$i.log" 2>&1 &
        WORKER_PIDS+=("$!")
        log_info "started worker-$i (pid $!) grpc=$host data=$data"
    done

    # Poll rather than sleep: the worker binds its gRPC port only once it is ready to
    # accept query registrations.
    for i in $(seq 0 $((worker_count - 1))); do
        host=$(yq -r ".workers[$i].host" "$topology")
        grpc_port="${host##*:}"
        attempt=0
        while true; do
            if ! kill -0 "${WORKER_PIDS[i]}" 2>/dev/null; then
                log_warn "worker-$i died during startup, last lines:"
                tail -n 20 "$run_dir/worker-$i.log" >&2
                log_fatal "worker startup failed"
            fi
            if port_in_use "$grpc_port"; then
                break
            fi
            attempt=$((attempt + 1))
            if [ "$attempt" -ge "$READINESS_TIMEOUT_SECONDS" ]; then
                log_fatal "worker-$i did not open $grpc_port within ${READINESS_TIMEOUT_SECONDS}s; see $run_dir/worker-$i.log"
            fi
            sleep 1
        done
    done

    log_info "running systest against $topology"
    status=0
    # --data is deliberately omitted so the compiled-in SYSTEST_EXTERNAL_DATA_DIR wins;
    # callers needing another testdata tree just pass --data through.
    "$systest_bin" \
        --clusterConfig "$topology" \
        --remote \
        --log-path "$run_dir/systest.log" \
        --workingDir "$run_dir/systest-workdir" \
        "$@" || status=$?

    if [ "$status" -ne 0 ]; then
        printf '\n' >&2
        log_warn "systest exited with status $status"
        for i in $(seq 0 $((worker_count - 1))); do
            printf '\n--- worker-%s ---\n' "$i" >&2
            tail -n 20 "$run_dir/worker-$i.log" >&2
        done
        printf '\n' >&2
        log_info "systest log: $run_dir/systest.log"
        log_info "topology:    $topology"
    fi

    log_info "artifacts in $run_dir"
    exit "$status"
}

if [ "${1-}" = "--phase2" ]; then
    shift
    phase_two "$@"
else
    phase_one "$@"
fi
