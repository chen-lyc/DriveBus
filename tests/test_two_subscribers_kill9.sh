#!/usr/bin/env bash
#
# Black-box regression test for one publisher and two subscribers.
#
# It proves this observable contract:
#   1. Both subscribers consume messages.
#   2. One subscriber receives SIGKILL (kill -9).
#   3. The publisher and the surviving subscriber each advance by more than
#      one descriptor-ring cycle afterwards.
#
# Run from any directory:
#   bash tests/test_two_subscribers_kill9.sh
#
# Optional tuning:
#   CASE_COUNT=20 POST_KILL_SEQUENCE_DELTA=512 \
#     bash tests/test_two_subscribers_kill9.sh

set -Eeuo pipefail

readonly kRepoRoot="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
readonly kSocketPath='/tmp/demo.sock'
readonly kSharedMemoryPath='/dev/shm/shm'
readonly kLockPath='/tmp/drivebus-two-subscribers-kill9.lock'
readonly kDescriptorSlotCount=16

readonly kReadySequence="${READY_SEQUENCE:-64}"
readonly kPostKillSequenceDelta="${POST_KILL_SEQUENCE_DELTA:-128}"
readonly kPhaseTimeoutSeconds="${PHASE_TIMEOUT_SECONDS:-30}"
readonly kCaseCount="${CASE_COUNT:-2}"
readonly kCxx="${CXX:-g++}"

publisher_pid=''
subscriber_one_pid=''
subscriber_two_pid=''
publisher_filter_pid=''
subscriber_one_filter_pid=''
subscriber_two_filter_pid=''
current_case_dir=''
run_dir=''
owns_ipc_resources=0
test_passed=0

publisher_log=''
publisher_stderr=''
subscriber_one_log=''
subscriber_one_stderr=''
subscriber_two_log=''
subscriber_two_stderr=''
publisher_fifo=''
subscriber_one_fifo=''
subscriber_two_fifo=''

fail() {
    printf 'FAIL: %s\n' "$*" >&2
    if [[ -n "${current_case_dir}" ]]; then
        print_case_logs
    fi
    exit 1
}

is_positive_integer() {
    [[ "$1" =~ ^[1-9][0-9]*$ ]]
}

sequence_is_at_least() {
    local sequence="$1"
    local target="$2"

    [[ "$sequence" =~ ^[0-9]+$ ]] && (( 10#$sequence >= 10#$target ))
}

stop_process() {
    local pid="${1:-}"

    [[ -n "$pid" ]] || return 0

    if kill -0 "$pid" 2>/dev/null; then
        kill -TERM "$pid" 2>/dev/null || true
        for _ in {1..10}; do
            kill -0 "$pid" 2>/dev/null || break
            sleep 0.05
        done
        kill -KILL "$pid" 2>/dev/null || true
    fi

    wait "$pid" 2>/dev/null || true
}

cleanup() {
    local exit_status=$?

    trap - EXIT INT TERM
    stop_process "$publisher_pid"
    stop_process "$subscriber_one_pid"
    stop_process "$subscriber_two_pid"
    stop_process "$publisher_filter_pid"
    stop_process "$subscriber_one_filter_pid"
    stop_process "$subscriber_two_filter_pid"

    if (( owns_ipc_resources )); then
        rm -f -- "$kSocketPath" "$kSharedMemoryPath" || true
    fi
    rm -f -- "$publisher_fifo" "$subscriber_one_fifo" "$subscriber_two_fifo" || true

    if [[ -z "$run_dir" ]]; then
        exit "$exit_status"
    fi

    if (( exit_status == 0 && test_passed )); then
        rm -rf -- "$run_dir"
    else
        printf 'Test artifacts were kept in: %s\n' "$run_dir" >&2
    fi

    exit "$exit_status"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

print_file_tail() {
    local label="$1"
    local path="$2"

    [[ -f "$path" ]] || return 0
    printf '\n--- %s (%s) ---\n' "$label" "$path" >&2
    tail -n 40 "$path" >&2 || true
}

print_case_logs() {
    print_file_tail 'publisher output' "$publisher_log"
    print_file_tail 'publisher stderr' "$publisher_stderr"
    print_file_tail 'subscriber 1 output' "$subscriber_one_log"
    print_file_tail 'subscriber 1 stderr' "$subscriber_one_stderr"
    print_file_tail 'subscriber 2 output' "$subscriber_two_log"
    print_file_tail 'subscriber 2 stderr' "$subscriber_two_stderr"
}

filter_relevant_output() {
    grep --line-buffered -E \
        '^(receiver listening\.\.\.|read [0-9]+ byte, offset is [0-9]+, seq is [0-9]+|write [0-9]+ byte .*, seq is [0-9]+|subscriber_read_index is [0-9]+|error magic:.*|error seq:.*|.*size is too big.*|.*chunk free twice.*|.*chunk error use again.*|.*offset is out of range.*|.*error: invalid conn_fd.*|time out|all write over)$'
}

last_sequence() {
    local log_path="$1"
    local process_kind="$2"
    local sequence=''

    case "$process_kind" in
        publisher)
            sequence="$(awk '/^write [0-9]+ byte .*, seq is [0-9]+$/ { value = $NF } END { if (value != "") print value }' "$log_path")"
            ;;
        subscriber)
            sequence="$(awk '/^read [0-9]+ byte, offset is [0-9]+, seq is [0-9]+$/ { value = $NF } END { if (value != "") print value }' "$log_path")"
            ;;
        *)
            fail "unknown process kind: $process_kind"
            ;;
    esac

    [[ "$sequence" =~ ^[0-9]+$ ]] || return 1
    printf '%s\n' "$sequence"
}

has_failure_evidence() {
    grep -Eqs \
        'error magic:|error seq:|size is too big|chunk free twice|chunk error use again|offset is out of range|error: invalid conn_fd|^time out$' \
        "$publisher_log" "$publisher_stderr" \
        "$subscriber_one_log" "$subscriber_one_stderr" \
        "$subscriber_two_log" "$subscriber_two_stderr"
}

assert_running() {
    local name="$1"
    local pid="$2"

    if [[ -z "$pid" ]] || ! kill -0 "$pid" 2>/dev/null; then
        fail "$name exited before the required progress was observed"
    fi
}

assert_no_failure_evidence() {
    if has_failure_evidence; then
        fail 'program output contains a correctness or timeout failure'
    fi
}

assert_ipc_namespace_is_idle() {
    if [[ -e "$kSocketPath" || -S "$kSocketPath" || -e "$kSharedMemoryPath" ]]; then
        fail "refusing to touch an existing DriveBus IPC resource ($kSocketPath or $kSharedMemoryPath)"
    fi
}

wait_for_socket() {
    local deadline=$((SECONDS + kPhaseTimeoutSeconds))

    while (( SECONDS < deadline )); do
        assert_running 'publisher' "$publisher_pid"
        assert_no_failure_evidence
        [[ -S "$kSocketPath" ]] && return 0
        sleep 0.02
    done

    fail 'publisher did not create its Unix-domain socket before the deadline'
}

wait_for_both_subscribers() {
    local deadline=$((SECONDS + kPhaseTimeoutSeconds))
    local publisher_sequence
    local subscriber_one_sequence
    local subscriber_two_sequence

    while (( SECONDS < deadline )); do
        assert_running 'publisher' "$publisher_pid"
        assert_running 'subscriber 1' "$subscriber_one_pid"
        assert_running 'subscriber 2' "$subscriber_two_pid"
        assert_no_failure_evidence

        publisher_sequence="$(last_sequence "$publisher_log" publisher || true)"
        subscriber_one_sequence="$(last_sequence "$subscriber_one_log" subscriber || true)"
        subscriber_two_sequence="$(last_sequence "$subscriber_two_log" subscriber || true)"

        if sequence_is_at_least "$publisher_sequence" "$kReadySequence" &&
            sequence_is_at_least "$subscriber_one_sequence" "$kReadySequence" &&
            sequence_is_at_least "$subscriber_two_sequence" "$kReadySequence"; then
            return 0
        fi

        sleep 0.02
    done

    fail "both subscribers did not reach seq >= $kReadySequence before the deadline"
}

wait_for_post_kill_progress() {
    local survivor_name="$1"
    local survivor_pid="$2"
    local survivor_log="$3"
    local publisher_target="$4"
    local survivor_target="$5"
    local deadline=$((SECONDS + kPhaseTimeoutSeconds))
    local publisher_sequence
    local survivor_sequence

    while (( SECONDS < deadline )); do
        assert_running 'publisher' "$publisher_pid"
        assert_running "$survivor_name" "$survivor_pid"
        assert_no_failure_evidence

        publisher_sequence="$(last_sequence "$publisher_log" publisher || true)"
        survivor_sequence="$(last_sequence "$survivor_log" subscriber || true)"

        if sequence_is_at_least "$publisher_sequence" "$publisher_target" &&
            sequence_is_at_least "$survivor_sequence" "$survivor_target"; then
            return 0
        fi

        sleep 0.02
    done

    fail "$survivor_name and publisher did not advance by $kPostKillSequenceDelta messages after SIGKILL"
}

build_binaries() {
    local subscriber_binary="$run_dir/subscriber"
    local publisher_binary="$run_dir/publisher"
    local compiler_flags=(-std=gnu++17 -O0 -g -pthread -DENABLE_DEBUG_CHECKS)

    printf 'Building test binaries with %s...\n' "$kCxx"
    "$kCxx" "${compiler_flags[@]}" "$kRepoRoot/a.cpp" -o "$subscriber_binary"
    "$kCxx" "${compiler_flags[@]}" "$kRepoRoot/b.cpp" -o "$publisher_binary"
}

stop_case_processes() {
    stop_process "$publisher_pid"
    stop_process "$subscriber_one_pid"
    stop_process "$subscriber_two_pid"
    stop_process "$publisher_filter_pid"
    stop_process "$subscriber_one_filter_pid"
    stop_process "$subscriber_two_filter_pid"
    publisher_pid=''
    subscriber_one_pid=''
    subscriber_two_pid=''
    publisher_filter_pid=''
    subscriber_one_filter_pid=''
    subscriber_two_filter_pid=''

    rm -f -- "$kSocketPath" "$kSharedMemoryPath"
    rm -f -- "$publisher_fifo" "$subscriber_one_fifo" "$subscriber_two_fifo"
    publisher_fifo=''
    subscriber_one_fifo=''
    subscriber_two_fifo=''
    owns_ipc_resources=0
}

run_case() {
    local case_number="$1"
    local subscriber_binary="$run_dir/subscriber"
    local publisher_binary="$run_dir/publisher"
    local victim_name
    local victim_pid
    local survivor_name
    local survivor_pid
    local survivor_log
    local publisher_before_kill
    local survivor_before_kill
    local killed_status
    local jitter_milliseconds
    local jitter_seconds
    local publisher_target
    local survivor_target

    assert_ipc_namespace_is_idle

    current_case_dir="$run_dir/case-$case_number"
    mkdir -p -- "$current_case_dir"
    publisher_log="$current_case_dir/publisher.log"
    publisher_stderr="$current_case_dir/publisher.stderr"
    subscriber_one_log="$current_case_dir/subscriber-1.log"
    subscriber_one_stderr="$current_case_dir/subscriber-1.stderr"
    subscriber_two_log="$current_case_dir/subscriber-2.log"
    subscriber_two_stderr="$current_case_dir/subscriber-2.stderr"
    publisher_fifo="$current_case_dir/publisher.stdout.fifo"
    subscriber_one_fifo="$current_case_dir/subscriber-1.stdout.fifo"
    subscriber_two_fifo="$current_case_dir/subscriber-2.stdout.fifo"

    owns_ipc_resources=1
    mkfifo "$publisher_fifo" "$subscriber_one_fifo" "$subscriber_two_fifo"
    filter_relevant_output < "$publisher_fifo" > "$publisher_log" &
    publisher_filter_pid=$!
    "$publisher_binary" > "$publisher_fifo" 2> "$publisher_stderr" &
    publisher_pid=$!
    wait_for_socket

    filter_relevant_output < "$subscriber_one_fifo" > "$subscriber_one_log" &
    subscriber_one_filter_pid=$!
    "$subscriber_binary" > "$subscriber_one_fifo" 2> "$subscriber_one_stderr" &
    subscriber_one_pid=$!
    filter_relevant_output < "$subscriber_two_fifo" > "$subscriber_two_log" &
    subscriber_two_filter_pid=$!
    "$subscriber_binary" > "$subscriber_two_fifo" 2> "$subscriber_two_stderr" &
    subscriber_two_pid=$!

    wait_for_both_subscribers

    if (( case_number % 2 == 1 )); then
        victim_name='subscriber 1'
        victim_pid="$subscriber_one_pid"
        survivor_name='subscriber 2'
        survivor_pid="$subscriber_two_pid"
        survivor_log="$subscriber_two_log"
    else
        victim_name='subscriber 2'
        victim_pid="$subscriber_two_pid"
        survivor_name='subscriber 1'
        survivor_pid="$subscriber_one_pid"
        survivor_log="$subscriber_one_log"
    fi

    jitter_milliseconds=$((RANDOM % 50 + 1))
    printf -v jitter_seconds '0.%03d' "$jitter_milliseconds"
    sleep "$jitter_seconds"

    assert_running 'publisher' "$publisher_pid"
    assert_running "$survivor_name" "$survivor_pid"
    assert_running "$victim_name" "$victim_pid"
    assert_no_failure_evidence
    publisher_before_kill="$(last_sequence "$publisher_log" publisher)"
    survivor_before_kill="$(last_sequence "$survivor_log" subscriber)"

    printf 'Case %s: SIGKILL %s at publisher seq=%s, %s seq=%s\n' \
        "$case_number" "$victim_name" "$publisher_before_kill" "$survivor_name" "$survivor_before_kill"
    kill -KILL "$victim_pid"
    if wait "$victim_pid" 2>/dev/null; then
        fail "$victim_name unexpectedly exited successfully after SIGKILL"
    else
        killed_status=$?
        (( killed_status == 137 )) || fail "$victim_name exited with $killed_status, expected 137 from SIGKILL"
    fi

    if [[ "$victim_name" == 'subscriber 1' ]]; then
        subscriber_one_pid=''
    else
        subscriber_two_pid=''
    fi

    publisher_target=$((publisher_before_kill + kPostKillSequenceDelta))
    survivor_target=$((survivor_before_kill + kPostKillSequenceDelta))
    wait_for_post_kill_progress \
        "$survivor_name" "$survivor_pid" "$survivor_log" \
        "$publisher_target" "$survivor_target"

    printf 'Case %s: PASS (%s and publisher crossed %s post-kill messages)\n' \
        "$case_number" "$survivor_name" "$kPostKillSequenceDelta"
    stop_case_processes
    current_case_dir=''
}

is_positive_integer "$kReadySequence" || fail 'READY_SEQUENCE must be a positive integer'
is_positive_integer "$kPostKillSequenceDelta" || fail 'POST_KILL_SEQUENCE_DELTA must be a positive integer'
is_positive_integer "$kPhaseTimeoutSeconds" || fail 'PHASE_TIMEOUT_SECONDS must be a positive integer'
is_positive_integer "$kCaseCount" || fail 'CASE_COUNT must be a positive integer'
(( kPostKillSequenceDelta > kDescriptorSlotCount )) || fail 'POST_KILL_SEQUENCE_DELTA must exceed the 16 descriptor slots'
command -v "$kCxx" >/dev/null 2>&1 || fail "C++ compiler not found: $kCxx"
command -v flock >/dev/null 2>&1 || fail 'flock is required to prevent concurrent test-script runs'

run_dir="$(mktemp -d "${TMPDIR:-/tmp}/drivebus-two-subscribers-kill9.XXXXXX")"
exec 9>"$kLockPath"
flock -n 9 || fail 'another two-subscriber kill-9 test is already running'

build_binaries
for ((case_number = 1; case_number <= kCaseCount; ++case_number)); do
    run_case "$case_number"
done

test_passed=1
printf 'PASS: %s two-subscriber SIGKILL recovery case(s) completed.\n' "$kCaseCount"
