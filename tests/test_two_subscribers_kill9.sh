#!/usr/bin/env bash
#
# Black-box regression test for a publisher and dynamically changing
# subscribers.
#
# Each case starts a fresh broker, so it proves these observable contracts
# without relying on reusable subscriber slots:
#   1. The publisher can start with zero subscribers; a later subscriber makes
#      it begin publishing.
#   2. A late subscriber's first message is newer than the last publisher
#      sequence observed before that subscriber was started.
#   3. A subscriber can disappear while it owns an unread reference, and the
#      publisher plus every survivor still cross more than one descriptor-ring
#      cycle.
#   4. After the last subscriber disappears, the publisher quiesces safely and
#      a later subscriber can restart progress.
#
# Run from any directory:
#   bash tests/test_two_subscribers_kill9.sh
#
# Optional tuning:
#   CASE_COUNT=20 POST_KILL_SEQUENCE_DELTA=512 \
#     bash tests/test_two_subscribers_kill9.sh

set -Eeuo pipefail

readonly kRepoRoot="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
readonly kSocketPath='/tmp/broker.sock'
readonly kSharedMemoryPath='/dev/shm/shm'
readonly kLockPath='/tmp/drivebus-two-subscribers-kill9.lock'
readonly kDescriptorSlotCount=16

readonly kReadySequence="${READY_SEQUENCE:-64}"
readonly kJoinSequenceStride="${JOIN_SEQUENCE_STRIDE:-17}"
readonly kJoinedSubscriberSequenceDelta="${JOINED_SUBSCRIBER_SEQUENCE_DELTA:-32}"
readonly kPostKillSequenceDelta="${POST_KILL_SEQUENCE_DELTA:-128}"
readonly kQuiescentObservations="${QUIESCENT_OBSERVATIONS:-25}"
readonly kPhaseTimeoutSeconds="${PHASE_TIMEOUT_SECONDS:-30}"
readonly kCaseCount="${CASE_COUNT:-4}"
readonly kCxx="${CXX:-g++}"

broker_pid=''
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

broker_log=''
broker_stderr=''
publisher_log=''
publisher_stderr=''
subscriber_one_log=''
subscriber_one_stderr=''
subscriber_two_log=''
subscriber_two_stderr=''
publisher_fifo=''
subscriber_one_fifo=''
subscriber_two_fifo=''
publisher_quiescent_sequence=''
joined_subscriber_first_sequence=''

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
    stop_process "$broker_pid"
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
    print_file_tail 'broker output' "$broker_log"
    print_file_tail 'broker stderr' "$broker_stderr"
    print_file_tail 'publisher output' "$publisher_log"
    print_file_tail 'publisher stderr' "$publisher_stderr"
    print_file_tail 'subscriber 1 output' "$subscriber_one_log"
    print_file_tail 'subscriber 1 stderr' "$subscriber_one_stderr"
    print_file_tail 'subscriber 2 output' "$subscriber_two_log"
    print_file_tail 'subscriber 2 stderr' "$subscriber_two_stderr"
}

filter_relevant_output() {
    grep --line-buffered -E \
        '^(receiver listening\.\.\.|subscriber_count is [0-9]+|read [0-9]+ byte, offset is [0-9]+, seq is [0-9]+|write [0-9]+ byte .*, seq is [0-9]+|subscriber_slot_index is [0-9]+|subscriber_read_index is [0-9]+|error magic:.*|error seq:.*|.*size is too big.*|.*chunk free twice.*|.*chunk error use again.*|.*offset is out of range.*|.*error: invalid conn_fd.*|time out|all write over)$'
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

first_subscriber_sequence() {
    local log_path="$1"
    local sequence=''

    sequence="$(awk '/^read [0-9]+ byte, offset is [0-9]+, seq is [0-9]+$/ { print $NF; exit }' "$log_path")"
    [[ "$sequence" =~ ^[0-9]+$ ]] || return 1
    printf '%s\n' "$sequence"
}

subscriber_slot_index() {
    local log_path="$1"
    local slot_index=''

    slot_index="$(awk '/^subscriber_slot_index is [0-9]+$/ { print $NF; exit }' "$log_path")"
    [[ "$slot_index" =~ ^[0-9]+$ ]] || return 1
    printf '%s\n' "$slot_index"
}

has_failure_evidence() {
    grep -Eqs \
        'error magic:|error seq:|size is too big|chunk free twice|chunk error use again|offset is out of range|error: invalid conn_fd|^time out$|Failed to receive|registered message_type error|registration slot_index out of range|not find subscriber_slot_index|event_fd not found|receive other message' \
        "$broker_log" "$broker_stderr" \
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
        assert_running 'broker' "$broker_pid"
        assert_no_failure_evidence
        [[ -S "$kSocketPath" ]] && return 0
        sleep 0.02
    done

    fail 'broker did not create its Unix-domain socket before the deadline'
}

wait_for_publisher_without_subscribers() {
    local deadline=$((SECONDS + kPhaseTimeoutSeconds))

    while (( SECONDS < deadline )); do
        assert_running 'broker' "$broker_pid"
        assert_running 'publisher' "$publisher_pid"
        assert_no_failure_evidence

        if grep -Eq '^subscriber_count is 0$' "$publisher_log"; then
            return 0
        fi

        sleep 0.02
    done

    fail 'publisher did not complete its zero-subscriber registration before the deadline'
}

wait_for_subscriber_registration() {
    local subscriber_name="$1"
    local subscriber_pid="$2"
    local subscriber_log="$3"
    local expected_slot_index="$4"
    local deadline=$((SECONDS + kPhaseTimeoutSeconds))
    local actual_slot_index

    while (( SECONDS < deadline )); do
        assert_running 'broker' "$broker_pid"
        assert_running 'publisher' "$publisher_pid"
        assert_running "$subscriber_name" "$subscriber_pid"
        assert_no_failure_evidence

        actual_slot_index="$(subscriber_slot_index "$subscriber_log" || true)"
        if [[ "$actual_slot_index" == "$expected_slot_index" ]]; then
            return 0
        fi

        sleep 0.02
    done

    fail "$subscriber_name did not complete broker registration with slot $expected_slot_index before the deadline"
}

wait_for_initial_progress() {
    local target="$1"
    local deadline=$((SECONDS + kPhaseTimeoutSeconds))
    local publisher_sequence
    local subscriber_sequence

    while (( SECONDS < deadline )); do
        assert_running 'broker' "$broker_pid"
        assert_running 'publisher' "$publisher_pid"
        assert_running 'subscriber 1' "$subscriber_one_pid"
        assert_no_failure_evidence

        publisher_sequence="$(last_sequence "$publisher_log" publisher || true)"
        subscriber_sequence="$(last_sequence "$subscriber_one_log" subscriber || true)"

        if sequence_is_at_least "$publisher_sequence" "$target" &&
            sequence_is_at_least "$subscriber_sequence" "$target"; then
            return 0
        fi

        sleep 0.02
    done

    fail "publisher and subscriber 1 did not reach seq >= $target before the deadline"
}

wait_for_late_subscriber_progress() {
    local publisher_sequence_before_join="$1"
    local deadline=$((SECONDS + kPhaseTimeoutSeconds))
    local first_sequence
    local last_sequence_value

    joined_subscriber_first_sequence=''

    while (( SECONDS < deadline )); do
        assert_running 'broker' "$broker_pid"
        assert_running 'publisher' "$publisher_pid"
        assert_running 'subscriber 2' "$subscriber_two_pid"
        assert_no_failure_evidence

        first_sequence="$(first_subscriber_sequence "$subscriber_two_log" || true)"
        last_sequence_value="$(last_sequence "$subscriber_two_log" subscriber || true)"

        if sequence_is_at_least "$first_sequence" "$((publisher_sequence_before_join + 1))" &&
            sequence_is_at_least "$last_sequence_value" "$((first_sequence + kJoinedSubscriberSequenceDelta))"; then
            joined_subscriber_first_sequence="$first_sequence"
            return 0
        fi

        sleep 0.02
    done

    fail "subscriber 2 did not consume only post-join messages and then advance by $kJoinedSubscriberSequenceDelta"
}

wait_for_publisher_progress() {
    local publisher_target="$1"
    local deadline=$((SECONDS + kPhaseTimeoutSeconds))
    local publisher_sequence

    while (( SECONDS < deadline )); do
        assert_running 'broker' "$broker_pid"
        assert_running 'publisher' "$publisher_pid"
        assert_no_failure_evidence

        publisher_sequence="$(last_sequence "$publisher_log" publisher || true)"
        if sequence_is_at_least "$publisher_sequence" "$publisher_target"; then
            return 0
        fi

        sleep 0.02
    done

    fail "publisher did not advance to seq >= $publisher_target before the deadline"
}

wait_for_publisher_and_survivor_progress() {
    local survivor_name="$1"
    local survivor_pid="$2"
    local survivor_log="$3"
    local publisher_target="$4"
    local survivor_target="$5"
    local phase="$6"
    local deadline=$((SECONDS + kPhaseTimeoutSeconds))
    local publisher_sequence
    local survivor_sequence

    while (( SECONDS < deadline )); do
        assert_running 'broker' "$broker_pid"
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

    fail "$phase: publisher and $survivor_name did not advance by $kPostKillSequenceDelta messages"
}

wait_for_publisher_quiescence() {
    local deadline=$((SECONDS + kPhaseTimeoutSeconds))
    local observed_sequence=''
    local stable_observations=0
    local publisher_sequence

    publisher_quiescent_sequence=''

    while (( SECONDS < deadline )); do
        assert_running 'broker' "$broker_pid"
        assert_running 'publisher' "$publisher_pid"
        assert_no_failure_evidence

        publisher_sequence="$(last_sequence "$publisher_log" publisher || true)"
        [[ "$publisher_sequence" =~ ^[0-9]+$ ]] || fail 'publisher has no sequence while waiting for zero-subscriber quiescence'

        if [[ "$publisher_sequence" == "$observed_sequence" ]]; then
            ((++stable_observations))
        else
            observed_sequence="$publisher_sequence"
            stable_observations=0
        fi

        if (( stable_observations >= kQuiescentObservations )); then
            publisher_quiescent_sequence="$publisher_sequence"
            return 0
        fi

        sleep 0.02
    done

    fail 'publisher did not become quiescent after the last subscriber exited'
}

wait_for_process_stopped() {
    local name="$1"
    local pid="$2"
    local deadline=$((SECONDS + kPhaseTimeoutSeconds))
    local state=''

    while (( SECONDS < deadline )); do
        assert_running "$name" "$pid"
        state="$(awk '/^State:/ { print $2; exit }' "/proc/$pid/status" 2>/dev/null || true)"
        [[ "$state" == 'T' ]] && return 0
        sleep 0.01
    done

    fail "$name did not enter SIGSTOP state before the deadline"
}

pause_process() {
    local name="$1"
    local pid="$2"

    assert_running "$name" "$pid"
    kill -STOP "$pid" || fail "could not SIGSTOP $name"
    wait_for_process_stopped "$name" "$pid"
}

resume_process() {
    local name="$1"
    local pid="$2"

    kill -CONT "$pid" || fail "could not SIGCONT $name"
    assert_running "$name" "$pid"
}

kill_subscriber() {
    local subscriber_name="$1"
    local subscriber_pid="$2"
    local killed_status

    assert_running "$subscriber_name" "$subscriber_pid"
    kill -KILL "$subscriber_pid"
    if wait "$subscriber_pid" 2>/dev/null; then
        fail "$subscriber_name unexpectedly exited successfully after SIGKILL"
    else
        killed_status=$?
        (( killed_status == 137 )) || fail "$subscriber_name exited with $killed_status, expected 137 from SIGKILL"
    fi
}

build_binaries() {
    printf 'Building test binaries with %s...\n' "$kCxx"
    make -C "$kRepoRoot" -B "CXX=$kCxx" DEBUG_CHECKS=1 DEBUG_SYMBOLS=1 all
}

start_publisher() {
    filter_relevant_output < "$publisher_fifo" > "$publisher_log" &
    publisher_filter_pid=$!
    "$publisher_binary" > "$publisher_fifo" 2> "$publisher_stderr" &
    publisher_pid=$!
}

start_subscriber_one() {
    filter_relevant_output < "$subscriber_one_fifo" > "$subscriber_one_log" &
    subscriber_one_filter_pid=$!
    "$subscriber_binary" > "$subscriber_one_fifo" 2> "$subscriber_one_stderr" &
    subscriber_one_pid=$!
}

start_subscriber_two() {
    filter_relevant_output < "$subscriber_two_fifo" > "$subscriber_two_log" &
    subscriber_two_filter_pid=$!
    "$subscriber_binary" > "$subscriber_two_fifo" 2> "$subscriber_two_stderr" &
    subscriber_two_pid=$!
}

stop_case_processes() {
    stop_process "$publisher_pid"
    stop_process "$subscriber_one_pid"
    stop_process "$subscriber_two_pid"
    stop_process "$broker_pid"
    stop_process "$publisher_filter_pid"
    stop_process "$subscriber_one_filter_pid"
    stop_process "$subscriber_two_filter_pid"
    publisher_pid=''
    subscriber_one_pid=''
    subscriber_two_pid=''
    broker_pid=''
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
    local case_kind=$(( (case_number - 1) % 4 + 1 ))
    local broker_binary="$kRepoRoot/broker.out"
    local subscriber_binary="$kRepoRoot/a.out"
    local publisher_binary="$kRepoRoot/b.out"
    local initial_progress_target=$((kReadySequence + (case_number - 1) * kJoinSequenceStride))
    local publisher_sequence_before_join
    local publisher_sequence_before_kill
    local survivor_sequence_before_kill
    local publisher_target
    local survivor_target

    current_case_dir="$run_dir/case-$case_number"
    mkdir -p -- "$current_case_dir"
    broker_log="$current_case_dir/broker.log"
    broker_stderr="$current_case_dir/broker.stderr"
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
    "$broker_binary" > "$broker_log" 2> "$broker_stderr" &
    broker_pid=$!
    wait_for_socket

    start_publisher
    wait_for_publisher_without_subscribers

    start_subscriber_one
    wait_for_subscriber_registration 'subscriber 1' "$subscriber_one_pid" "$subscriber_one_log" 0
    wait_for_initial_progress "$initial_progress_target"

    case "$case_kind" in
        1)
            printf 'Case %s: last subscriber exits, then a replacement joins\n' "$case_number"
            pause_process 'subscriber 1' "$subscriber_one_pid"
            publisher_sequence_before_kill="$(last_sequence "$publisher_log" publisher)"
            wait_for_publisher_progress "$((publisher_sequence_before_kill + 1))"
            kill_subscriber 'subscriber 1' "$subscriber_one_pid"
            subscriber_one_pid=''

            wait_for_publisher_quiescence
            publisher_sequence_before_join="$publisher_quiescent_sequence"

            start_subscriber_two
            wait_for_subscriber_registration 'subscriber 2' "$subscriber_two_pid" "$subscriber_two_log" 1
            wait_for_late_subscriber_progress "$publisher_sequence_before_join"

            publisher_target=$((publisher_sequence_before_join + kPostKillSequenceDelta))
            survivor_target=$((joined_subscriber_first_sequence + kPostKillSequenceDelta))
            wait_for_publisher_and_survivor_progress \
                'subscriber 2' "$subscriber_two_pid" "$subscriber_two_log" \
                "$publisher_target" "$survivor_target" \
                'replacement subscriber did not restart progress'
            ;;
        2)
            printf 'Case %s: late subscriber joins, then original subscriber exits with a pending reference\n' "$case_number"
            publisher_sequence_before_join="$(last_sequence "$publisher_log" publisher)"
            start_subscriber_two
            wait_for_subscriber_registration 'subscriber 2' "$subscriber_two_pid" "$subscriber_two_log" 1
            wait_for_late_subscriber_progress "$publisher_sequence_before_join"

            pause_process 'subscriber 1' "$subscriber_one_pid"
            publisher_sequence_before_kill="$(last_sequence "$publisher_log" publisher)"
            wait_for_publisher_progress "$((publisher_sequence_before_kill + 1))"
            kill_subscriber 'subscriber 1' "$subscriber_one_pid"
            subscriber_one_pid=''

            publisher_sequence_before_kill="$(last_sequence "$publisher_log" publisher)"
            survivor_sequence_before_kill="$(last_sequence "$subscriber_two_log" subscriber)"
            publisher_target=$((publisher_sequence_before_kill + kPostKillSequenceDelta))
            survivor_target=$((survivor_sequence_before_kill + kPostKillSequenceDelta))
            wait_for_publisher_and_survivor_progress \
                'subscriber 2' "$subscriber_two_pid" "$subscriber_two_log" \
                "$publisher_target" "$survivor_target" \
                'original subscriber cleanup'
            ;;
        3)
            printf 'Case %s: late subscriber joins, then exits with a pending reference\n' "$case_number"
            publisher_sequence_before_join="$(last_sequence "$publisher_log" publisher)"
            start_subscriber_two
            wait_for_subscriber_registration 'subscriber 2' "$subscriber_two_pid" "$subscriber_two_log" 1
            wait_for_late_subscriber_progress "$publisher_sequence_before_join"

            pause_process 'subscriber 2' "$subscriber_two_pid"
            publisher_sequence_before_kill="$(last_sequence "$publisher_log" publisher)"
            wait_for_publisher_progress "$((publisher_sequence_before_kill + 1))"
            kill_subscriber 'subscriber 2' "$subscriber_two_pid"
            subscriber_two_pid=''

            publisher_sequence_before_kill="$(last_sequence "$publisher_log" publisher)"
            survivor_sequence_before_kill="$(last_sequence "$subscriber_one_log" subscriber)"
            publisher_target=$((publisher_sequence_before_kill + kPostKillSequenceDelta))
            survivor_target=$((survivor_sequence_before_kill + kPostKillSequenceDelta))
            wait_for_publisher_and_survivor_progress \
                'subscriber 1' "$subscriber_one_pid" "$subscriber_one_log" \
                "$publisher_target" "$survivor_target" \
                'late subscriber cleanup'
            ;;
        4)
            printf 'Case %s: publisher receives adjacent late-registration and disconnect messages\n' "$case_number"
            publisher_sequence_before_kill="$(last_sequence "$publisher_log" publisher)"
            survivor_sequence_before_kill="$(last_sequence "$subscriber_one_log" subscriber)"

            pause_process 'publisher' "$publisher_pid"
            start_subscriber_two
            wait_for_subscriber_registration 'subscriber 2' "$subscriber_two_pid" "$subscriber_two_log" 1
            kill_subscriber 'subscriber 2' "$subscriber_two_pid"
            subscriber_two_pid=''
            resume_process 'publisher' "$publisher_pid"

            publisher_target=$((publisher_sequence_before_kill + kPostKillSequenceDelta))
            survivor_target=$((survivor_sequence_before_kill + kPostKillSequenceDelta))
            wait_for_publisher_and_survivor_progress \
                'subscriber 1' "$subscriber_one_pid" "$subscriber_one_log" \
                "$publisher_target" "$survivor_target" \
                'adjacent registration/disconnect cleanup'
            ;;
    esac

    printf 'Case %s: PASS\n' "$case_number"
    stop_case_processes
    current_case_dir=''
}

is_positive_integer "$kReadySequence" || fail 'READY_SEQUENCE must be a positive integer'
is_positive_integer "$kJoinSequenceStride" || fail 'JOIN_SEQUENCE_STRIDE must be a positive integer'
is_positive_integer "$kJoinedSubscriberSequenceDelta" || fail 'JOINED_SUBSCRIBER_SEQUENCE_DELTA must be a positive integer'
is_positive_integer "$kPostKillSequenceDelta" || fail 'POST_KILL_SEQUENCE_DELTA must be a positive integer'
is_positive_integer "$kQuiescentObservations" || fail 'QUIESCENT_OBSERVATIONS must be a positive integer'
is_positive_integer "$kPhaseTimeoutSeconds" || fail 'PHASE_TIMEOUT_SECONDS must be a positive integer'
is_positive_integer "$kCaseCount" || fail 'CASE_COUNT must be a positive integer'
(( kReadySequence > kDescriptorSlotCount )) || fail 'READY_SEQUENCE must exceed the 16 descriptor slots'
(( kJoinedSubscriberSequenceDelta > kDescriptorSlotCount )) || fail 'JOINED_SUBSCRIBER_SEQUENCE_DELTA must exceed the 16 descriptor slots'
(( kPostKillSequenceDelta > kDescriptorSlotCount )) || fail 'POST_KILL_SEQUENCE_DELTA must exceed the 16 descriptor slots'
command -v "$kCxx" >/dev/null 2>&1 || fail "C++ compiler not found: $kCxx"
command -v make >/dev/null 2>&1 || fail 'make is required to build the current binaries'
command -v flock >/dev/null 2>&1 || fail 'flock is required to prevent concurrent test-script runs'

run_dir="$(mktemp -d "${TMPDIR:-/tmp}/drivebus-two-subscribers-kill9.XXXXXX")"
exec 9>"$kLockPath"
flock -n 9 || fail 'another two-subscriber kill-9 test is already running'

assert_ipc_namespace_is_idle
build_binaries
for ((case_number = 1; case_number <= kCaseCount; ++case_number)); do
    run_case "$case_number"
done

test_passed=1
printf 'PASS: %s dynamic subscriber lifecycle case(s) completed.\n' "$kCaseCount"
