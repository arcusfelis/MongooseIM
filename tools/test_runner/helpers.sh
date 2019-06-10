export ROOT_SCRIPT_PID=${ROOT_SCRIPT_PID:-$$}

# Delays pipe by buffering data and flushing it periodically.
# This function creates subshell.
function time_buffered
{
    tools/test_runner/time_buffered $1
}

# It is very important to properly kill the children,
# otherwise "docker exec" would never return.
function buffered_async_tail
{
    local THREAD_NAME=$1
    local LOG_FILE=$2

    mkdir -p $(dirname "$LOG_FILE")
    touch "$LOG_FILE"

    # Redirect file into subshell
    tail -F "$LOG_FILE" \
        > >(
        # Kill current subshell when ROOT_SCRIPT_PID dies
        ./tools/kill_processes_on_exit.sh "$ROOT_SCRIPT_PID" "$$" &
        time_buffered 5 | "$SED" -e 's/^/'["$THREAD_NAME"']    /'
        ) &

    # Subshell is started before tail
    local TAIL_PID=$!
    # Use negative pid to kill the process and its children
    # (i.e. we want to kill tail)
    ./tools/kill_processes_on_exit.sh "$ROOT_SCRIPT_PID" "$TAIL_PID" &
}

function buffered_async_helper
{
    local THREAD_NAME=$1
    shift 1
    ./tools/kill_processes_on_exit.sh $ROOT_SCRIPT_PID $$ &

    local ret_val=0 output=""
    local LOG_FILE=_build/.test_runner/logs/$THREAD_NAME
    rm -f "$LOG_FILE"

    buffered_async_tail "$THREAD_NAME" "$LOG_FILE"

    "$@" > "$LOG_FILE" || ret_val="$?"

    echo "FINISHED: $THREAD_NAME returns $ret_val"
    return "$ret_val"
}

function wait_for_pids
{
    ## wait for all pids
    wait "$@" || true
    ## wait for pids one by one, so script can be stopped on error
    for pid in "$@"; do
        wait "$pid"
    done
}
