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

    local ret_val=0 output=""
    local LOG_FILE=_build/.test_runner/logs/$THREAD_NAME
    rm -f "$LOG_FILE"

    buffered_async_tail "$THREAD_NAME" "$LOG_FILE"

    "$@" > "$LOG_FILE" || ret_val="$?"

    echo "FINISHED: $THREAD_NAME returns $ret_val"
    return "$ret_val"
}

function seconds
{
    date +%s
}

PIDS_DESCRIPTIONS=()

function describe_pid
{
    PIDS_DESCRIPTIONS[$1]=$2
}

# Usage: pid_info PID1 PID2...
function pid_info
{
    for pid in "$@"
    do
        echo -n ${PIDS_DESCRIPTIONS[$pid]:-}" "
    done
}

function wait_for_pids_log
{
    echo -e "\n$1"
}

function wait_for_pids
{
    local pidsArray=( "$@" ) # pids to wait for
    local max_time="3600" # If execution takes longer than $max_time seconds, will stop execution.

    local standby_interval=60 ## In seconds

    local seconds_begin=$(seconds) # Seconds since the beginning of the script
    local exec_time=0 # Seconds since the beginning of this function

    local retval=0 # return value of monitored pid process
    local errorcount=0 # Number of pids that finished with errors

    local pidCount # number of given pids
    local next_standby_alarm=$standby_interval

    pidCount=${#pidsArray[@]}
    pidDuration=()
    pidResults=()

    originalPidsArray=("${pidsArray[@]}")

    wait_for_pids_log "WAITING_STARTED for $pidCount tasks."

    while true; do
        newPidsArray=()
        for pid in "${pidsArray[@]}"; do
            if kill -0 $pid > /dev/null 2>&1; then
                newPidsArray+=($pid)
            else
                result=0
                wait $pid || result=$? && true

                exec_time=$(($(seconds) - $seconds_begin))
                pidDuration[$pid]=$exec_time
                pidResults[$pid]=$result
                wait_for_pids_log "WAITING_FINISHED Pid $pid $(pid_info $pid) with exitcode $result after $exec_time seconds."
                if [ $result -ne 0 ]; then
                    errorcount=$((errorcount+1))
                fi
            fi
        done

        if [ ${#newPidsArray[@]} -eq 0 ]; then
            break
        fi

        # Log a standby message
        exec_time=$(($(seconds) - $seconds_begin))
        if [ $exec_time -ge $next_standby_alarm ]; then
            next_standby_alarm=$(($next_standby_alarm + $standby_interval))
            wait_for_pids_log "WAITING_PROGRESS Current tasks still running with pids ${newPidsArray[@]} $(pid_info ${newPidsArray[@]}) after $exec_time seconds."
        fi

        if [ $exec_time -gt $max_time ] && [ $max_time -ne 0 ]; then
            wait_for_pids_log "WAITING_FAILED Max execution time exceeded for pids ${newPidsArray[@]}. Stopping task execution."
            kill -SIGTERM ${newPidsArray[@]}
            errrorcount=$((errorcount+1))
        fi

        pidsArray=("${newPidsArray[@]}")
        sleep 1
    done

    exec_time=$(($(seconds) - $seconds_begin))
    wait_for_pids_log "WAITING_DONE ended using $pidCount subprocesses with $errorcount errors after $exec_time seconds."

    # Print summary
    for pid in "${originalPidsArray[@]}"; do
        exec_time=${pidDuration[$pid]:-UNKNOWN}
        result=${pidResults[$pid]:-UNKNOWN}
        wait_for_pids_log "TASK $pid $(pid_info $pid) took $exec_time seconds. Exit code $result."
    done

    return $errorcount
}
