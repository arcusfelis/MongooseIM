#!/usr/bin/env bash

MAIN_PID=$$

# This script has no arguments
# You can override a list of nodes to start by using DEV_NODES env variable
# DEV_NODES="mim1 mim2" ./tools/start-nodes.sh

# Stop script on an error
set -e

# We use BASE and DEV_NODES_ARRAY variables from here
source tools/travis-common-vars.sh
source tools/test_runner/helpers.sh

# Starts node in background
# First argument is node directory name
# Does not fail if the node is already running (but prints a message)
# Fails if release for the node is not compiled
start_node() {
  echo -n "${1} start: "
  ${BASE}/_build/${1}/rel/mongooseim/bin/mongooseimctl start && echo ok || echo failed
}

check_node() {
 if [ "$START_NODES" = true ]; then
   ${BASE}/_build/${1}/rel/mongooseim/bin/mongooseimctl started
 else
   ${BASE}/_build/${1}/rel/mongooseim/bin/mongooseim ping | grep pong >/dev/null
 fi
}

print_logs() {
    cat _build/$1/rel/mongooseim/log/crash.log | "$SED" -e 's/^/[crash.log]    /'
}

# Ensures that node is up
# Prints node information
# First argument is node directory name
# Fails if the node does not appear after 1 minute
wait_for_node() {
  echo "waiting for ${1}: "
  exit_code=0
  check_node "$1" || exit_code="$?"
  if [ $exit_code -ne 0 ]; then
      echo "Node $1 not running"
      print_logs $1
  fi
  ${BASE}/_build/${1}/rel/mongooseim/bin/mongooseimctl status
  return $exit_code
}

# DEV_NODES_ARRAY is defined in travis-common-vars.sh
# and contains node names mim1, mim2, ...
start_nodes() {
  local pids=()
  for node in ${DEV_NODES_ARRAY[@]}; do
    async_helper "start_node_$node" start_node $node &
    HELPER_PID=$!
    describe_pid "$HELPER_PID" " {start_node_$node} "
    pids+=("$HELPER_PID")
  done
 ./tools/kill_processes_on_exit.sh $MAIN_PID "${pids[@]}" &
  wait_for_pids "${pids[@]}"
}

wait_for_nodes() {
  local pids=()
  for node in ${DEV_NODES_ARRAY[@]}; do
    async_helper "wait_for_node_$node" wait_for_node $node &
    HELPER_PID=$!
    describe_pid "$HELPER_PID" " {wait_for_node_$node} "
    pids+=("$HELPER_PID")
  done
 ./tools/kill_processes_on_exit.sh $MAIN_PID "${pids[@]}" &
  wait_for_pids "${pids[@]}"
}

follow_logs() {
  local pids=()
  for node in ${DEV_NODES_ARRAY[@]}; do
    buffered_async_tail $node:erlang.log.1 ${BASE}/_build/$node/rel/mongooseim/log/erlang.log.1
    pids+=("$!")
    buffered_async_tail $node:crash.log ${BASE}/_build/$node/rel/mongooseim/log/crash.log
    pids+=("$!")
  done
 ./tools/kill_processes_on_exit.sh $ROOT_SCRIPT_PID "${pids[@]}" &
}

if [ "$PRINT_MIM_LOGS" = true ]; then
  follow_logs
fi

if [ "$START_NODES" = true ]; then
  start_nodes
else
  echo "Skipping MongooseIM nodes start"
fi

wait_for_nodes
