#!/usr/bin/env bash
#
# Env variables:
# - SMALL_TESTS
# - COVER_ENABLED
# - STOP_NODES (default false)
set -o pipefail
IFS=$'\n\t'

DEFAULT_PRESET=internal_mnesia
PRESET="${PRESET-$DEFAULT_PRESET}"
SMALL_TESTS="${SMALL_TESTS:-true}"
COVER_ENABLED="${COVER_ENABLED:-true}"
RETRY_BIG_TESTS="${RETRY_BIG_TESTS:-false}"
SKIP_AUTO_COMPILE="${SKIP_AUTO_COMPILE:-false}"

while getopts ":p::s::e::c:" opt; do
  case $opt in
    p)
      PRESET=$OPTARG
      ;;
    s)
      SMALL_TESTS=$OPTARG
      ;;
    c)
      COVER_ENABLED=$OPTARG
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      exit 1
      ;;
    :)
      echo "Option -$OPTARG requires an argument." >&2
      exit 1
      ;;
  esac
done

source tools/travis-common-vars.sh
source tools/travis-helpers.sh

if [ -n "${AWS_SECRET_ACCESS_KEY}" ]; then
  CT_REPORTS=$(ct_reports_dir)

  echo "Test results will be uploaded to:"
  echo $(s3_url ${CT_REPORTS})
fi
# Print ct_progress_hook output
echo "" > /tmp/progress
tail -f /tmp/progress &
PRINT_PROGRESS_PID=$!
CURRENT_SCRIPT_PID=$$
./tools/kill_processes_on_exit.sh $CURRENT_SCRIPT_PID $PRINT_PROGRESS_PID &

echo ${BASE}

# Example: choose_newest_directory dir1 dir2 dir3
# Returns: a directory, that was modified last
choose_newest_directory() {
  if [ "$#" -eq 0 ]; then
      echo "No arguments passed"
      exit 1
  fi

  if [ `uname` = "Darwin" ]; then
    ls -dt "$@" | head -n 1
  else
    ls -d "$@" --sort time | head -n 1
  fi
}

run_small_tests() {
  tools/print-dots.sh start
  tools/print-dots.sh monitor $$
  rm -f _build/test/logs/retry.spec
  make ct
  if [ -f "_build/test/logs/retry.spec" ]; then
      echo "Print _build/test/logs/retry.spec:"
      cat _build/test/logs/retry.spec
      echo ""
  fi
  tools/print-dots.sh stop
  SMALL_SUMMARIES_DIRS=${BASE}/_build/test/logs/ct_run*
  SMALL_SUMMARIES_DIR=$(choose_newest_directory ${SMALL_SUMMARIES_DIRS})
  ${TOOLS}/summarise-ct-results ${SMALL_SUMMARIES_DIR}
}

maybe_run_small_tests() {
  if [ "$SMALL_TESTS" = "true" ]; then
    echo "############################"
    echo "Running small tests (test/)"
    echo "############################"
    echo "Advice: "
    echo "    Add option \"-s false\" to skip embeded common tests"
    echo "Example: "
    echo "    ./tools/travis-test.sh -s false"
    run_small_tests
  else
    echo "############################"
    echo "Small tests skipped"
    echo "############################"
  fi
}

run_test_preset() {
  tools/print-dots.sh start
  tools/print-dots.sh monitor $$
  cd ${BASE}/big_tests
  local MAKE_RESULT=0
  TESTSPEC=${TESTSPEC:-default.spec}
  PREPARE="prepare"
  if [ "$SKIP_AUTO_COMPILE" = "true" ]; then
      PREPARE=""
  fi
  if [ "$COVER_ENABLED" = "true" ]; then
    make cover_test_preset $MAKE_ARGS TESTSPEC=$TESTSPEC PRESET=$PRESET PREPARE=$PREPARE
    MAKE_RESULT=$?
  else
    make test_preset $MAKE_ARGS TESTSPEC=$TESTSPEC PRESET=$PRESET PREPARE=$PREPARE
    MAKE_RESULT=$?
  fi
  cd -
  tools/print-dots.sh stop
  return ${MAKE_RESULT}
}

print_running_nodes() {
    echo "Running nodes:"
    # Expand wildcard into a bash array
    EPMDS=( "${BASE}"/_build/mim1/rel/mongooseim/erts-*/bin/epmd )
    # Missing index expands into ${EPMDS[0]}
    "$EPMDS" -names
}

maybe_pause_before_test() {
  if [ "$PAUSE_BEFORE_BIG_TESTS" -gt 0 ] 2>/dev/null; then
    local read_ret_val
    tools/print-dots.sh start_countdown "$PAUSE_BEFORE_BIG_TESTS" "continue in" $$
    read -es -p $'press enter to pause before the big_tests\n' -t "$PAUSE_BEFORE_BIG_TESTS"
    read_ret_val="$?"
    tools/print-dots.sh stop
    [ "$read_ret_val" -ne 0 ] && { echo; return; }
    echo "[PAUSED]"
    read -es -p $'press enter to continue\n'
  fi
}

run_tests() {
  maybe_run_small_tests
  SMALL_STATUS=$?
  echo "SMALL_STATUS=$SMALL_STATUS"
  echo ""
  echo "############################"
  echo "Running big tests (big_tests)"
  echo "############################"

  rm -f /tmp/ct_summary

  time ${TOOLS}/start-nodes.sh || { echo "Failed to start MongooseIM nodes"; return 1; }

  maybe_pause_before_test

  run_test_preset
  BIG_STATUS=$?

  # If /tmp/ct_summary is not empty file
  if [ "$RETRY_BIG_TESTS" = true ] && [ -s /tmp/ct_summary ]; then
      echo "Stopping MongooseIM nodes before retry"
      ./tools/stop-nodes.sh

      # Resetting Mnesia
      rm -rf _build/*/rel/mongooseim/Mnesia*

      echo "Starting MongooseIM nodes for retry"
      time ${TOOLS}/start-nodes.sh || { echo "Failed to start MongooseIM nodes"; return 1; }
      maybe_pause_before_test

      echo "Failed cases after first run:"
      cat /tmp/ct_summary
      echo ""

      echo "Generate retry auto_big_tests.spec"
      ./tools/test_runner/selected-tests-to-test-spec.sh $(cat /tmp/ct_summary)
      export TESTSPEC=auto_big_tests.spec

      # Disable cover for rerun
      COVER_ENABLED=false run_test_preset
      BIG_STATUS=$?
  fi

  SUMMARIES_DIRS=${BASE}/big_tests/ct_report/ct_run*
  SUMMARIES_DIR=$(choose_newest_directory ${SUMMARIES_DIRS})
  echo "SUMMARIES_DIR=$SUMMARIES_DIR"
  ${TOOLS}/summarise-ct-results ${SUMMARIES_DIR}
  BIG_STATUS_BY_SUMMARY=$?

  echo
  echo "All tests done."

  grep "fail_ci_build=true" ${BASE}/_build/mim*/rel/mongooseim/log/ejabberd.log
  # If phrase found than exit with code 1
  test $? -eq 1
  LOG_STATUS=$?

  if [ $SMALL_STATUS -eq 0 -a $BIG_STATUS -eq 0 -a $BIG_STATUS_BY_SUMMARY -eq 0 -a $LOG_STATUS -eq 0 ]
  then
    RESULT=0
    echo "Build succeeded"
  else
    RESULT=1
    echo "Build failed:"
    [ $SMALL_STATUS -ne 0 ] && echo "    small tests failed"
    [ $BIG_STATUS_BY_SUMMARY -ne 0 ]   && echo "    big tests failed"
    [ $BIG_STATUS -ne 0 ]   && echo "    big tests failed - missing suites (error code: $BIG_STATUS)"
    [ $LOG_STATUS -ne 0 ]   && echo "    log contains errors"
    print_running_nodes
  fi

  # If /tmp/ct_summary is not empty file
  if [ -s /tmp/ct_summary ]; then
      echo "Failed big cases:"
      cat /tmp/ct_summary
      echo ""
  fi

  # Do not stop nodes if big tests failed
  if [ "$STOP_NODES" = true ] && [ $BIG_STATUS -eq 0 ] && [ $BIG_STATUS_BY_SUMMARY -eq 0 ]; then
      echo "Stopping MongooseIM nodes"
      ./tools/stop-nodes.sh
  else
      echo "Keep MongooseIM nodes running"
  fi

  exit ${RESULT}
}

enable_tls_dist () {
  for node in ${DEV_NODES_ARRAY[@]}; do
    # Reenable commented out TLS dist options,
    # i.e. remove the single leading comment character on lines
    # commented out with just a single comment character.
    $SED -i -e 's/^#\([^#]\)/\1/' ${BASE}/_build/"$node"/rel/mongooseim/etc/vm.dist.args
  done
}

build_pkg () {
  set -e
  local platform=$1
  cd tools/pkg
  ./build $platform
  ./run $platform
  set +e
}

if [ "$PRESET" == "dialyzer_only" ]; then
  tools/print-dots.sh start
  tools/print-dots.sh monitor $$
  ./rebar3 dialyzer
  RESULT=$?
  tools/print-dots.sh stop
  exit ${RESULT}
elif [ "$PRESET" == "pkg" ]; then
  build_pkg $pkg_PLATFORM
elif [ "$PRESET" == "small_tests" ]; then
  time run_small_tests
  RESULT=$?
  exit ${RESULT}
else
  [ x"$TLS_DIST" == xyes ] && enable_tls_dist
  run_tests
fi

