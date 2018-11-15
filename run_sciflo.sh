#!/bin/bash
BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)

# source PGE env
export PYTHONPATH=$BASE_PATH:$PYTHONPATH
export PATH=$BASE_PATH:$PATH

# source environment
source $HOME/verdi/bin/activate

echo "##########################################" 1>&2
echo -n "Running run_sciflo.py on $1: " 1>&2
date 1>&2
$BASE_PATH/run_sciflo.py $BASE_PATH/$1 _context.json > run_sciflo.log 2>&1
STATUS=$?
echo -n "Finished running $1 run_sciflo.py: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to run $1 run_sciflo.py." 1>&2
  cat run_sciflo.log 1>&2
  echo "{}"
  exit $STATUS
fi
