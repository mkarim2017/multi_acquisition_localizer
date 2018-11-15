#!/bin/bash
BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)

# source PGE env
export PYTHONPATH=$BASE_PATH:$PYTHONPATH
export PYTHONPATH=${PYTHONPATH}:${HOME}/verdi/etc 
export PATH=$BASE_PATH:$PATH

# source environment
source $HOME/verdi/bin/activate

echo "##########################################" 1>&2
echo -n "Running acquisition_localizer.py on $1: " 1>&2
date 1>&2
$BASE_PATH/acquisition_localizer.py $BASE_PATH/$1 > acquisition_localizer.log 2>&1
STATUS=$?
echo -n "Finished running $1 acquisition_localizer.py: " 1>&2
date 1>&2
if [ $STATUS -ne 0 ]; then
  echo "Failed to run $1 acquisition_localizer.py." 1>&2
  cat acquisition_localizer.log 1>&2
  echo "{}"
  exit $STATUS
fi
