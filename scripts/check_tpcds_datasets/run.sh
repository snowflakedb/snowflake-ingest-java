#!/bin/bash

# setup venv
PVENV_DIR=${PWD}/venv
if [[ ! -d "${PVENV_DIR}" ]]; then
    python3.6 -m venv $PVENV_DIR > /dev/null
    source $PVENV_DIR/bin/activate > /dev/null
    pip3 install --upgrade pip > /dev/null
    pip3 install -r $PWD/requirements.txt > /dev/null
else
    source $PVENV_DIR/bin/activate > /dev/null
fi

${PWD}/tpcds_bdec_latest_database.py
exit $?