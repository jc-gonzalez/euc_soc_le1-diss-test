#!/bin/bash
#----------------------------------------------------------------------------
# Script for the execution of the LE1 generation and ingestion for the
# LE1 DSS Dissemination Test
#
# See https://euclid.roe.ac.uk/projects/easbench/wiki/DSSLE1Test
#
#----------------------------------------------------------------------------

SCRIPTPATH=$(cd $(dirname $0); pwd; cd - > /dev/null)
SCRIPTNAME=$(basename $0)

CURDIR=$(pwd)
cd ${SCRIPTPATH}

CURRDATE=$(date +"%Y%m%d_%H%M%S")
LOG="./log/fulllog_${CURRDATE}.log"

NOBS=$1
SLEEP=$2

time python3.6 ./LE1_Dissemination_Test.py --num_obs ${NOBS} --sleep ${SLEEP} 2>&1 | \
    tee ${LOG}


