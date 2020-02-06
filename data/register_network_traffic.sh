#!/bin/bash

curr_date=$(date +"%Y%m%d_%H%M%S")
datafile="network_traffic_${curr_date}.dat"

echo "" > ${datafile}

while true ; do

    ifconfig eth0 | \
        awk -v thedate=$(date +"%Y%m%d_%H%M%S") 'BEGIN{printf "%s ",thedate;}{printf "%s ",$0;}' | \
        sed -e 's/[ ]+/ /g' | tee -a ${datafile}

    sleep 10

    if [ -f "./STOP" ]; then
        break
    fi

done
