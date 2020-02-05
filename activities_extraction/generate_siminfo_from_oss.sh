#!/bin/bash
# Extracts (and sends to console) the relevant parameters for the generation of
# LE1 data products, for the LE1 Dissemination Test
#
# Usage: $0 <ossfile> <fromObsId> <toObsId>
#

OSSFILE=$1
FROM=$2
TO=$3

cat $OSSFILE | tr '<>/="' '     ' | \
awk -v from=$FROM -v to=$TO '
BEGIN {
    print "{\n    \"acts\": [";
    isfirst = 1;
}
($1 == "Observation") {
    obs_id = $3; fld_id = $9;
    if (length(obs_id) < 1) { next; }
    if ((obs_id < from) || (obs_id > to)) {
        getline;
        while ($1 != "Observation") { getline; }
        next;
    }
}
($1 == "Pointing") {
    dither = $5;
    seq_id = $7; next;
}
($1 == "PointingActivity") {
    act = $2;
}
($1 == "Longitude") {
    lon = $2; getline;
    lat = $2; getline;
    angle = $2;
}
($1 == "StartTimeUtc") {
    start_time = $2;
}
($1 == "ParameterSet") {
    if ((obs_id >= from) && (obs_id <= to)) {
        if (isfirst < 1) { printf ",\n"; }
        printf "        { \"obs\": \"%05d\", \"field\": %d, \"dither\": %1d, ", obs_id, fld_id, dither + 1;
        printf "\"seq_id\": \"%d\", \"act\": \"%s\", \"start\": \"%s\", ", seq_id, act, start_time;
        printf "\"lon\": \"%d\", \"lat\": \"%s\", \"angle\": \"%s\" }", lon, lat, angle;
        isfirst = 0;
    }
}
END { print "\n    ]\n}\n"; }
'
