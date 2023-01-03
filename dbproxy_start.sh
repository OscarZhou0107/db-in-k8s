#!/bin/bash

REPLICA_ID=${HOSTNAME: -1}
echo "Starting DBProxy $REPLICA_ID..."
if [[ $REPLICA_ID -gt '1' ]]
then
    cargo run 0 --dbproxy -c ./o2versioner/replicates$[REPLICA_ID-2].toml &
    sleep 3
    { echo block; echo connect $[REPLICA_ID-2]; echo unblock; echo break; } | netcat scheduler-deployment-0.scheduler-service.default.svc.cluster.local 9999
    while true; do sleep 1; done
else
    cargo run 0 --dbproxy -c ./o2versioner/conf_dbproxy$REPLICA_ID.toml
fi
