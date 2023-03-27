#!/bin/bash

REPLICA_ID=${HOSTNAME: -1}
echo "Starting DBProxy $REPLICA_ID..."
if [[ $REPLICA_ID -gt '0' ]]
then
    { echo block; sleep 1; echo connect $[REPLICA_ID-1]; sleep 1; echo unblock; echo break; } | netcat scheduler-deployment-0.scheduler-service.default.svc.cluster.local 9999 &
    cargo run 0 --dbproxy -c ./o2versioner/replicates$[REPLICA_ID-1].toml 
else
    cargo run 0 --dbproxy -c ./o2versioner/conf_dbproxy$REPLICA_ID.toml
fi
