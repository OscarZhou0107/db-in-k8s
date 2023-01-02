#!/bin/bash

REPLICA_ID=${HOSTNAME: -1}
echo "Starting DBProxy $REPLICA_ID..."
if [[ $REPLICA_ID -eq '2' ]]
then
    cargo run 0 --dbproxy -c ./o2versioner/replicates.toml
else
    cargo run 0 --dbproxy -c ./o2versioner/conf_dbproxy$REPLICA_ID.toml
fi