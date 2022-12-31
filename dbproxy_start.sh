#!/bin/bash

REPLICA_ID=${HOSTNAME: -1}
echo "Starting DBProxy $REPLICA_ID..."
cargo run 0 --dbproxy -c ./o2versioner/conf_dbproxy$REPLICA_ID.toml