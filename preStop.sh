#!/bin/bash

REPLICA_ID=${HOSTNAME: -1}
echo "Dropping DBProxy $REPLICA_ID..."

if [[ $REPLICA_ID -gt '1' ]]
then
    { sleep 10; echo block; echo drop $[REPLICA_ID-2]; echo unblock; echo break; } | netcat scheduler-deployment-0.scheduler-service.default.svc.cluster.local 9999 &
fi