#!/bin/bash

REPLICA_ID=${HOSTNAME: -1}
echo "Dropping DBProxy $REPLICA_ID..."

{ echo block; sleep 1; echo drop $MY_POD_IP; sleep 1; echo unblock; echo break; } | netcat scheduler-deployment-0.scheduler-service.default.svc.cluster.local 9999 &