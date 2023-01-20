#!/bin/bash

REPLICA_ID=${HOSTNAME: -1}
echo "Dropping DBProxy $REPLICA_ID..."

{ echo block; echo drop $MY_POD_IP; echo unblock; echo break; } | netcat scheduler-deployment-0.scheduler-service.default.svc.cluster.local 9999 &