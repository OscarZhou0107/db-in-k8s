#!/bin/bash
read -p "Please wait for some time to initialize the dashboard... (Press any key to continue)" flag
echo ===================================Delete Old Deployment==========================================
kubectl delete -f k8s/deployment
sleep 10 # Wait for old deployment fully deleted
# docker container prune
echo ===================================Starting Dbproxies=============================================
kubectl apply -f k8s/deployment/dbproxy0-deployment.yaml
sleep 5
echo ===================================Starting Scheduler_sequencer===================================
kubectl apply -f k8s/deployment/scheduler-deployment.yaml
sleep 5 # Need time for system to establish connection
echo ===================================System Started=================================================
kubectl get pods # Display the running pods
echo ===================================Start TPC-W====================================================
read -p "Press any key to start TPC-W..." key
kubectl apply -f k8s/deployment/load-generator-deployment.yaml
echo Running TPC-W...
# kubectl top pods

sleep 10
echo ===================================Scale One dbproxy Up===========================================
kubectl scale statefulsets dbproxy0-deployment --replicas=3
# kubectl exec scheduler-deployment-0 -- bash -c "netcat -e new_dbproxy_start.sh scheduler-deployment-0.scheduler-service.default.svc.cluster.local 9999"