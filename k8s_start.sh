#!/bin/bash
read -p "Please wait for some time to initialize the dashboard... (Press any key to continue)" flag
echo ===================================Delete Old Deployment==========================================
kubectl delete -f k8s/deployment
sleep 10 # Wait for old deployment fully deleted
docker container prune
echo ===================================Starting Dbproxies=============================================
kubectl apply -f k8s/deployment/dbproxy0-deployment.yaml
# kubectl apply -f k8s/deployment/dbproxy1-deployment.yaml
echo ===================================Starting Scheduler_sequencer===================================
kubectl apply -f k8s/deployment/scheduler-deployment.yaml
sleep 3 # Need time for system to establish connection
echo ===================================System Started=================================================
kubectl get pods # Display the running pods
echo ===================================Start TPC-W====================================================
read -p "Press any key to start TPC-W..." key
kubectl apply -f k8s/deployment/load-generator-deployment.yaml
echo Running TPC-W...
kubectl top pods