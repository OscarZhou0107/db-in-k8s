#!/bin/bash
read -p "Please wait for some time to initialize the dashboard... (Press any key to continue)" flag
echo ===================================Delete Old Deployment==========================================
kubectl delete -f k8s/deployment
sleep 10 # Wait for old deployment fully deleted
docker container prune
echo ===================================Starting Dbproxies=============================================
kubectl apply -f k8s/deployment/dbproxy0-deployment.yaml
sleep 5
echo ===================================Starting Scheduler_sequencer===================================
cat k8s/deployment/scheduler-deployment.yaml | sed s+{{path}}+$(pwd)+g > k8s/deployment/scheduler-deployment_tmp.yaml
kubectl apply -f k8s/deployment/scheduler-deployment_tmp.yaml
sleep 5 # Need time for system to establish connection
echo ===================================System Started=================================================
kubectl get pods # Display the running pods
echo Please go to another machine under the same WI-FI network and execute: \"python3 ./load_generator/launcher.py --port 32077 --mix 3 --range 0 200 --ip \<your_local_IP\> --mock_db --path ./load_generator\"
# echo ===================================Start TPC-W====================================================
# read -p "Press any key to start TPC-W..." key
# kubectl apply -f k8s/deployment/load-generator-deployment.yaml
# echo Running TPC-W...
# kubectl top pods

# sleep 10
# echo ===================================Scale One dbproxy Up===========================================
# kubectl scale statefulsets dbproxy0-deployment --replicas=3
# sleep 5
# kubectl scale statefulsets dbproxy0-deployment --replicas=4
# kubectl exec scheduler-deployment-0 -- bash -c "netcat -e new_dbproxy_start.sh scheduler-deployment-0.scheduler-service.default.svc.cluster.local 9999"

