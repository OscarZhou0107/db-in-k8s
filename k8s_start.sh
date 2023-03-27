#!/bin/bash
read -p "Please wait for some time to initialize the dashboard... (Press any key to continue)" flag
echo ===================================Delete Old Deployment==========================================
kubectl delete -f k8s/deployment
echo "Deleting old containers..."
while : ; do
    msg=$(kubectl get pod 2>&1)
    if [[ $msg == "No resources"* ]]
    then
        break
    fi
done
docker container prune
echo ===================================Start Metrics Server=============================================
kubectl apply -f k8s/k8s_dashboard/metrics-server.yaml
echo "Initializing Metrics Server..."
while : ; do
    msg=$(kubectl top pod 2>&1)
    if [[ "$msg" != "Error"* && "$msg" != "error"* ]]
    then
        break
    fi
done
kubectl top pod
echo ===================================Start Dbproxies=============================================
read -p "Please specify the number of initial DB proxies [1-8]: " replicas
kubectl apply -f k8s/deployment/dbproxy0-deployment.yaml
sleep 2
echo ===================================Start Scheduler_sequencer===================================
cat k8s/deployment/scheduler-deployment.yaml | sed s+{{path}}+$(pwd)+g > k8s/deployment/scheduler-deployment_tmp.yaml
kubectl apply -f k8s/deployment/scheduler-deployment_tmp.yaml
sleep 10 # Need time for system to establish connection
kubectl scale statefulsets dbproxy0-deployment --replicas=$replicas
while : ; do
    msg=$(kubectl get pod | grep dbproxy0-deployment | grep -c Running 2>&1)
    if [[ $msg == $replicas ]]
    then
        break
    fi
done
sleep 2
echo ===================================System Started=================================================
kubectl get pods # Display the running pods
echo ===================================Start HPA Controller=================================================
kubectl apply -f k8s/deployment/dbproxy-hpa.yaml
echo "Initializing HPA controller..."
while : ; do
    msg=$(kubectl get hpa 2>&1)
    if [[ "$msg" != *"unknown"* ]]
    then
        break
    fi
done
echo ===================================Start TPC-W====================================================
echo Please go to another machine under the same WI-FI network and execute: \"python3 ./load_generator/launcher.py --port 32077 --mix 3 --range 0 200 --ip \<your_local_IP\> --mock_db --path ./load_generator\"
read -p "Press any key to confirm TPC-W started..." key

# Running 600s(10min), and collect the perf
gtimeout --foreground 600 watch -n 1 -d kubectl describe hpa 
kubectl exec scheduler-deployment-0 -- bash -c "{ echo perf; echo break; } | netcat scheduler-deployment-0.scheduler-service.default.svc.cluster.local 9999"

# Rename perf folder
perf_folder=$(ls perf | grep 23 | sort -rn | head -n 1)
min_replica=$(grep 'minReplicas' k8s/deployment/dbproxy-hpa.yaml | sed 's/[^0-9]*//g')
max_replica=$(grep 'maxReplicas' k8s/deployment/dbproxy-hpa.yaml | sed 's/[^0-9]*//g')
threshold=$(grep 'averageUtilization' k8s/deployment/dbproxy-hpa.yaml | sed 's/[^0-9]*//g')
scaleDown_stablization=$(grep -A 1 'scaleDown' k8s/deployment/dbproxy-hpa.yaml | grep 'stabilizationWindowSeconds' | sed 's/[^0-9]*//g')
scaleUp_stablization=$(grep -A 1 'scaleUp' k8s/deployment/dbproxy-hpa.yaml | grep 'stabilizationWindowSeconds' | sed 's/[^0-9]*//g')
scaleDown_periodSeconds=$(grep -A 5 'scaleDown' k8s/deployment/dbproxy-hpa.yaml | grep 'periodSeconds' | sed 's/[^0-9]*//g')
scaleUp_periodSeconds=$(grep -A 5 'scaleUp' k8s/deployment/dbproxy-hpa.yaml | grep 'periodSeconds' | sed 's/[^0-9]*//g')
new_name=perf/min$[min_replica]_max$[max_replica]_thre_$[threshold]_SD_$[scaleDown_stablization]_$[scaleDown_periodSeconds]_SU_$[scaleUp_stablization]_$[scaleUp_periodSeconds]
mv perf/$perf_folder $new_name
echo "Save to perf folder: $new_name"

# Save HPA scaling event
kubectl describe hpa > $new_name/scaling_event_log.txt
echo "Save HPA events to $new_name/scaling_event_log.txt"

# sleep 10
# echo ===================================Scale One dbproxy Up===========================================
# kubectl scale statefulsets dbproxy0-deployment --replicas=3
# sleep 5
# kubectl scale statefulsets dbproxy0-deployment --replicas=4
# kubectl exec scheduler-deployment-0 -- bash -c "netcat -e new_dbproxy_start.sh scheduler-deployment-0.scheduler-service.default.svc.cluster.local 9999"

