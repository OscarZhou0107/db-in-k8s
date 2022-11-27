#!/bin/bash
echo ===================================Delete Old Deployment==========================================
kubectl -n kubernetes-dashboard delete serviceaccount admin-user
kubectl -n kubernetes-dashboard delete clusterrolebinding admin-user
pkill kubectl proxy
kubectl delete -f k8s/k8s_dashboard
echo ===================================Deploy Metrics Server==========================================
kubectl apply -f k8s/k8s_dashboard/metrics-server.yaml
echo ===================================Deploy the Dashboard UI========================================
kubectl apply -f k8s/k8s_dashboard/dashboard.yaml
echo Starting the Dashboard Server...
kubectl proxy&
echo Done
echo ===================================Create Dashboard User==========================================
kubectl apply -f k8s/k8s_dashboard/dashboard-adminuser.yaml
echo ===================================Please login with Token========================================
echo Please enter the token to http://localhost:8001/api/v1/namespaces/kubernetes-dashboard/services/https:kubernetes-dashboard:/proxy/
kubectl -n kubernetes-dashboard create token admin-user
