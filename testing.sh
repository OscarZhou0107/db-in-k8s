#!/bin/bash
for mix_num in 1 2 3
do
    export mix_num
    perl -i -p -e 's/"--mix", "[0-9]+/"--mix", "$ENV{mix_num}/g' k8s/deployment/load-generator-deployment.yaml
    for threshold in 70 90
    do
        export threshold
        perl -i -p -e 's/averageUtilization: [0-9]+/averageUtilization: $ENV{threshold}/g' k8s/deployment/dbproxy-hpa.yaml
        for initial_db in 8 6 5 3 2 1
        do
            ./k8s_start.sh $initial_db
        done
    done
done


for mix_num in 1 2 3
do
    export mix_num
    perl -i -p -e 's/"--mix", "[0-9]+/"--mix", "$ENV{mix_num}/g' k8s/deployment/load-generator-deployment.yaml
    for initial_db in 1 2 3 5 6 8
    do
        ./k8s_start_fix.sh $initial_db
    done
done