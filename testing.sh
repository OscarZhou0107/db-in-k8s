#!/bin/bash
# for mix_num in 3
# do
#     export mix_num
#     perl -i -p -e 's/"--mix", "[0-9]+/"--mix", "$ENV{mix_num}/g' k8s/deployment/load-generator-deployment.yaml
#     for threshold in 70 90
#     do
#         export threshold
#         perl -i -p -e 's/averageUtilization: [0-9]+/averageUtilization: $ENV{threshold}/g' k8s/deployment/dbproxy-hpa.yaml
#         for initial_db in 8 6 5 3 2 1
#         do
#             ./k8s_start.sh $initial_db
#         done
#     done
# done

mix_num=3
threshold=35
initial_db=3
export mix_num
export threshold
perl -i -p -e 's/"--mix", "[0-9]+/"--mix", "$ENV{mix_num}/g' k8s/deployment/load-generator-deployment.yaml
perl -i -p -e 's/averageUtilization: [0-9]+/averageUtilization: $ENV{threshold}/g' k8s/deployment/dbproxy-hpa.yaml
./k8s_start.sh $initial_db

mix_num=3
threshold=50
initial_db=3
export mix_num
export threshold
perl -i -p -e 's/"--mix", "[0-9]+/"--mix", "$ENV{mix_num}/g' k8s/deployment/load-generator-deployment.yaml
perl -i -p -e 's/averageUtilization: [0-9]+/averageUtilization: $ENV{threshold}/g' k8s/deployment/dbproxy-hpa.yaml
./k8s_start.sh $initial_db

mix_num=3
threshold=25
initial_db=8
export mix_num
export threshold
perl -i -p -e 's/"--mix", "[0-9]+/"--mix", "$ENV{mix_num}/g' k8s/deployment/load-generator-deployment.yaml
perl -i -p -e 's/averageUtilization: [0-9]+/averageUtilization: $ENV{threshold}/g' k8s/deployment/dbproxy-hpa.yaml
./k8s_start.sh $initial_db

mix_num=1
threshold=70
initial_db=6
export mix_num
export threshold
perl -i -p -e 's/"--mix", "[0-9]+/"--mix", "$ENV{mix_num}/g' k8s/deployment/load-generator-deployment.yaml
perl -i -p -e 's/averageUtilization: [0-9]+/averageUtilization: $ENV{threshold}/g' k8s/deployment/dbproxy-hpa.yaml
./k8s_start.sh $initial_db

mix_num=2
threshold=70
initial_db=6
export mix_num
export threshold
perl -i -p -e 's/"--mix", "[0-9]+/"--mix", "$ENV{mix_num}/g' k8s/deployment/load-generator-deployment.yaml
perl -i -p -e 's/averageUtilization: [0-9]+/averageUtilization: $ENV{threshold}/g' k8s/deployment/dbproxy-hpa.yaml
./k8s_start.sh $initial_db

mix_num=2
threshold=25
initial_db=8
export mix_num
export threshold
perl -i -p -e 's/"--mix", "[0-9]+/"--mix", "$ENV{mix_num}/g' k8s/deployment/load-generator-deployment.yaml
perl -i -p -e 's/averageUtilization: [0-9]+/averageUtilization: $ENV{threshold}/g' k8s/deployment/dbproxy-hpa.yaml
./k8s_start.sh $initial_db

mix_num=2
initial_db=3
export mix_num
export threshold
perl -i -p -e 's/"--mix", "[0-9]+/"--mix", "$ENV{mix_num}/g' k8s/deployment/load-generator-deployment.yaml
./k8s_start_fix.sh $initial_db

mix_num=3
initial_db=2
export mix_num
export threshold
perl -i -p -e 's/"--mix", "[0-9]+/"--mix", "$ENV{mix_num}/g' k8s/deployment/load-generator-deployment.yaml
./k8s_start_fix.sh $initial_db


# for mix_num in 1 2 3
# do
#     export mix_num
#     perl -i -p -e 's/"--mix", "[0-9]+/"--mix", "$ENV{mix_num}/g' k8s/deployment/load-generator-deployment.yaml
#     for initial_db in 1 2 3 5 6 8
#     do
#         ./k8s_start_fix.sh $initial_db
#     done
# done