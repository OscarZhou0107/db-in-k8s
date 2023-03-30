#!/bin/bash

git checkout testing
for mix_num in 3
do
    for replicas in 8
    do
        ./test_document.sh $replicas $mix_num
    done
done