#!/bin/bash

# Clean up unused images, containers and bridges
docker container prune
docker image prune
docker network rm db-in-k8s

# Create the bridge
docker network create db-in-k8s

# Build the images
docker build ./ -t systems
docker build ./load_generator -t load_generator -f load_generator/Dockerfile

# Run the system
# docker run -d -it --rm --name systems --net db-in-k8s systems 
# docker run -d -it --rm --net db-in-k8s load_generator