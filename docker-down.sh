#!/bin/bash

docker kill hnlink
docker kill zookeeper

docker rm -f hnlink
docker rm -f zookeeper