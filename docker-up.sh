#!/bin/bash

docker run -d --name zookeeper \
  -p 2181:2181 \
  -v /data/zookeeper:/var/lib/zookeeper \
  confluent/zookeeper

sleep 4

docker run -d --name kafka \
  -p 9092:9092 \
  -v /data/kafka:/var/lib/kafka \
  -v /data/kafka_logs:/var/log/kafka \
  -v /git/hnlinks/etc/server.properties:/etc/kafka/server.properties \
  --link zookeeper:zookeeper \
  confluent/kafka

# Start Schema Registry and expose port 8081 for use by the host machine
#docker run -d --name schema-registry \
#  -p 8081:8081 \
#  --link zookeeper:zookeeper \
#  --link kafka:kafka \
#  confluent/schema-registry

# Start REST Proxy and expose port 8082 for use by the host machine
#docker run -d --name rest-proxy \
#  -p 8082:8082 \
#  --link zookeeper:zookeeper \
#  --link kafka:kafka \
#  --link schema-registry:schema-registry \
#  confluent/rest-proxy
    
    
#docker run -d --name hnlinks \
#  -v /data:/data \
#  -v /git/hnlink:/usr/src/app \
#  -w /usr/src/app \
#  node:4 \
#  ./prepare.sh
