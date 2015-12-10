#!/bin/bash

docker run -it \
  --name hnlink \
  -v /data/hnlink:/data \
  -v /git/hnlink:/usr/src/app \
  node:4 \
  ./prepare.sh
