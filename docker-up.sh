#!/bin/bash

docker run -it \
  --name hnlink \
  -v "$PWD":/usr/src/app \
  -w /usr/src/app \
  node:4 \
  prepare.sh
