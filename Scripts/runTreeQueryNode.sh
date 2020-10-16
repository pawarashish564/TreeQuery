#!/bin/bash

DOCKERIMAGE=$1
WORKDIR=$2

docker run -it --rm -v $WORKDIR:/opt/config  \
-e CLUSTERCFG=treequeryCluster.templ.yaml \
-e DBCFG=DatabaseConnection.templ.yaml \
-e NODE_HOSTNAME=$NODE_HOSTNAME \
-e NODE_PORT=$NODE_PORT \
-e CLUSTERNAME=$CLUSTERNAME \
-e SERVICE_DISCOVERY_HOSTNAME=$SERVICE_DISCOVERY_HOSTNAME \
-e SERVICE_DISCOVERY_PORT=$SERVICE_DISCOVERY_PORT \
-e AWS_REGION=$AWS_S3_REGION \
-e AWS_ACCESS_KEY=$AWS_ACCESS_KEY \
-e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
-p $NODE_PORT:$NODE_PORT $DOCKERIMAGE