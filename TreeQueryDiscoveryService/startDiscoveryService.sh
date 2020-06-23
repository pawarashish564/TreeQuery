#!/bin/bash

if [ -z "$PORT" ] ; then PORT=8082; fi
if [ -z "$REGION" ] ; then REGION="us-west-2"; fi

buildVersion=1.0-SNAPSHOT
java -jar ./TreeQueryDiscoveryService-${buildVersion}.jar -p ${PORT} -r ${REGION} \
--accessKey=${ACCESSKEY} --secretKey=${SECRETKEY}