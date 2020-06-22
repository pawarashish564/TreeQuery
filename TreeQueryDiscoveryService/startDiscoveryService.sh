#!/bin/bash

buildVersion=1.0-SNAPSHOT
java -jar ./TreeQueryDiscoveryService-${buildVersion}.jar -p ${PORT} -r ${REGION} \
--accessKey=${ACCESSKEY} --secretKey=${SECRETKEY}