REGION=$1
AWS_REPOSITORY=192592784707.dkr.ecr.${REGION}.amazonaws.com
aws ecr get-login-password --region ${REGION} | docker login --username AWS --password-stdin ${AWS_REPOSITORY}

IMAGENAME=treequery.discoveryservice:v0
docker tag pigpiggcp/${IMAGENAME} ${AWS_REPOSITORY}/${IMAGENAME}
docker push ${AWS_REPOSITORY}/${IMAGENAME}

IMAGENAME=treequery.node:v0
docker tag pigpiggcp/${IMAGENAME} ${AWS_REPOSITORY}/${IMAGENAME}
docker push ${AWS_REPOSITORY}/${IMAGENAME}
