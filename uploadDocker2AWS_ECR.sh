REGION=$1
TAG=$2
AWS_REPOSITORY=192592784707.dkr.ecr.${REGION}.amazonaws.com
aws ecr get-login-password --region ${REGION} | docker login --username AWS --password-stdin ${AWS_REPOSITORY}

IMAGENAME=treequery.discoveryservice:${TAG}
docker tag pigpiggcp/${IMAGENAME} ${AWS_REPOSITORY}/${IMAGENAME}
docker push ${AWS_REPOSITORY}/${IMAGENAME}
docker rmi --force ${AWS_REPOSITORY}/${IMAGENAME}

IMAGENAME=treequery.node:${TAG}
docker tag pigpiggcp/${IMAGENAME} ${AWS_REPOSITORY}/${IMAGENAME}
docker push ${AWS_REPOSITORY}/${IMAGENAME}
docker rmi --force ${AWS_REPOSITORY}/${IMAGENAME}
