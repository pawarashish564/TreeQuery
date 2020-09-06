./gradlew clean build -x test -x integration

docker build --tag pigpiggcp/treequery.discoveryservice:v0 TreeQueryDiscoveryService
docker build --tag pigpiggcp/treequery.node:v0 TreeQueryNodeServerExecutable

docker push pigpiggcp/treequery.discoveryservice:v0
docker push pigpiggcp/treequery.node:v0