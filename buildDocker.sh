./gradlew clean build -x test -x integration

pushd TreeQueryDiscoveryService
docker build --tag pigpiggcp/treequery.discoveryservice:v0 .
popd

pushd TreeQueryNodeServerExecutable
docker build --tag pigpiggcp/treequery.node:v0 .
popd

docker push pigpiggcp/treequery.discoveryservice:v0
docker push pigpiggcp/treequery.node:v0