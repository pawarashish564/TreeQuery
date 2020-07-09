
##Build Status

Overall: [![CircleCI](https://circleci.com/gh/dexterchan/TreeQuery.svg?style=svg)](https://circleci.com/gh/dexterchan/TreeQuery) <br>
Master: [![CircleCI](https://circleci.com/gh/dexterchan/TreeQuery/tree/master.svg?style=svg)](https://circleci.com/gh/dexterchan/TreeQuery/tree/master) <br>

##Discovery Service
###Docker build
Build service discovery
```
docker build --tag pigpiggcp/treequery.discoveryservice:v0 -f DockerFile.DiscoveryService .
```
Run
````
docker run -it --rm -p ${PORT}:8082 -e REGION=${$REGION}  -e ACCESSKEY=${ACCESSKEY} -e SECRETKEY=${SECRETKEY} pigpiggcp/treequery.discoveryservice:v0
````

##SQL schema
````
create table DailyGovBondPrice(
	AsOfDate varchar(12),
    Tenor varchar(10),
    Price decimal(10,2),
    primary key(AsOfDate, Tenor)
)
````

###Upload to maven repository
``````
gradlew -Pnexus uploadArchives
``````

#### Execute server
TreeQueryNodeServerExecutable
java -jar TreeQueryNodeServerExecutable-1.0-SNAPSHOT.jar < file name of treeQuery.yaml>
```
Sample of treeQuery.yaml
for cacheFilePath, you would put absolute path 
or put ${TMPDIR} for system temporary directory.
---
servicehostname: "localhost"
servicePort: 9002
cacheFilePath: "${TMPDIR}"
cluster: "A"
```