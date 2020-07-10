# TreeQuery node
Each Tree query node requires two configurations:

##### 1) Tree Query cluster yaml 
Example: <br>
${TMPDIR} represents system temp directory
````
---
servicehostname: "<DNS name/IP address hosting Tree Query Node e.g. 192.168.1.14>"
servicePort: <port number of Tree Query node>
cacheFilePath: "${TMPDIR}"
cluster: "<cluster name>"
serviceDiscoveryHostName: "<DNS name/IP address hosting of service discovery>"
serviceDiscoveryPort: "<DNS name/IP address hosting of service discovery port>"

````
##### 2)  Data base configuration
Example: <br>
You would inject value as environment variable.

````
---
mongo.UserName: "mongoadmin"
mongo.Password: ${MONGO_PWD}
mongo.Hostname: ${MONGO_HOSTNAME}
mongo.Port: ${MONGO_PORT}

jdbc.Type: "mysql"
jdbc.Hostname: ${JDBC_HOSTNAME}
jdbc.Port: ${JDBC_PORT}
jdbc.Database: ${JDBC_DATABASE}
jdbc.Driver: ${JDBC_DRIVER}
jdbc.UserName : ${JDBC_USER}
jdbc.Password : ${JDBC_PWD}

````


## Execution without Docker
````
cd build/distributions/TreeQueryNodeServerExecutable-1.0-SNAPSHOT/bin
./TreeQueryNodeServerExecutable -c <tree query cluster yaml> -d <database connection yaml>

#example:
./TreeQueryNodeServerExecutable -c /Users/dexter/temp/run/treeQueryA.yaml -d /Users/dexter/temp/run/DatabaseConnection2.yaml
./TreeQueryNodeServerExecutable -c /Users/dexter/temp/run/treeQueryB.yaml -d /Users/dexter/temp/run/DatabaseConnection2.yaml
````

## Build Docker image
````
docker build --tag pigpiggcp/treequery.node:v0 . 
````

## Execution
````
docker run -it --rm -v /Users/dexter/temp/run:/opt/config -v /Users/dexter/temp/run:/opt/data \
-e CLUSTERCFG=treeQueryA.yaml -e DBCFG=DatabaseConnection2.yaml -p 9012:9012 pigpiggcp/treequery.node:v0 

docker run -it --rm -v /Users/dexter/temp/run:/opt/config -v /Users/dexter/temp/run:/opt/data \
-e CLUSTERCFG=treeQueryB.yaml -e DBCFG=DatabaseConnection2.yaml -p 9013:9013 pigpiggcp/treequery.node:v0 
````