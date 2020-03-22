from cluster.TreeQueryCluster import Node, ClusterDependencyGraph, readTreeInput
import avro.schema

import json


class AvroSchemaHelper:
    def getAvroSchema(self, node:Node):
        return self.__dfsHelper(node)

    def __dfsHelper(self, node: Node):

        if len(node.children) == 0:
            #Get avro schema
            return node.getAvroSchemaObj()

        newSchema = {

        }
        schemaLst = []
        for child in node.children:
            #Join children
            schemaLst.append(self.__dfsHelper(child))




        #Join the schemaSet
        return avro.schema



def testSimpleAvroCluster():
    SimpleAvroCluster = "resource/SimpleAvroReadCluster.json"
    rootNode = readTreeInput(SimpleAvroCluster)
    clusterDepGraph = ClusterDependencyGraph()
    clusterDepGraph.constructDependencyGraph(rootNode)

    avroSchemaHelper = AvroSchemaHelper()

    while True:
        clusterList = clusterDepGraph.findClusterWithoutDependency()
        if len(clusterList) == 0:
            break
        for c in clusterList:
            # DFS to leaf node
            schema = avroSchemaHelper.getAvroSchema(c)
            assert (schema is not None)
            node = clusterDepGraph.removeClusterDependency(c)

def testClusterOfThree():
    AvroCluster="resource/TreeQueryInput.json"
    rootNode = readTreeInput(AvroCluster)
    clusterDepGraph = ClusterDependencyGraph()
    clusterDepGraph.constructDependencyGraph(rootNode)

    avroSchemaHelper = AvroSchemaHelper()

    while True:
        clusterList = clusterDepGraph.findClusterWithoutDependency()
        if len(clusterList) == 0:
            break
        for c in clusterList:
            # DFS to leaf node
            schema = avroSchemaHelper.getAvroSchema(c)
            assert (schema is not None)
            node = clusterDepGraph.removeClusterDependency(c)

if __name__ == "__main__":
    testClusterOfThree()