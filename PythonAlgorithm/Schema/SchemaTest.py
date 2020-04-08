from cluster.TreeQueryCluster import Node, ClusterDependencyGraph, readTreeInput
import avro.schema
from Model.TreeNode import  JoinNode
from typing import Dict
import json


class AvroSchemaHelper:
    def getAvroSchema(self, node:Node):
        schemaJson = self.__dfsHelper(node)
        schemaJsonStr = json.dumps(schemaJson)
        return avro.schema.parse(schemaJsonStr)

    def __dfsHelper(self, node: Node)->Dict:

        if len(node.children) == 0:
            #Get avro schema
            return json.loads(node.getAvro_schema())

        #nodeType = node.
        newSchema = {
            "fields": []
        }
        schemaLst = []
        for child in node.children:
            #Join children
            sch = self.__dfsHelper(child)
            schemaLst.append(sch)
        if isinstance(node, JoinNode):
            newSchema["name"] = node.name
            newSchema["type"] = "record"
            newSchema["namespace"] = "org.treequery.join"
            for key in node.getKeys():
                newType = {}
                newType["name"] = key.leftLabel
                newType["type"] = schemaLst[key.leftinx]
                newSchema["fields"].append(newType)
                newType = {}
                newType["name"] = key.rightLabel
                newType["type"] = schemaLst[key.rightinx]
                newSchema["fields"].append(newType)
        else:
            if len(schemaLst) > 0:
                newSchema = schemaLst[0]
            else:
                raise Exception ("No schema from child nodes")
        return newSchema



def testSimpleAvroCluster():
    SimpleAvroCluster = "resource/SimpleAvroReadCluster.json"
    rootNode = readTreeInput(SimpleAvroCluster)
    clusterDepGraph = ClusterDependencyGraph()
    clusterDepGraph.constructDependencyGraph(rootNode)

    avroSchemaHelper = AvroSchemaHelper()

    while True:
        clusterList = clusterDepGraph.popClusterWithoutDependency()
        if len(clusterList) == 0:
            break
        for c in clusterList:
            # DFS to leaf node
            schema = avroSchemaHelper.getAvroSchema(c)
            assert (schema is not None)

def testClusterOfThree():
    AvroCluster="resource/TreeQueryInput.json"
    rootNode = readTreeInput(AvroCluster)
    clusterDepGraph = ClusterDependencyGraph()
    clusterDepGraph.constructDependencyGraph(rootNode)

    avroSchemaHelper = AvroSchemaHelper()

    while True:
        clusterList = clusterDepGraph.popClusterWithoutDependency()
        if len(clusterList) == 0:
            break
        for c in clusterList:
            # DFS to leaf node
            schema = avroSchemaHelper.getAvroSchema(c)
            assert (schema is not None)

if __name__ == "__main__":
    testClusterOfThree()