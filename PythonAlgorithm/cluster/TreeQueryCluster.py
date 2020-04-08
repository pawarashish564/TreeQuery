
#Cluster Tree query problem
# Diagram
# https://github.com/dexterchan/TreeQuery/blob/master/resource/TreeQueryArchitectureCluster.png
#Each node is stateless with identical code.
#A json file describes the tree schema.
#The request to the root node carries this json file.
#Nodes run in different clusters representing different zones/regions.
#In the diagram,
#10Y analysis INNER JOIN, I/O running in cluster B.
#5Y analysis INNER JOIN, I/O running in cluster A.
#In the end, FLATTEN of 5Y and 10Y result running in cluster A

#Json file is in TreeQueryInput.json
JsonInput = "resource/TreeQueryInput3.json"
import json
from collections import deque
from typing import Dict, Set, List
from Model.TreeNode import Node, nodeFactory




class __JNodeInsert:
    def __init__(self, jNode:Dict, parent: Node = None):
        self.jNode = jNode
        self.parent = parent




def readTreeInput (JsonInput):
    rootNode = None

    jsonNodeRoot = None
    stack = []
    with open(JsonInput) as json_file:
        jsonNodeRoot = json.load(json_file)
    #Load Dict into Node

    stack.append(__JNodeInsert(jsonNodeRoot))

    while len(stack)>0:
        __jNodeInsert = stack.pop()
        jNode, parentNode = __jNodeInsert.jNode, __jNodeInsert.parent
        newNode = nodeFactory(jNode)
        if rootNode is None:
            rootNode = newNode
        else:
            parentNode.insertChildren(newNode)

        if "children" in jNode:
            for jCnode in jNode["children"]:
                stack.append(__JNodeInsert(jCnode, newNode))
    return rootNode


class DepCluster:
    def __init__(self, node: Node, parentClusterNode: Node):
        self.node = node
        self.parentClusterNode = parentClusterNode



class ClusterDependencyGraph():
    def __init__(self):
        self.clusterDepGraph = {}
        self.cacheDependency = {}
    def constructDependencyGraph(self, rootNode:Node):
        stack = []
        stack.append(DepCluster(rootNode, None))

        while len(stack) > 0:
            depCluster = stack.pop()
            node, parentClusterNode = depCluster.node, depCluster.parentClusterNode

            if parentClusterNode is None:
                self.clusterDepGraph[node] = set()
                self.cacheDependency[node] = None
                self.__insertChildren2Stack(stack, node, node)
            elif parentClusterNode.isSameCluster(node):
                self.__insertChildren2Stack(stack, node, parentClusterNode)
            else:
                self.clusterDepGraph[parentClusterNode].add(node)
                self.cacheDependency[node] = parentClusterNode
                self.clusterDepGraph[node] = set()
                self.__insertChildren2Stack(stack, node, node)


    def __insertChildren2Stack(self, stack, node, parentClusterNode):
        for cNode in node.children:
            stack.append(DepCluster(cNode, parentClusterNode))

    def popClusterWithoutDependency(self) -> List[Node]:
        result = self.__findClusterWithoutDependency()
        for node in result:
            self.__removeClusterDependency(node)
        return result

    def __findClusterWithoutDependency(self) -> List[Node]:
        result = []
        for node, nodeSet in self.clusterDepGraph.items():
            if len(nodeSet) == 0:
                result.append(node)
        return result

    def __removeClusterDependency(self, node:Node)->Node:
        if node not in self.cacheDependency:
            raise Exception("No cache dependency found")
        if len(self.clusterDepGraph[node])==0:
            del (self.clusterDepGraph[node] )
        parentClusterNode = self.cacheDependency[node]
        if parentClusterNode is None:
            return parentClusterNode
        if parentClusterNode not in self.clusterDepGraph:
            raise Exception ("Cluster dependency Graph not found")
        self.clusterDepGraph[parentClusterNode].remove(node)

        return parentClusterNode


if __name__ == "__main__":
    rootNode = readTreeInput(JsonInput)
    #print (rootNode)

    clusterDepGraph = ClusterDependencyGraph()
    clusterDepGraph.constructDependencyGraph(rootNode)

    step = 0
    while True:
        cnt = 0
        wList = clusterDepGraph.popClusterWithoutDependency()
        if len(wList) == 0 :
            break
        print ("step %d begin"%(step))
        for w in wList:
            print("\t%d Work Node begin" % (cnt))
            print (w)
            print("\t%d Work Node end" % (cnt))
            cnt += 1
        print ("step %d end"%(step))
        step+=1
    #Expected
    # step 0 begin
    #  0 Work Node begin
    # Join 10Y data,
    # Query Mongo Static,Load BondTrades B,
    #  0 Work Node end
    # step 0 end
    # step 1 begin
    #  1 Work Node begin
    # Flatten 5Y+10Y data,
    # Join 10Y data,Join 5Y data,
    # Query Mongo Static,Load BondTrades B,Query Mongo Static,Load BondTrades A,
    #  1 Work Node end
    # step 1 end
