from MyProject.TreeQueryCluster import Node, ClusterDependencyGraph, readTreeInput
import abc
JsonInput = "TreeQueryInput3.json"

# Demonstrate Postorder traversal of node tree for execution

from typing import List
from collections import defaultdict
from collections import deque
class NodePipeline(abc.ABC):
    @abc.abstractmethod
    def addNodeToPipeline(self, parentNode:Node, node:Node):
        pass
    def __retrieveCachedResult(self, identifier):
        pass

#Decorator of normal Node
class CacheNode(Node):
    def __init__(self, node:Node):
        Node.__init__(self, node.description, node.action, node.cluster)
        self.originalNode = node
        self.value = None

    def __eq__(self, other):
        return other == self.originalNode
    def __hash__(self):
        return self.originalNode.__hash__()

    def __str__(self):
        return "CacheData(%s)"%(self.originalNode.description)

    def setRetrievedValue(self, value):
        self.value = value



class AbstractNodePipeline(NodePipeline):
    def __init__(self, cluster):
        #Init the pipeline here
        #We use Graph to model a pipeline
        self.graph = defaultdict(list)
        self.depends = defaultdict(list)
        self.cluster = cluster
        self.pipelineBuilder = None

    def addNodeToPipeline(self, parentNode:Node, node: Node):
        newParentNode = None

        if node is None:
            self.graph[parentNode]=[]
            return
        if parentNode.cluster == node.cluster:
            newParentNode = parentNode

        else:
            cacheNode = CacheNode(parentNode)
            retrievedValue = self.__retrieveCachedResult(parentNode.identifier())
            cacheNode.setRetrievedValue(retrievedValue)
            assert (cacheNode==parentNode)
            newParentNode = cacheNode
        self.graph[newParentNode].append(node)
        self.depends[node].append(newParentNode)

    def getPipelineBuilder(self):
        #Fill in blank dependency for root
        s = deque()
        for rNode in self.graph.keys():
            l = self.depends[rNode]
            if len(l) == 0:
                s.append(rNode)

        while len(s) > 0:
            node = s.popleft()
            dependOnList = self.depends[node]
            if len(dependOnList) == 0:
                self.abstractInsertNode2Pipeline([None], node)
            else:
                self.abstractInsertNode2Pipeline(dependOnList, node)
            nextChildLst = self.graph[node]
            for c in nextChildLst:
                try:
                    s.index(c)
                except ValueError as ve:
                    s.append(c)


        return self.pipelineBuilder

    def abstractInsertNode2Pipeline(self, parentList:List[Node], node):
        for p in parentList:
            print("Insert node %s to parents %s"%(node, p))

    def __retrieveCachedResult(self, identifier):
        return "%sCache"%(identifier)

    def __str__(self):
        output = "Cluster:%s\n"%(self.cluster)
        for key, value in self.graph.items():
            output = output + str(key) +"->" +  str(list(map(lambda x:str(x), value)))+","
        return output


class NodeTraverser:
    def postOrderTraversalExecution(self, node:Node, parentNode:Node, jobList:List[Node], nodePipeline: NodePipeline)->List[Node]:
        parentCluster = node.cluster
        for child in node.children:
            if parentCluster == child.cluster:
                self.postOrderTraversalExecution(child, node, jobList, nodePipeline)
            else:
                nodePipeline.addNodeToPipeline(child, node)
        nodePipeline.addNodeToPipeline(node, parentNode)
        jobList.append(node)

        return jobList


if __name__ == "__main__":
    rootNode = readTreeInput(JsonInput)
    #print (rootNode)

    clusterDepGraph = ClusterDependencyGraph()
    clusterDepGraph.constructDependencyGraph(rootNode)

    #Get workablecluster
    wList = clusterDepGraph.findClusterWithoutDependency()

    solu = NodeTraverser()

    step = 0
    while True:
        cnt = 0
        wList = clusterDepGraph.findClusterWithoutDependency()
        if len(wList) == 0:
            break
        print("step %d begin" % (step))

        for w in wList:
            print("\tPipeline List begin")
            nodePipeline = AbstractNodePipeline(w.cluster)
            jobList = solu.postOrderTraversalExecution(w, None,[], nodePipeline)
            nodePipeline.getPipelineBuilder()
            node = clusterDepGraph.removeClusterDependency(w)
            cnt += 1
            print("\tPipeline List end")

        print("step %d end" % (step))
        step += 1