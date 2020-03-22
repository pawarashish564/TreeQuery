import abc
import avro.schema
from collections import deque

class DataSource(abc.ABC):
    @abc.abstractmethod
    def getSource(self)->str:
        pass

    @abc.abstractmethod
    def getAvro_schema(self) -> str:
        pass

    @abc.abstractmethod
    def getAvroSchemaObj(self) -> avro.schema:
        pass


class Node:
    def __init__(self, description, action, cluster):
        self.description = description
        self.action = action
        self.cluster = cluster
        self.children = []

    def insertChildren(self, node):
        self.children.append(node)

    def isSameCluster(self, node):
        return self.cluster == node.cluster

    def clone(self):
        newNode = Node()
        newNode.description = self.description
        newNode.action = self.action
        newNode.cluster = self.cluster
        return newNode

    def identifier(self):
        return "identifier_%s"%(self.description)

    def __str__(self):
        return self.description

    def setBasicValue(self, jNode):
        self.description = jNode["description"]
        self.action = jNode["action"]
        self.cluster = jNode["cluster"]

    #for debug of bfs only
    def bfs(self):
        level = 0
        queue = deque()
        queue.append((self, 0))
        result = "("
        while len(queue) > 0:
            node, lvl = queue.popleft()
            if lvl != level:
                result = result + "\n"
                level = lvl
            result = result + node.description +","

            for cNode in node.children:
                queue.append((cNode, lvl+1))
        result = result+")"
        return result


class LoadLeafNode(DataSource, Node):
    def __init__(self, description, action, cluster, source, avro_schema):
        Node.__init__(self, description, action, cluster)
        self.source = source
        self.avro_schema = avro_schema
    def __init__ (self, jNode):
        self.children=[]
        self.source = jNode["source"]
        self.avro_schema = jNode["avro_schema"]
        self.setBasicValue(jNode)
    def getSource(self) -> str:
        return self.source
    def getAvro_schema(self) -> str:
        return self.avro_schema
    def getAvroSchemaObj(self) -> avro.schema:
        schema = avro.schema.parse(self.avro_schema)
        return schema

class MongoQueryLeafNode(DataSource, Node):
    def __init__ (self, jNode):
        self.children=[]
        self.source = jNode["source"]
        self.avro_schema = jNode["avro_schema"]
        self.setBasicValue(jNode)
    def getSource(self) -> str:
        return self.source
    def getAvro_schema(self) -> str:
        return self.avro_schema
    def getAvroSchemaObj(self) -> avro.schema:
        schema = avro.schema.parse(self.avro_schema)

        return schema

def nodeFactory(jNode):
    action = jNode["action"]


    if action == "LOAD":
        return LoadLeafNode(jNode)
    elif action == "QUERY" :
        queryType = jNode["queryType"]
        if queryType == "MONGO":
            return MongoQueryLeafNode(jNode)

    return Node(jNode["description"], jNode["action"], jNode["cluster"])