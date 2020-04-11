import abc
import avro.schema
from collections import deque
from typing import List

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
    def __init__(self, description, action, cluster, jNode,name=None):
        self.description = description
        self.action = action
        self.cluster = cluster
        self.name = name
        self.children = []
        self.jNode = jNode

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
        if self.name is None:
            return self.description
        else:
            return f"({self.name}:{self.description})"

    def setBasicValue(self, jNode):
        self.description = jNode["description"]
        self.action = jNode["action"]
        self.cluster = jNode["cluster"]
        self.name = jNode["name"] if "name" in jNode else None
        self.jNode = jNode

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
    def __init__(self, description, action, cluster, source, avro_schema, jNode):
        Node.__init__(self, description, action, cluster, jNode)
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

class KeyColumn():
    def __init__(self, leftColumn, rightColumn):
        self.leftColumn = leftColumn
        self.rightColumn = rightColumn
class JoinKey():
    def __init__(self, leftinx, rightinx, leftLabel, rightLabel, keyColumnLst:List[KeyColumn]):
        self.leftinx = leftinx
        self.rightinx = rightinx
        self.leftLabel = leftLabel
        self.rightLabel = rightLabel
        self.keyColumnLst = keyColumnLst

class JoinNode(Node):

    def __init__ (self, jNode):
        self.setBasicValue(jNode)
        self.keyList = []
        self.setKeys(jNode)
        self.children = []

    def setKeys(self, jNode):
        jKeysList = jNode["keys"]
        for k in jKeysList:
            columnList = []
            for c in k["on"]:
                columnList.append(KeyColumn(c["left"], c["right"]))
            newKey = JoinKey(k["left"], k["right"], k["labels"]["left"], k["labels"]["right"],columnList)
            self.keyList.append(newKey)


    def getKeys(self)->List[JoinKey]:
        return self.keyList


def nodeFactory(jNode):
    action = jNode["action"]


    if action == "LOAD":
        return LoadLeafNode(jNode)
    elif action == "QUERY" :
        queryType = jNode["queryType"]
        if queryType == "MONGO":
            return MongoQueryLeafNode(jNode)
    elif action == "INNER_JOIN":
        return JoinNode(jNode)

    return Node(jNode["description"], jNode["action"], jNode["cluster"], jNode)