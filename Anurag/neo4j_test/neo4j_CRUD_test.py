import py2neo
from py2neo import Graph,NodeMatcher
import neo4j
from neo4j import GraphDatabase
from py2neo.data import Node, Relationship
import pandas as pd
from pymongo import cursor

# setting up the database connection


driver = GraphDatabase.driver("neo4j://localhost:7687",auth=("neo4j", "test"))

print(driver)

session = driver.session()

# Query to insert nodes and relationships

queryGraph = """CREATE (cornell:university { name: "Cornell University"}),

(yale:university { name: "Yale University"}),

(princeton:university { name: "Princeton University"}),

(harvard:university { name: "Harvard University"}),

 

(cornell)-[:connects_in {miles: 259}]->(yale),

(cornell)-[:connects_in {miles: 210}]->(princeton),

(cornell)-[:connects_in {miles: 327}]->(harvard),

 

(yale)-[:connects_in {miles: 259}]->(cornell),

(yale)-[:connects_in {miles: 133}]->(princeton),

(yale)-[:connects_in {miles: 133}]->(harvard),

 

(harvard)-[:connects_in {miles: 327}]->(cornell),

(harvard)-[:connects_in {miles: 133}]->(yale),

(harvard)-[:connects_in {miles: 260}]->(princeton),

 

(princeton)-[:connects_in {miles: 210}]->(cornell),

(princeton)-[:connects_in {miles: 133}]->(yale),

(princeton)-[:connects_in {miles: 260}]->(harvard)"""

# Query the graph

session.run(queryGraph)


# Query nodes

queryNodes = "MATCH (x:university) RETURN x"

nodes = session.run(queryNodes)

print("List of Ivy League universities present in the graph:")

for node in nodes:
    print(node)

# Query edges

queryEdges = "MATCH (x:university {name:'Yale University'})-[r]->(y:university) RETURN y.name,r.miles"

nodes = session.run(queryEdges)

print("Distance from Yale University to the other Ivy League universities present in the graph:")

for node in nodes:
    print(node)


# delete one relationship

queryDeleteOneRelationship = "MATCH (n {name: 'Yale University'})-[r:connects_in]->() DELETE r"

nodes = session.run(queryDeleteOneRelationship)

for node in nodes:
    print(node)

# delete one specific node

queryDeleteOneNode = "MATCH (x:university {name:'Yale University'}) DETACH DELETE x"

session.run(queryDeleteOneNode)

print("Yale university has been deleted:")

nodes = session.run(queryNodes)

for node in nodes:
    print(node)


# Updating the university lable to Top_Universities

queryUpdate = "MATCH (x:university) SET x:Top_Universities REMOVE x:university"

session.run(queryUpdate)







