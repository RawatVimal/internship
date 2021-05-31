import datetime
from py2neo import Graph
from py2neo.bulk import create_nodes
import json
import random
import re
import mongodbBenchmarkTest
import os

# setting up the database connection

def create_connection(containerName):
    bolt_port_no = re.split(pattern='_', string=containerName)[-1]  # <---- get port no from docker container's name
    username = "neo4j"
    password = "test"

    connection_string = "bolt://localhost:"+bolt_port_no

    try:

        driver = Graph(connection_string, user=username, password=password)
        # print("Connection to neo4j is established.")

        return driver

    except Exception as e:
        print("Failed to create the driver:", e)


def insertNodesIntoProfiles(containerName):
    with open('data.json', errors='ignore') as json_data:
        data = json.load(json_data)

    containerName = containerName
    driver = create_connection(containerName)

    create_nodes(driver.auto(), data, labels={"profiles"})

def createRelationships(containerName):
    containerName = containerName
    driver = create_connection(containerName)

    with open('relations.json', errors='ignore') as json_data:
        data = json.load(json_data)

    _fromList =[d['_from'] for d in data]
    _toList = [d['_to'] for d in data]
    length = len(_fromList)

    for value in range(length):
        my_node = driver.evaluate('MATCH (p:profiles), (f:profiles) WHERE p.user_id = "%s" AND f.user_id = "%s" CREATE (p)-[:HasAFriend]->(f)'%(_fromList[value],_toList[value]))

    #print(my_node)


def deleteAllNodesAndRelationships(containerName):
    containerName = containerName
    driver = create_connection(containerName)
    driver.delete_all()
    #print("All the nodes and relationships have been deleted.")


def createUserIDList():
    containerName = "neo4j_latest_3003"
    driver = create_connection(containerName)

    dataframe_userID = driver.run('MATCH (a:profiles) RETURN a.user_id').to_data_frame()

    userId_list = dataframe_userID.values.tolist()

    randomlist = random.sample(userId_list,2)

    randomTwoUserIDs = [val for sublist in randomlist for val in sublist]

    return randomTwoUserIDs


def singleRead(containerName):
    containerName = containerName
    driver = create_connection(containerName)
    randomUserIDList = createUserIDList()
    start_time = datetime.datetime.now()

    my_node = driver.evaluate('MATCH (a:profiles {user_id: "%s"}) RETURN a'%(randomUserIDList[0]))

    end_time = datetime.datetime.now()
    print(my_node)
    return (end_time - start_time).total_seconds() * 1000

def singleWrite(containerName):
    containerName = containerName
    driver = create_connection(containerName)
    randomUserIDList = createUserIDList()
    print(randomUserIDList)
    start_time = datetime.datetime.now()

    write = driver.run('MATCH (n:profiles {user_id: "%s"}) SET n.AGE = "%s"'%(randomUserIDList[0],randomUserIDList[1]))

    end_time = datetime.datetime.now()
    return (end_time - start_time).total_seconds() * 1000

def aggregate(containerName):
    containerName = containerName
    driver = create_connection(containerName)
    start_time = datetime.datetime.now()

    aggregate = driver.run('MATCH (x:profiles) RETURN sum(toInteger(x.AGE))')

    end_time = datetime.datetime.now()
    #print("Aggregate AGE is : %s"%aggregate)
    return (end_time - start_time).total_seconds() * 1000

def neighbors(containerName):
    containerName = containerName
    driver = create_connection(containerName)
    randomUserIDList = createUserIDList()
    start_time = datetime.datetime.now()

    result = driver.run('MATCH (s:profiles {user_id:"%s"})-->(n:profiles) RETURN n.user_id'%(randomUserIDList[0])).to_table()

    end_time = datetime.datetime.now()
    #print(f"Neighbors of user_id = {randomUserIDList[0]} are:")
    #print(result)
    return (end_time - start_time).total_seconds() * 1000

def neighbors2(containerName):
    containerName = containerName
    driver = create_connection(containerName)
    randomUserIDList = createUserIDList()
    start_time = datetime.datetime.now()

    result = driver.run('MATCH (s:profiles {user_id:"%s"})-[*1..2]->(n:profiles) RETURN DISTINCT n.user_id'%(randomUserIDList[0])).to_table()

    end_time = datetime.datetime.now()
    #print(f"Immediate and first level neighbors of user_id = {randomUserIDList[0]} are:")
    #print(result)
    return (end_time - start_time).total_seconds() * 1000

def neighbors2data(containerName):
    containerName = containerName
    driver = create_connection(containerName)
    randomUserIDList = createUserIDList()
    start_time = datetime.datetime.now()

    result = driver.run('MATCH (s:profiles {user_id:"%s"})-[*1..2]->(n:profiles) RETURN DISTINCT n.user_id, n'%(randomUserIDList[0])).to_table()

    end_time = datetime.datetime.now()
    #print(f"Profiles of neighbors of user_id = {randomUserIDList[0]} are:")
    #print(result)
    return (end_time - start_time).total_seconds() * 1000

def shortestPath(containerName):
    containerName = containerName
    driver = create_connection(containerName)
    randomUserIDList = createUserIDList()
    start_time = datetime.datetime.now()
    result = driver.run('MATCH (s:profiles {user_id:"%s"}),(n:profiles {user_id:"%s"}), p = shortestPath((s)-[*]->(n)) RETURN [x in nodes(p) | x.user_id] as path'%(randomUserIDList[0],randomUserIDList[1])).to_table()
    end_time = datetime.datetime.now()
    return (end_time - start_time).total_seconds() * 1000

if __name__ == "__main__":
    #create_connection("neo4j_latest_3003")
    insertNodesIntoProfiles("neo4j_latest_3003")
    #createRelationships("neo4j_latest_3003")
    #deleteAllNodesAndRelationships("neo4j_latest_3003")
    #singleRead()
    #singleWrite()
    #aggregate()
    #neighbors()
    #neighbors2()
    #neighbors2data()
    #shortestPath()
    #createUserIDList()
