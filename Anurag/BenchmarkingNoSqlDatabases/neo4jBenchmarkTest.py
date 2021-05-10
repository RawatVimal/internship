import datetime
from py2neo import Graph
from py2neo.bulk import create_nodes
import json
import random
import mongodbBenchmarkTest
import os

# setting up the database connection

def create_connection():
    host_address = "localhost"
    port_no = "7474"
    username = "neo4j"
    password = "test"

    connection_string = "http://"+host_address+":"+port_no+"/browser/"

    try:

        driver = Graph(connection_string, user=username, password=password)
        #print("Connection to neo4j is established.")
        return driver

    except Exception as e:
        print("Failed to create the driver:", e)


def insertNodesIntoProfiles():
    with open('soc-pokec-profiles500.json', errors='ignore') as json_data:
        data = json.load(json_data)

    driver = create_connection()

    create_nodes(driver.auto(), data, labels={"profiles"})

def createRelationships():
    driver = create_connection()

    with open('soc-pokec-relationship5000.json', errors='ignore') as json_data:
        data = json.load(json_data)

    _fromList =[d['_from'] for d in data]
    _toList = [d['_to'] for d in data]
    length = len(_fromList)

    for value in range(length):
        my_node = driver.evaluate('MATCH (p:profiles), (f:profiles) WHERE p.user_id = "%s" AND f.user_id = "%s" CREATE (p)-[:HasAFriend]->(f)'%(_fromList[value],_toList[value]))

    #print(my_node)


def deleteAllNodesAndRelationships():

    driver = create_connection()
    driver.delete_all()
    #print("All the nodes and relationships have been deleted.")


def createUserIDList():
    driver = create_connection()

    dataframe_userID = driver.run('MATCH (a:profiles) RETURN a.user_id').to_data_frame()

    userId_list = dataframe_userID.values.tolist()

    randomlist = random.sample(userId_list,2)

    randomTwoUserIDs = [val for sublist in randomlist for val in sublist]

    return randomTwoUserIDs


def singleRead():

    driver = create_connection()
    cpuMemoryList = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
    start_time = datetime.datetime.now()

    my_node = driver.evaluate('MATCH (a:profiles) RETURN a')

    end_time = datetime.datetime.now()
    execTime = (end_time - start_time).total_seconds() * 1000
    print(my_node)
    print("Query execution time = %s Milliseconds" % execTime)
    print(f"CPU used = {cpuMemoryList[0]:.4f}%")
    print(f"MEMORY used = {cpuMemoryList[1]:.4f}%")

def singleWrite():
    driver = create_connection()
    cpuMemoryList = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
    start_time = datetime.datetime.now()

    write = driver.run('CREATE (n:profiles) SET n.user_id = 5420, n.AGE = 23')

    end_time = datetime.datetime.now()
    execTime = (end_time - start_time).total_seconds() * 1000
    print("Query execution time = %s Milliseconds" % execTime)
    print(f"CPU used = {cpuMemoryList[0]:.4f}%")
    print(f"MEMORY used = {cpuMemoryList[1]:.4f}%")

def aggregate():

    driver = create_connection()
    cpuMemoryList = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
    start_time = datetime.datetime.now()

    aggregate = driver.run('MATCH (x:profiles) RETURN sum(toInteger(x.AGE))')

    end_time = datetime.datetime.now()
    execTime = (end_time - start_time).total_seconds() * 1000
    print("Aggregate AGE is : %s"%aggregate)
    print("Query execution time = %s Milliseconds" % execTime)
    print(f"CPU used = {cpuMemoryList[0]:.4f}%")
    print(f"MEMORY used = {cpuMemoryList[1]:.4f}%")

def neighbors():

    driver = create_connection()
    cpuMemoryList = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
    start_time = datetime.datetime.now()

    result = driver.run('MATCH (s:profiles {user_id:"1"})-->(n:profiles) RETURN n.user_id').to_table()

    end_time = datetime.datetime.now()
    execTime = (end_time - start_time).total_seconds() * 1000
    print("Neighbors of user_id = 1 are:")
    print(result)
    print("Query execution time = %s Milliseconds" % execTime)
    print(f"CPU used = {cpuMemoryList[0]:.4f}%")
    print(f"MEMORY used = {cpuMemoryList[1]:.4f}%")

def neighbors2():

    driver = create_connection()
    cpuMemoryList = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
    start_time = datetime.datetime.now()

    result = driver.run('MATCH (s:profiles {user_id:"15"})-[*1..2]->(n:profiles) RETURN DISTINCT n.user_id').to_table()

    end_time = datetime.datetime.now()
    execTime = (end_time - start_time).total_seconds() * 1000
    print("Immediate and first level neighbors of user_id = 15 are:")
    print(result)
    print("Query execution time = %s Milliseconds" % execTime)
    print(f"CPU used = {cpuMemoryList[0]:.4f}%")
    print(f"MEMORY used = {cpuMemoryList[1]:.4f}%")

def neighbors2data():

    driver = create_connection()
    cpuMemoryList = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
    start_time = datetime.datetime.now()

    result = driver.run('MATCH (s:profiles {user_id:"20"})-[*1..2]->(n:profiles) RETURN DISTINCT n.user_id, n').to_table()

    end_time = datetime.datetime.now()
    execTime = (end_time - start_time).total_seconds() * 1000

    print("Profiles of neighbors of user_id = 20 are:")
    print(result)
    print("Query execution time = %s Milliseconds" % execTime)
    print(f"CPU used = {cpuMemoryList[0]:.4f}%")
    print(f"MEMORY used = {cpuMemoryList[1]:.4f}%")


def shortestPath():

    driver = create_connection()
    randomUserIDList = createUserIDList()
    start_time = datetime.datetime.now()
    result = driver.run('MATCH (s:profiles {user_id:"%s"}),(n:profiles {user_id:"%s"}), p = shortestPath((s)-[*]->(n)) RETURN [x in nodes(p) | x.user_id] as path'%(randomUserIDList[0],randomUserIDList[1])).to_table()
    end_time = datetime.datetime.now()
    return (end_time - start_time).total_seconds() * 1000

if __name__ == "__main__":
    #create_connection()
    #insertNodesIntoProfiles()
    #createRelationships()
    #deleteAllNodesAndRelationships()
    #singleRead()
    #singleWrite()
    #aggregate()
    #neighbors()
    neighbors2()
    #neighbors2data()
    #shortestPath()
    #createUserIDList()
