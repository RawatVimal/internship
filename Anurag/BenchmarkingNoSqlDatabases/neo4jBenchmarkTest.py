from py2neo import Graph
from py2neo.bulk import create_nodes
import json

# setting up the database connection

def create_connection():
    host_address = "localhost"
    port_no = "7474"
    username = "neo4j"
    password = "test"

    connection_string = "http://"+host_address+":"+port_no+"/browser/"

    try:

        driver = Graph(connection_string, user=username, password=password)
        print("Connection to neo4j is established.")
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

    print(my_node)


def deleteAllNodesAndRelationships():

    driver = create_connection()
    driver.delete_all()
    print("All the nodes and relationships have been deleted.")

def singleRead():

    driver = create_connection()

    my_node = driver.evaluate('match (x:profiles) return x')

    print(my_node)

def singleWrite():
    driver = create_connection()

    write = driver.run('CREATE (n:profiles) SET n.user_id = 5420, n.AGE = 23')


def aggregate():

    driver = create_connection()

    aggregate = driver.run('MATCH (x:profiles) RETURN sum(toInteger(x.AGE))')

    print(aggregate)

def neighbors():

    driver = create_connection()

    result = driver.run('MATCH (s:profiles {user_id:"1"})-->(n:profiles) RETURN n.user_id').to_table()

    print(result)

def neighbors2():

    driver = create_connection()

    result = driver.run('MATCH (s:profiles {user_id:"15"})-[*1..2]->(n:profiles) RETURN DISTINCT n.user_id').to_table()

    print(result)

def neighbors2data():

    driver = create_connection()

    result = driver.run('MATCH (s:profiles {user_id:"20"})-[*1..2]->(n:profiles) RETURN DISTINCT n.user_id, n').to_table()

    print(result)


def shortestPath():

    driver = create_connection()

    result = driver.run('MATCH (s:profiles {user_id:"1"}),(n:profiles {user_id:"423"}), p = shortestPath((s)-[*..5]->(n)) RETURN [x in nodes(p) | x.user_id] as path').to_table()

    print(result)

if __name__ == "__main__":
    #create_connection()
    #insertNodesIntoProfiles()
    #createRelationships()
    #deleteAllNodesAndRelationships()
    singleRead()
    #singleWrite()
    #aggregate()
    #neighbors()
    #neighbors2()
    #neighbors2data()
    #shortestPath()
