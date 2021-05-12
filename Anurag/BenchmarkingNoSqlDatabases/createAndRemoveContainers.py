import docker
import subprocess
import os



def createContainers():
    mongo_command = "docker container run -d --name=mongodb_test -e MONGO_INITDB_ROOT_USERNAME=mongo -e MONGO_INITDB_ROOT_PASSWORD=secret -p 27017:27017 -e PGDATA=$HOME/mongodb_data/db -v $HOME/mongodb_data/db:/pgdata mongo"
    completed = subprocess.run(["powershell", "-Command", mongo_command], capture_output=True)
    print(completed)

    postgres_command = "docker container run -d --name=postgres_test -p 5432:5432 -e POSTGRES_PASSWORD=secret -e PGDATA=$HOME/postgres_data/db -v $HOME/postgres_data/db:/pgdata postgres"
    completed = subprocess.run(["powershell", "-Command", postgres_command], capture_output=True)
    print(completed)

    neo4j_command = "docker container run -d --name=neo4j_test -p 7474:7474 -p7687:7687 -e PGDATA=$HOME/neo4j_data/db -v $HOME/neo4j_data/db:/pgdata --env NEO4J_AUTH=neo4j/test neo4j"
    completed = subprocess.run(["powershell", "-Command", neo4j_command], capture_output=True)
    print(completed)

def deleteContainers():
    mongo_command = "docker stop mongodb_test"
    completed = subprocess.run(["powershell", "-Command", mongo_command], capture_output=True)
    postgres_command = "docker stop postgres_test"
    completed = subprocess.run(["powershell", "-Command", postgres_command], capture_output=True)
    neo4j_command = "docker stop neo4j_test"
    completed = subprocess.run(["powershell", "-Command", neo4j_command], capture_output=True)

    mongo_command = "docker rm mongodb_test"
    completed = subprocess.run(["powershell", "-Command", mongo_command], capture_output=True)
    postgres_command = "docker rm postgres_test"
    completed = subprocess.run(["powershell", "-Command", postgres_command], capture_output=True)
    neo4j_command = "docker rm neo4j_test"
    completed = subprocess.run(["powershell", "-Command", neo4j_command], capture_output=True)

if __name__ == "__main__":

    #createContainers()
    deleteContainers()