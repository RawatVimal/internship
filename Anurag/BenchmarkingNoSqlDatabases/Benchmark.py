import os
import click
import functools
import js2py
import neo4jBenchmarkTest,postgresBenchmarkTest,mongodbBenchmarkTest

@click.group()
def cli():
    pass

def common_options(f):
    options = [
        click.option('--database', '-d', type=click.Choice(
            ['mongodb', 'neo4j', 'postgres'],
            case_sensitive=True),
                     help="Choose Database names from one of the following: 'mongodb', 'neo4j', 'postgres'"),

        click.option('--query', '-q', type=click.Choice(
            ['singleRead', 'singleWrite', 'aggregate','neighbors', 'neighbors2', 'neighbors2data','shortestPath','deleteTables'],
            case_sensitive=True),
                     help="Choose one query from one of the following: 'singleRead', 'singleWrite', 'aggregate','neighbors', 'neighbors2', 'neighbors2data','shortestPath'"),
        click.option('--number', '-n', type=int,
                     help="Choose how many times you want to run the query.")
    ]
    return functools.reduce(lambda x, opt: opt(x), options, f)

@cli.command()
@common_options
def performance(**kwargs):

    database = kwargs["database"]
    query = kwargs["query"]
    number = kwargs["number"]

    if database == "neo4j":
        if query == "singleRead":
            exec_time_list = []
            cpuMemoryList = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            for i in range(number):
                exec_time = neo4jBenchmarkTest.singleRead()
                exec_time_list.append(exec_time)
            avg_exec_time = sum(exec_time_list) / len(exec_time_list)
            print("Average execution time by running the query %s times is: %s milliseconds" % (number, avg_exec_time))
            print(f"CPU used = {cpuMemoryList[0]:.4f}%")
            print(f"MEMORY used = {cpuMemoryList[1]:.4f}%")
        elif query == "singleWrite":
            exec_time_list = []
            cpuMemoryList = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            for i in range(number):
                exec_time = neo4jBenchmarkTest.singleWrite()
                exec_time_list.append(exec_time)
            avg_exec_time = sum(exec_time_list) / len(exec_time_list)
            print("Average execution time by running the query %s times is: %s milliseconds" % (number, avg_exec_time))
            print(f"CPU used = {cpuMemoryList[0]:.4f}%")
            print(f"MEMORY used = {cpuMemoryList[1]:.4f}%")
        elif query == "aggregate":
            exec_time_list = []
            cpuMemoryList = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            for i in range(number):
                exec_time = neo4jBenchmarkTest.aggregate()
                exec_time_list.append(exec_time)
            avg_exec_time = sum(exec_time_list) / len(exec_time_list)
            print("Average execution time by running the query %s times is: %s milliseconds" % (number, avg_exec_time))
            print(f"CPU used = {cpuMemoryList[0]:.4f}%")
            print(f"MEMORY used = {cpuMemoryList[1]:.4f}%")
        elif query == "neighbors":
            exec_time_list = []
            cpuMemoryList = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            for i in range(number):
                exec_time = neo4jBenchmarkTest.neighbors()
                exec_time_list.append(exec_time)
            avg_exec_time = sum(exec_time_list) / len(exec_time_list)
            print("Average execution time by running the query %s times is: %s milliseconds" % (number, avg_exec_time))
            print(f"CPU used = {cpuMemoryList[0]:.4f}%")
            print(f"MEMORY used = {cpuMemoryList[1]:.4f}%")
        elif query == "neighbors2":
            exec_time_list = []
            cpuMemoryList = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            for i in range(number):
                exec_time = neo4jBenchmarkTest.neighbors2()
                exec_time_list.append(exec_time)
            avg_exec_time = sum(exec_time_list) / len(exec_time_list)
            print("Average execution time by running the query %s times is: %s milliseconds" % (number, avg_exec_time))
            print(f"CPU used = {cpuMemoryList[0]:.4f}%")
            print(f"MEMORY used = {cpuMemoryList[1]:.4f}%")
        elif query == "neighbors2data":
            exec_time_list = []
            cpuMemoryList = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            for i in range(number):
                exec_time = neo4jBenchmarkTest.neighbors2data()
                exec_time_list.append(exec_time)
            avg_exec_time = sum(exec_time_list) / len(exec_time_list)
            print("Average execution time by running the query %s times is: %s milliseconds"%(number,avg_exec_time))
            print(f"CPU used = {cpuMemoryList[0]:.4f}%")
            print(f"MEMORY used = {cpuMemoryList[1]:.4f}%")
        elif query == "shortestPath":
            exec_time_list = []
            cpuMemoryList = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            for i in range(number):
                exec_time = neo4jBenchmarkTest.shortestPath()
                exec_time_list.append(exec_time)
            avg_exec_time = sum(exec_time_list) / len(exec_time_list)
            print("Average execution time by running the query %s times is: %s milliseconds"%(number,avg_exec_time))
            print(f"CPU used = {cpuMemoryList[0]:.4f}%")
            print(f"MEMORY used = {cpuMemoryList[1]:.4f}%")
        elif query == "deleteTables":
            neo4jBenchmarkTest.deleteAllNodesAndRelationships()
    elif database == "postgres":
        if query == "singleRead":
            exec_time_list = []
            cpuMemoryList = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            for i in range(number):
                exec_time = postgresBenchmarkTest.singleRead()
                exec_time_list.append(exec_time)
            avg_exec_time = sum(exec_time_list) / len(exec_time_list)
            print("Average execution time by running the query %s times is: %s milliseconds" % (number, avg_exec_time))
            print(f"CPU used = {cpuMemoryList[0]:.4f}%")
            print(f"MEMORY used = {cpuMemoryList[1]:.4f}%")
        elif query == "singleWrite":
            exec_time_list = []
            cpuMemoryList = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            for i in range(number):
                exec_time = postgresBenchmarkTest.singleWrite()
                exec_time_list.append(exec_time)
            avg_exec_time = sum(exec_time_list) / len(exec_time_list)
            print("Average execution time by running the query %s times is: %s milliseconds" % (number, avg_exec_time))
            print(f"CPU used = {cpuMemoryList[0]:.4f}%")
            print(f"MEMORY used = {cpuMemoryList[1]:.4f}%")
        elif query == "aggregate":
            exec_time_list = []
            cpuMemoryList = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            for i in range(number):
                exec_time = postgresBenchmarkTest.aggregate()
                exec_time_list.append(exec_time)
            avg_exec_time = sum(exec_time_list) / len(exec_time_list)
            print("Average execution time by running the query %s times is: %s milliseconds" % (number, avg_exec_time))
            print(f"CPU used = {cpuMemoryList[0]:.4f}%")
            print(f"MEMORY used = {cpuMemoryList[1]:.4f}%")
        elif query == "neighbors":
            exec_time_list = []
            cpuMemoryList = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            for i in range(number):
                exec_time = postgresBenchmarkTest.neighbors()
                exec_time_list.append(exec_time)
            avg_exec_time = sum(exec_time_list) / len(exec_time_list)
            print("Average execution time by running the query %s times is: %s milliseconds" % (number, avg_exec_time))
            print(f"CPU used = {cpuMemoryList[0]:.4f}%")
            print(f"MEMORY used = {cpuMemoryList[1]:.4f}%")
        elif query == "neighbors2":
            exec_time_list = []
            cpuMemoryList = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            for i in range(number):
                exec_time = postgresBenchmarkTest.neighbors2()
                exec_time_list.append(exec_time)
            avg_exec_time = sum(exec_time_list) / len(exec_time_list)
            print("Average execution time by running the query %s times is: %s milliseconds" % (number, avg_exec_time))
            print(f"CPU used = {cpuMemoryList[0]:.4f}%")
            print(f"MEMORY used = {cpuMemoryList[1]:.4f}%")
        elif query == "neighbors2data":
            exec_time_list = []
            cpuMemoryList = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            for i in range(number):
                exec_time = postgresBenchmarkTest.neighbors2data()
                exec_time_list.append(exec_time)
            avg_exec_time = sum(exec_time_list) / len(exec_time_list)
            print("Average execution time by running the query %s times is: %s milliseconds" % (number, avg_exec_time))
            print(f"CPU used = {cpuMemoryList[0]:.4f}%")
            print(f"MEMORY used = {cpuMemoryList[1]:.4f}%")
        elif query == "deleteTables":
            postgresBenchmarkTest.drop_tables("profiles")
            postgresBenchmarkTest.drop_tables("relations")
    elif database == "mongodb":
        if query == "singleRead":
            exec_time_list = []
            cpuMemoryList = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            for i in range(number):
                exec_time = mongodbBenchmarkTest.singleRead()
                exec_time_list.append(exec_time)
            avg_exec_time = sum(exec_time_list) / len(exec_time_list)
            print("Average execution time by running the query %s times is: %s milliseconds" % (number, avg_exec_time))
            print(f"CPU used = {cpuMemoryList[0]:.4f}%")
            print(f"MEMORY used = {cpuMemoryList[1]:.4f}%")
        elif query == "singleWrite":
            exec_time_list = []
            cpuMemoryList = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            for i in range(number):
                exec_time = mongodbBenchmarkTest.singleWrite()
                exec_time_list.append(exec_time)
            avg_exec_time = sum(exec_time_list) / len(exec_time_list)
            print("Average execution time by running the query %s times is: %s milliseconds" % (number, avg_exec_time))
            print(f"CPU used = {cpuMemoryList[0]:.4f}%")
            print(f"MEMORY used = {cpuMemoryList[1]:.4f}%")
        elif query == "aggregate":
            exec_time_list = []
            cpuMemoryList = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            for i in range(number):
                exec_time = mongodbBenchmarkTest.aggregate()
                exec_time_list.append(exec_time)
            avg_exec_time = sum(exec_time_list) / len(exec_time_list)
            print("Average execution time by running the query %s times is: %s milliseconds" % (number, avg_exec_time))
            print(f"CPU used = {cpuMemoryList[0]:.4f}%")
            print(f"MEMORY used = {cpuMemoryList[1]:.4f}%")
        elif query == "neighbors":
            exec_time_list = []
            cpuMemoryList = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            for i in range(number):
                exec_time = mongodbBenchmarkTest.neighbors()
                exec_time_list.append(exec_time)
            avg_exec_time = sum(exec_time_list) / len(exec_time_list)
            print("Average execution time by running the query %s times is: %s milliseconds" % (number, avg_exec_time))
            print(f"CPU used = {cpuMemoryList[0]:.4f}%")
            print(f"MEMORY used = {cpuMemoryList[1]:.4f}%")
        elif query == "neighbors2":
            exec_time_list = []
            cpuMemoryList = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            for i in range(number):
                exec_time = mongodbBenchmarkTest.neighbors2()
                exec_time_list.append(exec_time)
            avg_exec_time = sum(exec_time_list) / len(exec_time_list)
            print("Average execution time by running the query %s times is: %s milliseconds" % (number, avg_exec_time))
            print(f"CPU used = {cpuMemoryList[0]:.4f}%")
            print(f"MEMORY used = {cpuMemoryList[1]:.4f}%")
        elif query == "neighbors2data":
            exec_time_list = []
            cpuMemoryList = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            for i in range(number):
                exec_time = mongodbBenchmarkTest.neighbors2data()
                exec_time_list.append(exec_time)
            avg_exec_time = sum(exec_time_list) / len(exec_time_list)
            print("Average execution time by running the query %s times is: %s milliseconds" % (number, avg_exec_time))
            print(f"CPU used = {cpuMemoryList[0]:.4f}%")
            print(f"MEMORY used = {cpuMemoryList[1]:.4f}%")
        elif query == "deleteTables":
            mongodbBenchmarkTest.dropProfilesCollection()
            mongodbBenchmarkTest.dropRelationsCollection()


if __name__ == "__main__":
    cli()