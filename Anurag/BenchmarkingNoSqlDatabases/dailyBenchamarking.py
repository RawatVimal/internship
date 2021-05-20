import datetime
import os
import csv

import pandas as pd

import mongodbBenchmarkTest,postgresBenchmarkTest,neo4jBenchmarkTest,createAndRemoveContainers,text_to_json_conversion,downloadPokecDataset

# define how many times you want to run each query
# queries will be executed that many times and avg exec time,avg cpu and avg memory is returned
number = 20

exec_time_list = []         # list to store avg exec time
cpu_consumption = []        # list to store avg cpu consumption
memory_consumption = []     # list to store avg memory consumption

mongodb_container_names = ["mongodb_latest_27019","mongo_4.2_27018"]    # list of mongodb container names
postgres_container_names = ["postgres_latest_5434","postgres_10.0_5435"]    # list of mongodb container names
neo4j_container_names = ["neo4j_latest_3003","neo4j_3.5.22_3004"]    # list of mongodb container names

def downloadDatasetsAndConvertToJson():
    #download datasets,unzip, convert to json and remove zip and text files
    downloadPokecDataset.downloadDataset()
    print("Conversion of from txt to json started..")
    text_to_json_conversion.text_to_json_Profiles_table()
    text_to_json_conversion.text_to_json_Relationship_table()
    print("Conversion of from txt to json finished..")

    if os.path.isfile("benchmarkingResults.csv"):
        print("benchmarkingResults.csv already exists")
    else:
        with open('benchmarkingResults.csv', 'w', newline='') as csvfile:
            headers = ['Date', 'Database', 'Database Version', 'Query', 'Avg_Exec_Time', 'Avg_Memory_Used', 'Avg_Cpu_Used']
            csv_writer = csv.writer(csvfile)
            csv_writer.writerow(headers)  # write header

def setupPostgresDatabase():
    # setup postgres (create db and insert data)
    print("Setting up of postgres database and insertion of data started..")
    for containerName in postgres_container_names:
        postgresBenchmarkTest.create_database(containerName)
        postgresBenchmarkTest.create_tables(containerName)
        postgresBenchmarkTest.Insert_INTO_profiles_table(containerName)
        postgresBenchmarkTest.Insert_INTO_relations_table(containerName)
    print("Setting up of postgres database and insertion of data finished..")

def setupMongodbDatabase():
    # setup mongodb (create db and insert data)
    print("Setting up of mongodb database and insertion of data started..")

    for containerName in mongodb_container_names:
        mongodbBenchmarkTest.createDB(containerName)
        mongodbBenchmarkTest.createProfileCollection(containerName)
        mongodbBenchmarkTest.createRelationsCollection(containerName)
        mongodbBenchmarkTest.insertIntoProfilesCollection(containerName)
        mongodbBenchmarkTest.insertIntoRelationsCollection(containerName)
    print("Setting up of mongodb database and insertion of data finished..")

def setupNeo4jDatabase():
    # setup neo4j (create db and insert data)
    print("Setting up of neo4j database and insertion of data started..")
    for containerName in neo4j_container_names:
        neo4jBenchmarkTest.insertNodesIntoProfiles(containerName)
        neo4jBenchmarkTest.createRelationships(containerName)
    print("Setting up of neo4j database and insertion of data finished..")



def singleReadQuery():

    #mongodb read query
    for containerName in mongodb_container_names:
        data = containerName
        versionName = "_".join(data.split("_")[:2])
        current_timeM = datetime.datetime.now()

        for i in range(number):

            cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            exec_timeM = mongodbBenchmarkTest.singleRead(containerName)
            exec_time_list.append(exec_timeM)
            cpu_consumption.append(cpuMemoryListM[0])
            memory_consumption.append(cpuMemoryListM[1])

        avg_exec_time = sum(exec_time_list) / len(exec_time_list)
        avg_cpu_consumption = sum(cpu_consumption) / len(cpu_consumption)
        avg_memory_consumption = sum(memory_consumption) / len(memory_consumption)
        print("Average execution time by running the query %s times is: %s milliseconds" % (number,'{:.4f}'.format(avg_exec_time) ))
        print(f"Average CPU used = {avg_cpu_consumption:.4f}%")
        print(f"Average MEMORY used = {avg_memory_consumption:.4f}%")

        #df for mongodb
        newRows = {'Date' : [current_timeM],
                    'Database' : ['Mongodb'],
                    'Database Version' : [versionName],
                    'Query' : ['singleRead'],
                    'ExecTime' : [format(avg_exec_time, ".4f")],
                    'Memory_Used' : [format(avg_memory_consumption, ".4f")],
                    'Cpu_Used' : [format(avg_cpu_consumption, ".4f")]}
        dfToCsv = pd.DataFrame(newRows)
        with open('benchmarkingResults.csv', 'a', newline='') as f:
             dfToCsv.to_csv(f, index=False, header=False)

    #postgres read query

    for containerName in postgres_container_names:
        data = containerName
        versionName = "_".join(data.split("_")[:2])
        current_timeM = datetime.datetime.now()

        for i in range(number):
            cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            exec_timeM = postgresBenchmarkTest.singleRead(containerName)
            exec_time_list.append(exec_timeM)
            cpu_consumption.append(cpuMemoryListM[0])
            memory_consumption.append(cpuMemoryListM[1])

        avg_exec_time = sum(exec_time_list) / len(exec_time_list)
        avg_cpu_consumption = sum(cpu_consumption) / len(cpu_consumption)
        avg_memory_consumption = sum(memory_consumption) / len(memory_consumption)
        print("Average execution time by running the query %s times is: %s milliseconds" % (
        number, '{:.4f}'.format(avg_exec_time)))
        print(f"Average CPU used = {avg_cpu_consumption:.4f}%")
        print(f"Average MEMORY used = {avg_memory_consumption:.4f}%")

        # df for postgres
        newRows = {'Date': [current_timeM],
                   'Database': ['Postgres'],
                   'Database Version': [versionName],
                   'Query': ['singleRead'],
                   'ExecTime': [format(avg_exec_time, ".4f")],
                   'Memory_Used': [format(avg_memory_consumption, ".4f")],
                   'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
        dfToCsv = pd.DataFrame(newRows)
        with open('benchmarkingResults.csv', 'a', newline='') as f:
            dfToCsv.to_csv(f, index=False, header=False)

    #Neo4j read query

    for containerName in neo4j_container_names:
        data = containerName
        versionName = "_".join(data.split("_")[:2])
        current_timeM = datetime.datetime.now()

        for i in range(number):
            cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            exec_timeM = neo4jBenchmarkTest.singleRead(containerName)
            exec_time_list.append(exec_timeM)
            cpu_consumption.append(cpuMemoryListM[0])
            memory_consumption.append(cpuMemoryListM[1])

        avg_exec_time = sum(exec_time_list) / len(exec_time_list)
        avg_cpu_consumption = sum(cpu_consumption) / len(cpu_consumption)
        avg_memory_consumption = sum(memory_consumption) / len(memory_consumption)
        print("Average execution time by running the query %s times is: %s milliseconds" % (
            number, '{:.4f}'.format(avg_exec_time)))
        print(f"Average CPU used = {avg_cpu_consumption:.4f}%")
        print(f"Average MEMORY used = {avg_memory_consumption:.4f}%")

        # df for neo4j
        newRows = {'Date': [current_timeM],
                   'Database': ['Neo4j'],
                   'Database Version': [versionName],
                   'Query': ['singleRead'],
                   'ExecTime': [format(avg_exec_time, ".4f")],
                   'Memory_Used': [format(avg_memory_consumption, ".4f")],
                   'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
        dfToCsv = pd.DataFrame(newRows)
        with open('benchmarkingResults.csv', 'a', newline='') as f:
            dfToCsv.to_csv(f, index=False, header=False)

def singleWriteQuery():

    # mongodb write query
    for containerName in mongodb_container_names:
        data = containerName
        versionName = "_".join(data.split("_")[:2])
        current_timeM = datetime.datetime.now()

        for i in range(number):
            cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            exec_timeM = mongodbBenchmarkTest.singleWrite(containerName)
            exec_time_list.append(exec_timeM)
            cpu_consumption.append(cpuMemoryListM[0])
            memory_consumption.append(cpuMemoryListM[1])

        avg_exec_time = sum(exec_time_list) / len(exec_time_list)
        avg_cpu_consumption = sum(cpu_consumption) / len(cpu_consumption)
        avg_memory_consumption = sum(memory_consumption) / len(memory_consumption)
        print("Average execution time by running the query %s times is: %s milliseconds" % (
            number, '{:.4f}'.format(avg_exec_time)))
        print(f"Average CPU used = {avg_cpu_consumption:.4f}%")
        print(f"Average MEMORY used = {avg_memory_consumption:.4f}%")

        # df for mongodb
        newRows = {'Date': [current_timeM],
                   'Database': ['Mongodb'],
                   'Database Version': [versionName],
                   'Query': ['singleWrite'],
                   'ExecTime': [format(avg_exec_time, ".4f")],
                   'Memory_Used': [format(avg_memory_consumption, ".4f")],
                   'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
        dfToCsv = pd.DataFrame(newRows)
        with open('benchmarkingResults.csv', 'a', newline='') as f:
            dfToCsv.to_csv(f, index=False, header=False)

    # postgres write query
    for containerName in postgres_container_names:
        data = containerName
        versionName = "_".join(data.split("_")[:2])
        current_timeM = datetime.datetime.now()

        for i in range(number):
            cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            exec_timeM = postgresBenchmarkTest.singleWrite(containerName)
            exec_time_list.append(exec_timeM)
            cpu_consumption.append(cpuMemoryListM[0])
            memory_consumption.append(cpuMemoryListM[1])

        avg_exec_time = sum(exec_time_list) / len(exec_time_list)
        avg_cpu_consumption = sum(cpu_consumption) / len(cpu_consumption)
        avg_memory_consumption = sum(memory_consumption) / len(memory_consumption)
        print("Average execution time by running the query %s times is: %s milliseconds" % (
            number, '{:.4f}'.format(avg_exec_time)))
        print(f"Average CPU used = {avg_cpu_consumption:.4f}%")
        print(f"Average MEMORY used = {avg_memory_consumption:.4f}%")

        # df for mongodb
        newRows = {'Date': [current_timeM],
                   'Database': ['Postgres'],
                   'Database Version': [versionName],
                   'Query': ['singleWrite'],
                   'ExecTime': [format(avg_exec_time, ".4f")],
                   'Memory_Used': [format(avg_memory_consumption, ".4f")],
                   'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
        dfToCsv = pd.DataFrame(newRows)
        with open('benchmarkingResults.csv', 'a', newline='') as f:
            dfToCsv.to_csv(f, index=False, header=False)

    # Neo4j write query
    for containerName in neo4j_container_names:
        data = containerName
        versionName = "_".join(data.split("_")[:2])
        current_timeM = datetime.datetime.now()

        for i in range(number):
            cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            exec_timeM = neo4jBenchmarkTest.singleWrite(containerName)
            exec_time_list.append(exec_timeM)
            cpu_consumption.append(cpuMemoryListM[0])
            memory_consumption.append(cpuMemoryListM[1])

        avg_exec_time = sum(exec_time_list) / len(exec_time_list)
        avg_cpu_consumption = sum(cpu_consumption) / len(cpu_consumption)
        avg_memory_consumption = sum(memory_consumption) / len(memory_consumption)
        print("Average execution time by running the query %s times is: %s milliseconds" % (
            number, '{:.4f}'.format(avg_exec_time)))
        print(f"Average CPU used = {avg_cpu_consumption:.4f}%")
        print(f"Average MEMORY used = {avg_memory_consumption:.4f}%")

        # df for neo4j
        newRows = {'Date': [current_timeM],
                   'Database': ['Neo4j'],
                   'Database Version': [versionName],
                   'Query': ['singleWrite'],
                   'ExecTime': [format(avg_exec_time, ".4f")],
                   'Memory_Used': [format(avg_memory_consumption, ".4f")],
                   'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
        dfToCsv = pd.DataFrame(newRows)
        with open('benchmarkingResults.csv', 'a', newline='') as f:
            dfToCsv.to_csv(f, index=False, header=False)

def aggregateQuery():
    # mongodb aggregate query
    for containerName in mongodb_container_names:
        data = containerName
        versionName = "_".join(data.split("_")[:2])
        current_timeM = datetime.datetime.now()

        for i in range(number):
            cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            exec_timeM = mongodbBenchmarkTest.aggregate(containerName)
            exec_time_list.append(exec_timeM)
            cpu_consumption.append(cpuMemoryListM[0])
            memory_consumption.append(cpuMemoryListM[1])

        avg_exec_time = sum(exec_time_list) / len(exec_time_list)
        avg_cpu_consumption = sum(cpu_consumption) / len(cpu_consumption)
        avg_memory_consumption = sum(memory_consumption) / len(memory_consumption)
        print("Average execution time by running the query %s times is: %s milliseconds" % (
            number, '{:.4f}'.format(avg_exec_time)))
        print(f"Average CPU used = {avg_cpu_consumption:.4f}%")
        print(f"Average MEMORY used = {avg_memory_consumption:.4f}%")

    # df for mongodb
    newRows = {'Date': [current_timeM],
               'Database': ['Mongodb'],
               'Database Version': [versionName],
               'Query': ['aggregate'],
               'ExecTime': [format(avg_exec_time, ".4f")],
               'Memory_Used': [format(avg_memory_consumption, ".4f")],
               'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
    dfToCsv = pd.DataFrame(newRows)
    with open('benchmarkingResults.csv', 'a', newline='') as f:
        dfToCsv.to_csv(f, index=False, header=False)

    # postgres aggregate query
    for containerName in postgres_container_names:
        data = containerName
        versionName = "_".join(data.split("_")[:2])
        current_timeM = datetime.datetime.now()

        for i in range(number):
            cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            exec_timeM = postgresBenchmarkTest.aggregate(containerName)
            exec_time_list.append(exec_timeM)
            cpu_consumption.append(cpuMemoryListM[0])
            memory_consumption.append(cpuMemoryListM[1])

        avg_exec_time = sum(exec_time_list) / len(exec_time_list)
        avg_cpu_consumption = sum(cpu_consumption) / len(cpu_consumption)
        avg_memory_consumption = sum(memory_consumption) / len(memory_consumption)
        print("Average execution time by running the query %s times is: %s milliseconds" % (
            number, '{:.4f}'.format(avg_exec_time)))
        print(f"Average CPU used = {avg_cpu_consumption:.4f}%")
        print(f"Average MEMORY used = {avg_memory_consumption:.4f}%")

        # df for postgres
        newRows = {'Date': [current_timeM],
                   'Database': ['Postgres'],
                   'Database Version': [versionName],
                   'Query': ['aggregate'],
                   'ExecTime': [format(avg_exec_time, ".4f")],
                   'Memory_Used': [format(avg_memory_consumption, ".4f")],
                   'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
        dfToCsv = pd.DataFrame(newRows)
        with open('benchmarkingResults.csv', 'a', newline='') as f:
            dfToCsv.to_csv(f, index=False, header=False)

    # Neo4j aggregate query
    for containerName in neo4j_container_names:
        data = containerName
        versionName = "_".join(data.split("_")[:2])
        current_timeM = datetime.datetime.now()

        for i in range(number):
            cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            exec_timeM = neo4jBenchmarkTest.aggregate(containerName)
            exec_time_list.append(exec_timeM)
            cpu_consumption.append(cpuMemoryListM[0])
            memory_consumption.append(cpuMemoryListM[1])

        avg_exec_time = sum(exec_time_list) / len(exec_time_list)
        avg_cpu_consumption = sum(cpu_consumption) / len(cpu_consumption)
        avg_memory_consumption = sum(memory_consumption) / len(memory_consumption)
        print("Average execution time by running the query %s times is: %s milliseconds" % (
            number, '{:.4f}'.format(avg_exec_time)))
        print(f"Average CPU used = {avg_cpu_consumption:.4f}%")
        print(f"Average MEMORY used = {avg_memory_consumption:.4f}%")

        # df for neo4j
        newRows = {'Date': [current_timeM],
                   'Database': ['Neo4j'],
                   'Database Version': [versionName],
                   'Query': ['aggregate'],
                   'ExecTime': [format(avg_exec_time, ".4f")],
                   'Memory_Used': [format(avg_memory_consumption, ".4f")],
                   'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
        dfToCsv = pd.DataFrame(newRows)
        with open('benchmarkingResults.csv', 'a', newline='') as f:
            dfToCsv.to_csv(f, index=False, header=False)


def neighborsQuery():
    # mongodb neighbors query
    for containerName in mongodb_container_names:
        data = containerName
        versionName = "_".join(data.split("_")[:2])
        current_timeM = datetime.datetime.now()

        for i in range(number):
            cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            exec_timeM = mongodbBenchmarkTest.neighbors(containerName)
            exec_time_list.append(exec_timeM)
            cpu_consumption.append(cpuMemoryListM[0])
            memory_consumption.append(cpuMemoryListM[1])

        avg_exec_time = sum(exec_time_list) / len(exec_time_list)
        avg_cpu_consumption = sum(cpu_consumption) / len(cpu_consumption)
        avg_memory_consumption = sum(memory_consumption) / len(memory_consumption)
        print("Average execution time by running the query %s times is: %s milliseconds" % (
            number, '{:.4f}'.format(avg_exec_time)))
        print(f"Average CPU used = {avg_cpu_consumption:.4f}%")
        print(f"Average MEMORY used = {avg_memory_consumption:.4f}%")

        # df for mongodb
        newRows = {'Date': [current_timeM],
                   'Database': ['Mongodb'],
                   'Database Version': [versionName],
                   'Query': ['neighbors'],
                   'ExecTime': [format(avg_exec_time, ".4f")],
                   'Memory_Used': [format(avg_memory_consumption, ".4f")],
                   'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
        dfToCsv = pd.DataFrame(newRows)
        with open('benchmarkingResults.csv', 'a', newline='') as f:
            dfToCsv.to_csv(f, index=False, header=False)

    # postgres neighbors query
    for containerName in postgres_container_names:
        data = containerName
        versionName = "_".join(data.split("_")[:2])
        current_timeM = datetime.datetime.now()

        for i in range(number):
            cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            exec_timeM = postgresBenchmarkTest.neighbors(containerName)
            exec_time_list.append(exec_timeM)
            cpu_consumption.append(cpuMemoryListM[0])
            memory_consumption.append(cpuMemoryListM[1])

        avg_exec_time = sum(exec_time_list) / len(exec_time_list)
        avg_cpu_consumption = sum(cpu_consumption) / len(cpu_consumption)
        avg_memory_consumption = sum(memory_consumption) / len(memory_consumption)
        print("Average execution time by running the query %s times is: %s milliseconds" % (
            number, '{:.4f}'.format(avg_exec_time)))
        print(f"Average CPU used = {avg_cpu_consumption:.4f}%")
        print(f"Average MEMORY used = {avg_memory_consumption:.4f}%")

        # df for postgres
        newRows = {'Date': [current_timeM],
                   'Database': ['Postgres'],
                   'Database Version': [versionName],
                   'Query': ['neighbors'],
                   'ExecTime': [format(avg_exec_time, ".4f")],
                   'Memory_Used': [format(avg_memory_consumption, ".4f")],
                   'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
        dfToCsv = pd.DataFrame(newRows)
        with open('benchmarkingResults.csv', 'a', newline='') as f:
            dfToCsv.to_csv(f, index=False, header=False)

    # Neo4j neighbors query
    for containerName in neo4j_container_names:
        data = containerName
        versionName = "_".join(data.split("_")[:2])
        current_timeM = datetime.datetime.now()

        for i in range(number):
            cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            exec_timeM = neo4jBenchmarkTest.neighbors(containerName)
            exec_time_list.append(exec_timeM)
            cpu_consumption.append(cpuMemoryListM[0])
            memory_consumption.append(cpuMemoryListM[1])

        avg_exec_time = sum(exec_time_list) / len(exec_time_list)
        avg_cpu_consumption = sum(cpu_consumption) / len(cpu_consumption)
        avg_memory_consumption = sum(memory_consumption) / len(memory_consumption)
        print("Average execution time by running the query %s times is: %s milliseconds" % (
            number, '{:.4f}'.format(avg_exec_time)))
        print(f"Average CPU used = {avg_cpu_consumption:.4f}%")
        print(f"Average MEMORY used = {avg_memory_consumption:.4f}%")

        # df for neo4j
        newRows = {'Date': [current_timeM],
                   'Database': ['Neo4j'],
                   'Database Version': [versionName],
                   'Query': ['neighbors'],
                   'ExecTime': [format(avg_exec_time, ".4f")],
                   'Memory_Used': [format(avg_memory_consumption, ".4f")],
                   'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
        dfToCsv = pd.DataFrame(newRows)
        with open('benchmarkingResults.csv', 'a', newline='') as f:
            dfToCsv.to_csv(f, index=False, header=False)

def neighbors2Query():
    # mongodb neighbors2 query
    for containerName in mongodb_container_names:
        data = containerName
        versionName = "_".join(data.split("_")[:2])
        current_timeM = datetime.datetime.now()

        for i in range(number):
            cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            exec_timeM = mongodbBenchmarkTest.neighbors2(containerName)
            exec_time_list.append(exec_timeM)
            cpu_consumption.append(cpuMemoryListM[0])
            memory_consumption.append(cpuMemoryListM[1])

        avg_exec_time = sum(exec_time_list) / len(exec_time_list)
        avg_cpu_consumption = sum(cpu_consumption) / len(cpu_consumption)
        avg_memory_consumption = sum(memory_consumption) / len(memory_consumption)
        print("Average execution time by running the query %s times is: %s milliseconds" % (
            number, '{:.4f}'.format(avg_exec_time)))
        print(f"Average CPU used = {avg_cpu_consumption:.4f}%")
        print(f"Average MEMORY used = {avg_memory_consumption:.4f}%")

        # df for mongodb
        newRows = {'Date': [current_timeM],
                   'Database': ['Mongodb'],
                   'Database Version': [versionName],
                   'Query': ['neighbors2'],
                   'ExecTime': [format(avg_exec_time, ".4f")],
                   'Memory_Used': [format(avg_memory_consumption, ".4f")],
                   'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
        dfToCsv = pd.DataFrame(newRows)
        with open('benchmarkingResults.csv', 'a', newline='') as f:
            dfToCsv.to_csv(f, index=False, header=False)

    # postgres neighbors2 query
    for containerName in postgres_container_names:
        data = containerName
        versionName = "_".join(data.split("_")[:2])
        current_timeM = datetime.datetime.now()

        for i in range(number):
            cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            exec_timeM = postgresBenchmarkTest.neighbors2(containerName)
            exec_time_list.append(exec_timeM)
            cpu_consumption.append(cpuMemoryListM[0])
            memory_consumption.append(cpuMemoryListM[1])

        avg_exec_time = sum(exec_time_list) / len(exec_time_list)
        avg_cpu_consumption = sum(cpu_consumption) / len(cpu_consumption)
        avg_memory_consumption = sum(memory_consumption) / len(memory_consumption)
        print("Average execution time by running the query %s times is: %s milliseconds" % (
            number, '{:.4f}'.format(avg_exec_time)))
        print(f"Average CPU used = {avg_cpu_consumption:.4f}%")
        print(f"Average MEMORY used = {avg_memory_consumption:.4f}%")

        # df for postgres
        newRows = {'Date': [current_timeM],
                   'Database': ['Postgres'],
                   'Database Version': [versionName],
                   'Query': ['neighbors2'],
                   'ExecTime': [format(avg_exec_time, ".4f")],
                   'Memory_Used': [format(avg_memory_consumption, ".4f")],
                   'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
        dfToCsv = pd.DataFrame(newRows)
        with open('benchmarkingResults.csv', 'a', newline='') as f:
            dfToCsv.to_csv(f, index=False, header=False)

    # Neo4j neighbors2 query
    for containerName in neo4j_container_names:
        data = containerName
        versionName = "_".join(data.split("_")[:2])
        current_timeM = datetime.datetime.now()

        for i in range(number):
            cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            exec_timeM = neo4jBenchmarkTest.neighbors2(containerName)
            exec_time_list.append(exec_timeM)
            cpu_consumption.append(cpuMemoryListM[0])
            memory_consumption.append(cpuMemoryListM[1])

        avg_exec_time = sum(exec_time_list) / len(exec_time_list)
        avg_cpu_consumption = sum(cpu_consumption) / len(cpu_consumption)
        avg_memory_consumption = sum(memory_consumption) / len(memory_consumption)
        print("Average execution time by running the query %s times is: %s milliseconds" % (
            number, '{:.4f}'.format(avg_exec_time)))
        print(f"Average CPU used = {avg_cpu_consumption:.4f}%")
        print(f"Average MEMORY used = {avg_memory_consumption:.4f}%")

        # df for neo4j
        newRows = {'Date': [current_timeM],
                   'Database': ['Neo4j'],
                   'Database Version': [versionName],
                   'Query': ['neighbors2'],
                   'ExecTime': [format(avg_exec_time, ".4f")],
                   'Memory_Used': [format(avg_memory_consumption, ".4f")],
                   'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
        dfToCsv = pd.DataFrame(newRows)
        with open('benchmarkingResults.csv', 'a', newline='') as f:
            dfToCsv.to_csv(f, index=False, header=False)


def neighbors2dataQuery():
    # mongodb neighbors2data query
    for containerName in mongodb_container_names:
        data = containerName
        versionName = "_".join(data.split("_")[:2])
        current_timeM = datetime.datetime.now()

        for i in range(number):
            cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            exec_timeM = mongodbBenchmarkTest.neighbors2data(containerName)
            exec_time_list.append(exec_timeM)
            cpu_consumption.append(cpuMemoryListM[0])
            memory_consumption.append(cpuMemoryListM[1])

        avg_exec_time = sum(exec_time_list) / len(exec_time_list)
        avg_cpu_consumption = sum(cpu_consumption) / len(cpu_consumption)
        avg_memory_consumption = sum(memory_consumption) / len(memory_consumption)
        print("Average execution time by running the query %s times is: %s milliseconds" % (
            number, '{:.4f}'.format(avg_exec_time)))
        print(f"Average CPU used = {avg_cpu_consumption:.4f}%")
        print(f"Average MEMORY used = {avg_memory_consumption:.4f}%")

        # df for mongodb
        newRows = {'Date': [current_timeM],
                   'Database': ['Mongodb'],
                   'Database Version': [versionName],
                   'Query': ['neighbors2data'],
                   'ExecTime': [format(avg_exec_time, ".4f")],
                   'Memory_Used': [format(avg_memory_consumption, ".4f")],
                   'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
        dfToCsv = pd.DataFrame(newRows)
        with open('benchmarkingResults.csv', 'a', newline='') as f:
            dfToCsv.to_csv(f, index=False, header=False)

    # postgres neighbors2data query
    for containerName in postgres_container_names:
        data = containerName
        versionName = "_".join(data.split("_")[:2])
        current_timeM = datetime.datetime.now()

        for i in range(number):
            cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            exec_timeM = postgresBenchmarkTest.neighbors2data(containerName)
            exec_time_list.append(exec_timeM)
            cpu_consumption.append(cpuMemoryListM[0])
            memory_consumption.append(cpuMemoryListM[1])

        avg_exec_time = sum(exec_time_list) / len(exec_time_list)
        avg_cpu_consumption = sum(cpu_consumption) / len(cpu_consumption)
        avg_memory_consumption = sum(memory_consumption) / len(memory_consumption)
        print("Average execution time by running the query %s times is: %s milliseconds" % (
            number, '{:.4f}'.format(avg_exec_time)))
        print(f"Average CPU used = {avg_cpu_consumption:.4f}%")
        print(f"Average MEMORY used = {avg_memory_consumption:.4f}%")

        # df for postgres
        newRows = {'Date': [current_timeM],
                   'Database': ['Postgres'],
                   'Database Version': [versionName],
                   'Query': ['neighbors2data'],
                   'ExecTime': [format(avg_exec_time, ".4f")],
                   'Memory_Used': [format(avg_memory_consumption, ".4f")],
                   'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
        dfToCsv = pd.DataFrame(newRows)
        with open('benchmarkingResults.csv', 'a', newline='') as f:
            dfToCsv.to_csv(f, index=False, header=False)

    # Neo4j neighbors2data query
    for containerName in neo4j_container_names:
        data = containerName
        versionName = "_".join(data.split("_")[:2])
        current_timeM = datetime.datetime.now()

        for i in range(number):
            cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            exec_timeM = neo4jBenchmarkTest.neighbors2data(containerName)
            exec_time_list.append(exec_timeM)
            cpu_consumption.append(cpuMemoryListM[0])
            memory_consumption.append(cpuMemoryListM[1])

        avg_exec_time = sum(exec_time_list) / len(exec_time_list)
        avg_cpu_consumption = sum(cpu_consumption) / len(cpu_consumption)
        avg_memory_consumption = sum(memory_consumption) / len(memory_consumption)
        print("Average execution time by running the query %s times is: %s milliseconds" % (
            number, '{:.4f}'.format(avg_exec_time)))
        print(f"Average CPU used = {avg_cpu_consumption:.4f}%")
        print(f"Average MEMORY used = {avg_memory_consumption:.4f}%")

        # df for neo4j
        newRows = {'Date': [current_timeM],
                   'Database': ['Neo4j'],
                   'Database Version': [versionName],
                   'Query': ['neighbors2data'],
                   'ExecTime': [format(avg_exec_time, ".4f")],
                   'Memory_Used': [format(avg_memory_consumption, ".4f")],
                   'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
        dfToCsv = pd.DataFrame(newRows)
        with open('benchmarkingResults.csv', 'a', newline='') as f:
            dfToCsv.to_csv(f, index=False, header=False)


def shortestpathQuery():

    # Neo4j shortestpathQuery query
    for containerName in neo4j_container_names:
        data = containerName
        versionName = "_".join(data.split("_")[:2])
        current_timeM = datetime.datetime.now()

        for i in range(number):
            cpuMemoryListM = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
            exec_timeM = neo4jBenchmarkTest.shortestPath(containerName)
            exec_time_list.append(exec_timeM)
            cpu_consumption.append(cpuMemoryListM[0])
            memory_consumption.append(cpuMemoryListM[1])

        avg_exec_time = sum(exec_time_list) / len(exec_time_list)
        avg_cpu_consumption = sum(cpu_consumption) / len(cpu_consumption)
        avg_memory_consumption = sum(memory_consumption) / len(memory_consumption)
        print("Average execution time by running the query %s times is: %s milliseconds" % (
            number, '{:.4f}'.format(avg_exec_time)))
        print(f"Average CPU used = {avg_cpu_consumption:.4f}%")
        print(f"Average MEMORY used = {avg_memory_consumption:.4f}%")

        # df for neo4j
        newRows = {'Date': [current_timeM],
                   'Database': ['Neo4j'],
                   'Database Version': [versionName],
                   'Query': ['shortestPath'],
                   'ExecTime': [format(avg_exec_time, ".4f")],
                   'Memory_Used': [format(avg_memory_consumption, ".4f")],
                   'Cpu_Used': [format(avg_cpu_consumption, ".4f")]}
        dfToCsv = pd.DataFrame(newRows)
        with open('benchmarkingResults.csv', 'a', newline='') as f:
            dfToCsv.to_csv(f, index=False, header=False)


def createAndRunContainers():
        createAndRemoveContainers.createContainers()

def stopAndRemoveContainersAndDatasets():

        createAndRemoveContainers.deleteContainers()

        #deleting datasets in json format after setting up the databases with data
        os.remove("data.json")
        os.remove("relations.json")

def deleteDatabasesAndDataset():
    #delete mongodb database
    for containerName in mongodb_container_names:
        mongodbBenchmarkTest.dropDatabase(containerName)

    #delete postgres database
    for containerName in postgres_container_names:
        postgresBenchmarkTest.drop_tables('profiles',containerName)
        postgresBenchmarkTest.drop_tables('relations',containerName)
        postgresBenchmarkTest.drop_database(containerName)

    #delete postgres database
    for containerName in neo4j_container_names:
        neo4jBenchmarkTest.deleteAllNodesAndRelationships(containerName)

    #delete datasets
    # os.remove("data.json")
    # os.remove("relations.json")

if __name__ == "__main__":
     #createAndRunContainers()
     #downloadDatasetsAndConvertToJson()
     setupPostgresDatabase()
     setupMongodbDatabase()
     setupNeo4jDatabase()
     singleReadQuery()
     singleWriteQuery()
     aggregateQuery()
     neighborsQuery()
     neighbors2Query()
     neighbors2dataQuery()
     shortestpathQuery()
     deleteDatabasesAndDataset()
     #stopAndRemoveContainersAndDatasets()