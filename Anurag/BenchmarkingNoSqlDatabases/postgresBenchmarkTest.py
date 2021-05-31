import datetime
import json
import os
import re
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2 import sql
import psycopg2 as psycopg2
import mongodbBenchmarkTest,neo4jBenchmarkTest

###Check connection

def create_connection(containerName):
    port_no = re.split(pattern='_', string=containerName)[-1] # <-----get port no from docker container's name
    print(port_no)
    conn = psycopg2.connect(user='postgres',

                            host='localhost',

                            port=port_no,

                            password='secret')
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    conn.autocommit = True
    return conn

def create_database(containerName):
    global conn
    try:
        containerName = containerName
        conn = create_connection(containerName)
        cur = conn.cursor()
        query = 'CREATE DATABASE test'
        cur.execute(query)

        # close communication with the PostgreSQL database server

        cur.close()

        # commit the changes

        conn.commit()

    except(Exception, psycopg2.DatabaseError) as error:

        print(error)

    finally:

        if conn is not None:

            conn.close()


def create_tables(containerName):
    global conn
    try:
        containerName = containerName
        conn = create_connection(containerName)
        cur = conn.cursor()
        with open('data.json', errors='ignore') as json_data:
            data = json.load(json_data)
        columnData = list(data[0].keys())
        queryData = "CREATE TABLE profiles ( " + columnData[0] + " INTEGER PRIMARY KEY"
        for i in range((len(columnData))-1):
            queryData = queryData + "," + columnData[i+1] + " text"
        queryData = queryData + ");"

        cur.execute(queryData)

        with open('relations.json', errors='ignore') as json_data:
            data = json.load(json_data)
        columnData = list(data[0].keys())
        queryRelations = "CREATE TABLE relations ( " + columnData[0] + " text," + columnData[1] + " text );"


        cur.execute(queryRelations)

        # close communication with the PostgreSQL database server
        cur.close()

        # commit the changes

        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:

        print(error)

    finally:

        if conn is not None:

            conn.close()



def drop_database(containerName):
    global conn
    try:
        containerName = containerName
        conn = create_connection(containerName)
        cur = conn.cursor()
        query = 'DROP DATABASE test'
        cur.execute(query)

        # close communication with the PostgreSQL database server

        cur.close()

        # commit the changes

        conn.commit()

    except(Exception, psycopg2.DatabaseError) as error:

        print(error)

    finally:

        if conn is not None:

            conn.close()



def drop_tables(table_name,containerName):
    global conn
    table_name = table_name
    try:
        containerName = containerName
        conn = create_connection(containerName)
        cur = conn.cursor()
        query = "DROP TABLE " + table_name + ';'

        cur.execute(query)

        # close communication with the PostgreSQL database server

        cur.close()

        # commit the changes

        conn.commit()

    except(Exception, psycopg2.DatabaseError) as error:

        print(error)

    finally:

        if conn is not None:
            conn.close()



def Insert_INTO_profiles_table(containerName):
    global conn
    try:
        #use Python's open() function to load the JSON data
        with open('data.json',errors='ignore') as json_data:
            data = json.load(json_data)
            query_sql = """ insert into profiles
                    select * from json_populate_recordset(NULL::profiles, %s) """
            containerName = containerName
            conn = create_connection(containerName)
            cur = conn.cursor()
            cur.execute(query_sql, (json.dumps(data),))
            conn.commit()

            #print ('\nfinished INSERT INTO profiles table')
            cur.close()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:

        if conn is not None:
            conn.close()


def Insert_INTO_relations_table(containerName):
    global conn

    try:
        #use Python's open() function to load the JSON data
        with open('relations.json',errors='ignore') as json_data:
            data = json.load(json_data)
            query_sql = """ insert into relations
                    select * from json_populate_recordset(NULL::relations, %s) """
            containerName = containerName
            conn = create_connection(containerName)
            cur = conn.cursor()
            cur.execute(query_sql, (json.dumps(data),))
            conn.commit()
            #print ('\nfinished INSERT INTO relations table')
            cur.close()

    except (Exception, psycopg2.DatabaseError) as error:

        print(error)

    finally:

        if conn is not None:

            conn.close()

def singleRead(containerName):
    global conn
    try:
        randomUserIDList = neo4jBenchmarkTest.createUserIDList()
        select_profiles_query = "SELECT * FROM profiles WHERE user_id = '%s'"%(randomUserIDList[0])
        containerName = containerName
        conn = create_connection(containerName)
        cur = conn.cursor()
        start_time = datetime.datetime.now()

        cur.execute(select_profiles_query)

        end_time = datetime.datetime.now()
        profiles = cur.fetchall()

        #print(f"Profile of user id {randomUserIDList[0]} ::")
        #print(profiles)
        cur.close()

        return (end_time - start_time).total_seconds() * 1000

    except (Exception, psycopg2.DatabaseError) as error:

        print(error)

    finally:

        if conn is not None:

            conn.close()


def singleWrite(containerName):
    global conn
    try:
        randomUserIDList = neo4jBenchmarkTest.createUserIDList()
        #print(randomUserIDList)
        select_profiles_query = "UPDATE profiles SET Age = '%s' WHERE user_id = '%s'"%(randomUserIDList[0],randomUserIDList[1])
        containerName = containerName
        conn = create_connection(containerName)
        cur = conn.cursor()
        start_time = datetime.datetime.now()

        cur.execute(select_profiles_query)

        end_time = datetime.datetime.now()
        cur.close()
        return (end_time - start_time).total_seconds() * 1000

    except (Exception, psycopg2.DatabaseError) as error:

        print(error)

    finally:

        if conn is not None:

            conn.close()


def Read_relationship_table(containerName):
    global conn
    try:
        select_relationship_query = "SELECT * FROM relations"
        containerName = containerName
        conn = create_connection(containerName)
        cur = conn.cursor()
        cpuMemoryList = mongodbBenchmarkTest.calculateCPUandMemoryUsage(os.getpid())
        start_time = datetime.datetime.now()


        cur.execute(select_relationship_query)

        end_time = datetime.datetime.now()
        execTime = (end_time - start_time).total_seconds() * 1000
        relations = cur.fetchall()
        print("Table contents after insertion ::")
        print(relations)
        print("Query execution time = %s Milliseconds" % execTime)
        print(f"CPU used = {cpuMemoryList[0]:.4f}%")
        print(f"MEMORY used = {cpuMemoryList[1]:.4f}%")

        cur.close()

    except (Exception, psycopg2.DatabaseError) as error:

        print(error)

    finally:

        if conn is not None:

            conn.close()


def aggregate(containerName):
    global conn
    try:
        aggregate_query = "select AGE, count(*) from profiles group by AGE"
        containerName = containerName
        conn = create_connection(containerName)
        cur = conn.cursor()
        start_time = datetime.datetime.now()

        cur.execute(aggregate_query)

        end_time = datetime.datetime.now()
        age = cur.fetchall()
        #print("Aggregate of AGE ::")
        #print(age)
        cur.close()
        return (end_time - start_time).total_seconds() * 1000

    except (Exception, psycopg2.DatabaseError) as error:

        print(error)

    finally:

        if conn is not None:

            conn.close()

def neighbors(containerName):
    global conn
    try:
        randomUserIDList = neo4jBenchmarkTest.createUserIDList()
        neighbors_query = "SELECT DISTINCT _to FROM relations WHERE _from = '%s'"%(randomUserIDList[0])
        containerName = containerName
        conn = create_connection(containerName)
        cur = conn.cursor()
        start_time = datetime.datetime.now()

        cur.execute(neighbors_query)

        end_time = datetime.datetime.now()
        relations = cur.fetchall()

        #print(f"Neighbors of user_id = {randomUserIDList[0]} are:")
        #print(relations)
        cur.close()
        return (end_time - start_time).total_seconds() * 1000

    except (Exception, psycopg2.DatabaseError) as error:

        print(error)

    finally:

        if conn is not None:

            conn.close()


def neighbors2(containerName):
    global conn
    try:
        randomUserIDList = neo4jBenchmarkTest.createUserIDList()
        neighbors2_query = "select _to from relations where _from = '%s' union distinct select _to from relations" \
                          " where _to != '%s' and _from in (select  _to from relations where _from = '%s')"%(randomUserIDList[0],randomUserIDList[0],randomUserIDList[0])
        containerName = containerName
        conn = create_connection(containerName)
        cur = conn.cursor()
        start_time = datetime.datetime.now()

        cur.execute(neighbors2_query)

        end_time = datetime.datetime.now()
        execTime = (end_time - start_time).total_seconds() * 1000
        relations = cur.fetchall()

        #print(f"Immediate and first level neighbors of user_id = {randomUserIDList[0]} are:")
        #print(relations)
        cur.close()
        return (end_time - start_time).total_seconds() * 1000

    except (Exception, psycopg2.DatabaseError) as error:

        print(error)

    finally:

        if conn is not None:

            conn.close()

def neighbors2data(containerName):
    global conn
    try:
        randomUserIDList = neo4jBenchmarkTest.createUserIDList()
        neighbors2data_query = "select * from profiles where user_id::text IN (select _to from relations where _from = '%s' union" \
                           " distinct select _to from relations where _to != '%s' and _from IN" \
                           " (select  _to from relations where _from = '%s'))"%(randomUserIDList[0],randomUserIDList[0],randomUserIDList[0])
        containerName = containerName
        conn = create_connection(containerName)
        cur = conn.cursor()
        start_time = datetime.datetime.now()

        cur.execute(neighbors2data_query)

        end_time = datetime.datetime.now()
        relations = cur.fetchall()

        #print(f"Profiles of neighbors of user_id = {randomUserIDList[0]} are:")
        #print(relations)
        cur.close()
        return (end_time - start_time).total_seconds() * 1000

    except (Exception, psycopg2.DatabaseError) as error:

        print(error)

    finally:

        if conn is not None:

            conn.close()


if __name__ == "__main__":
    #create_connection("postgres_latest_5435")
    #create_database("postgres_latest_5434")
    #create_tables("postgres_latest_5434")
    #drop_tables('profiles',"postgres_latest_5434")
    #drop_tables('relations',"postgres_latest_5434")
    #drop_database("postgres_latest_5435")
    #Insert_INTO_profiles_table("postgres_latest_5434")
    Insert_INTO_relations_table("postgres_latest_5434")
    #singleRead()
    #singleWrite()
    #Read_relationship_table()
    #neighbors()
    #neighbors2()
    #neighbors2data()
    #aggregate()
