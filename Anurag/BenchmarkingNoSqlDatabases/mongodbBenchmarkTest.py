import pymongo
import pprint
import json
import pandas as pd
from pymongo import MongoClient
import pql
import datetime


#------------- Create db connection and create database ----------------------------

def create_connection():
    try:
        conn_string = "mongodb://mongo:secret@localhost:27017/" # <----enter username,pwd,host address and port no accordingly
        myclient = MongoClient(conn_string)
        print("Connected successfully!!!")
        print(myclient)
    except:
        print("Could not connect to MongoDB")

    return myclient

def createDB():
    myclient = create_connection()

    dblist = myclient.list_database_names()
    if "test" in dblist:
        print("The test database exists.")
    else:
        init_time = datetime.datetime.now()
        mydb = myclient["test"]
        end_time = datetime.datetime.now()
        exec_time =  end_time - init_time
        print ('exec_time  = {} Microseconds '.format( exec_time.microseconds))

    myclient.close()

def createProfileCollection():
    myclient = create_connection()
    db = myclient.test
    init_time = datetime.datetime.now()
    profilesCollection = db["profiles"]
    end_time = datetime.datetime.now()
    exec_time =  end_time - init_time
    print ('exec_time  = {} Microseconds '.format( exec_time.microseconds))
    print(db)
    myclient.close()

def createRelationsCollection():
    myclient = create_connection()
    db = myclient.test
    init_time = datetime.datetime.now()
    relationsCollection = db["relations"]
    end_time = datetime.datetime.now()
    exec_time =  end_time - init_time
    print ('exec_time  = {} Microseconds '.format( exec_time.microseconds))
    print(db)
    myclient.close()

def insertIntoProfilesCollection():
    myclient = create_connection()
    db = myclient.test
    profileCol = db.profiles
    # Loading or Opening the json file
    with open('soc-pokec-profiles500.json',errors='ignore') as json_data:
        data = json.load(json_data)
    # Inserting the loaded data in the Collection
    # if JSON contains data more than one entry
    # insert_many is used else inser_one is used
    if isinstance(data, list):
        init_time = datetime.datetime.now()
        profileCol.insert_many(data)
        end_time = datetime.datetime.now()
        exec_time =  end_time - init_time
        print ('exec_time  = {} Microseconds '.format( exec_time.microseconds))
    else:
        profileCol.insert_one(data)

    myclient.close()

def insertIntoRelationsCollection():
    myclient = create_connection()
    db = myclient.test
    relationsCol = db.relations
    # Loading or Opening the json file
    with open('soc-pokec-relationship5000.json',errors='ignore') as json_data:
        data = json.load(json_data)
    # Inserting the loaded data in the Collection
    # if JSON contains data more than one entry
    # insert_many is used else inser_one is used
    if isinstance(data, list):
        init_time = datetime.datetime.now()
        relationsCol.insert_many(data)
        end_time = datetime.datetime.now()
        exec_time =  end_time - init_time
        print ('exec_time  = {} Microseconds '.format( exec_time.microseconds))
    else:
        relationsCol.insert_one(data)

    myclient.close()

def readProfilesCollection():
    myclient = create_connection()
    db = myclient.test
    mycol = db.profiles
    init_time = datetime.datetime.now()
    for x in mycol.find():
        print(x)
    end_time = datetime.datetime.now()
    exec_time =  end_time - init_time
    print ('exec_time  = {} Microseconds '.format( exec_time.microseconds))
    myclient.close()

def readRelationsCollection():
    myclient = create_connection()
    db = myclient.test
    mycol = db.relations
    init_time = datetime.datetime.now()
    for x in mycol.find():
        print(x)
    end_time = datetime.datetime.now()
    exec_time =  end_time - init_time
    print ('exec_time  = {} Microseconds '.format( exec_time.microseconds))
    myclient.close()


def dropProfilesCollection():
    myclient = create_connection()
    db = myclient.test
    mycol = db.profiles
    init_time = datetime.datetime.now()
    mycol.drop()
    end_time = datetime.datetime.now()
    exec_time =  end_time - init_time
    print ('exec_time  = {} Microseconds '.format( exec_time.microseconds))
    print("profiles connection has been deleted.")
    myclient.close()

def dropRelationsCollection():
    myclient = create_connection()
    db = myclient.test
    mycol = db.relations
    init_time = datetime.datetime.now()
    mycol.drop()
    end_time = datetime.datetime.now()
    exec_time =  end_time - init_time
    print ('exec_time  = {} Microseconds '.format( exec_time.microseconds))
    print("relations connection has been deleted.")
    myclient.close()

def singleRead():
    myclient = create_connection()
    db = myclient.test
    mycol = db.profiles
    init_time = datetime.datetime.now()
    x = mycol.find_one()
    end_time = datetime.datetime.now()
    exec_time =  end_time - init_time
    print ('exec_time  = {} Microseconds '.format( exec_time.microseconds))
    print(x)
    myclient.close()

def singleWrite():
    myclient = create_connection()
    db = myclient.test
    mycol = db.profiles
    with open('soc-pokec-profiles500.json',errors='ignore') as json_data:
        data = json.load(json_data)
    init_time = datetime.datetime.now()
    mycol.insert_one(data[400])
    end_time = datetime.datetime.now()
    exec_time =  end_time - init_time
    print ('exec_time  = {} Microseconds '.format( exec_time.microseconds))
    myclient.close()

def aggregation():
    myclient = create_connection()
    db = myclient.test
    mycol = db.profiles
    init_time = datetime.datetime.now()
    aggregate_result = mycol.aggregate([{
                                        "$group":
                                            {
                                                "_id" : "&AGE",
                                                "Aggregate Age" : {"$sum":1}
                                            }
                                        }])
    end_time = datetime.datetime.now()
    exec_time =  end_time - init_time
    print ('exec_time  = {} Microseconds '.format( exec_time.microseconds))

    for i in aggregate_result:
        print(i)
    myclient.close()

def neighbors():
    myclient = create_connection()
    db = myclient.test
    mycol = db.relations
    init_time = datetime.datetime.now()

    record = mycol.find({"_from" : '2'},{"_to": 1}).limit(100)

    end_time = datetime.datetime.now()
    exec_time =  end_time - init_time
    print ('exec_time  = {} Microseconds '.format( exec_time.microseconds))

    for doc in record:
        print(doc)

    myclient.close()

def neighbors2():
    myclient = create_connection()
    db = myclient.test
    mycol = db.relations
    #this is sql query
    # sql_query = "select _to from relations where _from = '15' union" \
    #             " distinct select _to from relations where _to != '15' " \
    #             "and" \
    #             " _from in (select  _to from relations where _from = '15')"



    init_time = datetime.datetime.now()
    #result of IN operator
    array = mycol.find({"_from" : '15'},{"_to" : 1,"_id":0})
    list_IN = []
    for x in array :
        list_IN.append(x["_to"])
    record = mycol.find({ "$and" : [{"$or": [{ "_to " : { "$ne":  '15' }  },{ "_from" :  '15'}] },
                                    {"_from": {"$in": list_IN}}]},{"_to": 1 })

    end_time = datetime.datetime.now()
    exec_time =  end_time - init_time
    print ('exec_time  = {} Microseconds '.format( exec_time.microseconds))
    for doc in record:
        print(doc)
    myclient.close()


def neighbors2data():
    myclient = create_connection()
    db = myclient.test
    mycol = db.relations

    #this is a sql query
    # neighbors2data_query = "select * from profiles where user_id::text IN (select _to from relations where _from = '20' union" \
    #                        " distinct select _to from relations where _to != '20' and _from IN" \
    #                        " (select  _to from relations where _from = '20'))"

    init_time = datetime.datetime.now()
    #result of IN operator
    array = mycol.find({"_from" : '15'},{"_to" : 1,"_id":0})
    list_IN = []
    for x in array :
        list_IN.append(x["_to"])
    innerQuery = mycol.find({ "$and" : [{"$or": [{ "_to " : { "$ne":  '15' }  },{ "_from" :  '15'}] },
                                    {"_from": {"$in": list_IN}}]},{"_to": 1 })
    list_inner_query = []
    for x in innerQuery :
        list_inner_query.append(x["_to"])
    #print(list_inner_query)

    mycol_profiles = db.profiles
    mainQuery = mycol_profiles.find({"user_id":{"$in" : list_inner_query}})

    end_time = datetime.datetime.now()
    exec_time =  end_time - init_time
    print ('exec_time  = {} Microseconds '.format( exec_time.microseconds))

    #for doc in mainQuery:
     #   print(doc)

    myclient.close()

if __name__ == "__main__":
    #createDB()
    #createProfileCollection()
    #createRelationsCollection()
    #insertIntoProfilesCollection()
    #insertIntoRelationsCollection()
    #readProfilesCollection()
    #readRelationsCollection()
    #dropProfilesCollection()
    #dropRelationsCollection()
    #singleRead()
    #singleWrite()
    #aggregation()
    #neighbors()
    #neighbors2()
    neighbors2data()











