import pymongo
import pprint
import json
import pandas as pd
from pymongo import MongoClient


#------------- Create db connection ----------------------------
try:
    myclient = MongoClient("mongodb://localhost:27017/")
    print("Connected successfully!!!")
    print(myclient)
except:
    print("Could not connect to MongoDB")

# ------------- Create db ----------------------------

mydb = myclient["testMongodbDatabase"]

# ------------- Create table in the db ----------------

collist = mydb.list_collection_names()
if "test-collection" in collist:
  mycol = mydb["test-collection"]
  mycol.drop()

collection = mydb['test-collection']

collist = mydb.list_collection_names()

print(collist)
# ------------- Create a list to be inserted into the table ----------------

mylist = [
  { "name": "Amy", "address": "Apple st 652"},
  { "name": "Hannah", "address": "Mountain 21"},
  { "name": "Michael", "address": "Valley 345"},
  { "name": "Sandy", "address": "Ocean blvd 2"},
  { "name": "Betty", "address": "Green Grass 1"},
  { "name": "Richard", "address": "Sky st 331"},
  { "name": "Susan", "address": "One way 98"},
  { "name": "Vicky", "address": "Yellow Garden 2"},
  { "name": "Ben", "address": "Park Lane 38"},
  { "name": "William", "address": "Central st 954"},
  { "name": "Chuck", "address": "Main Road 989"},
  { "name": "Viola", "address": "Sideway 1633"}
]

# ------------- insert a list into the table ----------------

id_list = collection.insert_many(mylist)

# ------------- print contents of the table ----------------

for x in collection.find({}, {"_id":0 }):
    print(x)


# ------------- print specific content of the table ----------------

myquery = { "name": "Chuck" }

mydoc = collection.find(myquery)

for x in mydoc:
  print(x)

# ------------- delete specific document from the table ----------------

myquery = { "address": "Green Grass 1" }

collection.delete_one(myquery)
for x in collection.find({}, {"_id":0 }):
    print(x)

# ------------- update specific document from the table ----------------

myquery = { "address": "Valley 345" }
newvalues = { "$set": { "address": "Canyon 123" } }

collection.update_one(myquery, newvalues)

for x in collection.find({}, {"_id":0 }):
    print(x)

# ------------- read csv file using pandas ----------------

df = pd.read_csv("crime.csv")

df_mongo=df.head(20)

# ------------- create collection to store first 20 records of csv ----------------

collectionList = mydb.list_collection_names()
if "CSV_to_Table" in collectionList:
  mycol = mydb["CSV_to_Table"]
  mycol.drop()

csvToTable = mydb['CSV_to_Table']

df_mongo.reset_index(inplace=True)
data_dict = df_mongo.to_dict("records")
csvToTable.insert_many(data_dict)

# ------------- printing contents of 'CSV_to_Table' collection ----------------
print("Contents of 'CSV_to_Table'::")
x=csvToTable.find()
for data in x:
    print(data)
