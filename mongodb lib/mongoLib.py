from dotenv import load_dotenv , find_dotenv
import os
import pprint
from pymongo import MongoClient
from bson.objectid import ObjectId
printer = pprint.PrettyPrinter()

load_dotenv(find_dotenv())


class MongoLIb():
  def __init__(self,mongoLink, databaseName) :
    self.client = MongoClient(mongoLink)
    self.db = self.client[databaseName]
    
  # insert one data group to collection
  def insertOne(self,data,collectionName):
    collection = self.db[collectionName]
    result = collection.insert_one(data)
    print(result.inserted_id)
    
  #  insert grroup of dataset to collection 
  #  the data that use in this func is a alist of docs 
  def insertMany(self ,data,collectionName):
    collection = self.db[collectionName]
    result =collection.insert_many(data)
    print(result.inserted_ids)
    
    
# ----------------------------query-------------------------------------

  def findAll(self,collectionName):
   collection = self.db[collectionName]
   results =collection.find({})
   for result in results:
     printer.pprint(result)
     
  def find(self,collectionName ,queryName,queryData):
   collection = self.db[collectionName]
   state = True
   query ={}
   if len(queryName) != len(queryData):
     state =False
     
   if state:
     for i in range(len(queryName)):
       query[queryName[i]] = queryData[i]
   results = collection.find({query})
   
     
  def collectionCount(self,collectionName ,queryName,queryData ):
    collection = self.db[collectionName]
    state = True
    query ={}
    if len(queryName) != len(queryData):
     state =False
     
    if state:
     for i in range(len(queryName)):
       query[queryName[i]] = queryData[i] 
    results = collection.count_documents(filter={query}).count()
    for result in results:
     printer.pprint(result)
     
     
  def findById(self,collectionName , ID):
   collection = self.db[collectionName]
   
   _id = ObjectId(ID)
   result = collection.find_one({"_id": _id})
   printer.pprint(result)
   
  def findGRT(self,collectionName , numQuery, min):
    collection = self.db[collectionName]
    query ={
      {"$and":[
        {f"{numQuery}":{"$gte": min }}
        ]}
    }
    
    results =collection.find(query).sort(numQuery)
    for result in results:
     printer.pprint(result)
   
  def findLTE(self,collectionName , numQuery, max):
    collection = self.db[collectionName]
    query ={
      {"$and":[
        {f"{numQuery}":{"$lte": max }}
        ]}
    }
    
    results =collection.find(query).sort(numQuery)
    for result in results:
     printer.pprint(result)
     
  def findRange(self,collectionName , numQuery, min,max):
    collection = self.db[collectionName]
    query ={
      {"$and":[
        {f"{numQuery}":{"$gte": min }},
        {f"{numQuery}":{"$lte": max }}
        ]}
    }
    
    results =collection.find(query).sort(numQuery)
    for result in results:
     printer.pprint(result)
  
  def findSelectedColumns(self,collectionName,column ,queryName,queryData):
    collection = self.db[collectionName]
    columns={}
    state = True
    query ={}
    if len(queryName) != len(queryData):
     state =False
     
    if state:
     for i in range(len(queryName)):
       query[queryName[i]] = queryData[i]
    for col in column:
      columns[col] = 1
    results = collection.find(query,columns)
    for result in results:
     printer.pprint(result)
     
# _______________________________update__________________________

  def updatebyId(self,collectionName,ID,updateField,updateData):
    collection = self.db[collectionName]
   
    _id = ObjectId(ID)
    update ={
      "$inc": {f"{updateField}": updateData}
    }
    result = collection.update_one({"_id": _id},update)
    print(result)
    
  def setHeader(self,collectionName,ID,headerName,):
    collection = self.db[collectionName]
   
    _id = ObjectId(ID)
    update ={
      "$set": {f"{headerName}": True}
    }
    result = collection.update_one({"_id": _id},update)
    print(result)
    
  def updateHeader(self,collectionName,ID,headerName,newHeaderName):
    collection = self.db[collectionName]
   
    _id = ObjectId(ID)
    update ={
      "$rename": {f"{headerName}": newHeaderName}
    }
    result = collection.update_one({"_id": _id},update)
    print(result)
    
    
    
    
  def DeleteHeader(self,collectionName,ID,headerName):
    collection = self.db[collectionName]
   
    _id = ObjectId(ID)
    update ={
      "$unset": {f"{headerName}": ""}
    }
    result = collection.update_one({"_id": _id},update)
    print(result)
    
    
  def replaceOne(self,collectionName,ID,fieldsName,newData):
    collection = self.db[collectionName]
    state = True
    update={}
    if len(fieldsName) != len(newData):
      state=False
    if state:
     _id = ObjectId(ID)
     for name, data in zip(fieldsName,newData):
       update[f"{name}"]=data
       
    result =collection.replace_one({"_id": _id},update)
    printer.pprint(result)

# __________________________delete________________________

  def deleteByID(self,collectionName,ID):
       collection = self.db[collectionName]
       _id = ObjectId(ID)
       result = collection.delete_one({"_id":_id})
  

      
    
      
   
   
    
           
        
    
        
        
       
    