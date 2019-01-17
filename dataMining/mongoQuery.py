"""
test for using python for mongoDB
10/22/2018
"""

from pymongo import MongoClient
from bson import Code
import json
import pprint
from dataMining.generateJson import writeJson
# from generateJson import writeJson


def askMongo(dict):
    conn = MongoClient('127.0.0.1', 27017)
    db = conn.testDB
    result = generalQuery(db, dict)
    print(result)
    return result

numberlist = ["latitude", "longitude", "stars", "review_count", "average_star", "rstars"]

def generalQuery(db, dict):
    print("enter generalQuery")
    print(dict)
    print(db.list_collection_names())
    result = {}
    if dict["collection"] == "Business":
        collection = db.Business
    elif dict["collection"] == "User":
        collection = db.User
    elif dict["collection"] == "Review":
        collection = db.Review
    else:
        result["error"] = "Collection " + dict["collection"] + " doesn't find"
        return result 
    
    index = 1
    for i in range(len(dict["key"])):
        if dict["key"][i] not in numberlist:
            docs = collection.find({dict["key"][i] : dict["value"][i]})
        elif dict["key"][i] == "star" and dict["value"][i][0] == dict["value"][i][1]:
            docs = collection.find({"star" : float(dict["value"][i][0])})
        elif dict["key"][i] == "star":
            docs = collection.find({"star" : {"$gte" : float(dict["value"][i][0]), "$lte" : float(dict["value"][i][0])}})
        else:
            docs = collection.find({dict["key"][i] : float(dict["value"][i])})
        for doc in docs:
            result[index] = doc
            index += 1

    # if dict["key"] in numberlist:
    #     dict["value"] = float(dict["value"])

    # docs = collection.find({dict["key"] : dict["value"]})

    # index = 1
    # for doc in docs:
    #     result[index] = doc
    #     index += 1
    
    return result

if __name__ == '__main__':
    dict = {}
    dict["collection"] = "Business"
    dict["key"] = "city"
    dict["value"] = "Pittsburgh"
    input_dict = {"collection": "Business", "key": "business_id", "value": "aYE3ARaHRuk5GQ4K5FNqvw"}
    askMongo(input_dict)