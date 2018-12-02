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
    # countStates(db)
    result = generalQuery(db, dict)
    print(result)
    return result


# def countStates(db):
#     business = db.business

#     answer = {}
#     states = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY',
#               'LA',
#               'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH',
#               'OK',
#               'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY']

#     business_state_count = business.aggregate([
#         {'$group': {
#             '_id': '$state',
#             'count': {'$sum': 1}
#         }
#         }
#     ])

#     for dic in business_state_count:
#         if dic['count'] > 100 and dic['_id'] in states:
#             answer[dic['_id']] = dic['count']

#     writeJson(answer, 'businessCount')

def generalQuery(db, dict):
    print("enter generalQuery")
    print(dict)
    print(db.list_collection_names())
    result = {}
    if dict["collection"] == "Business":
        collection = db.Business
        print(collection.count())
    elif dict["collection"] == "User":
        collection = db.User
    elif dict["collection"] == "Review":
        collection = db.Review
    else:
        result["error"] = "Collection " + dict["collection"] + " doesn't find"
        return result 
    
    docs = collection.find({dict["key"] : dict["value"]})

    index = 1
    for doc in docs:
        result[index] = doc
        index += 1
    
    return result

if __name__ == '__main__':
    dict = {}
    dict["collection"] = "Business"
    dict["key"] = "city"
    dict["value"] = "Pittsburgh"
    input_dict = {"collection": "Business", "key": "business_id", "value": "aYE3ARaHRuk5GQ4K5FNqvw"}
    askMongo(input_dict)