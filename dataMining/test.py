"""
test for using python for mongoDB
10/22/2018
"""

from pymongo import MongoClient
from bson import Code
import matplotlib.pyplot as plt
import json

conn = MongoClient('127.0.0.1', 27017)
db = conn.yelp
business = db.business
review = db.review

# business.aggregate(
#     {"$group":{"_id":"$state", "sum":{"$sum":1}}}
# )

# key = {'state': 1}
# condition = {}
# initial = {'count': 0}
# reducer = Code("""function(obj, prev) { prev.count++; }""")
# list_count = business.group(key, condition, initial, reducer)
#
# print(list_count)

answer = {}
states = ['AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY']

# for dic in list_count:
#     if dic['count'] > 100 and dic['state'] in states:
#         answer[dic['state']] = dic['count']
#
# print(answer)


business_state_count = business.aggregate([
    {'$group': {
        '_id': '$state',
        'count': {'$sum': 1}
    }
    }
])

for dic in business_state_count:
    if dic['count'] > 100 and dic['_id'] in states:
        answer[dic['_id']] = dic['count']

print(answer)

state = list(answer.keys())
counts = list(answer.values())
plt.bar(state, counts, color='red')
plt.show()



join = review.aggregate([
    {'$limit': 100},
    {'$lookup': {
         'from': "business",
         'localField': "business_id",
         'foreignField': "business_id",
         'as': "fromItems"
      }
    },
    { '$replaceRoot': {'newRoot': {'$mergeObjects': [ { '$arrayElemAt': [ "$fromItems", 0 ] }, "$$ROOT" ] } }
    },
    { '$project': { 'business': 1 , 'state': 1, 'stars': 1} },
    { '$group':{
        '_id':'$state',
        'avgAmount': {'$avg': '$stars'}
        }
    }
])

data = []
for doc in join:
    print(doc)
    data.append(doc)

json_data = json.dumps(data)
print(json_data)
