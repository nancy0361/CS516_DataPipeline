"""
test for using python for mongoDB
10/22/2018
"""

from pymongo import MongoClient
from bson import Code
import matplotlib.pyplot as plt

conn = MongoClient('127.0.0.1', 27017)
db = conn.yelp
business = db.business

# business.aggregate(
#     {"$group":{"_id":"$state", "sum":{"$sum":1}}}
# )

key = {'state': 1}
condition = {}
initial = {'count': 0}
reducer = Code("""function(obj, prev) { prev.count++; }""")
list_count = business.group(key, condition, initial, reducer)

print(list_count)
answer = {}
states = ['AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY']

for dic in list_count:
    if dic['count'] > 100:
        answer[dic['state']] = dic['count']

print(answer)


state = list(answer.keys())
counts = list(answer.values())
plt.bar(state, counts)
plt.show()
