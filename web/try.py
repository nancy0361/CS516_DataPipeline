import matplotlib.pyplot as plt
import json
import string
from pymongo import MongoClient

conn = MongoClient('127.0.0.1', 27017)
db = conn.yelp
business = db.business
review = db.review
state_star_distribution = business.aggregate([
        {'$group':{
                '_id':{'state':'$state',
                	   'stars': {'$floor':'$stars'}
                	  },
                'count':{'$sum':1}
            }},
        # {'$project', {'_id.state':1, '_id.stars':1, 'count':1}}
    ])
distribution = {}

for row in list(filter(lambda x:x['_id']['state'] == 'NC', state_star_distribution)):
        distribution[row['_id']['stars']] = row['count']
size=[]
for i in range(1,6):
    if i in distribution:
        size.append(distribution[i])
print(size)