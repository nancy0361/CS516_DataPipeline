import matplotlib.pyplot as plt
import json
import string
from pymongo import MongoClient

conn = MongoClient('127.0.0.1', 27017)
db = conn.yelp
business = db.business
review = db.review

def get_ratings_by_season():

    # display rating of a specific business over time
    rating_by_month = review.aggregate([
    	{'$match':{'business_id':'_c3ixq9jYKxhLUB0czi0ug'}},
    	{'$group':{
    		'_id':{
    			'season': {'$ceil':{'$divide':[
    				{'$month': {'$dateFromString':{'dateString':'$date'}}},
    				4
    			]}}
    			, 
    			'year':{'$year': {'$dateFromString':{'dateString':'$date'}}}
    		},
    		'avg':{'$avg':'$stars'}
    	}},
    	{'$sort':{'_id.year':1, '_id.month':1}}
    ])

    # for dist in rating_by_season:
    # 	print(dist)
    dates, rates = [], []
    for r in rating_by_month:
    	dates.append(r['_id']['season'] + r['_id']['year'] * 1000)
    	rates.append(r['avg'])
    return (dates, rates)

# show distribution of stars in each state
def get_distribution():
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
    for row in list(filter(
    	lambda x:x['_id']['state'] == 'NC', state_star_distribution)):
    	distribution[row['_id']['stars']] = row['count']

    size=[]
    for i in range(1,6):
    	if i in distribution:
    		size.append(distribution[i])
    print(size)
    return size