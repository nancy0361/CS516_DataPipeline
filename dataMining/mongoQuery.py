"""
test for using python for mongoDB
10/22/2018
"""

from pymongo import MongoClient
from bson import Code
import json
from dataMining.generateJson import writeJson


def askMongo():
    conn = MongoClient('127.0.0.1', 27017)
    db = conn.yelp
    countStates(db)


def countStates(db):
    business = db.business

    answer = {}
    states = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY',
              'LA',
              'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH',
              'OK',
              'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY']

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

    writeJson(answer, 'businessCount')




# if __name__ == '__main__':
#     askMongo()