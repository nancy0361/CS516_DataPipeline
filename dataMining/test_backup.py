
from pymongo import MongoClient
from bson import Code
import matplotlib.pyplot as plt
import json
import string
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from wordcloud import WordCloud, STOPWORDS
import re
from nltk.stem import SnowballStemmer
from nltk.stem import WordNetLemmatizer
from collections import Counter

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
# db.business.remove({'state': {'$nin':states}})

# business_state_count = business.aggregate([
#     {'$group': {
#         '_id': '$state',
#         'count': {'$sum': 1}
#     }
#     }
# ])

# show each state average stars
# state_avg_star = business.aggregate([
#     {'$group':{
#             '_id':'$state',
#             'avg':{'$avg':'$stars'}
#         }},
#     {'$project':{'state':1, 'avg':1}}
# ])

# for star in state_avg_star:
#     print(star)

# show distribution of stars in each state
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

names='Rating: 1.0', 'Rating: 2.0', 'Rating: 3.0', 'Rating: 4.0','Rating: 5.0'
size=[]
for i in range(1,6):
  if i in distribution:
    size.append(distribution[i])

pie = plt.pie(size,autopct='%1.1f%%',explode=(0, 0, 0, 0, 0.08), colors=['red','orange','grey','skyblue','pink'])
plt.title('Fake Reivews Rating Distribution',fontsize = 10)
plt.axis('equal')
plt.legend(pie[0], labels = names,fontsize = 12)

plt.show()



# display rating of a specific business over time
# rating_by_month = review.aggregate([
#   {'$match':{'business_id':'_c3ixq9jYKxhLUB0czi0ug'}},
#   {'$group':{
#     '_id':{
#       'season': {'$ceil':{'$divide':[
#         {'$month': {'$dateFromString':{'dateString':'$date'}}},
#         4
#       ]}}
#       , 
#       'year':{'$year': {'$dateFromString':{'dateString':'$date'}}}
#     },
#     'avg':{'$avg':'$stars'}
#   }},
#   {'$sort':{'_id.year':1, '_id.month':1}}
# ])

# for dist in rating_by_month:
#   print(dist)
# dates, rates = [], []
# for r in rating_by_month:
#   dates.append(r['_id']['season'] + r['_id']['year'] * 1000)
#   rates.append(r['avg'])
# for d in dates:
#   print(d)

# plt.xticks(range(0,len(dates)),dates,rotation = 70,fontsize = 8)
# plt.plot(range(0,len(dates)),rates,linewidth=3.0,color = 'blue')
# plt.title(' Average Review Rating By Season', fontsize = 20)
# plt.show()

# for dic in business_state_count:
#     if dic['count'] > 100 and dic['_id'] in states:
#         answer[dic['_id']] = dic['count']

# print(answer)

# state = list(answer.keys())
# counts = list(answer.values())
# plt.bar(state, counts, color='red')
# plt.show()

# word cloud of one business review
# stopwords = set(STOPWORDS)
# reviews = review.aggregate([
#   {'$match':{'business_id':'_c3ixq9jYKxhLUB0czi0ug'}},
#   {'$project':{'text':1, '_id':0}}
# ])
# review_list = list(reviews)

# review_list = [{'text':"So this is what it is like at the Works for me--- can\'t you see why so much love? "}, 
# {'text':"The fries were good."}, {'text':"The chicken strip are disappointing though, get a burger."}]
# for i in range(len(review_list)):
#     line = review_list[i]['text']
#     line = line.lower() # lower case
#     translation = str.maketrans("","", string.punctuation);
#     line = line.translate(translation)
#     split = word_tokenize(line)
#      # filter out any tokens not containing letters (e.g., numeric tokens, raw punctuation)
#     filtered = []
#     for token in split:
#         if re.search('[a-zA-Z]', token):    
#             filtered.append(token)
#     word = [i for i in filtered if i not in stopwords]
#     # d = [stemmer.stem(word) for word in word] 
#     # d = [wordnet_lemmatizer.lemmatize(word) for word in d]
#     review_list[i] = word
# # print(sum(review_list, []))
# words = sum(review_list, [])
# counts = Counter(words)
# print(counts)
# wordcloud = WordCloud(    
#                       background_color='white',
#                       max_words=100,
#                       max_font_size=50,
#                       min_font_size=10,
#                       random_state=40,
#                      ).fit_words(counts)

# # fig = plt.figure(1)
# plt.imshow(wordcloud)
# plt.axis('off')  # remove axis
# plt.show()

# join = review.aggregate([
#     # {'$limit': 100},
#     {'$lookup': {
#          'from': "business",
#          'localField': "business_id",
#          'foreignField': "business_id",
#          'as': "fromItems"
#       }
#     },
#     {'$match':{"fromItems":{"$ne":[]}}},
#     { '$replaceRoot': {'newRoot': {'$mergeObjects': [ { '$arrayElemAt': [ "$fromItems", 0 ] }, "$$ROOT" ] } }
#     },
#     { '$project': { 'business': 1 , 'state': 1, 'stars': 1} },
#     { '$group':{
#         '_id':'$state',
#         'avgAmount': {'$avg': '$stars'}
#         }
#     }
# ])

# data = []


# for doc in join:   
#     print(doc) 
    # data.append(doc)

# json_data = json.dumps(data)
# print(json_data)
test.py
当前显示test.py。