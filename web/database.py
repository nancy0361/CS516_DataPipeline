import matplotlib.pyplot as plt
import json
import string
from pymongo import MongoClient
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from wordcloud import WordCloud, STOPWORDS
import re
from collections import Counter

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

def get_wordcloud():
    # word cloud of one business review
    stopwords = set(STOPWORDS)
    # reviews = review.aggregate([
    #   {'$match':{'business_id':'_c3ixq9jYKxhLUB0czi0ug'}},
    #   {'$project':{'text':1, '_id':0}}
    # ])
    # review_list = list(reviews)
    review_list = [{'text':"So this is what it is like at the Works for me--- can\'t you see why so much love? "}, 
                   {'text':"The fries were good."}, {'text':"The chicken strip are disappointing though, get a burger."}]
    for i in range(len(review_list)):
        line = review_list[i]['text']
        line = line.lower() # lower case
        translation = str.maketrans("","", string.punctuation)
        line = line.translate(translation)
        split = word_tokenize(line)
         # filter out any tokens not containing letters (e.g., numeric tokens, raw punctuation)
        filtered = []
        for token in split:
            if re.search('[a-zA-Z]', token):    
                filtered.append(token)
        word = [i for i in filtered if i not in stopwords]
        # d = [stemmer.stem(word) for word in word] 
        # d = [wordnet_lemmatizer.lemmatize(word) for word in d]
        review_list[i] = word
    words = sum(review_list, [])
    counts = Counter(words)
    # print(counts)
    wordcloud = WordCloud(    
                          background_color='white',
                          max_words=100,
                          max_font_size=50,
                          min_font_size=10,
                          random_state=40,
                         ).fit_words(counts)
    return wordcloud