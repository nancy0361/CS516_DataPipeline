import sys
sys.path.append('..')

from flask import Flask, render_template, request, send_file
from flask_uploads import UploadSet, configure_uploads
from dataMining.mongoQuery import askMongo
from dataMining.databaseInit import initializeDatabase
import json
from database import *
from io import BytesIO
import findspark
findspark.init()
import pickle
import os
import random
from pyspark import SparkContext, SQLContext
from pyspark.ml.recommendation import ALSModel
from pyspark.sql import Row
from pyspark.sql.functions import *

app = Flask(__name__)
app.config.from_pyfile('config.py')
sc = SparkContext(appName="Yelp")
sc.setLogLevel("ERROR")

sqlc = SQLContext(sc)

with open('../data/Charlotte_Restaurants_review.pickle', 'rb') as f:
    all_visited = pickle.load(f)
    
with open('../data/Charlotte_Restaurants_business.pickle', 'rb') as f:
    rest = pickle.load(f)

with open('../data/Charlotte_Restaurants_user.pickle', 'rb') as f:
    user = pickle.load(f)
    
n_business = len(rest)
    
best_model = ALSModel.load('../data/Charlotte_als_model')

@app.route("/random_user", methods=["GET"])
def random_user():
    return random.choice(list(user.keys()))

@app.route("/recommend/<user_id>/<n>", methods=["POST"])
def make_pred(user_id, n=5):
    print("n " + str(n))
    print("user_id " + str(user_id))
    user_idn = user[user_id]
    visited = all_visited[user_idn]
    test_user = sqlc.createDataFrame([Row(user_idn=user_idn, business_idn=float(i)) for i in list(set(range(n_business)).difference(set(visited)))])

    pred_test = best_model.transform(test_user).na.fill(-5.0)
    top_pred = pred_test.orderBy(desc('prediction')).select('business_idn').rdd.map(lambda row: row.business_idn).take(int(n))
    response = list(map(lambda idn: rest[idn], top_pred))
    # response_visited = list(sorted(map(lambda idn: rest[idn], visited), key=lambda k: k['stars'], reverse=True)[:int(n)])
    response_visited = list(sorted(map(lambda idn: rest[idn], visited), key=lambda k: k['stars'], reverse=True))
    print(response_visited)
    return json.dumps({0:response, 1:response_visited})

# @app.route("/list", methods=["GET"])
# def list_ratings():
#     user_id = request.args.get('user')
#     try:
#         n = int(request.args.get('n'))
#     except (ValueError, TypeError):
#         n = 5

#     user_idn = user[user_id]
#     visited = all_visited[user_idn]
#     response = sorted(map(lambda idn: rest[idn], visited), key=lambda k: k['stars'], reverse=True)[:int(n)]
#     return json.dumps(response)

# @app.route('/hello')
# def open_homepage():
#     return render_template('hello.html')

@app.route('/homepage')
def open_homepage():
    return render_template('homepage.html')

@app.route('/background')
def open_background():
    return render_template('background.html')

@app.route('/acknowledge')
def open_acknowledge():
    return render_template('acknowledge.html')

@app.route('/status')
def open_status():
    return render_template('status.html')

dataset = UploadSet(name='dataset', extensions='json')
configure_uploads(app, dataset)
@app.route("/upload", methods=['Post', 'Get'])
def upload():
    if request.method == 'POST' and 'dataset' in request.files:
        file = request.files['dataset']
        dataset.save(file)
        return render_template('upload.html', temp="Upload Successfully")
    return render_template('upload.html')

# @app.route('/bubble-chart')
# def bubble_chart():
#     test = [
#         {"text": "Java", "count": "236"},
#         {"text": ".Net", "count": "382"},
#         {"text": "Php", "count": "170"},
#         {"text": "Ruby", "count": "123"},
#         {"text": "D", "count": "12"},
#         {"text": "Python", "count": "170"},
#         {"text": "C/C++", "count": "382"},
#         {"text": "Pascal", "count": "10"},
#         {"text": "Something", "count": "170"},
#       ]
#     return render_template('bubble_chart.html', temp=json.dumps(test))

@app.route('/analysis')
def open_analysis_page():
    return render_template('analysis.html')


@app.route("/input", methods=['POST'])
def receiveInput():
    if request.json:
        askMongo()
        data = request.get_json()
        return "Thanks. Your age is %s\n" % data['age']

    else:
        return "no json received\n"





@app.route("/requirement", methods=['Post'])
def getRequirement():
    # print("enter get requirement")
    data = request.data
    print(data)
    data = data.decode('utf-8')
    requirement = json.loads(data)
    print(requirement)
    info = initializeDatabase(requirement)
    print(info)
    # return render_template('upload.html', temp=info)
    return json.dumps(info)

@app.route("/ratings/image/<business_id>", methods=['Get'])
def show_ratings(business_id):
    print("enter ratings 1")
    (dates, rates) = get_ratings_by_season(business_id)
    plt.xticks(range(0,len(dates)),dates,rotation = 70,fontsize = 8)
    plt.plot(range(0,len(dates)),rates,linewidth=3.0,color = 'blue')
    plt.title(' Average Review Rating By Season', fontsize = 20)
    img = BytesIO()
    plt.savefig(img)
    plt.clf()
    img.seek(0)
    return send_file(img, mimetype='image/png')

# @app.route("/ratings")
# def ratings_page():
#     return render_template("ratings.html", title="ratings")

# @app.route("/distribution", methods=['Get'])
# def distribution_page():
#     return render_template("distribution.html", title="distribution of stars")

@app.route("/distribution/image/<state>", methods=['Get'])
def show_distribution(state):
    print("enter show distribution1")
    res = get_distribution(state)
    names='Rating: 1.0', 'Rating: 2.0', 'Rating: 3.0', 'Rating: 4.0','Rating: 5.0'
    pie = plt.pie(res,autopct='%1.1f%%',explode=(0, 0, 0, 0, 0.08), colors=['red','orange','grey','skyblue','pink'])
    plt.title('Star distribution in ' + state,fontsize = 10)
    plt.axis('equal')
    plt.legend(pie[0], labels = names,fontsize = 12)
    print("enter show distribution2")
    img = BytesIO()
    plt.savefig(img)
    plt.clf()
    img.seek(0)
    return send_file(img, mimetype='image/png')

@app.route("/wordcloud/image/<business_id>", methods=['Get'])
def show_wordcloud(business_id):
    wordcloud = get_wordcloud(business_id)
    plt.imshow(wordcloud)
    plt.axis('off')  # remove axis
    img = BytesIO()
    plt.savefig(img)
    img.seek(0)
    return send_file(img, mimetype='text/plain')

# @app.route("/wordcloud", methods=['Get'])
# def wordcloud_page():
#     return render_template('wordcloud.html', title="wordcloud")

@app.route('/recommendation')
def open_recommendation():
    return render_template('recommendation.html')

@app.route("/simple_query/<collection>/<key>/<value>", methods=["POST"])
def get_simple_query(collection, key, value):
    print(collection)
    print(key)
    print(value)
    input_dict = {"collection": collection, "key": key, "value": value}
    output = askMongo(input_dict)
    print(output)
    return json.dumps(output)


if __name__ == '__main__':
    # app.run(host='0.0.0.0')
    app.run()
