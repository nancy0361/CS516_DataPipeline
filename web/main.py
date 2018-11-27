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

@app.route("/recommend", methods=["GET"])
def make_pred():
    user_id = request.args.get('user')
    try:
        n = int(request.args.get('n'))
    except (ValueError, TypeError):
        n = 5

    user_idn = user[user_id]
    visited = all_visited[user_idn]
    test_user = sqlc.createDataFrame([Row(user_idn=user_idn, business_idn=float(i)) for i in list(set(range(n_business)).difference(set(visited)))])

    pred_test = best_model.transform(test_user).na.fill(-5.0)
    top_pred = pred_test.orderBy(desc('prediction')).select('business_idn').rdd.map(lambda row: row.business_idn).take(n)
    response = map(lambda idn: rest[idn], top_pred)
    return render_template("recommend.html", restaurants = list(response))
    # return json.dumps(list(response))

@app.route("/list", methods=["GET"])
def list_ratings():
    user_id = request.args.get('user')
    try:
        n = int(request.args.get('n'))
    except (ValueError, TypeError):
        n = 5

    user_idn = user[user_id]
    visited = all_visited[user_idn]
    response = sorted(map(lambda idn: rest[idn], visited), key=lambda k: k['stars'], reverse=True)[:n]
    return json.dumps(response)

@app.route('/hello')
def open_homepage():
    return render_template('hello.html')

@app.route('/homepage')
def open_hello():
    return render_template('homepage.html')

@app.route('/Upload Database')
def open_upload_page():
    return render_template('upload.html')

@app.route('/bubble-chart')
def bubble_chart():
    test = [
        {"text": "Java", "count": "236"},
        {"text": ".Net", "count": "382"},
        {"text": "Php", "count": "170"},
        {"text": "Ruby", "count": "123"},
        {"text": "D", "count": "12"},
        {"text": "Python", "count": "170"},
        {"text": "C/C++", "count": "382"},
        {"text": "Pascal", "count": "10"},
        {"text": "Something", "count": "170"},
      ]
    return render_template('bubble_chart.html', temp=json.dumps(test))

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

dataset = UploadSet(name='dataset', extensions='json')
configure_uploads(app, dataset)

@app.route("/upload", methods=['Post', 'Get'])
def upload():
    if request.method == 'POST' and 'dataset' in request.files:
        file = request.files['dataset']
        dataset.save(file)
        return file.filename
    return render_template('uploadTest.html')

@app.route("/requirement", methods=['Post'])
def getRequirement():
    print("enter get requirement")
    data = request.data
    print("get data"+data)
    info = initializeDatabase(data)
    print(info)
    return render_template('upload.html')

@app.route("/ratings/image", methods=['Get'])
def show_ratings():
    (dates, rates) = get_ratings_by_season()
    plt.xticks(range(0,len(dates)),dates,rotation = 70,fontsize = 8)
    plt.plot(range(0,len(dates)),rates,linewidth=3.0,color = 'blue')
    plt.title(' Average Review Rating By Season', fontsize = 20)
    img = BytesIO()
    plt.savefig(img)
    plt.clf()
    img.seek(0)
    return send_file(img, mimetype='image/png')

@app.route("/ratings")
def ratings_page():
    return render_template("ratings.html", title="ratings")

@app.route("/distribution", methods=['Get'])
def distribution_page():
    return render_template("distribution.html", title="distribution of stars")

@app.route("/distribution/image", methods=['Get'])
def show_distribution():
    print("enter show distribution1")
    res = get_distribution()
    names='Rating: 1.0', 'Rating: 2.0', 'Rating: 3.0', 'Rating: 4.0','Rating: 5.0'
    pie = plt.pie(res,autopct='%1.1f%%',explode=(0, 0, 0, 0, 0.08), colors=['red','orange','grey','skyblue','pink'])
    plt.title('Fake Reivews Rating Distribution',fontsize = 10)
    plt.axis('equal')
    plt.legend(pie[0], labels = names,fontsize = 12)
    print("enter show distribution2")
    img = BytesIO()
    plt.savefig(img)
    plt.clf()
    img.seek(0)
    return send_file(img, mimetype='image/png')

@app.route("/wordcloud/image", methods=['Get'])
def show_wordcloud():
    wordcloud = get_wordcloud()
    plt.imshow(wordcloud)
    plt.axis('off')  # remove axis
    img = BytesIO()
    plt.savefig(img)
    img.seek(0)
    return send_file(img, mimetype='text/plain')

@app.route("/wordcloud", methods=['Get'])
def wordcloud_page():
    return render_template('wordcloud.html', title="wordcloud")


if __name__ == '__main__':
    app.run(host='0.0.0.0')
    # app.run()
