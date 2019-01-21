from pyspark.sql.functions import *
from pyspark.sql import Row
from pyspark.ml.recommendation import ALSModel
from pyspark import SparkContext, SQLContext
import random
import os
import pickle
import findspark
from io import BytesIO
from database import *
import json
import sys
sys.path.append('..')
from dataMining.checkDB import checkStatus
from dataMining.databaseInit import initializeDatabase
from dataMining.mongoQuery import askMongo
from flask_uploads import UploadSet, configure_uploads
from flask import Flask, render_template, request, send_file



findspark.init()

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

    conn = MongoClient('127.0.0.1', 27017)
    db = conn.yelp
    collection = db.user
    result = collection.find({"user_id" : str(user_id)})
    for doc in result:
        name = doc['name']

    user_idn = user[user_id]
    visited = all_visited[user_idn]
    test_user = sqlc.createDataFrame([Row(user_idn=user_idn, business_idn=float(
        i)) for i in list(set(range(n_business)).difference(set(visited)))])

    pred_test = best_model.transform(test_user).na.fill(-5.0)
    top_pred = pred_test.orderBy(desc('prediction')).select(
        'business_idn').rdd.map(lambda row: row.business_idn).take(int(n))
    # response = list(map(lambda idn: rest[idn], top_pred))
    response = list(sorted(map(lambda idn: rest[idn], top_pred), key=lambda k: k['stars'], reverse=True))
    response_visited = list(sorted(
        map(lambda idn: rest[idn], visited), key=lambda k: k['stars'], reverse=True))
    print(response_visited)
    return json.dumps({0: response, 1: response_visited, 2: name})

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




@app.route('/homepage')
def open_homepage():
    return render_template('homepage.html')


@app.route('/background')
def open_background():
    return render_template('background.html')


@app.route('/acknowledge')
def open_acknowledge():
    return render_template('acknowledge.html')


@app.route('/status', methods=["Get"])
def open_status():
    return render_template('status.html')

@app.route('/status_content', methods=["Post"])
def show_status():
    report = checkStatus()
    print(report)
    return json.dumps(report)


dataset = UploadSet(name='dataset', extensions='json')
configure_uploads(app, dataset)


@app.route("/upload", methods=['Post', 'Get'])
def upload():
    if request.method == 'POST' and 'dataset' in request.files:
        file = request.files['dataset']
        dataset.save(file)
        return render_template('upload.html', temp="Upload Successfully")
    return render_template('upload.html')


@app.route('/analysis')
def open_analysis_page():
    return render_template('analyze_direct.html')


@app.route('/customer_query')
def open_customer_page():
    return render_template('customer.html')


@app.route('/business_query')
def open_business_page():
    return render_template('business.html')


@app.route("/requirement", methods=['Post'])
def getRequirement():
    print("enter get requirement")
    data = request.data
    print(data)
    data = data.decode('utf-8')
    requirement = json.loads(data)
    print(requirement)
    info = initializeDatabase(requirement)
    print(info)
    return json.dumps(info)


@app.route("/ratings/image/<business_id>", methods=['Get'])
def show_ratings(business_id):
    print("enter ratings 1")
    (dates, rates) = get_ratings_by_season(business_id)
    plt.xticks(range(0, len(dates)), dates, rotation=70, fontsize=8)
    plt.plot(range(0, len(dates)), rates, linewidth=3.0, color='blue')
    plt.title(' Average Review Rating By Season', fontsize=20)
    img = BytesIO()
    plt.savefig(img)
    plt.clf()
    img.seek(0)
    return send_file(img, mimetype='image/png')


@app.route("/distribution/image/<state>", methods=['Get'])
def show_distribution(state):
    print("enter show distribution1")
    res = get_distribution(state)
    names = 'Rating: 1.0', 'Rating: 2.0', 'Rating: 3.0', 'Rating: 4.0', 'Rating: 5.0'
    pie = plt.pie(res, autopct='%1.1f%%', explode=(0, 0, 0, 0, 0.08), colors=[
                  'red', 'orange', 'grey', 'skyblue', 'pink'])
    plt.title('Star distribution in ' + state, fontsize=10)
    plt.axis('equal')
    plt.legend(pie[0], labels=names, fontsize=12)
    print("enter show distribution2")
    img = BytesIO()
    plt.savefig(img)
    plt.clf()
    img.seek(0)
    return send_file(img, mimetype='image/png')


@app.route("/wordcloud/image/<business_id>", methods=['Get'])
def show_wordcloud(business_id):
    print(business_id)
    wordcloud = get_wordcloud(business_id)
    plt.imshow(wordcloud)
    plt.axis('off')  # remove axis
    img = BytesIO()
    plt.savefig(img)
    img.seek(0)
    return send_file(img, mimetype='text/plain')


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


@app.route("/search_user/<user_id>/<user_name>", methods=["POST"])
def search_user(user_id, user_name):
    print("Search_user--User_id: " + user_id)
    print("Search_user--User_name: " + user_name)
    key = []
    value = []
    if user_id != '_':
        key.append('_id')
        value.append(user_id)
    if user_name != '_':
        key.append('name')
        value.append(user_name)
    input_dict = {'collection': 'User', "key": key, "value": value}
    output = askMongo(input_dict)
    print(output)
    return json.dumps(output)


@app.route("/search_business/<business_name>/<state>/<review_count>/<star_small>/<star_big>", methods=["POST"])
def search_business(business_name, state, review_count, star_small, star_big):
    print("Search_business--Business_name: " + business_name)
    print("Search_business--State: " + state)
    print("Search_business--Review_count: " + review_count)
    print("Search_business--Star_small: " + star_small)
    print("Search_business--Star_big: " + star_big)
    key = []
    value = []
    if business_name != '_':
        key.append('name')
        value.append(business_name)
    if state != '_':
        key.append('state')
        value.append(state)
    if review_count != '_':
        key.append('review_count')
        value.append(review_count)
    print(star_small)
    print(star_big)
    star_max = star_big
    star_min = star_small
    # star_max = max(star_small, star_big)
    # star_min = min(star_small, star_big)
    # star_min = star_min if star_min != '' else star_max
    if star_max != '_':
        key.append('star')
        value.append([star_min, star_max])
    input_dict = {'collection': 'Business', "key": key, "value": value}
    output = askMongo(input_dict)
    print(output)
    return json.dumps(output)

@app.route("/check_business_info/<business_id>/<business_name>", methods=["POST"])
def check_business_info(business_id, business_name):
    print("Check_business_info--Business_id: " + business_id)
    print("Check_business_info--Business_name: " + business_name)
    key = []
    value = []
    if business_id != '_':
        key.append('_id')
        value.append(business_id)
    if business_name != '_':
        key.append('name')
        value.append(business_name)
    input_dict = {'collection': 'Business', "key": key, "value": value}
    output = askMongo(input_dict)
    print(output)
    return json.dumps(output)
    


if __name__ == '__main__':
    # app.run(host='0.0.0.0')
    app.run()
