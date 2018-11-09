import sys
sys.path.append('..')

from flask import Flask, render_template, request
from dataMining.mongoQuery import askMongo
import json

app = Flask(__name__)

@app.route('/homepage')
def open_homepage():
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


@app.route("/input", methods=['POST'])
def receiveInput():
    if request.json:
        askMongo()
        data = request.get_json()
        return "Thanks. Your age is %s\n" % data['age']

    else:
        return "no json received\n"


if __name__ == '__main__':
    app.run(debug=True)