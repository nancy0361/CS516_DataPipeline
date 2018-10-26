import sys
sys.path.append('..')

from flask import Flask, render_template, request
from dataMining.mongoQuery import askMongo


app = Flask(__name__)
app.config.from_pyfile('config.py')

@app.route('/homepage')
def hello_world():
    return render_template('homepage.html')

@app.route('/bubble-chart')
def bubble_chart():
    return render_template('bubble_chart.html')


@app.route("/input", methods=['POST'])
def receiveInput():
    app.logger.debug("receiving Json...")
    if request.json:
        askMongo()
        data = request.get_json()
        return "Thanks. Your age is %s\n" % data['age']

    else:
        return "no json received\n"


if __name__ == '__main__':
    app.run()