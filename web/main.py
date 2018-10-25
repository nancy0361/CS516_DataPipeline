from flask import Flask, render_template, request
app = Flask(__name__)

@app.route('/test')
def hello_world():
    # return 'Hello, World!'

    return render_template('hello.html')


@app.route("/input", methods=['POST'])
def receiveInput():

    if request.json:
        data = request.get_json()
        return "Thanks. Your age is %s\n" % data['age']

    else:
        return "no json received\n"


@app.route("/getOutput", methods=['GET','POST'])
def sendOutput():

