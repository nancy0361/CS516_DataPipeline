from flask import Flask, render_template, request

app = Flask(__name__)
app.config.from_pyfile('config.py')

@app.route('/test')
def hello_world():
    return render_template('html_test.html')


@app.route("/input", methods=['POST'])
def receiveInput():
    app.logger.debug("receiving Json...")
    if request.json:
        data = request.get_json()
        return "Thanks. Your age is %s\n" % data['age']

    else:
        return "no json received\n"


if __name__ == '__main__':
    app.run()