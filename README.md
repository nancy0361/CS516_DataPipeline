# dataPipeline



This dataPipeline is a course work project for CS516 course at Duke University.

## Dataset
The example dataset is Yelp Dataset from https://www.yelp.com/dataset/challenge

## Environment Set Up

### Python

Please download the following packages:
* pymongo
* bson

### MongoDB

To download and install MongoDB, follow the instruction here:
https://www.mongodb.com/download-center/community

After install the MongoDB, run it locally on your computer by running the following
command under <path to MongoDB>/bin/

`./mongod --dbpath <path to data directory>`

The following line shows it is running successfully:

`[initandlisten] waiting for connections on port 27017`

Then open a new terminal window, run the mongo-shell under <path to MongoDB>/bin/ using:

`mongo --host 127.0.0.1:27017`

Create the database by:

`use yelp`

Import the data from JSON file by (the files can be found in yelp_dataset directory ):

`./mongoimport --db yelp -c photo --type json yelp_academic_dataset_photo.json`

### Flask

The web server is developed use the Flask framework. In order to run and test the server on your local machine 
download and install Flask following this link: http://flask.pocoo.org/

## Run the Server

To open the debug mode, go to the config.py and set "DEBUG" flag to true.

To run the server on your local machine, run this command under the path CS516_DataPipeline/web:

`python main.py`

You can use the url(http://127.0.0.1:5000/test) to visit the test "hello world" page, or try to upload the example 
json file using the command:

`curl -H POST http://127.0.0.1:5000/input -d @example.json --header "Content-Type: application/json`