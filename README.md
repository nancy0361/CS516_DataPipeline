# dataPipeline



This dataPipeline is a course work project for CS516 course at Duke University.

##Dataset
The example dataset is Yelp Dataset from https://www.yelp.com/dataset/challenge

## Environment Set Up

###Python

###MongoDB

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