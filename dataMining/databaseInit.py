import pymongo
import json

def createDatabase(dbName, myClient):

    dblist = myClient.list_database_names()

    if dbName in dblist:
        print("The database exists.")
    else:
        mydb = myClient[dbName]
        print("Database " + dbName + " created.")
    
    return mydb


def removeDatabase(dbName, myClient):
    myClient.drop_database(dbName)
    print("Database " + dbName + " droped.")


def createCollection(collName, myClient, mydb):
    mycol = mydb[collName]
    print("Collection " + collName + " created.")
    return mycol


if __name__ == '__main__':

    myClient = pymongo.MongoClient("mongodb://localhost:27017/")
    removeDatabase("testDB",myClient)
    tableName = ["Business", "User", "Review"]

    mydb = createDatabase("testDB", myClient)

    requirement = json.loads(open("../input/Require_example.json").read())
    
    for table in tableName:
        if table not in requirement:
            continue
        
        mycol = createCollection(table, myClient, mydb)
        dataset = json.loads(open("../input/" + table + "_example.json").read())
        doc = {}
        for data in dataset:
            for field, originalName in requirement[table].items():
                doc[field] = data[originalName]
            mycol.insert_one(doc)

        print("In collection " + table)
        for one in mycol.find():
            print(one)

    print(myClient.list_database_names())
    print(mydb.list_collection_names())
    removeDatabase("testDB",myClient)