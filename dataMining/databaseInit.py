import pymongo
import json

numberField = ["star", "latitude", "longtitude", "review_count", "average_star", "rstars"]

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
    collectionNames = mydb.collection_names()
    if collName in collectionNames:
        mydb[collName].drop()
    mycol = mydb[collName]
    print("Collection " + collName + " created.")
    return mycol

def checkStatus():
    print("checking database status...")
    conn = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = conn.testDB
    report = {}
    report['Database'] = mydb.command("dbstats")
    report['Collection'] = {}
    report['Collection']['collection_names'] = mydb.list_collection_names()
    for table in mydb.list_collection_names():
        stats = mydb.command("collstats", table)
        report['Collection'][table] = {}
        report['Collection'][table]['name'] = stats['ns']
        report['Collection'][table]['pk'] = '_id'
        report['Collection'][table]['documents'] = stats['count']
        report['Collection'][table]['size'] = stats['size']
    print(report)  
    return report

def initializeDatabase(requirement):
    print("init database....")
    myClient = pymongo.MongoClient("mongodb://localhost:27017/")

    # create database if not exists
    dbnames = myClient.list_database_names()
    if "testDB" in dbnames:
        mydb = myClient.testDB
    else:
        mydb = createDatabase("testDB", myClient)

    # extract input file name
    index = requirement["Filename"].rfind("\\") + 1
    filename = "../input/" + requirement["Filename"][index:]
    tableName = ["Business", "User", "Review"]

    for table in tableName:
        if table not in requirement:
            continue
        mycol = createCollection(table, myClient, mydb)
        dataset = json.loads(open(filename).read())
        doc = {}
        for data in dataset:
            for field, originalName in requirement[table].items():
                if originalName not in data:
                    if field in numberField:
                        data[originalName] = 0.0
                    if field == "name":
                        data[originalName] = "anonymous"
                if field in numberField:
                    doc[field] = float(data[originalName])
                doc[field] = data[originalName]
            mycol.insert_one(doc)
     
    return checkStatus()

if __name__ == '__main__':

    requirement = json.loads(open("../input/Require_example.json").read())
    initializeDatabase(requirement)