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

    myClient = pymongo.MongoClient("mongodb://localhost:27017/")
    tableName = ["Business", "User", "Review"]
    
    removeDatabase("testDB",myClient)
    mydb = createDatabase("testDB", myClient)
    
    for table in tableName:
        if table not in requirement:
            continue
        
        mycol = createCollection(table, myClient, mydb)
        dataset = json.loads(open("../input/" + table + "_example.json").read())
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