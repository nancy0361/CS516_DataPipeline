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
                doc[field] = data[originalName]
            mycol.insert_one(doc)

        # print("In collection " + table)
        # for one in mycol.find():
        #     print(one)

    # collect the basic information about the database
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
        
    return report

# if __name__ == '__main__':

#     myClient = pymongo.MongoClient("mongodb://localhost:27017/")
#     removeDatabase("testDB",myClient)
#     tableName = ["Business", "User", "Review"]

#     mydb = createDatabase("testDB", myClient)

#     requirement = json.loads(open("../input/Require_example.json").read())
    
#     for table in tableName:
#         if table not in requirement:
#             continue
        
#         mycol = createCollection(table, myClient, mydb)
#         dataset = json.loads(open("../input/" + table + "_example.json").read())
#         doc = {}
#         for data in dataset:
#             for field, originalName in requirement[table].items():
#                 doc[field] = data[originalName]
#             mycol.insert_one(doc)

#         print("In collection " + table)
#         for one in mycol.find():
#             print(one)

#     print('---------------------------------------------')
#     report = {}
#     report['Database'] = mydb.command("dbstats")
#     report['Collection'] = {}
#     report['Collection']['collection_names'] = mydb.list_collection_names()
#     for table in mydb.list_collection_names():
#         stats = mydb.command("collstats", table)
#         report['Collection'][table] = {}
#         report['Collection'][table]['name'] = stats['ns']
#         report['Collection'][table]['pk'] = '_id'
#         report['Collection'][table]['documents'] = stats['count']
#         report['Collection'][table]['size'] = stats['size']
        
#     print(report)
    # print(myClient.list_database_names())
    # print(mydb.list_collection_names())
    # removeDatabase("testDB",myClient)