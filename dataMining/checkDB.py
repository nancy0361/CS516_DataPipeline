from pymongo import MongoClient

def checkStatus():
    conn = MongoClient('127.0.0.1', 27017)
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
    return report