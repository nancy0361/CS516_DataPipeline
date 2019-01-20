from pymongo import MongoClient

def checkStatus():
    report = {}
    conn = MongoClient('127.0.0.1', 27017)
    
    dbNames = conn.list_database_names()
    if 'testDB' not in dbNames:
        report['error'] = "Oops! The database haven't been created."        
    
    mydb = conn.testDB
    databaseStatus = mydb.command("dbstats")
    report['Database'] = {}
    report['Database']['Database Name'] =  databaseStatus['db']
    report['Database']['Number of Tables'] = databaseStatus['collections']
    report['Database']['Number of Views'] = databaseStatus['views']
    report['Database']['Total Number of Records'] = databaseStatus['objects']
    report['Database']['Used Disk Size'] = str(format(databaseStatus['fsUsedSize'] / 1000000000, '.2f')) + ' GB'
    report['Database']['Total Disk SIze'] = str(format(databaseStatus['fsTotalSize'] / 1000000000, '.2f')) + ' GB'
    report['Collection'] = {}
    report['Collection']['collection_names'] = mydb.list_collection_names()
    for table in mydb.list_collection_names():
        stats = mydb.command("collstats", table)
        report['Collection'][table] = {}
        report['Collection'][table]['Name'] = stats['ns']
        report['Collection'][table]['Primary Key'] = '_id'
        report['Collection'][table]['Records Number'] = stats['count']
        report['Collection'][table]['Table Size'] = str(stats['size']) + ' Bytes'
    return report