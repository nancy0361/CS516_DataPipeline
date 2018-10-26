import json

def writeJson(data, path = '../output/test.json'):

    if path != '../output/test.json':
        path = '../output/' + path + '.json'

    with open(path, 'w') as outfile:
        json.dump(data, outfile)