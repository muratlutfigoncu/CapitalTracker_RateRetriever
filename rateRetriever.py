from requests.exceptions import HTTPError
from pymongo import MongoClient
from datetime import datetime
from environs import Env
import pymongo.errors
import requests
import json
import time


requestList = [
    {
        'name': "Forex",
        'url': "https://fcsapi.com/api-v2/forex/latest?symbol=EUR/USD,USD/TRY,EUR/TRY,CHF/TRY,GBP/TRY",
    },
    {
        'name': "Crypto",
        'url': "https://fcsapi.com/api-v2/crypto/latest?id=78,79,80,81,82,83,84,85,86,2782,2484,3497,3738,4523,3826,4320,4823,5191,5192,6614,6615,6745,6746",
    },
    {
        'name': "Stock",
        'url': "https://fcsapi.com/api-v2/stock/latest?id=15,38,112,4910,12009",
    }
]

symbolList = [
    {
        "type": "Forex",
        "list": ['EUR/TRY', 'USD/TRY', 'CHF/TRY', "GBP/TRY",'EUR/USD', 'CHF/USD', "GBP/USD"],
        'url':"https://fcsapi.com/api-v2/forex/history?symbol=",
        'queryParam': "&period=1d&from=2019-11-01&to=2020-05-09"
    },
    {
        "type": "Stock",
        "list": ["15","38","112","4910","120090"],
        'url':"https://fcsapi.com/api-v2/stock/history?id=",
        'queryParam': "&period=1d&from=2019-11-01&to=2020-05-09"
    },
    {
        "type": "Crypto",
        "list": ['78','79','80','81','82','83','84','85','86','2782','2484','3497',
                 '3738','4523','3826','4320','4823','5191','5192','6614','6615','6745','6746'],
        'url':"https://fcsapi.com/api-v2/crypto/history?id=",
        'queryParam': "&period=1d&from=2019-11-01&to=2020-05-09"
    }

]

client = MongoClient('mongodb://localhost:27017')
db = client['capital-tracker']
rates = db.rates

def mongoBulkImport(response, rates):

    try:
        new_result = rates.insert_many(response, ordered=False)
        print('[MongoDB] - Multiple posts: {0}'.format(new_result.inserted_ids))
    except pymongo.errors.BulkWriteError as error:

        errLen = len(error.details['writeErrors'])
        print('[MongoDB] - Inserted {0} documents. Number of erroneous docs {1}'.format(len(response) - errLen ,errLen))
        # for err in error.details['writeErrors']:
        #     if int(err['code']) == 11000:
        #         pass
        #     else:
        #         print(err['errmsg'])


def fixDocument(itemList, reqItem):

    for item in itemList:

        if reqItem['name'] == "Stock":

            item['datetime'] = datetime.strptime(item['dateTime'],'%Y-%m-%d %H:%M:%S')
            del item['dateTime']
        else:
            item['datetime'] = datetime.strptime(item['last_changed'],'%Y-%m-%d %H:%M:%S')
            del item['last_changed']
        item['type'] = reqItem['name']
        item["_id"] = item['symbol'] + "-" + str(item["datetime"])

    return itemList


def requestRates(item, CREDENTIAL):

    try:

        url = item['url'] + "&access_key=" + CREDENTIAL
        response = requests.request("GET", url)

        if response.status_code != 200:
            print("Error on Request")
            return "Error"

        return response.json()['response']

    except HTTPError as Http_err:
        print(str(Http_err))
    except Exception as err:
        print(str(err))


def historicDataTransformer(dataList, dataInfo, item):

    for data in dataList:
        try:

            data['datetime'] = datetime.strptime(data['tm'],'%Y-%m-%d %H:%M:%S')
            data["_id"] = dataInfo['symbol'] + "-" + str(data["datetime"])
            data['type'] = item['type']
            data['symbol'] = dataInfo['symbol']
            data['price'] = data['c']
            data["id"] = dataInfo['id']
            data['high'] = data['h']
            data['low'] =  data['l']

            del data['tm']
            del data['o']
            del data['c']
            del data['v']
            del data['t']

            print(data)


        except Exception as err:
            print(str(err))

    return dataList


def historicDataHandler(item, CREDENTIAL, rates):

    try:

        for element in item['list']:
            url = item['url'] + element + item['queryParam'] +"&access_key=" + CREDENTIAL
            time.sleep(20)
            response = requests.request("GET", url)

            if response.status_code != 200:
                print("Error on Request")
                return "Error"

            data = historicDataTransformer(response.json()['response'], response.json()['info'],item)
            print(data)
            mongoBulkImport(data, rates)

        return "[Historic Data Handler]: Operation Complete."

    except HTTPError as Http_err:
        print(str(Http_err))
    except Exception as err:
        print(str(err))



if __name__ == '__main__':

    env = Env()
    env.read_env(".env", recurse=False)
    API_CREDENTIAL = env("API_CREDENTIAL")


    # Run this block to Import Current Rates
    # for reqItem in requestList:
    #
    #     print("[Request]: " + reqItem['name'])
    #     response = requestRates(reqItem, API_CREDENTIAL)
    #     print("[Response]: " + json.dumps(response))
    #
    #     if response != "Error":
    #
    #         responseLength = len(response)
    #         itemList = fixDocument(response, reqItem)
    #         mongoBulkImport(itemList, rates)


    # Run this block to retrieve and import historic rate data.

    for item in symbolList:

        try:

            historicDataHandler(item, API_CREDENTIAL, rates)

        except Exception as E:
            print(str(E))



