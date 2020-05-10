from requests.exceptions import HTTPError
from pymongo import MongoClient
from datetime import datetime
from environs import Env
import pymongo.errors
import requests
import json


requestList = [
    {
        'name': "Forex",
        'url': "https://fcsapi.com/api-v2/forex/latest?symbol=EUR/USD,USD/TRY,EUR/TRY,CHF/TRY,GBP/TRY",
    },
    {
        'name': "Crypto",
        'url': "https://fcsapi.com/api-v2/crypto/latest?id=78,79,80,81,82,83,,84,85,86,2782,2484,3497,3738,4523,3826,4320,4823,5191,5192,6614,6615,6745,6746",
    },
    {
        'name': "Stock",
        'url': "https://fcsapi.com/api-v2/stock/latest?id=15,38,112,4910,120090",
    }
]

client = MongoClient('mongodb://localhost:27017')
db = client['finance-tracker']
rates = db.rates

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


if __name__ == '__main__':

    env = Env()
    env.read_env(".env", recurse=False)
    API_CREDENTIAL = env("API_CREDENTIAL")

    for reqItem in requestList:

        print("[Request]: " + reqItem['name'])
        response = requestRates(reqItem, API_CREDENTIAL)

        print("[Response]: " + json.dumps(response))

        if response != "Error":

            responseLength = len(response)
            for item in response:

                if reqItem['name'] == "Stock":

                    item['datetime'] = datetime.strptime(item['dateTime'],'%Y-%m-%d %H:%M:%S')
                    del item['dateTime']
                else:
                    item['datetime'] = datetime.strptime(item['last_changed'],'%Y-%m-%d %H:%M:%S')
                    del item['last_changed']
                item['type'] = reqItem['name']
                item["_id"] = item['symbol'] + "-" + str(item["datetime"])

            try:
                new_result = rates.insert_many(response, ordered=False)
                print('[MongoDB] - Multiple posts: {0}'.format(new_result.inserted_ids))
            except pymongo.errors.BulkWriteError as error:
                print('[MongoDB] - Inserted {0} documents. Number of erroneous docs {1}'.format(responseLength - len(error.details['writeErrors']),len(error.details['writeErrors'])))

                # for err in error.details['writeErrors']:
                #     if int(err['code']) == 11000:
                #         pass
                #     else:
                #         print(err['errmsg'])
