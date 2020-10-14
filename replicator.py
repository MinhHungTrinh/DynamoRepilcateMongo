from __future__ import print_function

import pymongo
import json
import boto3
import os
import time
import uuid
import re
from datetime import datetime
from decimal import Decimal

from dynamodb_json import json_util as json

def lambda_handler(event, context):

    # read env variables for mongodb connection
    urlDb = os.environ['mongodburl']
    database = os.environ['database']
    rootEventSourceARN = os.environ['eventSourceARN']
    regex = rootEventSourceARN + ":table\/(.+)\/stream.*"

    # configure pymongo connection
    myclient = pymongo.MongoClient(urlDb)
    mydb = myclient[database]

    count = 0

    with myclient.start_session() as session:

        for record in event['Records']:

            ddb = record['dynamodb']
            eventSourceARN = record['eventSourceARN']
            keys = re.findall(regex, eventSourceARN)
            if (len(keys) > 0):
                table = keys[0]
                mycol = mydb[table]
            else:
                session.end_session()
                myclient.close()
                print('eventSourceARN is fail!')
                return {
                    'statusCode': 500,
                    'body': eventSourceARN
                }

            if (record['eventName'] == 'INSERT' or record['eventName'] == 'MODIFY'):

                newimage = ddb['NewImage']
                newimage_conv = json.loads(newimage)

                # create the explicit _id
                newimage_conv['_id'] = newimage_conv['id']

                ### custom conversions ###

                # add a field if it not exists
#                 if "age" not in newimage_conv:
#                     newimage_conv['age'] = None
                # convert epoch time to ISODate
#                 newimage_conv['created_at'] = datetime.utcfromtimestamp(newimage_conv['created_at'])

                try:
                    mycol.update_one({"_id":newimage_conv['_id']}, { "$set" : newimage_conv}, upsert=True, session=session)
                    count = count + 1

                except Exception as e:
                    print("ERROR update _id=",newimage_conv['_id']," ",type(e),e)

            elif (record['eventName'] == 'REMOVE'):

                oldimage = ddb['OldImage']
                oldimage_conv = json.loads(oldimage)

                try:
                    mycol.delete_one({"_id":oldimage_conv['id']}, session=session)
                    count = count + 1

                except Exception as e:
                    print("ERROR delete _id",oldimage_conv['id']," ",type(e),e)

    session.end_session()

    myclient.close()

    # return response code to Lambda and log on CloudWatch
    if count == len(event['Records']):
        print('Successfully processed %s records.' % str(len(event['Records'])))
        return {
            'statusCode': 200,
            'body': json.dumps('OK')
        }
    else:
        print('Processed only ',str(count),' records on %s' % str(len(event['Records'])))
        return {
            'statusCode': 500,
            'body': json.dumps('ERROR')
        }
