import json
import boto3
from botocore.exceptions import ClientError
from datetime import datetime
s3_client = boto3.client("s3")
db = boto3.resource('dynamodb')
table = db.Table("yelp-restaurants")

def lambda_handler(event, context):
    # Id is the primary/paritition key
    csvfile = s3_client.get_object(Bucket="cloud-computing-assignment1", Key="restaurants.json")
    data = csvfile['Body'].read().decode('utf-8')
    json_data = json.loads(data)
    insert_data(json_data)
    # lookup_data({'Id': 'XsXLVWr1UZWVhKThNvNiaA'})

    return


def insert_data(data_list):
    # overwrite if the same index is provided
    for data in data_list:
        data['insertedTimestamp'] = str(datetime.utcnow())
        response = table.put_item(Item=data)
    return response


def lookup_data(key, db=None, table='yelp-restaurants'):
    if not db:
        db = boto3.resource('dynamodb')
    table = db.Table(table)
    try:
        response = table.get_item(Key=key)
    except ClientError as e:
        print('Error', e.response['Error']['Message'])
    else:
        print(response['Item'])
        return response['Item']
