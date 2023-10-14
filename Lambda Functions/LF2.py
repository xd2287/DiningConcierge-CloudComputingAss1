import json
import os

import boto3
import random
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
from botocore.exceptions import ClientError

REGION = 'us-west-2'
HOST = 'search-restaurants-4r56cfjvgg64rfky22lf7qygu4.us-west-2.es.amazonaws.com'
INDEX = 'restaurants'
QueueUrl = 'https://sqs.us-west-2.amazonaws.com/261553161374/SearchRestaurantQueue'
ses_client = boto3.client('ses', region_name='us-west-2')
sqs_client = boto3.client('sqs')


def lambda_handler(event, context):
    print('Received event: ' + json.dumps(event))
    
    response = sqs_client.receive_message(QueueUrl=QueueUrl, MaxNumberOfMessages=10)
    print(f"response from sqs is {response}")
    if "Messages" in response:
        for message in response["Messages"]:
            if type(message) != type(None):
                if "Body" in message:
                    print(f"message from sqs is {message}")
                    receipt_handle = message['ReceiptHandle']
                    print(f"receipt_handle to delete is {receipt_handle}")
                    sqs_client.delete_message(QueueUrl=QueueUrl, ReceiptHandle=receipt_handle)
                    data = json.loads(message["Body"])
                    cuisine = data["Cuisine"]
                    toAddress = data["EmailAddress"]
                
                    results = query(cuisine)
                    
                    print(f"The query results of {cuisine} restaurant is {results}")
                    index = random.randint(0, len(results)-1)
                    restaurant = lookup_data({'Id': results[index]["Id"]})
                    print(f"The restaurant of index {index} is {restaurant}")
                    
                    message = f"Hi, \nHere is a restaurant suggestion according to your requirement.\nRestaurant name: {restaurant['Name']}\nCuisine: {restaurant['Cuisine']}\nAddress: {restaurant['Address']}\nRating: {restaurant['Rating']}"
        
                    response = ses_client.send_email(
                        Destination={
                            'ToAddresses': [toAddress]
                        },
                        Message={
                            'Body': {
                                'Text': {
                                    'Charset': 'UTF-8',
                                    'Data': message,
                                }
                            },
                            'Subject': {
                                'Charset': 'UTF-8',
                                'Data': 'Restaurant Suggestion',
                            },
                        },
                        Source='dxdxd2333@gmail.com'
                    )
                    
                    print("response to user is",response)
                
                    return {
                        'statusCode': 200,
                        'body': json.dumps("Email Sent Successfully. MessageId is: " + response['MessageId'])
                    }
            else:
                return
    else:
        return


def query(term):
    q = {'size': 5, 'query': {'multi_match': {'query': term}}}

    client = OpenSearch(hosts=[{
        'host': HOST,
        'port': 443
    }],
                        http_auth=get_awsauth(REGION, 'es'),
                        use_ssl=True,
                        verify_certs=True,
                        connection_class=RequestsHttpConnection)

    res = client.search(index=INDEX, body=q)
    print(res)

    hits = res['hits']['hits']
    results = []
    for hit in hits:
        results.append(hit['_source'])

    return results
    
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


def get_awsauth(region, service):
    cred = boto3.Session().get_credentials()
    return AWS4Auth(cred.access_key,
                    cred.secret_key,
                    region,
                    service,
                    session_token=cred.token)
