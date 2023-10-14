
import re
import boto3
import json
from datetime import datetime
sqs_client = boto3.client('sqs')
db_client = boto3.resource('dynamodb')
table = db_client.Table("DiningConciergeHistory")

cuisines = ["Chinese", "Japanese", "Italian", "Thai", "Mexican", "French", "American"]
locations = ["New York", "Manhattan", "new york", "manhattan"]
email_addresses = ["dxdxd2333@gmail.com"]

def lambda_handler(event, context):
    print(event)

    # Check the Cloudwatch logs to understand data inside event and
    # parse it to handle logic to validate user input and send it to Lex

    # Lex called LF1 with the user message and previous related state so
    # you can verify the user input. Validate and let Lex know what to do next.
    resp = {"statusCode": 200, "sessionState": event["sessionState"]}

    # Lex will propose a next state if available but if user input is not valid,
    # you will modify it to tell Lex to ask the same question again (meaning ask
    # the current slot question again)
    if "proposedNextState" not in event:
        if "intent" not in event["sessionState"]:
            print("intent not in event[interpretations]")
            resp["sessionState"]["dialogAction"] = {"type": "Close"}
        else:
            slots = event["sessionState"]["intent"]["slots"]
            if (type(slots["EmailAddress"]) != type(None) and 
                    "value" in slots["EmailAddress"] and
                    "interpretedValue" in slots["EmailAddress"]["value"] and 
                    slots["EmailAddress"]["value"]["interpretedValue"] not in email_addresses):
                        resp["sessionState"]["dialogAction"] = {"type":"ElicitSlot", 
                                                                "slotToElicit": "EmailAddress",
                                                                "message": {
                                                                  "contentType": "PlainText",
                                                                  "content": "Please provide a valid value of EmailAddress."
                                                                }}
            elif event["sessionState"]["intent"]["name"] == "DiningSuggestionsIntent":
                resp["sessionState"]["dialogAction"] = {"type": "Close"}
                sqs_client.send_message(
                    QueueUrl = 'https://sqs.us-west-2.amazonaws.com/261553161374/SearchRestaurantQueue',
                    MessageBody = json.dumps({"Cuisine": slots["Cuisine"]["value"]["interpretedValue"], "EmailAddress":slots["EmailAddress"]["value"]["interpretedValue"]})
                    )
                history_data = {"Location":slots["Location"]["value"]["interpretedValue"], "Cuisine": slots["Cuisine"]["value"]["interpretedValue"], "DiningTime": slots["DiningTime"]["value"]["interpretedValue"], "NumberOfPeople": slots["NumberOfPeople"]["value"]["interpretedValue"], "EmailAddress": slots["EmailAddress"]["value"]["interpretedValue"]}
                insert_data(history_data)
            else:
                resp["sessionState"]["dialogAction"] = {"type": "Close"}
                
    else:
        print("proposedNextState is", event["proposedNextState"])
        if "intent" not in event["proposedNextState"]:
            resp["sessionState"]["dialogAction"] = {"type": "Close"}
        else:
            slots = event["proposedNextState"]["intent"]["slots"]
            print("slots is",slots)
            print("type of slots is",type(slots))
            print("Cuisine in slots is","Cuisine" in slots)
            if (type(slots["Cuisine"]) != type(None) and 
                "value" in slots["Cuisine"] and 
                "interpretedValue" in slots["Cuisine"]["value"] and 
                slots["Cuisine"]["value"]["interpretedValue"] not in cuisines):
                    resp["sessionState"]["dialogAction"] = {"type":"ElicitSlot", 
                                                            "slotToElicit": "Cuisine",
                                                            "message": {
                                                              "contentType": "PlainText",
                                                              "content": "Please provide a valid value of Cuisine."
                                                            }}
            elif (type(slots["Location"]) != type(None) and 
                "value" in slots["Location"] and
                "interpretedValue" in slots["Location"]["value"] and 
                slots["Location"]["value"]["interpretedValue"] not in locations):
                    resp["sessionState"]["dialogAction"] = {"type":"ElicitSlot", 
                                                            "slotToElicit": "Location",
                                                            "message": {
                                                              "contentType": "PlainText",
                                                              "content": "Please provide a valid value of Location."
                                                            }}
            else:
                resp["sessionState"]["dialogAction"] = event["proposedNextState"]["dialogAction"]
        

    return resp
    
def insert_data(data):
    # overwrite if the same index is provided
    data['insertedTimestamp'] = str(datetime.utcnow())
    response = table.put_item(Item=data)
    print('@insert_data: data', data)
    print('@insert_data: response', response)
    return response