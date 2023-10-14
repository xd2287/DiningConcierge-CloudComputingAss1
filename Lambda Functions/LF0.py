import boto3
import json

# Define the client to interact with Lex
client = boto3.client('lexv2-runtime')

def lambda_handler(event, context):

    # msg_from_user = event['messages'][0]

    # change this to the message that user submits on 
    # your website using the 'event' variable
    msg_from_user = "Hello"
    
    print(event)
    if "body" in event and event["body"]:
        # Parse the 'body' as JSON
        print(event)
        print(type(event))
        body = json.loads(event['body'])
        print(body)
        print(type(body))
        if "message" in body:
            msg_from_user = body["message"]
    

    print(f"Message from frontend: {msg_from_user}")

    # Initiate conversation with Lex
    response = client.recognize_text(
            botId='M0KCCUJ2C0', # MODIFY HERE
            botAliasId='5A62VAZEKZ', # MODIFY HERE
            localeId='en_US',
            sessionId='testuser',
            text=msg_from_user)
    
    msg_from_lex = response.get('messages', [])
    if msg_from_lex:
        
        print(f"Message from Chatbot: {msg_from_lex[0]['content']}")
        print(response)

        resp = {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': '*',
            },
            'body': json.dumps({"messages": [msg_from_lex[0]['content']]})
            
        }

        # modify resp to send back the next question Lex would ask from the user
        
        # format resp in a way that is understood by the frontend
        # HINT: refer to function insertMessage() in chat.js that you uploaded
        # to the S3 bucket

        return resp
    
    return {
        'statusCode': 200,
        'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': '*',
            },
        'body': json.dumps({"messages": ["Sorry, I can not answer the question"]})
    }


