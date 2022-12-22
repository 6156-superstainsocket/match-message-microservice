import json
import boto3
from datetime import datetime
import ast
import random
import string

db_client = boto3.client('dynamodb') 

key_to_type = {
    "has_read":"BOOL",
    "email":"S",
    "uid":"N",
    "type":"N",
    "content":"M",
    "from_user":"M",
    "to_user":"M",
    "tag":"M",
    "group":"M",
    "id":"N",
    "is_google":"BOOL",
    "name":"S",
    "phone":"S",
    "description":"S",
    "iconid":"N",
    "user":"N",
    "icon_id":"N",
    "allow_without_approval":"BOOL",
    "has_accept":"BOOL",
    "tags":"L"
}

def json_to_dynamo(content):
    result = {}
    for k, v in content.items():
        #print(k)
        if k not in key_to_type:
            continue 
        t = key_to_type[k]
        if t == 'M':
            result[k] = {t: json_to_dynamo(v)}
        elif t == 'L':
            result[k] = {t: [{'M':json_to_dynamo(i)} for i in v]}  
        elif t == 'N':
            result[k] = {t: str(v)}
        else: 
            result[k] = {t: v} 
    return result

def generate_mid():

    unique = False
    while not unique:
        mid = ''.join(random.choices(string.ascii_lowercase+string.digits, k=8))
        response = db_client.query(
            TableName='message',
            KeyConditionExpression = '#id = :mid',
            ExpressionAttributeNames = {
                '#id': 'id'
        },
        ExpressionAttributeValues ={
            ':mid':{'S':mid}
        }
        )
        unique = (len(response['Items'])==0)
    return mid

def post_message(message): 
    mid = generate_mid()
    now = datetime.now()
     
    message = json_to_dynamo(message)
    message['id'] = {"S": mid} 
    message['created_at'] = {'S': now.strftime("%m/%d/%Y, %H:%M:%S")}
    print(message) 
    response = db_client.put_item(
            TableName='message',
            Item= message
        )
    return 'record inserted'   


def lambda_handler(event, context):
    """event['body'] = {
                        "has_read":false,
                        "email":"",
                        "uid":0,
                        "type":2,
                        "content":{
                            "from_user":{
                                "id":1,
                                "is_google":true,
                                "name":"alice smith",
                                "phone":"123-456-7890",
                                "description":"007",
                                "iconid":2,
                                "user":1
                            },
                            "to_user":{
                                "id":3,
                                "is_google":true,
                                "name":"ariana grande",
                                "phone":"123-456-7890",
                                "description":"baby",
                                "iconid":2,
                                "user":5
                            },
                            "tag":{
                                "id":1,
                                "name":"like",
                                "icon_id":1,
                                "description":""
                            },
                            "group":{
                                "id":38,
                                "name":"test",
                                "description":"jkkkk",
                                "icon_id":9,
                                "allow_without_approval":true,
                                "tags":[

                                ]
                            },
                            "has_accept":false
                        }
                    }
    """
    #print(event['body'])
    #uid = event['queryStringParameters']['uid']
    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "*", 
        "Access-Control-Allow-Methods": "*"
    }
    message = json.loads(event['Records'][0]['Sns']['Message']) 
    return {
        'statusCode': 200,
        'headers': headers, 
        'body': json.dumps(post_message(message))
    }  
 