import json
import boto3
from datetime import datetime
from boto3.dynamodb.types import TypeDeserializer

db_client = boto3.client('dynamodb')
 
def dynamodb_to_json(content):
    data = dict({})
    data['M'] = content
    return _unmarshal_value(data)


def _unmarshal_value(node):
    if type(node) is not dict:
        return node

    for key, value in node.items():
        # S – String - return string
        # N – Number - return int or float (if includes '.')
        # B – Binary - not handled
        # BOOL – Boolean - return Bool
        # NULL – Null - return None
        # M – Map - return a dict
        # L – List - return a list
        # SS – String Set - not handled
        # NN – Number Set - not handled
        # BB – Binary Set - not handled
        key = key.lower()
        if key == 'bool':
            return value
        if key == 'null':
            return None
        if key == 's':
            return value
        if key == 'n':
            if '.' in str(value):
                return float(value)
            return int(value)
        if key in ['m', 'l']:
            if key == 'm':
                data = {}
                for key1, value1 in value.items():
                    if key1.lower() == 'l':
                        data = [_unmarshal_value(n) for n in value1]
                    else:
                        if type(value1) is not dict:
                            return _unmarshal_value(value)
                        data[key1] = _unmarshal_value(value1)
                return data
            data = []
            for item in value:
                data.append(_unmarshal_value(item))
            return data

def get_message_by_uid(uid, page, limit, mtype):
    response = db_client.scan(
        FilterExpression = '(#uid = :uid) AND (#type = :type)',
        ExpressionAttributeNames = {
            '#uid': 'uid',
            '#type': 'type'
        },
        ExpressionAttributeValues ={
            ':uid':{"N":uid}, 
            ':type':{"N":mtype} 
        },
        TableName='message',
        Select = 'ALL_ATTRIBUTES'
    )
    #print(response)
    count = len(response["Items"])
    result = response["Items"]
    
    pageSize = limit  
    if pageSize*page >= len(result):
        return count, []
    result = result[pageSize*page:pageSize*page+pageSize]
    result = [dynamodb_to_json(x) for x in result]
    return count, result 

def sort_by_unread(data):
    unread = []
    read = []
    for message in data:
        if "hasRead" in message["content"] and message["content"]["hasRead"]:
            read.append(message) 
        else:
            unread.append(message)
    return unread + read 
    
def lambda_handler(event, context):
    # TODO implement 
    headers = { 
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token",
        "Access-Control-Allow-Methods": "GET,OPTIONS,POST,PUT"
    }
    print(event) 
    if "uid" not in event["pathParameters"] or event["pathParameters"]["uid"] ==None:
        return {
                'statusCode': 400,
                'headers':headers,
                'body': json.dumps("please enter user id")
            } 
    uid = event["pathParameters"]["uid"]
    print(uid)
    page, limit, mtype = event['queryStringParameters']['page'], event['queryStringParameters']['limit'], event['queryStringParameters']['type']
    page, limit = int(page), int(limit)  
    count, response = get_message_by_uid(uid, page, limit, mtype)
    response = sort_by_unread(response)
    result = {
        "count": count,
        "data": response
    }
    
    return {
        'statusCode': 200,
        'headers':headers,
        'body': json.dumps(result)
    }
