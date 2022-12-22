import json
import boto3
from datetime import datetime

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
    #"tags":"L" 
}

def get_parameters(content):
    updateExpression = 'set '
    attributeNames = {}
    attributeValues = {}
    def get_expression(content):
        result = []
        for k, v in content.items(): 
            if k not in key_to_type.keys() or k=='id' or v==None:  
                continue
            if (type(v) is int): 
                v = str(v)
            if type(v) is dict:
                results = get_expression(v)
                result += ["#%s."%k+i for i in results] 
                if len(results)>0:
                    attributeNames['#'+ k] = k
            else:
                result += ["#%s = :%s"%(k, k)]
                attributeValues[':'+k] = {key_to_type[k]: v}
                attributeNames['#'+ k] = k
                
        return result
    expression = get_expression(content)
    updateExpression = updateExpression + ', '.join(expression) if len(expression)>0 else ""
    return updateExpression, attributeNames, attributeValues


def patch(mid, content):
    updateExpression, attributeNames, attributeValues = get_parameters(content) 
    if updateExpression == "":
        return 400, "nothing to update"
    print(updateExpression)
    try:
        response = db_client.update_item(
            TableName='message',
            Key={
                'id':{'S':mid}
            },
            UpdateExpression=updateExpression,
            ExpressionAttributeNames=attributeNames,
            ExpressionAttributeValues=attributeValues
        ) 
    except:
        return 400, "incorrect uid or parameters" 
    else:
        return 200, 'status updated' 

def lambda_handler(event, context):
    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "*", 
        "Access-Control-Allow-Methods": "*"
    }
    body = json.loads(event['body'])
    print(body)
    id, content = body['id'], body 
    code, body = patch(id, content)
    return {  
        'statusCode': code, 
        'headers': headers, 
        'body': json.dumps(body) 
    }  