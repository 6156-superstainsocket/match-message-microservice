import json
import boto3

def send_email(message):
	types = {1:'match', 2:'invitation', 3:'management'}
	t = message["type"]
	mtype = types[t] if t in types.keys() else ''
	to_email = message["email"]
	from_name, to_name = message["content"]["from_user"]["name"], message["content"]["to_user"]["name"]
	raw = """Hello, %s
        
        This is an automatically generated notification from MATCH. You recieved a new %s message from %s. Thank you.
        
        """ 
	body = raw%(to_name, mtype, from_name) 
    

	client = boto3.client("ses", region_name="us-east-1")  
	client.send_email(
	    Source = 'zipeijiang@gmail.com',
	    Destination = {
		    'ToAddresses': [
			    to_email
		    ]
	    },
	    Message = {
		    'Subject': {
			    'Data': 'no-reply',  
			    'Charset': 'UTF-8'
		    },
		    'Body': {
			    'Text':{
				    'Data': body,
				    'Charset': 'UTF-8' 
			    }
		    }
	    }
    ) 

def lambda_handler(event, context):
    message = json.loads(event['Records'][0]['Sns']['Message']) 
    print(message)
    send_email(message)
    return {
        'statusCode': 200,
        'body': json.dumps('email sent successfully!')
    }
 
 