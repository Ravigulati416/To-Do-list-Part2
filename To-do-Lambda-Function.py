import json
import boto3
from botocore.exceptions import ClientError

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('Tasks')

def lambda_handler(event, context):
    print(f"i am the event: {event}", "END-Event")  # Log the event object to inspect its structure
    operation = event.get('httpMethod')

    if operation == 'GET':
        return get_task(event)
    elif operation == 'POST':
        return create_task(event)
    elif operation == 'PUT':
        return update_task(event)
    elif operation == 'DELETE':
        return delete_task(event)
    else:
        return {
            'statusCode': 400,
            'body': json.dumps('Invalid HTTP Method')
        }

def create_task(event):
    try:
        body = event.get('body')
        if isinstance(body, str):
            body = json.loads(body)
        elif not isinstance(body, dict):
            return {
                'statusCode': 400,
                'body': json.dumps('Invalid request body')
            }
        
        table.put_item(Item=body)
        return {
            'statusCode': 200,
            'body': json.dumps('Task created successfully')
        }
    except (json.JSONDecodeError, ValueError) as e:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': str(e)})
        }
    except ClientError as e:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': e.response['Error']['Message']})
        }

def get_task(event):
    if(event.get('httpMethod') == 'GET' and event.get('queryStringParameters', {}).get('TaskID') == 'all'):
        try:
            print("i am getting all Items")
            response = table.scan()
            return {
                'statusCode': 200,
                'body': json.dumps(response['Items'])
            }
        except ClientError as e:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': e.response['Error']['Message']})
            }
    task_id = event.get('queryStringParameters', {}).get('TaskID')
    if not task_id:
        return {
            'statusCode': 400,
            'body': json.dumps('TaskID is required')
        }

    try:
        response = table.get_item(Key={'TaskID': task_id})
        if 'Item' in response:
            return {
                'statusCode': 200,
                'body': json.dumps(response['Item'])
            }
        else:
            return {
                'statusCode': 404,
                'body': json.dumps('Task not found')
            }
    except ClientError as e:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': e.response['Error']['Message']})
        }

def update_task(event):
    try:
        body = event.get('body')
        if isinstance(body, str):
            body = json.loads(body)
        elif not isinstance(body, dict):
            raise ValueError("Request body is not a valid JSON object")
        
        task_id = body.get('TaskID')
        if not task_id:
            raise ValueError("TaskID is required")

        update_expression = 'SET TaskName = :name, TaskDescription = :desc, TaskStatus = :status'
        expression_values = {
            ':name': body['TaskName'],
            ':desc': body['TaskDescription'],
            ':status': body['TaskStatus']
        }
        
        table.update_item(
            Key={'TaskID': task_id},
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_values
        )
        return {
            'statusCode': 200,
            'body': json.dumps('Task updated successfully')
        }
    except (json.JSONDecodeError, ValueError) as e:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': str(e)})
        }
    except ClientError as e:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': e.response['Error']['Message']})
        }

def delete_task(event):
    task_id = event.get('queryStringParameters', {}).get('TaskID')
    if not task_id:
        return {
            'statusCode': 400,
            'body': json.dumps('TaskID is required')
        }

    try:
        table.delete_item(Key={'TaskID': task_id})
        return {
            'statusCode': 200,
            'body': json.dumps('Task deleted successfully')
        }
    except ClientError as e:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': e.response['Error']['Message']})
        }
