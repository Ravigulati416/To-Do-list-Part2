import json
import boto3
import datetime
from botocore.exceptions import ClientError

dynamodb = boto3.resource('dynamodb')
eventbridge = boto3.client('scheduler')
sns = boto3.client('sns')

table = dynamodb.Table('Tasks')
sns_topic_arn = 'arn:aws:sns:us-east-1::To-Do-Notification' # Replace it with your SNS Topic ARN
role_arn = 'arn:aws:iam:::role/service-role/Amazon_EventBridge_Scheduler_SNS'# Replace it with your EventBridge Scheduler IAM Role ARN

def lambda_handler(event, context):
    print(f"Our event: {event}", "END-Event")
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

        # Schedule EventBridge Scheduler to publish to SNS topic
        task_time_str = body.get('TaskTime')
        if task_time_str:
            schedule_time = convert_to_iso8601(task_time_str)
            print(f"schedule time: {schedule_time}")
            schedule_eventbridge(body, schedule_time)

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

# Convert TaskTime to ISO 8601 format
def convert_to_iso8601(task_time_str):
    # Parse the provided date and time in the format "YYYY-MM-DD HH:MM"
    task_time = datetime.datetime.strptime(task_time_str, "%Y-%m-%d %H:%M")

    
    schedule_time = task_time.isoformat()
    return schedule_time


def schedule_eventbridge(task, schedule_time):
    schedule_name = f"trigger_task_{task['TaskID']}"

    message = (
        f"This is a reminder mail for your To-Do item: {task['TaskName']}.\n\n"
        f"Please check below description of that To-Do Task for more details:\n"
        f"{task['TaskDescription']}"
    )
    
    try:
        # Create a schedule for eah To-Do Item added
        eventbridge.create_schedule(
            Name=schedule_name,
            ScheduleExpression=f"at({schedule_time})",
            Target={
                'Arn': sns_topic_arn,
                'RoleArn': role_arn,
                'Input': message
            },
            State='ENABLED',
            FlexibleTimeWindow={
                'Mode': 'OFF' 
            }
        )
    except ClientError as e:
        print(f"Error scheduling EventBridge Scheduler: {e}")

def get_task(event):
    try:
        http_method = event.get('httpMethod')
        query_params = event.get('queryStringParameters', {})
        task_id = query_params.get('TaskID')

        if http_method == 'GET' and task_id == 'all':
            print("Getting all Items")
            response = table.scan()
            return {
                'statusCode': 200,
                'body': json.dumps(response['Items'])
            }

        if not task_id:
            return {
                'statusCode': 400,
                'body': json.dumps('TaskID is required')
            }

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
        schedule_name = f"trigger_task_{task_id}"
        eventbridge.delete_schedule(Name=schedule_name)
        return {
            'statusCode': 200,
            'body': json.dumps('Task deleted successfully')
        }
    except ClientError as e:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': e.response['Error']['Message']})
        }
