"""
Database Update Lambda Function
==============================

This Lambda function provides secure update operations for DynamoDB records with account-based filtering.
It ensures that users can only update records that have their account_id as the associated_account.

API Interface
------------
Endpoint: POST /db-update
Authentication: Required (account_id and session)

Request Payload:
{
    "table_name": string,      # Required: Name of the DynamoDB table to update
    "index_name": string,      # Required: Name of the GSI to use for querying
    "key_name": string,        # Required: Name of the key attribute to query on
    "key_value": string,       # Required: Value to match against key_name
    "update_data": object,     # Required: Object containing attributes to update
    "account_id": string,      # Required: ID of the authenticated user
    "session": string          # Required: Session token for authentication
}

Response:
{
    "statusCode": number,      # HTTP status code
    "headers": object,         # CORS headers
    "body": string            # JSON stringified response body
}

Status Codes:
- 200: Success - Records updated successfully
- 400: Bad Request - Missing required parameters or invalid request format
- 401: Unauthorized - Invalid or expired session
- 429: Too Many Requests - Rate limit exceeded
- 500: Internal Server Error - DynamoDB update failed or rate limit check failed

Security:
- All requests must include valid account_id and session
- Records are filtered to only update those where associated_account matches account_id
- Rate limiting is enforced per account
- CORS headers are automatically applied
"""

import json
import boto3
from botocore.exceptions import ClientError
from utils import (
    create_response, LambdaError, parse_event, authorize, 
    DecimalEncoder
)
from config import logger
from decimal import Decimal

dynamodb = boto3.resource('dynamodb')
dynamodb_client = boto3.client('dynamodb')

def validate_update_data(table_name, update_data):
    try:
        table_description = dynamodb_client.describe_table(TableName=table_name)
        attr_types = {attr['AttributeName']: attr['AttributeType'] for attr in table_description['Table']['AttributeDefinitions']}
        for attr_name, attr_value in update_data.items():
            if attr_name not in attr_types:
                raise LambdaError(400, f"Attribute {attr_name} does not exist in table {table_name}")
            expected_type = attr_types[attr_name]
            if expected_type == 'S' and not isinstance(attr_value, str):
                raise LambdaError(400, f"Attribute {attr_name} must be a string")
            elif expected_type == 'N' and not isinstance(attr_value, (int, float, Decimal)):
                raise LambdaError(400, f"Attribute {attr_name} must be a number")
            elif expected_type == 'B' and not isinstance(attr_value, bytes):
                raise LambdaError(400, f"Attribute {attr_name} must be binary")
    except ClientError as e:
        raise LambdaError(500, f"Failed to validate update data: {e.response['Error']['Message']}")

def check_rate_limit(account_id, session_id):
    from utils import invoke_lambda
    response = invoke_lambda('RateLimitAWS', {'client_id': account_id, 'session': session_id})
    if response.get('statusCode') != 200:
        raise LambdaError(response.get('statusCode', 429), response.get('body', {}).get('message', 'Rate limit check failed.'))

def fetch_cors_headers():
    from utils import invoke_lambda
    try:
        response = invoke_lambda('Allow-Cors', {})
        return response.get('headers', {})
    except Exception as e:
        logger.error(f"Failed to fetch CORS headers: {e}")
        return {}

def db_update_item(table_name, key_name, key_value, index_name, update_data, account_id):
    """
    Updates or creates an item in DynamoDB, ensuring user authorization.
    """
    validate_update_data(table_name, update_data)
    
    table = dynamodb.Table(table_name)
    
    try:
        # Query to find items to update
        query_params = {
            'IndexName': index_name,
            'KeyConditionExpression': f"{key_name} = :key_value",
            'FilterExpression': "associated_account = :account_id",
            'ExpressionAttributeValues': {':key_value': key_value, ':account_id': account_id}
        }
        response = table.query(**query_params)
        items = response.get('Items', [])

        if not items:
            # Create new item
            new_item = {key_name: key_value, 'associated_account': account_id, **update_data}
            table.put_item(Item=new_item)
            return {"message": "Successfully created new item.", "operation": "create", "updated_count": 1}

        # Update existing items
        update_expression = "SET " + ", ".join(f"#{k}=:{k}" for k in update_data)
        expression_attribute_names = {f"#{k}": k for k in update_data}
        expression_attribute_values = {f":{k}": v for k, v in update_data.items()}

        updated_count = 0
        with table.batch_writer() as batch:
            for item in items:
                primary_key = {k: item[k] for k in table.key_schema}
                batch.update_item(
                    Key=primary_key,
                    UpdateExpression=update_expression,
                    ExpressionAttributeNames=expression_attribute_names,
                    ExpressionAttributeValues=expression_attribute_values
                )
                updated_count += 1
        
        return {"message": f"Successfully updated {updated_count} items.", "operation": "update", "updated_count": updated_count}

    except ClientError as e:
        raise LambdaError(500, f"Database operation failed: {e.response['Error']['Message']}")
    except Exception as e:
        raise LambdaError(500, f"An unexpected error occurred: {e}")

def lambda_handler(event, context):
    try:
        cors_headers = fetch_cors_headers()
        if event.get('httpMethod') == 'OPTIONS':
            return {'statusCode': 200, 'headers': cors_headers, 'body': ''}

        parsed_event = parse_event(event)
        body = parsed_event.get('body', {})
        
        required_fields = ['table_name', 'index_name', 'key_name', 'key_value', 'update_data', 'account_id', 'session']
        if any(field not in body for field in required_fields):
            raise LambdaError(400, "Missing required parameters.")

        account_id = body['account_id']
        session_id = body['session']

        authorize(account_id, session_id)
        check_rate_limit(account_id, session_id)

        result = db_update_item(
            body['table_name'],
            body['key_name'],
            body['key_value'],
            body['index_name'],
            body['update_data'],
            account_id
        )
        
        response = create_response(200, result)
        response['headers'].update(cors_headers)
        return response

    except LambdaError as e:
        response = create_response(e.status_code, {"error": e.message})
        response['headers'].update(fetch_cors_headers())
        return response
    except Exception as e:
        logger.error(f"Unhandled error: {e}")
        response = create_response(500, {"error": "Internal server error."})
        response['headers'].update(fetch_cors_headers())
        return response
