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
from utils import invoke_lambda
from config import logger, AUTH_BP
from decimal import Decimal

dynamodb = boto3.resource('dynamodb')
dynamodb_client = boto3.client('dynamodb')


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

def db_update_item(table_name, key_name, key_value, index_name, update_data, account_id, session_id):
    """
    Updates or creates an item in DynamoDB, ensuring user authorization.
    """
    
    table = dynamodb.Table(table_name)
    
    try:
        # Check if associated_account is part of the index name
        is_associated_account_key = 'associated_account' in index_name.lower()
        
        # Query to find items to update
        query_params = {
            'IndexName': index_name,
            'KeyConditionExpression': f"{key_name} = :key_value",
            'ExpressionAttributeValues': {':key_value': key_value}
        }
        
        # Only add associated_account filter if it's not part of the index key
        if not is_associated_account_key:
            query_params['FilterExpression'] = "associated_account = :account_id"
            query_params['ExpressionAttributeValues'][':account_id'] = account_id
        
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
                # If associated_account is the key, verify it matches account_id
                if is_associated_account_key and item.get('associated_account') != account_id:
                    continue
                    
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
        logger.info(f"Parsed event: {parsed_event}")
        session_id = parsed_event.get('session_id') or parsed_event.get('session') or json.loads(parsed_event.get('cookies')).get('session_id')
        account_id = parsed_event.get('account_id') or parsed_event.get('account') or parsed_event.get('client_id')
        
        if not session_id:
            raise LambdaError(401, "No session ID provided in body or cookies.")

        required_fields = ['table_name', 'key_name', 'key_value', 'index_name', 'account_id', 'session_id', 'update_data']
        if any(field not in parsed_event for field in required_fields):
            raise LambdaError(400, "Missing one or more required fields.")
        
        if session_id != AUTH_BP:
            logger.info(f"Authorizing account {account_id} with session {session_id}")
            authorize(account_id, session_id)
            # Check rate limit using the rate-limit Lambda
            rate_limit_response = invoke_lambda('RateLimitAWS', {
                'client_id': account_id,
                'session': session_id
            })
            
            if rate_limit_response.get('statusCode') == 429:
                logger.warning(f"Rate limit exceeded for account {account_id}")
                return create_response(429, {
                    'error': 'Rate limit exceeded',
                    'message': 'You have exceeded your AWS API rate limit. Please try again later.'
                })
            elif rate_limit_response.get('statusCode') == 401:
                logger.warning(f"Unauthorized request for account {account_id}")
                return create_response(401, {
                    'error': 'Unauthorized',
                    'message': 'Invalid or expired session'
                })
            elif rate_limit_response.get('statusCode') != 200:
                logger.error(f"Rate limit check failed: {rate_limit_response}")
                return create_response(500, {
                    'error': 'Rate limit check failed',
                    'message': 'An error occurred while checking rate limits'
                })
        
        message = db_update_item(
            parsed_event['table_name'],
            parsed_event['key_name'],
            parsed_event['key_value'],
            parsed_event['index_name'],
            json.loads(parsed_event['update_data']),
            account_id,
            session_id
        )
        
        response = create_response(200, message)
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
