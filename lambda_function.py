"""
Database Update Lambda Function
==============================

This Lambda function provides secure update operations for DynamoDB records with account-based filtering.
It ensures that users can only update records that have their account_id as the associated_account.

Attribute Collision Handling:
- If the same attribute name appears multiple times in update_data, the last occurrence takes precedence
- All existing attributes not being updated are preserved during the update operation
- Complex data types (dicts, lists) are automatically serialized to JSON strings
- Uses put_item with merge strategy to ensure complete attribute preservation

Update Strategy:
- For new items: Creates a new item with all provided attributes
- For existing items: Merges existing item with new update_data, preserving all existing attributes
- This approach is more reliable than DynamoDB's SET operation for ensuring attribute persistence

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
    DecimalEncoder, serialize_for_dynamodb
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

def validate_and_clean_update_data(update_data):
    """
    Validates and cleans update data to ensure it's suitable for DynamoDB operations.
    Handles edge cases and provides detailed error messages.
    """
    if not isinstance(update_data, dict):
        raise LambdaError(400, "update_data must be a dictionary")
    
    if not update_data:
        raise LambdaError(400, "update_data cannot be empty")
    
    cleaned_data = {}
    invalid_keys = []
    
    for key, value in update_data.items():
        # Validate key
        if not isinstance(key, str):
            invalid_keys.append(f"Key '{key}' is not a string")
            continue
        
        if not key.strip():
            invalid_keys.append("Empty key found")
            continue
        
        # Check for reserved DynamoDB words (basic check)
        reserved_words = {
            'name', 'value', 'key', 'item', 'table', 'index', 'attribute', 
            'expression', 'condition', 'filter', 'projection', 'scan', 'query'
        }
        
        if key.lower() in reserved_words:
            logger.warning(f"Key '{key}' is a DynamoDB reserved word. This may cause issues.")
        
        # Validate value
        if value is None:
            cleaned_data[key] = None
        elif isinstance(value, (str, int, float, bool)):
            cleaned_data[key] = value
        elif isinstance(value, (dict, list, tuple)):
            # Complex types will be serialized to JSON strings
            cleaned_data[key] = value
        else:
            # Convert other types to string
            cleaned_data[key] = str(value)
            logger.info(f"Converted value for key '{key}' to string: {value}")
    
    if invalid_keys:
        raise LambdaError(400, f"Invalid keys found: {', '.join(invalid_keys)}")
    
    logger.info(f"Validated and cleaned update data: {cleaned_data}")
    return cleaned_data

def db_update_item(table_name, key_name, key_value, index_name, update_data, account_id, session_id):
    """
    Updates or creates an item in DynamoDB, ensuring user authorization.
    """
    
    table = dynamodb.Table(table_name)
    
    try:
        # Validate and clean input parameters
        if not isinstance(key_value, (str, int, float, bool)) and key_value is not None:
            raise LambdaError(400, "key_value must be a primitive type (string, number, boolean, or null)")
        
        # Validate and clean update_data
        cleaned_update_data = validate_and_clean_update_data(update_data)
        
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
            # Create new item - ensure update_data doesn't contain nested dicts
            new_item = {key_name: key_value, 'associated_account': account_id}
            # Use the helper function to serialize update_data
            serialized_update_data = serialize_for_dynamodb(cleaned_update_data)
            new_item.update(serialized_update_data)
            table.put_item(Item=new_item)
            return {"message": "Successfully created new item.", "operation": "create", "updated_count": 1}

        # Update existing items
        # Use the helper function to serialize update_data for DynamoDB compatibility
        try:
            flattened_update_data = serialize_for_dynamodb(cleaned_update_data)
        except ValueError as e:
            logger.error(f"Serialization failed: {e}")
            raise LambdaError(400, f"Failed to serialize update data: {e}")
        
        # Log the update data for debugging
        logger.info(f"Updating items with data: {flattened_update_data}")
        
        updated_count = 0
        for item in items:
            # If associated_account is the key, verify it matches account_id
            if is_associated_account_key and item.get('associated_account') != account_id:
                continue
                
            # Get the primary key attributes from the table schema
            key_schema = table.key_schema
            primary_key = {}
            for key_attr in key_schema:
                attr_name = key_attr['AttributeName']
                if attr_name in item:
                    primary_key[attr_name] = item[attr_name]
                else:
                    logger.warning(f"Primary key attribute {attr_name} not found in item")
                    continue
            
            if not primary_key:
                logger.warning(f"Skipping item with no valid primary key: {item}")
                continue
            
            # Log the item being updated for debugging
            logger.info(f"Updating item with key {primary_key}, current item: {item}")
            
            try:
                # Create a merged item that preserves all existing attributes
                merged_item = item.copy()  # Start with all existing attributes
                
                # Log the original item for debugging
                logger.info(f"Original item attributes: {list(item.keys())}")
                logger.info(f"Original item: {item}")
                
                # Update with new attributes (this will override existing ones with new values)
                for attr_name, attr_value in flattened_update_data.items():
                    old_value = merged_item.get(attr_name, "NOT_PRESENT")
                    merged_item[attr_name] = attr_value
                    logger.info(f"Merged attribute '{attr_name}': '{old_value}' -> '{attr_value}'")
                
                # Log the final merged item for debugging
                logger.info(f"Final merged item attributes: {list(merged_item.keys())}")
                logger.info(f"Final merged item: {merged_item}")
                
                # Verify that all original attributes are preserved
                missing_attributes = []
                for attr_name in item.keys():
                    if attr_name not in merged_item:
                        missing_attributes.append(attr_name)
                
                if missing_attributes:
                    logger.warning(f"Missing attributes after merge: {missing_attributes}")
                else:
                    logger.info("All original attributes preserved in merged item")
                
                # Use put_item to ensure all attributes are preserved
                table.put_item(Item=merged_item)
                updated_count += 1
                logger.info(f"Successfully updated item with key {primary_key}")
            except ClientError as e:
                logger.error(f"Failed to update item with key {primary_key}: {e}")
                # Continue with other items instead of failing completely
                continue
        
        return {"message": f"Successfully updated {updated_count} items.", "operation": "update", "updated_count": updated_count}

    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        logger.error(f"DynamoDB ClientError: {error_code} - {error_message}")
        raise LambdaError(500, f"Database operation failed: {error_message}")
    except TypeError as e:
        logger.error(f"TypeError in db_update_item: {e}")
        raise LambdaError(400, f"Invalid data type provided: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in db_update_item: {e}", exc_info=True)
        raise LambdaError(500, f"An unexpected error occurred: {e}")

def lambda_handler(event, context):
    try:
        cors_headers = fetch_cors_headers()
        if event.get('httpMethod') == 'OPTIONS':
            return {'statusCode': 200, 'headers': cors_headers, 'body': ''}

        parsed_event = parse_event(event)
        logger.info(f"Parsed event: {parsed_event}")
        session_id = parsed_event.get('session_id') or parsed_event.get('session') or parsed_event.get('cookies', {}).get('session_id')
        account_id = parsed_event.get('account_id') or parsed_event.get('account') or parsed_event.get('client_id')

        if not session_id:
            raise LambdaError(401, "No session ID provided in body or cookies.")

        required_fields = ['table_name', 'key_name', 'key_value', 'index_name', 'update_data']
        if any(field not in parsed_event for field in required_fields):
            raise LambdaError(400, "Missing one or more required fields.")
        
        if not account_id:
            raise LambdaError(400, "No account ID provided in body or cookies.")
        
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
            parsed_event['update_data'],
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
