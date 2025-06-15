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
import logging
import time
from botocore.exceptions import ClientError
from typing import Dict, Any, Optional, Tuple
from decimal import Decimal
from utils import (
    invoke, parse_event, authorize, validate_update_data,
    check_rate_limit, fetch_cors_headers, AuthorizationError,
    UpdateValidationError, safe_json_dumps
)

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize DynamoDB resource
dynamodb = boto3.resource('dynamodb')
dynamodb_client = boto3.client('dynamodb')

# Custom JSON encoder to handle Decimal types
class DecimalEncoder(json.JSONEncoder):
    """Custom JSON encoder for handling Decimal types from DynamoDB"""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)

def safe_json_dumps(obj: Any) -> str:
    """
    Safely serialize an object to JSON string, handling Decimal types.
    
    Args:
        obj: The object to serialize
        
    Returns:
        str: JSON string representation of the object
    """
    return json.dumps(obj, cls=DecimalEncoder)

def db_update(table_name: str, key_name: str, key_value: str, index_name: str, update_data: dict, account_id: str) -> Optional[Dict[str, Any]]:
    """
    Updates items in the specified DynamoDB table that match a key value in a GSI.
    If no items exist, creates a new item with the provided key and update data.
    
    Args:
        table_name (str): Name of the DynamoDB table
        key_name (str): Name of the key attribute in the GSI
        key_value (str): Value to match in the GSI
        index_name (str): Name of the Global Secondary Index to query
        update_data (dict): Dictionary containing the attributes to update
        account_id (str): ID of the authenticated user
        
    Returns:
        dict: Summary of the update operation including number of items updated and any errors
    """
    logger.info(f"=== Starting db_update operation ===")
    logger.info(f"Table: {table_name}, Key: {key_name}={key_value}, Index: {index_name}")
    logger.debug(f"Full update data: {safe_json_dumps(update_data)}")
    
    # Validate update data against table schema
    try:
        validate_update_data(table_name, update_data)
    except UpdateValidationError as e:
        logger.error(f"Update validation failed: {str(e)}")
        return {
            'updated_count': 0,
            'error': str(e),
            'operation': 'validation_failed'
        }
    
    table = dynamodb.Table(table_name)
    
    try:
        # Check if associated_account is part of the index name (indicating it's a GSI)
        is_account_index = 'associated_account' in index_name.lower()
        logger.info(f"Index {index_name} {'is' if is_account_index else 'is not'} an account-based index")
        
        # First, query the GSI to get all matching items
        logger.info(f"Querying GSI {index_name} for items matching {key_name}={key_value}")
        
        if is_account_index:
            # If associated_account is part of the index, use it as the partition key
            logger.debug("Using associated_account as partition key in KeyConditionExpression")
            query_params = {
                'IndexName': index_name,
                'KeyConditionExpression': "#account = :account_id",
                'ExpressionAttributeNames': {
                    "#account": "associated_account"
                },
                'ExpressionAttributeValues': {
                    ":account_id": account_id
                }
            }
            # Add filter expression for key_name if it's not the sort key
            if key_name != "associated_account":
                query_params['FilterExpression'] = f"#{key_name} = :key_value"
                query_params['ExpressionAttributeNames'][f"#{key_name}"] = key_name
                query_params['ExpressionAttributeValues'][":key_value"] = key_value
        else:
            # If associated_account is not part of the index, use key_name as partition key
            # and filter by associated_account
            logger.debug("Using key_name as partition key and filtering by associated_account")
            query_params = {
                'IndexName': index_name,
                'KeyConditionExpression': f"#{key_name} = :key_value",
                'FilterExpression': "#account = :account_id",
                'ExpressionAttributeNames': {
                    f"#{key_name}": key_name,
                    "#account": "associated_account"
                },
                'ExpressionAttributeValues': {
                    ":key_value": key_value,
                    ":account_id": account_id
                }
            }
        
        logger.debug(f"Query parameters: {safe_json_dumps(query_params)}")
        
        response = table.query(**query_params)
        items = response.get('Items', [])
        logger.info(f"Query returned {len(items)} items for account {account_id}")
        
        # If no items exist, create a new item
        if not items:
            logger.info(f"No existing items found. Initiating item creation process.")
            
            # Create a new item with the key and update data
            new_item = {
                key_name: key_value,
                'associated_account': account_id,
                **update_data
            }
            logger.debug(f"Prepared new item data: {safe_json_dumps(new_item)}")
            
            try:
                # Get the table's key schema to determine the primary key
                logger.info("Retrieving table schema to validate primary key attributes")
                table_description = dynamodb_client.describe_table(TableName=table_name)
                key_schema = table_description['Table']['KeySchema']
                logger.debug(f"Table key schema: {safe_json_dumps(key_schema)}")
                
                # Build the primary key for the new item
                primary_key = {}
                for key in key_schema:
                    key_attr_name = key['AttributeName']
                    logger.debug(f"Checking required key attribute: {key_attr_name}")
                    if key_attr_name not in new_item:
                        error_msg = f"Missing required primary key attribute: {key_attr_name}"
                        logger.error(f"Validation failed: {error_msg}")
                        logger.error(f"Available attributes: {list(new_item.keys())}")
                        raise KeyError(error_msg)
                    primary_key[key_attr_name] = new_item[key_attr_name]
                
                logger.info(f"Primary key validation successful: {safe_json_dumps(primary_key)}")
                
                # Put the new item
                logger.info("Attempting to create new item in DynamoDB")
                put_params = {'Item': new_item}
                logger.debug(f"PutItem parameters: {safe_json_dumps(put_params)}")
                
                table.put_item(**put_params)
                logger.info(f"Successfully created new item with primary key: {safe_json_dumps(primary_key)}")
                
                result = {
                    'updated_count': 1,
                    'total_items': 1,
                    'message': f"Created new item with {key_name}={key_value}",
                    'operation': 'create',
                    'primary_key': primary_key
                }
                logger.info(f"Creation operation completed successfully: {safe_json_dumps(result)}")
                return result
                
            except Exception as create_error:
                error_msg = f"Failed to create new item: {str(create_error)}"
                logger.error(error_msg, exc_info=True)
                logger.error(f"Failed item data: {safe_json_dumps(new_item)}")
                return {
                    'updated_count': 0,
                    'error': error_msg,
                    'operation': 'create_failed'
                }
        
        logger.info(f"Processing {len(items)} existing items for update")
        
        # Get the primary key schema
        logger.info("Retrieving table schema for update operations")
        table_description = dynamodb_client.describe_table(TableName=table_name)
        key_schema = table_description['Table']['KeySchema']
        logger.debug(f"Table key schema for updates: {safe_json_dumps(key_schema)}")
        
        # Build the update expression and attribute values
        update_expr = "SET "
        expr_attr_values = {}
        expr_attr_names = {}
        
        logger.info("Building update expression and attribute mappings")
        for i, (attr_name, attr_value) in enumerate(update_data.items()):
            placeholder = f":val{i}"
            name_placeholder = f"#attr{i}"
            update_expr += f"{name_placeholder} = {placeholder}, "
            expr_attr_values[placeholder] = attr_value
            expr_attr_names[name_placeholder] = attr_name
            logger.debug(f"Added update mapping: {attr_name} -> {safe_json_dumps(attr_value)}")
        
        # Remove trailing comma and space
        update_expr = update_expr[:-2]
        
        logger.debug(f"Final update expression: {update_expr}")
        logger.debug(f"Expression attribute values: {safe_json_dumps(expr_attr_values)}")
        logger.debug(f"Expression attribute names: {safe_json_dumps(expr_attr_names)}")
        
        # Update each matching item using its primary key
        updated_count = 0
        errors = []
        
        for idx, item in enumerate(items, 1):
            logger.info(f"Processing item {idx} of {len(items)}")
            try:
                # Build the primary key for this item
                primary_key = {}
                for key in key_schema:
                    key_attr_name = key['AttributeName']
                    if key_attr_name not in item:
                        error_msg = f"Item {idx} missing required primary key attribute: {key_attr_name}"
                        logger.error(error_msg)
                        logger.error(f"Available attributes: {list(item.keys())}")
                        raise KeyError(error_msg)
                    primary_key[key_attr_name] = item[key_attr_name]
                
                logger.debug(f"Item {idx} primary key: {safe_json_dumps(primary_key)}")
                
                # Perform the update
                update_params = {
                    'Key': primary_key,
                    'UpdateExpression': update_expr,
                    'ExpressionAttributeValues': expr_attr_values,
                    'ExpressionAttributeNames': expr_attr_names,
                    'ReturnValues': "ALL_NEW"
                }
                logger.debug(f"Update parameters for item {idx}: {safe_json_dumps(update_params)}")
                
                response = table.update_item(**update_params)
                updated_count += 1
                logger.info(f"Successfully updated item {idx} with primary key: {safe_json_dumps(primary_key)}")
                logger.debug(f"Update response for item {idx}: {safe_json_dumps(response)}")
                
            except Exception as e:
                error_msg = f"Failed to update item {idx}: {str(e)}"
                logger.error(error_msg, exc_info=True)
                errors.append(error_msg)
                continue
        
        result = {
            'updated_count': updated_count,
            'total_items': len(items),
            'errors': errors if errors else None,
            'operation': 'update'
        }
        
        if errors:
            logger.warning(f"Update completed with {len(errors)} errors: {safe_json_dumps(errors)}")
        else:
            logger.info(f"Update completed successfully: {safe_json_dumps(result)}")
            
        return result
        
    except Exception as e:
        error_msg = f"Database operation failed: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return {
            'updated_count': 0,
            'error': error_msg,
            'operation': 'failed'
        }

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda function handler for database update operations
    
    Args:
        event (Dict[str, Any]): The event from API Gateway or direct Lambda invocation
        context (Any): Lambda context object
        
    Returns:
        Dict[str, Any]: Response with status code, headers, and body
    """
    logger.info("Lambda function started")
    logger.debug(f"Received event: {safe_json_dumps(event)}")
    
    cors_headers = fetch_cors_headers()
    logger.debug(f"CORS headers: {safe_json_dumps(cors_headers)}")

    # CORS preflight
    if event.get('httpMethod') == 'OPTIONS':
        logger.info("Handling OPTIONS request (CORS preflight)")
        return {
            'statusCode': 200,
            'headers': cors_headers
        }

    # Parse the event
    try:
        parsed_event = parse_event(event)
    except Exception as e:
        logger.error(f"Error parsing event: {str(e)}")
        return {
            'statusCode': 400,
            'headers': cors_headers,
            'body': safe_json_dumps({'error': 'Invalid request format'})
        }
    
    # Validate required parameters
    table_name = parsed_event.get('table_name')
    index_name = parsed_event.get('index_name')
    key_name = parsed_event.get('key_name')
    key_value = parsed_event.get('key_value')
    update_data = parsed_event.get('update_data')
    account_id = parsed_event.get('account_id')
    session_id = parsed_event.get('session_id')

    if not all([table_name, index_name, key_name, key_value, update_data, account_id, session_id]):
        missing_params = [param for param, value in [
            ('table_name', table_name),
            ('index_name', index_name),
            ('key_name', key_name),
            ('key_value', key_value),
            ('update_data', update_data),
            ('account_id', account_id),
            ('session_id', session_id)
        ] if not value]
        logger.error(f"Missing required parameters: {', '.join(missing_params)}")
        return {
            'statusCode': 400,
            'headers': cors_headers,
            'body': safe_json_dumps({
                'error': f"Missing required parameters: {', '.join(missing_params)}"
            })
        }
        
    # Authorize the request
    logger.info("Authorizing request")
    try:
        authorize(account_id, session_id)
    except AuthorizationError as e:
        logger.error(f"Authorization error: {str(e)}")
        return {
            'statusCode': 401,
            'headers': cors_headers,
            'body': safe_json_dumps({
                'error': 'Unauthorized',
                'message': 'Invalid or expired session'
            })
        }
        
    # Check rate limit
    is_allowed, error_message = check_rate_limit(account_id, session_id)
    if not is_allowed:
        logger.warning(f"Rate limit check failed: {error_message}")
        status_code = 429 if error_message == "Rate limit exceeded" else 401
        return {
            'statusCode': status_code,
            'headers': cors_headers,
            'body': safe_json_dumps({
                'error': error_message
            })
        }

    # Perform the update operation
    try:
        result = db_update(
            table_name=table_name,
            key_name=key_name,
            key_value=key_value,
            index_name=index_name,
            update_data=update_data,
            account_id=account_id
        )
        
        if result.get('error'):
            logger.error(f"Update operation failed: {result['error']}")
            return {
                'statusCode': 500,
                'headers': cors_headers,
                'body': safe_json_dumps({
                    'error': result['error']
                })
            }
            
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': safe_json_dumps(result)
        }
        
    except Exception as e:
        logger.error(f"Unexpected error during update operation: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': safe_json_dumps({
                'error': f"Update operation failed: {str(e)}"
            })
        }
    finally:
        logger.info("Lambda function execution completed")
