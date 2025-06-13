import json
import boto3
import logging
import time
from botocore.exceptions import ClientError
from typing import Dict, Any, Optional, Tuple
from decimal import Decimal

# Set up logging with more detailed format
logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
for handler in logger.handlers:
    handler.setFormatter(formatter)

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

def db_update(table_name: str, key_name: str, key_value: str, index_name: str, update_data: dict) -> Optional[Dict[str, Any]]:
    """
    Updates items in the specified DynamoDB table that match a key value in a GSI.
    If no items exist, creates a new item with the provided key and update data.
    
    Args:
        table_name (str): Name of the DynamoDB table
        key_name (str): Name of the key attribute in the GSI
        key_value (str): Value to match in the GSI
        index_name (str): Name of the Global Secondary Index to query
        update_data (dict): Dictionary containing the attributes to update
        
    Returns:
        dict: Summary of the update operation including number of items updated and any errors
    """
    logger.info(f"=== Starting db_update operation ===")
    logger.info(f"Table: {table_name}, Key: {key_name}={key_value}, Index: {index_name}")
    logger.debug(f"Full update data: {safe_json_dumps(update_data)}")
    
    table = dynamodb.Table(table_name)
    
    try:
        # First, query the GSI to get all matching items
        logger.info(f"Querying GSI {index_name} for items matching {key_name}={key_value}")
        query_params = {
            'IndexName': index_name,
            'KeyConditionExpression': f"#{key_name} = :value",
            'ExpressionAttributeNames': {
                f"#{key_name}": key_name
            },
            'ExpressionAttributeValues': {
                ":value": key_value
            }
        }
        logger.debug(f"Query parameters: {safe_json_dumps(query_params)}")
        
        response = table.query(**query_params)
        items = response.get('Items', [])
        logger.info(f"Query returned {len(items)} items")
        
        # If no items exist, create a new item
        if not items:
            logger.info(f"No existing items found. Initiating item creation process.")
            
            # Create a new item with the key and update data
            new_item = {key_name: key_value, **update_data}
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
                
            except Exception as item_error:
                error_msg = f"Failed to update item {idx} with primary key {safe_json_dumps(primary_key)}: {str(item_error)}"
                logger.error(error_msg, exc_info=True)
                logger.error(f"Failed item data: {safe_json_dumps(item)}")
                errors.append(error_msg)
        
        result = {
            'updated_count': updated_count,
            'total_items': len(items),
            'message': f"Successfully updated {updated_count} out of {len(items)} items",
            'operation': 'update'
        }
        
        if errors:
            result['errors'] = errors
            logger.warning(f"Update operation completed with {len(errors)} errors")
        else:
            logger.info("Update operation completed successfully with no errors")
            
        logger.info(f"Final operation summary: {safe_json_dumps(result)}")
        return result
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        logger.error(f"DynamoDB ClientError: {error_code} - {error_message}", exc_info=True)
        logger.error(f"Error response: {safe_json_dumps(e.response)}")
        return {
            'updated_count': 0,
            'error': f"DynamoDB error: {error_message}",
            'operation': 'error'
        }
    except Exception as e:
        logger.error(f"Unexpected error in db_update: {str(e)}", exc_info=True)
        return {
            'updated_count': 0,
            'error': str(e),
            'operation': 'error'
        }
    finally:
        logger.info("=== Completed db_update operation ===")

def check_rate_limit(account_id: str) -> Tuple[bool, Optional[str]]:
    """
    Checks if the account has exceeded its AWS API rate limit.
    
    Args:
        account_id (str): The AWS account ID to check rate limits for
        
    Returns:
        Tuple[bool, Optional[str]]: (is_allowed, error_message)
            - is_allowed: True if the request is allowed, False if rate limited
            - error_message: None if allowed, error message if rate limited
    """
    logger.info(f"Checking rate limit for account {account_id}")
    
    try:
        # First check the user's rate limit from Users table
        users_table = dynamodb.Table('Users')
        user_response = users_table.get_item(
            Key={'account_id': account_id},
            ProjectionExpression='rl_aws'
        )
        
        if 'Item' not in user_response:
            logger.warning(f"No rate limit found for account {account_id}, defaulting to 0")
            max_invocations = 0
        else:
            max_invocations = user_response['Item'].get('rl_aws', 0)
            logger.info(f"Found rate limit of {max_invocations} for account {account_id}")
        
        # Check current invocation count
        rl_table = dynamodb.Table('RL_AWS')
        current_time = int(time.time() * 1000)  # Current time in milliseconds
        ttl_time = current_time + (60 * 1000)  # 1 minute from now
        
        # Try to get existing record
        try:
            response = rl_table.get_item(
                Key={'associated_account': account_id}
            )
            
            if 'Item' in response:
                current_invocations = response['Item'].get('invocations', 0)
                logger.info(f"Current invocation count: {current_invocations}")
                
                if current_invocations >= max_invocations:
                    return False, f"Rate limit exceeded. Maximum {max_invocations} invocations per minute allowed."
                
                # Update invocation count
                rl_table.update_item(
                    Key={'associated_account': account_id},
                    UpdateExpression='SET invocations = invocations + :inc',
                    ExpressionAttributeValues={':inc': 1}
                )
            else:
                # Create new record
                rl_table.put_item(
                    Item={
                        'associated_account': account_id,
                        'invocations': 1,
                        'ttl': ttl_time
                    }
                )
                logger.info("Created new rate limit record")
            
            return True, None
            
        except ClientError as e:
            logger.error(f"Error accessing RL_AWS table: {str(e)}")
            return False, "Internal error checking rate limits"
            
    except Exception as e:
        logger.error(f"Unexpected error in rate limit check: {str(e)}")
        return False, "Internal error checking rate limits"

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda function handler for database updates with rate limiting.
    """
    logger.info("Lambda handler started")
    logger.debug(f"Received event: {safe_json_dumps(event)}")
    
    try:
        
        # Parse request body
        if not event.get('body'):
            logger.error("No request body provided")
            return {
                'statusCode': 400,
                'body': safe_json_dumps({'error': 'Request body is required'})
            }
        
        try:
            body = json.loads(event['body'])
            logger.debug(f"Parsed request body: {safe_json_dumps(body)}")
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse request body: {str(e)}")
            return {
                'statusCode': 400,
                'body': safe_json_dumps({'error': 'Invalid JSON in request body'})
            }
        
        # Validate required fields
        required_fields = ['table_name', 'key_name', 'key_value', 'index_name', 'update_data', 'account_id']
        missing_fields = [field for field in required_fields if field not in body]
        if missing_fields:
            error_msg = f"Missing required fields: {', '.join(missing_fields)}"
            logger.error(error_msg)
            return {
                'statusCode': 400,
                'body': safe_json_dumps({'error': error_msg})
            }
        
        # Validate update_data is a dictionary
        if not isinstance(body['update_data'], dict):
            logger.error("update_data must be a dictionary")
            return {
                'statusCode': 400,
                'body': safe_json_dumps({'error': 'update_data must be a dictionary'})
            }
            
            
                # Extract account ID from the request
        account_id = body['account_id']
        if not account_id:
            logger.error("No account ID found in request context")
            return {
                'statusCode': 400,
                'body': safe_json_dumps({'error': 'Account ID is required'})
            }
        
        # Check rate limits
        is_allowed, error_message = check_rate_limit(account_id)
        if not is_allowed:
            logger.warning(f"Rate limit exceeded for account {account_id}")
            return {
                'statusCode': 429,
                'body': safe_json_dumps({
                    'error': error_message,
                    'message': 'Rate limit exceeded. Please try again later.'
                })
            }

        
        # Perform the update
        result = db_update(
            body['table_name'],
            body['key_name'],
            body['key_value'],
            body['index_name'],
            body['update_data']
        )
        
        if result.get('error'):
            error_msg = result['error']
            logger.error(error_msg)
            return {
                'statusCode': 500,
                'body': safe_json_dumps({'error': error_msg})
            }
        
        logger.info("Update operation completed successfully")
        return {
            'statusCode': 200,
            'body': safe_json_dumps(result)
        }
        
    except Exception as e:
        error_msg = f"Internal server error: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return {
            'statusCode': 500,
            'body': safe_json_dumps({'error': error_msg})
        }
    finally:
        logger.info("Lambda handler completed")
