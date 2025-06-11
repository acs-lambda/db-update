import json
import boto3
import logging
from botocore.exceptions import ClientError
from typing import Dict, Any, Optional
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
    
    Args:
        table_name (str): Name of the DynamoDB table
        key_name (str): Name of the key attribute in the GSI
        key_value (str): Value to match in the GSI
        index_name (str): Name of the Global Secondary Index to query
        update_data (dict): Dictionary containing the attributes to update
        
    Returns:
        dict: Summary of the update operation including number of items updated and any errors
    """
    table = dynamodb.Table(table_name)
    logger.info(f"Attempting to update items in table {table_name} with {key_name}={key_value} using index {index_name}")
    logger.debug(f"Update data: {safe_json_dumps(update_data)}")
    
    try:
        # First, query the GSI to get all matching items
        logger.info(f"Querying GSI {index_name} for matching items")
        response = table.query(
            IndexName=index_name,
            KeyConditionExpression=f"#{key_name} = :value",
            ExpressionAttributeNames={
                f"#{key_name}": key_name
            },
            ExpressionAttributeValues={
                ":value": key_value
            }
        )
        
        items = response.get('Items', [])
        if not items:
            logger.warning(f"No items found with {key_name}={key_value} in index {index_name}")
            return {
                'updated_count': 0,
                'message': f"No items found with {key_name}={key_value} in index {index_name}"
            }
            
        logger.info(f"Found {len(items)} items to update")
        
        # Get the primary key schema
        table_description = dynamodb_client.describe_table(TableName=table_name)
        key_schema = table_description['Table']['KeySchema']
        
        # Build the update expression and attribute values
        update_expr = "SET "
        expr_attr_values = {}
        expr_attr_names = {}
        
        for i, (attr_name, attr_value) in enumerate(update_data.items()):
            placeholder = f":val{i}"
            name_placeholder = f"#attr{i}"
            update_expr += f"{name_placeholder} = {placeholder}, "
            expr_attr_values[placeholder] = attr_value
            expr_attr_names[name_placeholder] = attr_name
        
        # Remove trailing comma and space
        update_expr = update_expr[:-2]
        
        logger.debug(f"Update expression: {update_expr}")
        logger.debug(f"Expression attribute values: {safe_json_dumps(expr_attr_values)}")
        logger.debug(f"Expression attribute names: {safe_json_dumps(expr_attr_names)}")
        
        # Update each matching item using its primary key
        updated_count = 0
        errors = []
        
        for item in items:
            try:
                # Build the primary key for this item
                primary_key = {}
                for key in key_schema:
                    key_attr_name = key['AttributeName']
                    if key_attr_name not in item:
                        raise KeyError(f"Item missing required primary key attribute: {key_attr_name}")
                    primary_key[key_attr_name] = item[key_attr_name]
                
                # Perform the update
                response = table.update_item(
                    Key=primary_key,
                    UpdateExpression=update_expr,
                    ExpressionAttributeValues=expr_attr_values,
                    ExpressionAttributeNames=expr_attr_names,
                    ReturnValues="ALL_NEW"
                )
                updated_count += 1
                logger.debug(f"Successfully updated item with primary key: {safe_json_dumps(primary_key)}")
                
            except Exception as item_error:
                error_msg = f"Failed to update item with primary key {safe_json_dumps(primary_key)}: {str(item_error)}"
                logger.error(error_msg)
                errors.append(error_msg)
        
        result = {
            'updated_count': updated_count,
            'total_items': len(items),
            'message': f"Successfully updated {updated_count} out of {len(items)} items"
        }
        
        if errors:
            result['errors'] = errors
            
        logger.info(f"Update operation summary: {safe_json_dumps(result)}")
        return result
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        logger.error(f"DynamoDB ClientError: {error_code} - {error_message}")
        return {
            'updated_count': 0,
            'error': f"DynamoDB error: {error_message}"
        }
    except Exception as e:
        logger.error(f"Error updating items in {table_name}: {str(e)}")
        return {
            'updated_count': 0,
            'error': str(e)
        }

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda function handler for database updates.
    Expects a POST request with JSON body containing:
    {
        "table_name": "string",
        "key_name": "string",
        "key_value": "string",
        "index_name": "string",
        "update_data": {
            "attribute1": "value1",
            "attribute2": "value2"
        }
    }
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
        required_fields = ['table_name', 'key_name', 'key_value', 'index_name', 'update_data']
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
