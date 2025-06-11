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

def db_update(table_name: str, key_name: str, key_value: str, update_data: dict) -> Optional[Dict[str, Any]]:
    """
    Updates an item in the specified DynamoDB table.
    
    Args:
        table_name (str): Name of the DynamoDB table
        key_name (str): Name of the primary key attribute
        key_value (str): Value of the primary key
        update_data (dict): Dictionary containing the attributes to update
        
    Returns:
        dict: The updated item if successful, None if the update fails
    """
    table = dynamodb.Table(table_name)
    logger.info(f"Attempting to update item in table {table_name} with key {key_name}={key_value}")
    logger.debug(f"Update data: {safe_json_dumps(update_data)}")
    
    try:
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
        
        # Perform the update
        response = table.update_item(
            Key={key_name: key_value},
            UpdateExpression=update_expr,
            ExpressionAttributeValues=expr_attr_values,
            ExpressionAttributeNames=expr_attr_names,
            ReturnValues="ALL_NEW"
        )
        
        attributes = response.get('Attributes')
        logger.info(f"Successfully updated item. New attributes: {safe_json_dumps(attributes)}")
        return attributes
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        logger.error(f"DynamoDB ClientError: {error_code} - {error_message}")
        return None
    except Exception as e:
        logger.error(f"Error updating item in {table_name}: {str(e)}")
        return None

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Lambda function handler for database updates.
    Expects a POST request with JSON body containing:
    {
        "table_name": "string",
        "key_name": "string",
        "key_value": "string",
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
        required_fields = ['table_name', 'key_name', 'key_value', 'update_data']
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
            body['update_data']
        )
        
        if result is None:
            error_msg = 'Failed to update item in database'
            logger.error(error_msg)
            return {
                'statusCode': 500,
                'body': safe_json_dumps({'error': error_msg})
            }
        
        logger.info("Update operation completed successfully")
        return {
            'statusCode': 200,
            'body': safe_json_dumps({
                'message': 'Item updated successfully',
                'updated_item': result
            })
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
