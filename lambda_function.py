import json
import boto3
import logging
from botocore.exceptions import ClientError
from typing import Dict, Any, Optional

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize DynamoDB resource
dynamodb = boto3.resource('dynamodb')
dynamodb_client = boto3.client('dynamodb')

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
        
        # Perform the update
        response = table.update_item(
            Key={key_name: key_value},
            UpdateExpression=update_expr,
            ExpressionAttributeValues=expr_attr_values,
            ExpressionAttributeNames=expr_attr_names,
            ReturnValues="ALL_NEW"
        )
        
        return response.get('Attributes')
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
    try:
        # Parse request body
        if not event.get('body'):
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Request body is required'})
            }
        
        try:
            body = json.loads(event['body'])
        except json.JSONDecodeError:
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'Invalid JSON in request body'})
            }
        
        # Validate required fields
        required_fields = ['table_name', 'key_name', 'key_value', 'update_data']
        missing_fields = [field for field in required_fields if field not in body]
        if missing_fields:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'error': f'Missing required fields: {", ".join(missing_fields)}'
                })
            }
        
        # Validate update_data is a dictionary
        if not isinstance(body['update_data'], dict):
            return {
                'statusCode': 400,
                'body': json.dumps({'error': 'update_data must be a dictionary'})
            }
        
        # Perform the update
        result = db_update(
            body['table_name'],
            body['key_name'],
            body['key_value'],
            body['update_data']
        )
        
        if result is None:
            return {
                'statusCode': 500,
                'body': json.dumps({'error': 'Failed to update item in database'})
            }
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Item updated successfully',
                'updated_item': result
            })
        }
        
    except Exception as e:
        logger.error(f"Error in lambda handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f'Internal server error: {str(e)}'})
        }
