import json
import boto3
from typing import Dict, Any, Union, Optional, Tuple
from botocore.exceptions import ClientError
import logging
from decimal import Decimal
from config import logger, AWS_REGION

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
lambda_client = boto3.client("lambda", region_name=AWS_REGION)
dynamodb = boto3.resource('dynamodb')
dynamodb_client = boto3.client('dynamodb', region_name=AWS_REGION)
sessions_table = dynamodb.Table('Sessions')

class AuthorizationError(Exception):
    """Custom exception for authorization failures"""
    pass

class UpdateValidationError(Exception):
    """Custom exception for update validation failures"""
    pass

class LambdaError(Exception):
    def __init__(self, status_code, message):
        self.status_code = status_code
        self.message = message
        super().__init__(f"[{status_code}] {message}")

class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)

def create_response(status_code, body, headers={}):
    return {
        "statusCode": status_code,
        "headers": headers,
        "body": json.dumps(body, cls=DecimalEncoder),
    }

def invoke_lambda(function_name, payload, invocation_type="RequestResponse"):
    """
    Invokes another Lambda function and returns the entire response payload.
    """
    try:
        logger.info(f"Invoking {function_name}...")
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType=invocation_type,
            Payload=json.dumps(payload),
        )
        response_payload = response["Payload"].read().decode("utf-8")
        if not response_payload:
            return {}
        return json.loads(response_payload)
    except ClientError as e:
        raise LambdaError(500, f"Failed to invoke {function_name}: {e.response['Error']['Message']}")
    except Exception as e:
        raise LambdaError(500, f"An unexpected error occurred invoking {function_name}: {e}")

def parse_event(event):
    response = invoke_lambda('ParseEvent', event)
    if response.get('statusCode') != 200:
        raise LambdaError(response.get('statusCode', 500), "Failed to parse event.")
    return json.loads(response.get('body', '{}'))

def authorize(user_id, session_id):
    response = invoke_lambda('Authorize', {'user_id': user_id, 'session_id': session_id})
    body = json.loads(response.get('body', '{}'))
    if response.get('statusCode') != 200 or not body.get('authorized'):
        raise LambdaError(response.get('statusCode', 401), body.get('message', 'ACS: Unauthorized'))

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
    response = invoke_lambda('RateLimitAWS', {'client_id': account_id, 'session': session_id})
    if response.get('statusCode') != 200:
        raise LambdaError(response.get('statusCode', 429), response.get('body', {}).get('message', 'Rate limit check failed.'))

def fetch_cors_headers():
    try:
        response = invoke_lambda('Allow-Cors', {})
        return response.get('headers', {})
    except Exception as e:
        logger.error(f"Failed to fetch CORS headers: {e}")
        return {} 