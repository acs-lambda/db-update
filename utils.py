import json
import boto3
from typing import Dict, Any, Union, Optional, Tuple
from botocore.exceptions import ClientError
import logging
from decimal import Decimal

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
lambda_client = boto3.client('lambda')
dynamodb = boto3.resource('dynamodb')
dynamodb_client = boto3.client('dynamodb')
sessions_table = dynamodb.Table('Sessions')

class AuthorizationError(Exception):
    """Custom exception for authorization failures"""
    pass

class UpdateValidationError(Exception):
    """Custom exception for update validation failures"""
    pass

def invoke(function_name: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Invoke a Lambda function by name with the given payload
    
    Args:
        function_name (str): Name of the Lambda function to invoke
        payload (Dict[str, Any]): Payload to send to the Lambda function
        
    Returns:
        Dict[str, Any]: Response from the Lambda function
        
    Raises:
        ClientError: If Lambda invocation fails
    """
    try:
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='RequestResponse',
            Payload=json.dumps(payload)
        )
        
        # Parse the response payload
        response_payload = json.loads(response['Payload'].read().decode('utf-8'))
        
        # If the Lambda function returned an error
        if 'FunctionError' in response:
            logger.error(f"Lambda function {function_name} returned an error: {response_payload}")
            raise ClientError(
                error_response={'Error': {'Message': response_payload.get('errorMessage', 'Unknown error')}},
                operation_name='InvokeLambda'
            )
            
        return response_payload
        
    except ClientError as e:
        logger.error(f"Failed to invoke Lambda function {function_name}: {str(e)}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse Lambda response for {function_name}: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error invoking Lambda function {function_name}: {str(e)}")
        raise

def parse_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse an event from either API Gateway or direct Lambda invocation
    
    Args:
        event (Dict[str, Any]): The event to parse, either from API Gateway or direct Lambda
        
    Returns:
        Dict[str, Any]: Parsed event data including body and cookies if present
        
    Raises:
        ClientError: If Lambda invocation fails
        Exception: If parsing fails
    """
    try:
        # Invoke the parse-event Lambda function
        response = invoke('ParseEvent', event)
        
        # Check if the parsing was successful
        if response['statusCode'] != 200:
            logger.error(f"Failed to parse event: {response['body']}")
            raise Exception(f"Failed to parse event: {response['body'].get('message', 'Unknown error')}")
            
        return response['body']
        
    except Exception as e:
        logger.error(f"Error parsing event: {str(e)}")
        raise

def authorize(user_id: str, session_id: str) -> None:
    """
    Authorize a user by invoking the authorize Lambda function
    
    Args:
        user_id (str): The user ID to validate
        session_id (str): The session ID to validate
        
    Returns:
        None
        
    Raises:
        AuthorizationError: If authorization fails
    """
    try:
        # Invoke the authorize Lambda function
        response = invoke('Authorize', {
            'user_id': user_id,
            'session_id': session_id
        })
        
        # Check if authorization was successful
        if response['statusCode'] != 200 or not response['body'].get('authorized', False):
            raise AuthorizationError(response['body'].get('message', 'ACS: Unauthorized'))
            
    except ClientError as e:
        logger.error(f"Lambda invocation error during authorization: {str(e)}")
        raise AuthorizationError("ACS: Unauthorized")
    except Exception as e:
        logger.error(f"Unexpected error during authorization: {str(e)}")
        raise AuthorizationError("ACS: Unauthorized")

def validate_update_data(table_name: str, update_data: Dict[str, Any]) -> None:
    """
    Validate update data against table schema and constraints
    
    Args:
        table_name (str): Name of the DynamoDB table
        update_data (Dict[str, Any]): Data to validate for update
        
    Raises:
        UpdateValidationError: If validation fails
    """
    try:
        # Get table description
        table_description = dynamodb_client.describe_table(TableName=table_name)
        attribute_definitions = table_description['Table']['AttributeDefinitions']
        
        # Create a map of attribute names to their types
        attr_types = {attr['AttributeName']: attr['AttributeType'] for attr in attribute_definitions}
        
        # Validate each attribute in update_data
        for attr_name, attr_value in update_data.items():
            if attr_name not in attr_types:
                raise UpdateValidationError(f"Attribute {attr_name} does not exist in table {table_name}")
            
            # Validate attribute type
            expected_type = attr_types[attr_name]
            if expected_type == 'S' and not isinstance(attr_value, str):
                raise UpdateValidationError(f"Attribute {attr_name} must be a string")
            elif expected_type == 'N' and not isinstance(attr_value, (int, float, Decimal)):
                raise UpdateValidationError(f"Attribute {attr_name} must be a number")
            elif expected_type == 'B' and not isinstance(attr_value, bytes):
                raise UpdateValidationError(f"Attribute {attr_name} must be binary")
                
    except ClientError as e:
        logger.error(f"Failed to validate update data: {str(e)}")
        raise UpdateValidationError(f"Failed to validate update data: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error during update validation: {str(e)}")
        raise UpdateValidationError(f"Unexpected error during update validation: {str(e)}")

def check_rate_limit(account_id: str, session_id: str) -> Tuple[bool, Optional[str]]:
    """
    Check if the account has exceeded its rate limit
    
    Args:
        account_id (str): The account ID to check
        session_id (str): The session ID for authentication
        
    Returns:
        Tuple[bool, Optional[str]]: (is_allowed, error_message)
    """
    try:
        response = invoke('RateLimitAWS', {
            'client_id': account_id,
            'session': session_id
        })
        
        if response.get('statusCode') == 429:
            return False, "Rate limit exceeded"
        elif response.get('statusCode') == 401:
            return False, "Unauthorized"
        elif response.get('statusCode') != 200:
            return False, "Rate limit check failed"
            
        return True, None
        
    except Exception as e:
        logger.error(f"Error checking rate limit: {str(e)}")
        return False, f"Rate limit check failed: {str(e)}"

def fetch_cors_headers() -> Dict[str, str]:
    """
    Fetch CORS headers from the Allow-Cors Lambda function
    
    Returns:
        Dict[str, str]: CORS headers
    """
    try:
        response = invoke('Allow-Cors', {})
        return response.get('headers', {})
    except Exception as e:
        logger.error(f"Failed to fetch CORS headers: {str(e)}")
        return {} 