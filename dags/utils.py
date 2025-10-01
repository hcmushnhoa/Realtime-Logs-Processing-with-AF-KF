

from confluent_kafka import Producer
import logging, json,os
import boto3
from dotenv import load_dotenv

load_dotenv()
aws_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
aws_region = os.getenv("AWS_DEFAULT_REGION")
logger=logging.getLogger(__name__)

def get_secret(secret_name,region_name = aws_region):

    # Create a Secrets Manager client
    session = boto3.session.Session()
    # create client for secretsmanager service to call api secrets manager
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name,
        aws_access_key_id=aws_key,
        aws_secret_access_key=aws_secret
    )
    try:
        # SecretString or SecretBinary or Json string be saved with type dict{}
        response = client.get_secret_value(SecretId=secret_name) 
        #return json.loads(response['SecretString'])
    except Exception as e:
        logger.error(f"Secret retries error: {e}")
        raise

    if 'SecretString' in response and response['SecretString'] is not None:
        try:
            return json.loads(response['SecretString'])
        except json.JSONDecodeError:
            return response['SecretString']
    elif 'SecretBinary' in response and response['SecretBinary'] is not None:
        return response['SecretBinary']  # bytes    
    