import os
import json
import boto3
import logging
import traceback
import psycopg2

from botocore.exceptions import ClientError

kinesis_client = boto3.client('kinesis')
rds_client = boto3.client('rds')

def get_db_credentials():
    credential = {}

    secret_name = os.environ['SECRET_NAME']
    region = os.environ['REGION']

    secrets_client = boto3.client(service_name='secretsmanager', region_name=region)

    try:
        secret_response = secrets_client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        logging.error("ERROR: Unexpected error: Could not fetch secret from Secrets Manager")
        logging.error(e)

    secret_str = json.loads(secret_response['SecretString'])

    credential['DB_USER'] = secret_str['DB_USER']
    credential['DB_PASSWORD'] = secret_str['DB_PASSWORD']
    credential['DB_PROXY_HOST'] = secret_str['DB_PROXY_HOST']
    credential['DB_NAME'] = secret_str['DB_NAME']

    return credential

try:
    credentials = get_db_credentials()

    database_conn = psycopg2.connect(
        host=credentials['DB_PROXY_HOST'],
        database=credentials['DB_NAME'],
        user=credentials['DB_USER'],
        password=credentials['DB_PASSWORD']
    )
    logging.info("SUCCESS: Connection to RDS Proxy for Aurora Postgres established.")
except psycopg2.Error as e:
    logging.error("ERROR: Unexpected error: Could not connect to Postgres RDS Proxy")
    logging.error(e)

def lambda_handler(event, context):
    try:
        data = json.loads(event['body'])

        # consume records from kinesis
        # stream_name=os.environ.get('KINESIS_STREAM_NAME'),
        # stream_arn=os.environ.get('KINESIS_STREAM_ARN')

        # perform sentiment analysis with vader

        # save results to aurora
        with database_conn.cursor() as cur:
            cur.execute("select * from news")
            for row in cur.fetchall():
                logging.info(row)
        
        database_conn.commit()

        return True
    except Exception as e:
        traceback.print_exc()
        return str(e)
