import os
import json
import boto3
import logging
import traceback
import pg8000.dbapi

logger = logging.getLogger()

kinesis_client = boto3.client('kinesis')

try:
    logger.info("INFO: Attempting to connect to RDS Proxy for Aurora Postgres..")

    database_conn = pg8000.dbapi.connect(
        host=os.environ['DB_HOST'],
        database=os.environ['DB_NAME'],
        user=os.environ['DB_USER'], 
        password=os.environ['DB_PASSWORD']
    )

    logger.info("SUCCESS: Connection to RDS Proxy for Aurora Postgres established.")
except pg8000.Error as e:
    logger.error("ERROR: Unexpected error: Could not connect to Postgres RDS Proxy")
    logger.error(e)

def lambda_handler(event, context):
    try:
        # data = json.loads(event['body'])

        # consume records from kinesis
        # stream_name=os.environ.get('KINESIS_STREAM_NAME'),
        # stream_arn=os.environ.get('KINESIS_STREAM_ARN')

        # perform sentiment analysis with vader

        # save results to aurora
        cur = database_conn.cursor()
        try:
            cur.execute("select * from news")
            for row in cur.fetchall():
                logger.info(row)
        finally:
            cur.close()  # Ensure closure even if exceptions occur
        
        database_conn.commit()

        return True
    except Exception as e:
        traceback.print_exc()
        return str(e)
