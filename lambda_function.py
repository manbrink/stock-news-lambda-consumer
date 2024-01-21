import os
import json
import base64
import boto3
import logging
import traceback

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import pg8000.dbapi

logger = logging.getLogger()
logger.setLevel(logging.INFO)

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
        db_batch_size = 20
        cur = database_conn.cursor()

        final_records = []

        analyzer = SentimentIntensityAnalyzer()

        # Load the Kinesis records and perform sentiment analysis
        for record in event['Records']:
            try:
                logger.info("INFO: Attempting to decode and read Kinesis records..")

                record_data = json.loads(base64.b64decode(record['kinesis']['data']).decode('utf-8'))

                vs = analyzer.polarity_scores(record_data['title'])
                record_data['compound_score'] = vs['compound']

                final_records.append(tuple(record_data.values()))

                logger.info("SUCCESS: Kinesis data decoded, sentiment analysis completed.")
            except Exception as e:
                logger.error("ERROR: Unexpected error: something happened when reading from Kinesis or performing sentiment analysis.")
                logger.error(e)

        # Save the final records to Aurora cluster via RDS Proxy
        try:
            logger.info("INFO: Attempting to save records to Aurora..")

            for i in range(0, len(final_records), db_batch_size):
                batch = final_records[i:i + db_batch_size]

                cur.executemany("""
                    INSERT INTO news (symbol, collection_time, title, link, publisher, compound_score, modtime)
                    VALUES (%s, %s, %s, %s, %s, %s, NOW())
                """, batch)
            
            logger.info("SUCCESS: Data inserted in Aurora.")
        finally:
            cur.close()
        
        database_conn.commit()

        return True
    except Exception as e:
        traceback.print_exc()
        logger.error(e)
