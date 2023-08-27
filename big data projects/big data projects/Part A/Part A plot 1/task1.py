import os
import operator
import time
import json

from datetime import datetime
from pyspark.sql import SparkSession

import boto3

def check_transaction(line):
    """Checks if a line of CSV data representing an Ethereum transaction
    has the expected format and field types.

    Args:
        line (str): A string containing comma-separated values.

    Returns:
        bool: True if the line has the expected format and field types, False otherwise.
    """
    try:
        fields = line.split(',')
        if len(fields) != 15:
            return False
        int(fields[11])
        return True
    except:
        return False

if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder.appName("Ethereum").getOrCreate()

    # Set up Amazon S3 credentials
    s3_endpoint_url = os.environ['S3_ENDPOINT_URL'] + ':' + os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
    s3_bucket_name = os.environ['BUCKET_NAME']

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoop_conf.set("fs.s3a.access.key", s3_access_key_id)
    hadoop_conf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoop_conf.set("fs.s3a.path.style.access", "true")
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")

    # Read Ethereum transaction data from an S3 bucket
    t = spark.sparkContext.textFile(f"s3a://{s3_data_repository_bucket}/ECS765/ethereum-parvulus/transactions.csv")
    clean_lines = t.filter(check_transaction)

    # Map Ethereum transactions to their month and count the number of transactions for each month
    transactions = clean_lines.map(lambda b: (time.strftime("%m/%Y", time.gmtime(int(b.split(',')[11]))), 1))
    total_transactions = transactions.reduceByKey(operator.add)

    # Save the total number of transactions by month to a JSON file in an S3 bucket
    s3_client = boto3.client('s3',
        endpoint_url=f'http://{s3_endpoint_url}',
        aws_access_key_id=s3_access_key_id,
        aws_secret_access_key=s3_secret_access_key)
    
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
    s3_key = f'ethereum_{date_time}/transactions_total.txt'
    s3_body = json.dumps(total_transactions.take(100))

    s3_client.put_object(Bucket=s3_bucket_name, Key=s3_key, Body=s3_body)

    print(total_transactions.take(100))
    
    # Stop the SparkSession
    spark.stop()
