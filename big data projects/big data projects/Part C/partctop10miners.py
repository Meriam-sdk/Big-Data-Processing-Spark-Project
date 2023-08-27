import os
import json
from datetime import datetime

from pyspark.sql import SparkSession

import boto3

if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder.appName("Ethereum").getOrCreate()

    # Function to check if a line is a valid block
    def check_blocks(line):
        try:
            fields = line.split(',')
            if len(fields) == 19 and fields[1] != 'hash':
                return True
            else:
                return False
        except:
            return False

    # Function to extract miner and size from a block
    def features_blocks(line):
        try:
            fields = line.split(',')
            miner = str(fields[9])
            size = int(fields[12])
            return (miner, size)
        except:
            return (0, 1)

    # Set S3 configuration properties
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
    s3_endpoint_url = os.environ['S3_ENDPOINT_URL'] + ':' + os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket_name = os.environ['BUCKET_NAME']
    
    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false") 

    # Read in the blocks data from S3
    blocks = spark.sparkContext.textFile("s3a://{}/ECS765/ethereum-parvulus/blocks.csv".format(s3_data_repository_bucket))
    
    # Filter out invalid blocks
    clean_blocks = blocks.filter(check_blocks)
    
    # Extract miner and size from valid blocks
    bf = clean_blocks.map(features_blocks)
    
    # Reduce by key to get total size per miner
    blocks_reduced = bf.reduceByKey(lambda a, b: a + b)
    
    # Take the top 10 miners by size
    top10_miners = blocks_reduced.takeOrdered(10, key=lambda l: -l[1])
    
    # Upload the top 10 miners data to S3
    s3 = boto3.resource('s3', endpoint_url='http://' + s3_endpoint_url,
                        aws_access_key_id=s3_access_key_id, aws_secret_access_key=s3_secret_access_key)
    now = datetime.now()
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
    object_name = 'ethereum_partc_top10miners_{}/top10_miners.txt'.format(date_time)
    s3.Object(s3_bucket_name, object_name).put(Body=json.dumps(top10_miners))
    
    # Stop the SparkSession
    spark.stop()

    
    
    
    
    
    
    