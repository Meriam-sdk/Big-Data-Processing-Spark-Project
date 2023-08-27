import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("PartD") \
        .getOrCreate()

    # shared read-only object bucket containing datasets
    s3_data_repository_bucket = os.environ['DATA_REPOSITORY_BUCKET']
    s3_endpoint_url = os.environ['S3_ENDPOINT_URL'] + ':' + os.environ['BUCKET_PORT']
    s3_access_key_id = os.environ['AWS_ACCESS_KEY_ID']
    s3_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
    s3_bucket = os.environ['BUCKET_NAME']

    hadoopConf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoopConf.set("fs.s3a.endpoint", s3_endpoint_url)
    hadoopConf.set("fs.s3a.access.key", s3_access_key_id)
    hadoopConf.set("fs.s3a.secret.key", s3_secret_access_key)
    hadoopConf.set("fs.s3a.path.style.access", "true")
    hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")
    
    
    def calculate_bits(block):
        indexes = {4,5,6,7,8,12}
        useless_bits = 0 
        
        for i,field in enumerate(block):
            if i in indexes:
                useless_bits += len(field[2:])*4
        
        return ("wasted_bits",useless_bits)
    
    blocks = spark.read.option("header", True).csv("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/blocks.csv").rdd
    
    # Calculate the wasted bits for each block and sum them up
    total_wasted_bits = blocks.map(calculate_bits).reduceByKey(lambda x,y: x+y).collect()[0][1]
    
    # Save the result to a file in your S3 bucket
    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)
    
    date_time = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    my_result_object = my_bucket_resource.Object(bucket_name=s3_bucket, key='ethereum_overhead' + date_time + '/sha3_uncles.txt')
    my_result_object.put(Body=json.dumps(total_wasted_bits))
    
    spark.stop()

