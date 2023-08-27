import sys, string
import os
import socket
import time
import operator
import boto3
import json
from pyspark.sql import SparkSession
from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql.functions import sum


if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("Ethereum")\
        .getOrCreate()

    def check_transactions(line):
        try:
            fields = line.split(',')
            if len(fields) != 15:
                return False
            if fields[5] == fields[6]:
                return False
            float(fields[7])
            return True
        except:
            return False

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

    transactions = spark.sparkContext.textFile("s3a://" + s3_data_repository_bucket + "/ECS765/ethereum-parvulus/transactions.csv")
    trans = transactions.filter(check_transactions)
    
    # Define trans_map before using it
    trans_map = trans.map(lambda x: (x.split(',')[5], x.split(',')[6], x.split(',')[7], float(x.split(',')[7])))

    # Convert RDD to DataFrame
    trans_df = trans_map.toDF(["from_address", "value", "value_float"])

    # Group and aggregate using DataFrame API
    trans_agg = trans_df.groupBy(["from_address"]).agg(F.sum("value_float").alias("total_value"))

    # Filter, sort, and take top 10
    trans_filtered = trans_agg.filter(F.col("total_value") > 0)
    trans_sorted = trans_filtered.sort(F.col("total_value").desc())
    top10 = trans_sorted.take(10)

   

    

    my_bucket_resource = boto3.resource('s3',
            endpoint_url='http://' + s3_endpoint_url,
            aws_access_key_id=s3_access_key_id,
            aws_secret_access_key=s3_secret_access_key)

    now = datetime.now()
    date_time = now.strftime("%d-%m-%Y_%H:%M:%S")
    
    my_result_object = my_bucket_resource.Object(s3_bucket, 'ethereum_partd3_' + date_time + '/top10_washtrading_addresses.txt')
    my_result_object.put(Body=json.dumps(top10))

    spark.stop()
