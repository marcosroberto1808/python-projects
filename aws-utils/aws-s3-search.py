#!/usr/bin/python3
# coding: utf-8
__author__ = 'Marcos Roberto - marcos.roberto@hp.com'

from botocore.client import Config
from botocore.exceptions import ClientError
from datetime import datetime
import botocore
import boto3
import logging

# Setup Logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)  # or any level you want

# OnScreen Log Output
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)  # or any other level
logger.addHandler(ch)

# Log File Output
timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
fh = logging.FileHandler('aws-s3-search-{}.log'.format(timestamp))
fh.setLevel(logging.INFO)  # or any level you want
logger.addHandler(fh)

# Boto3 Timeout Config
config = Config(connect_timeout=10, retries={'max_attempts': 5})

# Objects S3 Instances
s3client = boto3.client('s3', config=config)
s3resource = boto3.resource('s3', config=config)
buckets = s3client.list_buckets()['Buckets']


# Filter buckets
def list_in_list(a, b):
    return any((True for x in a if x in b))


# List of exceptions
exceptions = [
    "audit",
    "cloudtrail",
    "ecr-test-jay",
    "qa-cats-test",
    "qa-sdm.ostore.sit-test-bucket",
    "tiago-melo-test-restrict-access"
]

filtered_buckets = list(filter(lambda item: not list_in_list(exceptions, item['Name']), buckets))


# Function to list all buckets
def list_all_buckets():
    for bucket in filtered_buckets:
        logger.info('Bucket Name: {}, Created on: {}'.format(bucket['Name'], bucket['CreationDate']))
        get_bucket_size(bucket['Name'])
    logger.info('Total S3 Buckets: {}'.format(len(buckets)))
    logger.info('Total Filtered Buckets: {}'.format(len(filtered_buckets)))
    return


# Function to get bucket objects count and total size
def get_bucket_size(bucket_name):
    try:
        size = 0
        count = 0
        s3bucket = s3resource.Bucket(bucket_name)
        for key in s3bucket.objects.all():
            count += 1
            size += key.size
        logger.info("Total Bucket Size: {:.3f} GB".format(size * 1.0 / 1024 / 1024 / 1024))
        logger.info("Total Objects Count: {}".format(count))
        logger.info(" ")
        return
    except botocore.exceptions.ClientError as error:
        logger.warning("{}, BucketName: {}".format(error, bucket_name))
        logger.warning(" ")
        pass


# Run list_all_buckets function
list_all_buckets()
