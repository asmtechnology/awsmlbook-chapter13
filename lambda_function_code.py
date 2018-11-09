import json
import boto3
import os
import sys
import uuid
import logging

def lambda_handler(event, context):
	
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    comprehend_client = boto3.client('comprehend')
    
    logger.info('Found event{}'.format(event))
    
    for record in event['Records']:
        # Read the value of the eventSource attribute. 
        #
        # You can use this to conditionally handle events 
        # from different triggers in the same lambda function.
        event_source = record['eventSource']
        logger.info(event_source)
        
        # read S3 bucket and object key
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key'] 
        
        # read the contents of the document uploaded to S3
        obj = boto3.resource('s3').Object(bucket, key)
        data = obj.get()['Body'].read().decode('utf-8') 
        
        # use Amazon comprehend to detect entities in the text
        entities = comprehend_client.detect_entities(Text=data, LanguageCode='en')

        #write results of entity analysis
        output_bucket = 'awsml-comprehend-entitydetection-result'
        output_key = 'entityanalysis-' + key
        output_obj = boto3.resource('s3').Object(output_bucket,output_key)
        output_obj.put(Body=json.dumps(entities))

    # return the entities that were detected.
    return {
        'statusCode': 200,
        'outputBucket': output_bucket
    }
