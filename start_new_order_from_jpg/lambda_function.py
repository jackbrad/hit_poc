import boto3
import json
import os
import re
from datetime import datetime
import uuid

ORDERS_DROP_BUCKET = os.environ['ORDERS_DROP_BUCKET']
ORDERS_BUCKET = os.environ['ORDERS_BUCKET']
ORDERS_METADATA_BUCKET= os.environ['ORDERS_METADATA_BUCKET']
s3 = boto3.client('s3')
s3res = boto3.resource('s3')

event_object_key = ""
new_order_uuid = uuid.uuid4().hex
now = datetime.now()
dt_string = now.strftime('%d/%m/%Y %H:%M:%S')

def lambda_handler(event, context):
  #start the order process if a new order comes via drop
  event_object_key = event['Records'][0]['s3']['object']['key']
  print('S3 order dropped:' + event_object_key)

  order_storage_key = new_order_uuid + '/' + event_object_key

  #save order to s3 bucket with new order prefix
  
  copy_source = {
        'Bucket': ORDERS_DROP_BUCKET,
        'Key': event_object_key
      }

  order_bucket = s3res.Bucket(ORDERS_BUCKET)
  order_bucket.copy(copy_source, order_storage_key)
  
  #create a meta data document about the order 
  item = {}
  item["email_id"] = ''
  item["event_object_key"] = event_object_key
  item["new_order_uuid"] = new_order_uuid
  item["source"] = 'dropped'
  item["subject"] = ''
  item["from"] = ''
  item["timestamp"] = dt_string
  item["email-attachments"] = ''
  item["named-values-inference"] = ''
  item["order_text"] = ''
  item["llm-summary"] = ''
  item["image-files"] = ''
  item["logo-text-inference-strings"] = ''
  
  #json the order file storage data
  Document={}
  S3Object={}
  FeatureTypes= ['FORMS']
  item["FeatureTypes"] = FeatureTypes
  S3Object["Bucket"] = ORDERS_BUCKET
  S3Object["Name"] = order_storage_key
  Document["S3Object"] = S3Object
  item["Document"] = Document
  
  #Save information about the iteam we just collected. 
  s3.put_object(
    Bucket=ORDERS_BUCKET, 
    ContentType='application/json',
    Key= new_order_uuid + '/order.json',
    Body=json.dumps(item))
    
  s3.put_object(
  Bucket=ORDERS_METADATA_BUCKET, 
  ContentType='application/json',
  Key= new_order_uuid + '.json',
  Body=json.dumps(item))
  

  return item
