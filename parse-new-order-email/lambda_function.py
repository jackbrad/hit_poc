import boto3
import json
import email
import os
from email import policy
import re
from datetime import datetime

ORDERS_BY_EMAIL_BUCKET = os.environ['ORDERS_BY_EMAIL_BUCKET']
ORDERS_BUCKET = os.environ['ORDERS_BUCKET']
ORDERS_METADATA_BUCKET= os.environ['ORDERS_METADATA_BUCKET']

EmailKey = ""

s3 = boto3.client('s3')

def lambda_handler(event, context):
  
  #start the order process if a new order comes via email.
  print(json.dumps(event))
  EmailKey = event['Records'][0]['s3']['object']['key']
  print('S3 order order via email key:' + EmailKey)
  
  #get the email    
  s3_raw_email = s3.get_object(Bucket=ORDERS_BY_EMAIL_BUCKET, Key=EmailKey)
  raw_email_str = s3_raw_email['Body'].read().decode('utf-8')
  raw_email = email.parser.Parser(policy=policy.strict).parsestr(raw_email_str)
  
  #get the attachments from the mail
  attachment_index = 0
  attachments = []
  now = datetime.now()
  dt_string = now.strftime('%d/%m/%Y %H:%M:%S')
  
  #get the attachments and store them in the orders folder
  for part in raw_email.walk():
    if part.is_attachment():
      if len(part.get_filename()):
        attachment_key = EmailKey + '/' + part.get_filename()
        attachment_index += 1
      else:
        attachment_id = str(attachment_index)
        attachment_key = EmailKey + '/' + attachment_key
        attachment_index += 1
        
      print('saving attachement:' + part.get_filename())
       
      #save attachments to s3 bucket
      s3.put_object(Bucket=ORDERS_BUCKET,Key=attachment_key, Body=part.get_content())

      attachments.append({
        'attachment_id': attachment_key,
        'content_type': part.get_content_type(),
        'key': attachment_key,
        })
  
  #create a meta data document about the order 
  item = {}
  item['email_id'] = EmailKey
  item['event_object_key'] = EmailKey
  item['new_order_uuid'] = EmailKey
  item['source'] = 'email'
  item['subject'] = raw_email["subject"]
  item['from'] = raw_email["From"]
  item['timestamp'] = dt_string
 
  item['email-attachments'] = attachments
  item['named-values-inference'] = ''
  item['receipt-schema-name-values'] = ''
  item['llm-summary'] = ''
  item['image-files'] = ''
  item['logo-text-inference-strings'] = ''
  
  #json the order file storage data
  Document={}
  S3Object={}
  FeatureTypes= ['FORMS']
  item['FeatureTypes'] = FeatureTypes
  S3Object['Bucket'] = ORDERS_BUCKET
  S3Object['Name'] = attachment_key
  #we only want to add the jpg to the item key for futher processing
  if ".pdf" in  attachment_key:
      S3Object['Name'] = re.sub(r'\.pdf$', '.jpg', attachment_key)
  Document['S3Object'] = S3Object
  item['Document'] = Document
  
  #Save information about the iteam we just collected. 
  s3.put_object(
    Bucket=ORDERS_BUCKET, 
    ContentType='application/json',
    Key= EmailKey + '/order.json',
    Body=json.dumps(item))
    
     
  s3.put_object(
  Bucket=ORDERS_METADATA_BUCKET, 
  ContentType='application/json',
  Key= EmailKey + '.json',
  Body=json.dumps(item))
  

  return item
