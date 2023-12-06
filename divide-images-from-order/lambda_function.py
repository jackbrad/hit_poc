
#Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#PDX-License-Identifier: MIT-0 (For details, see https://github.com/awsdocs/amazon-rekognition-custom-labels-developer-guide/blob/master/LICENSE-SAMPLECODE.)
import json
import boto3 
import io
from PIL import Image, ImageDraw, ExifTags, ImageColor, ImageFont
import os
 
#environment settings
IMAGE_OUTPUT_BUCKET = os.environ['IMAGE_OUTPUT_BUCKET']
MODEL = os.environ['MODEL']
CONFIDENCE_REQUIRED = int(os.environ['CONFIDENCE_REQUIRED'])
ORDERS_BUCKET = os.environ['ORDERS_BUCKET']

def lambda_handler(event, context):
    #load the meta_data file
    order_meta_document = event['detail']['object']['key']
    order_meta_bucket = event['detail']['bucket']['name']
    
    #read json doc from s3
    s3 = boto3.resource('s3')
    meta_document = s3.Object(order_meta_bucket, order_meta_document)
    file_content = meta_document.get()['Body'].read().decode('utf-8')
    meta_file = json.loads(file_content)
    print(json.dumps(meta_file))
    #get the doc to process for imagery
    image_storage_meta = meta_file['Document']
    #process 
    imgsegs = find_and_save_image_segments(MODEL,image_storage_meta, CONFIDENCE_REQUIRED)
    print(json.dumps(imgsegs))
    meta_file['image-files'] = imgsegs
  
    return {
        'statusCode': 200,
        'body': meta_file
    }

def find_and_save_image_segments(model,s3_pathJson, min_confidence):
     
    print("Order files to process:" + json.dumps(s3_pathJson))
    client=boto3.client('rekognition')
   
    #Call DetectCustomLabels
    response = client.detect_custom_labels(Image=s3_pathJson,
        MinConfidence=min_confidence,
        ProjectVersionArn=model)
        
    #save images detected
    img_pieces = save_image_segments(s3_pathJson['S3Object']['Bucket'],s3_pathJson['S3Object']['Name'],response)
   
    return img_pieces


def save_image_segments(bucket,photo,response):
    imgs = []
    
    # Load image from S3 bucket
    s3_connection = boto3.resource('s3')

    s3_object = s3_connection.Object(bucket,photo)
    s3_response = s3_object.get()

    stream = io.BytesIO(s3_response['Body'].read())
    image=Image.open(stream)

    # Ready image to draw bounding boxes on it.
    imgWidth, imgHeight = image.size
    draw = ImageDraw.Draw(image)
    i=0
    # calculate and display bounding boxes for each detected custom label
    for customLabel in response['CustomLabels']:
        i=i+1
        if 'Geometry' in customLabel:
            box = customLabel['Geometry']['BoundingBox']
            left = imgWidth * box['Left']
            top = imgHeight * box['Top']
            width = imgWidth * box['Width']
            height = imgHeight * box['Height']

            fnt = ImageFont.load_default()
            draw.text((left,top), customLabel['Name'], fill='#00d400', font=fnt)

            points = (
                (left,top), 
                (left + width, top),
                (left + width, top + height),
                (left , top + height),
                (left, top))
            
            #draw a box around the label        
            draw.line(points, fill='#00d400', width=5)

            # Setting the points for cropped image
            right = left + width
            bottom = top + height
            
            # Cropped image of above dimension and save it to s3
            piece = image.crop((left, top, right, bottom))
            file_name = file_name = photo.split('/')[0] + '/piece' + str(i) + '.jpg';
            
            upload_image(img=piece,bucket=bucket,file_name = file_name)
            imgs.append({
                "bucket": bucket,
                "file_name": file_name
            })
            
    return  imgs
            
def upload_image(img, bucket, file_name):
  
    # Save the image to an in-memory file
    in_mem_file = io.BytesIO()
    img.save(in_mem_file, format='JPEG')
    in_mem_file.seek(0)
    
    s3 = boto3.client('s3')
    # Upload image to s3
    s3.upload_fileobj(
        in_mem_file, 
        bucket,
        file_name
    )