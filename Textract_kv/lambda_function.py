import boto3
import sys
import re
import json
from collections import defaultdict

def lambda_handler(event, context):
    #process the file()
    print(event)
    ret = proc_file(event)
    return {
        'statusCode': 200,
        'body': ret
    }

def proc_file(evt):
    
    #textract for key_map_doc
    key_map, value_map, block_map = get_kv_map(evt)

    # Get Key Value relationship
    kvs = get_kv_relationship(key_map, value_map, block_map)
    
    print(json.dumps(kvs))
    
    print("\n\n== FOUND KEY : VALUE pairs ===\n")
    print_kvs(kvs)
    
    return json.dumps(kvs, indent=2)
    


def get_kv_map(evt):
    
    # process using image bytes
    session = boto3.Session()
    client = session.client('textract', region_name='us-east-1')
    
    bucket = evt['Document']['S3Object']['Bucket']
    document = evt['Document']['S3Object']['Name']
    
    
    response = client.analyze_document(Document={'S3Object': {'Bucket': bucket, 'Name': document}},
                                       FeatureTypes=["FORMS"]
                                       )
    
    # Get the text blocks
    blocks = response['Blocks']

    # get key and value maps
    key_map = {}
    value_map = {}
    block_map = {}
    for block in blocks:
        block_id = block['Id']
        block_map[block_id] = block
        if block['BlockType'] == "KEY_VALUE_SET":
            if 'KEY' in block['EntityTypes']:
                key_map[block_id] = block
            else:
                value_map[block_id] = block

    return key_map, value_map, block_map


def get_kv_relationship(key_map, value_map, block_map):
    kvs = defaultdict(list)
    for block_id, key_block in key_map.items():
        value_block = find_value_block(key_block, value_map)
        key = get_text(key_block, block_map)
        val = get_text(value_block, block_map)
        kvs[key].append(val)
        print(key)
    return kvs


def find_value_block(key_block, value_map):
    for relationship in key_block['Relationships']:
        if relationship['Type'] == 'VALUE':
            for value_id in relationship['Ids']:
                value_block = value_map[value_id]
    return value_block


def get_text(result, blocks_map):
    text = ''
    if 'Relationships' in result:
        for relationship in result['Relationships']:
            if relationship['Type'] == 'CHILD':
                for child_id in relationship['Ids']:
                    word = blocks_map[child_id]
                    if word['BlockType'] == 'WORD':
                        text += word['Text'] + ' '
                    if word['BlockType'] == 'SELECTION_ELEMENT':
                        if word['SelectionStatus'] == 'SELECTED':
                            text += 'X '

    return text


def print_kvs(kvs):
    for key, value in kvs.items():
        print(key, ":", value)




