import boto3
import json
import os

data_dir = '/tmp/data/'
base_url = 'https://archive.sensor.community/'
if not os.path.exists(data_dir):
        os.mkdir(data_dir)


def handler(event, context):
    bucket_name = event["ResultWriterDetails"]["Bucket"]
    key = event["ResultWriterDetails"]["Key"]
    filenale = key.split("/")[-1]

    bucket = boto3.resource("s3").Bucket(bucket_name)
    bucket.download_file(key, data_dir + filenale)
    with open(data_dir + filenale, "r") as f:
        data = json.load(f)
    os.remove(data_dir + filenale)

    if data['ResultFiles']["FAILED"] != []:
        # Data is not processed correctly
        return
    
    succeded = data['ResultFiles']["SUCCEEDED"][0]['Key']
    filename = succeded.split("/")[-1]

    bucket.download_file(succeded, data_dir + filename)
    with open(data_dir + filename, "r") as f:
        data = json.load(f)
    os.remove(data_dir + filename)
    
    input = json.loads(data[0]['Input'])
    link = input['Items'][0]
    filename = link.split("/")[-1]
    print(f"Link: {link}")
    # remove base_url and filename from link
    link = link.replace(base_url, "")
    folder = link.replace(filename, "")
    print(f"Folder: {folder}")
     
    mark_folder(folder)
    clear_file_list(folder, bucket_name)


def mark_folder(folder: str):
    """
    Set  DynamoDB item with key=folder processed field to True
    """
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('folders')
    table.update_item(
        Key={'folder': folder},
        UpdateExpression="set #name = :val",
        ExpressionAttributeValues={':val': True},
        ExpressionAttributeNames={'#name': 'processed'}
    )
    print(f"Marked folder {folder} as processed")
    
def clear_file_list(folder, bucket_name):
    """Remove file list from S3 (temporal file)"""
    prefix = "file_list/"
    bucket = boto3.resource("s3").Bucket(bucket_name)
    bucket.delete_objects(
        Delete={
            'Objects': [
                {
                    'Key': prefix+folder[:-1]+".json"
                }
            ]
        }
    )
    print(f"file-list for {folder} removed")