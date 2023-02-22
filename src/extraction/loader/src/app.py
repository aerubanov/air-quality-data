import json
import os
import boto3
import botocore
import httpx

s3 = boto3.client("s3")
bucket = "staging-area-bucket" #TODO: get from env var

index_folder = "file_index/new/"
processed_folder = "file_index/processed/"
files_folder = "files/new/"
data_dir = "/tmp/data/"
if not os.path.exists(data_dir):
    os.mkdir(data_dir)


def handler(event, context):
    print(f"Event: {event}")
    print(f"Context: {context}")
    link = event["filename"]
    load_file(link)
    

def load_file(link: str):
    """
    link like: https://archive.sensor.community/2015-10-01/2015-10-01_ppd42ns_sensor_27.csv
    """
    resp = httpx.get(link)
    data = resp.content.decode()
    path = data_dir + link.split("/")[-1]
    data_object = files_folder + link.split("/")[-1]
    with open(path, 'w') as f:
        f.write(data)
    try:
        s3.upload_file(path, bucket, data_object)
        return data_object        
    except botocore.exceptions.ClientError as e:
        print(e)
        raise e
    finally:
        os.remove(path)

if __name__ == "__main__":
    load_file("https://archive.sensor.community/2016-10-102016-10-10_sds011_sensor_183.csv")