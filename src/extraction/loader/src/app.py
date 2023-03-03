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
headers = {
    "Host": "archive.sensor.community",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/109.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}


def handler(event, context):
    print(f"Event: {event}")
    print(f"Context: {context}")
    for link in event["Items"]:
        load_file(link)
    

def load_file(link: str):
    """
    link like: https://archive.sensor.community/2015-10-01/2015-10-01_ppd42ns_sensor_27.csv
    """
    resp = httpx.get(link, headers=headers)
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