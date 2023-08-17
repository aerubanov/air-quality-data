import os
import boto3
from typing import Callable

from loading import load_sensor_data, load_location_data, load_time_data

bucket_name = os.environ['S3_BUCKET']
bucket = boto3.resource('s3').Bucket(bucket_name)
data_dir = '/tmp/data'
if not os.path.exists(data_dir):
    os.makedirs(data_dir)
 

def handler(event, context):
    data = []
    print(event)
    for item in event["Items"]:
        key = item['Key']
        filename = key.split('/')[-1]
        prefix = key.split('/')[-2]
        data.append(read_data(filename, prefix))
    loading_fn = select_loading_function(prefix)
    loading_fn(data)
    print(f"Successfully loaded {len(data)} records into dimension table")
    keys = [item["Key"] for item in event["Items"]]
    remove_s3_files(keys)
    print(f"Successfully removed {len(keys)} records from s3 bucket")
    return {
        "statusCode": 200,
    }


def select_loading_function(prefix: str) -> Callable:
    return {
            "time": load_time_data,
            "sensors": load_sensor_data,
            "locations": load_location_data,
        }.get(prefix)


def read_data(filename: str, prefix: str) -> dict:
    """Read data from s3 and return a dictionary"""
    bucket.download_file(prefix+'/'+filename, os.path.join(data_dir, filename))
    # read header and values from data file
    with open(os.path.join(data_dir, filename), 'r') as f:
        header = f.readline()
        values = f.readline()
    os.remove(os.path.join(data_dir, filename))
    header = header.replace('\n', '').split(',')
    values = values.replace('\n', '').split(',')
    return dict(zip(header, values))


def remove_s3_files(keys: list[str]) -> None:
    delete = {"Objects": [{"Key": key} for key in keys]}
    bucket.delete_objects(Delete=delete)