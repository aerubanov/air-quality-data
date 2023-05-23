import os
from typing import Callable
import boto3

from loading import load_temperature, load_concentration

bucket = boto3.resource('s3').Bucket("transformed-bucket")
data_dir = '/tmp/data'
if not os.path.exists(data_dir):
    os.makedirs(data_dir)


def handler(event, context):
    key = event['Key']
    print(key)
    filename = key.split('/')[-1]
    prefix = key.split('/')[-2]
    data = read_data(prefix, filename)
    loading_fn = select_loading_function(prefix)
    loading_fn(data)
    print(f"Successfully loaded {len(data)} records")


def select_loading_function(prefix: str) -> Callable:
    return {
            "temperature": load_temperature,
            "concentration": load_concentration,
    }.get(prefix)


def read_data(prefix: str, filename: str) -> list[dict]:
    bucket.download_file(prefix+'/'+filename, os.path.join(data_dir, filename))
    # read header and values from data file
    with open(os.path.join(data_dir, filename), 'r') as f:
        header = f.readline()
        values = f.readlines()[1:]
    os.remove(os.path.join(data_dir, filename))
    header = header.replace('\n', '').split(',')
    values = [item.replace('\n', '').split(',') for item in values]
    data = []
    for item in values:
        data.append(dict(zip(header, item)))
    return data


if __name__ == "__main__":
    handler({"Key": "temperature/2023-03-01_10006.csv"}, None)
