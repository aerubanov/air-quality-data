import os
import boto3
from typing import Callable

from loading import load_sensor_data, load_location_data, load_time_data


bucket = boto3.resource('s3').Bucket("transformed-bucket")
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
    bucket.download_file(prefix+'/'+filename, os.path.join(data_dir, filename))
    # read header and values from data file
    with open(os.path.join(data_dir, filename), 'r') as f:
        header = f.readline()
        values = f.readline()
    os.remove(os.path.join(data_dir, filename))
    header = header.replace('\n', '').split(',')
    values = values.replace('\n', '').split(',')
    print(header)
    print(values)
    return dict(zip(header, values))


def remove_s3_files(keys: list[str]) -> None:
    delete = {"Objects": [{"Key": key} for key in keys]}
    bucket.delete_objects(Delete=delete)
    


if __name__ == "__main__":
    event = {
    "Items": [
    {
      "Etag": "\"d300b278468dfe529c5ef02050edbc04\"",
      "Key": "locations/10510.csv",
      "LastModified": 1685463282,
      "Size": 153,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"4fc44a21ed1c4a12582934d40a127d5e\"",
      "Key": "locations/12223.csv",
      "LastModified": 1685463279,
      "Size": 135,
      "StorageClass": "STANDARD"
    },
    ]}
    handler(event, None)
    