import os
import boto3
from typing import Dict, Callable

from loading import load_sensor_data, load_location_data, load_time_data


bucket = boto3.resource('s3').Bucket("transformed-bucket")
data_dir = '/tmp/data'
if not os.path.exists(data_dir):
    os.makedirs(data_dir)
 

def handler(event, context):
    data = []
    for item in event["Items"]:
        key = item['Key']
        filename = key.split('/')[-1]
        prefix = key.split('/')[-2]
        data.append(read_data(filename, prefix))
    loading_fn = select_loading_function(prefix)
    loading_fn(data)
    print(f"Successfully loaded {len(data)} records")
    return {
        "statusCode": 200,
    }


def select_loading_function(prefix: str) -> Callable:
    return {
            "time": load_time_data,
            "sensor": load_sensor_data,
            "location": load_location_data,
        }.get(prefix)


def read_data(filename: str, prefix: str) -> Dict:
    bucket.download_file(prefix+'/'+filename, os.path.join(data_dir, filename))
    # read header and values from data file
    with open(os.path.join(data_dir, filename), 'r') as f:
        header = f.readline()
        values = f.readline()
    os.remove(os.path.join(data_dir, filename))
    header = header.replace('\n', '').split(',')
    values = values.replace('\n', '').split(',')
    return dict(zip(header, values))



if __name__ == "__main__":
    handler({"Items": [{"Key": "sensors/10006.csv"}]}, None)
    