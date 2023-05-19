import os
import boto3


bucket = boto3.resource('s3').Bucket("transformed-bucket")
data_dir = '/tmp/data'
if not os.path.exists(data_dir):
    os.makedirs(data_dir)

def handler(event, context):
    for item in event["Items"]:
        key = item['Key']
        print(key)
        filename = key.split('/')[-1]
        prefix = key.split('/')[-2]
        print(read_data(filename, prefix))



def read_data(filename: str, prefix: str) -> dict:
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
    