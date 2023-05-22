import os
import boto3
from typing import List, Dict

def get_secret_arn(secret_name = "aurora-secret"):
    client = boto3.client('secretsmanager')
    get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    return get_secret_value_response['ARN']


bucket = boto3.resource('s3').Bucket("transformed-bucket")
data_dir = '/tmp/data'
if not os.path.exists(data_dir):
    os.makedirs(data_dir)
aurora_arn = 'arn:aws:rds:eu-central-1:307660119800:cluster:aurora-cluster'
aurora_secret_arn = get_secret_arn()
database_name = "postgres"
db = boto3.client('rds-data')


def get_secret_arn(secret_name = "aurora-secret"):
    client = boto3.client('secretsmanager')
    get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    return get_secret_value_response['ARN']
    

def handler(event, context):
    data = []
    for item in event["Items"]:
        key = item['Key']
        filename = key.split('/')[-1]
        prefix = key.split('/')[-2]
        data.append(read_data(filename, prefix))



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

def load_sensor_data(data: List[Dict]) -> None:

    sql = f"INSERT INTO sensor (sensor_id, sensor_type) VALUES (:sensor_id, :sensor_type)"
    params = []
    for item in data:
        params.append([
            {
                "name": "sensor_id",
                "value": {"longValue": item["sensor_id"]},
            },
            {
                "name": "sensor_type",
                "value": {"stringValue": item["sensor_type"]},
            },
        ])
    db.execute_statement(
        resourceArn=aurora_arn,
        secretArn=aurora_secret_arn,
        database=database_name,
        sql=sql,
        parameters=params
    )


if __name__ == "__main__":
    handler({"Items": [{"Key": "sensors/10006.csv"}]}, None)
    