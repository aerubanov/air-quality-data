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
    """insert data in sensors table with columns sensor_id, sensor_type"""

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


def load_location_data(data: List[Dict]) -> None:
    """
    insert data in location table with columns
    location id, lat, lon, city, state, country, country_code, zipcode, timezone
    """
    sql = f"INSERT INTO location (location_id, lat, lon, city, state, country, zipcode, timezone) VALUES (:location_id, :lat, :lon, :city, :state, :country, :zipcode, :timezone)"
    params = []
    for item in data:
        params.append([
            {
                "name": "location_id",
                "value": {"longValue": item["location_id"]},
            },
            {
                "name": "lat",
                "value": {"doubleValue": item["lat"]},
            },
            {
                "name": "lon",
                "value": {"doubleValue": item["lon"]},
            },
            {
                "name": "city",
                "value": {"stringValue": item["city"]},
            },
            {
                "name": "state",
                "value": {"stringValue": item["state"]},
            },
            {
                "name": "country",
                "value": {"stringValue": item["country"]},
            },
            {
                "name": "zipcode",
                "value": {"stringValue": item["zipcode"]},
            },
            {
                "name": "timezone",
                "value": {"stringValue": item["timezone"]},
            },
        ])
    
    db.execute_statement(
        resourceArn=aurora_arn,
        secretArn=aurora_secret_arn,
        database=database_name,
        sql=sql,
        parameters=params
    )


def load_time_data(data: List[Dict]) -> None:
    """
    insert data in time table with columns
    time_id, year, month, day_of_week, timestamp, isweekend
    """
    sql = f"INSERT INTO time (time_id, year, month, day_of_week, timestamp, isweekend) VALUES (:time_id, :year, :month, :day_of_week, :timestamp, :isweekend)"
    params = []
    for item in data:
        params.append([
            {
                "name": "time_id",
                "value": {"longValue": item["time_id"]},
            },
            {
                "name": "year",
                "value": {"longValue": item["year"]},
            },
            {
                "name": "month",
                "value": {"longValue": item["month"]},
            },
            {
                "name": "day_of_week",
                "value": {"longValue": item["day_of_week"]},
            },
            {
                "name": "timestamp",
                "value": {"stringValue": item["timestamp"]},
                "typeHint": "TIMESTAMP"
            },
        ])
    db.execute_statement(
        resourceArn=aurora_arn,
        secretArn=aurora_secret_arn,
        database=database_name,
        sql=sql,
        parameters=params,
    )



if __name__ == "__main__":
    handler({"Items": [{"Key": "sensors/10006.csv"}]}, None)
    