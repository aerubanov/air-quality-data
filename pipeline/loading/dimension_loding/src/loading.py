from typing import List, Dict
import boto3

aurora_arn = 'arn:aws:rds:eu-central-1:307660119800:cluster:aurora-cluster'
database_name = "postgres"
db = boto3.client('rds-data')



def get_secret_arn(secret_name = "aurora-secret"):
    client = boto3.client('secretsmanager')
    get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    return get_secret_value_response['ARN']


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
    db.batch_execute_statement(
        resourceArn=aurora_arn,
        secretArn=get_secret_arn(),
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
    
    db.batch_execute_statement(
        resourceArn=aurora_arn,
        secretArn=get_secret_arn(),
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
    
    db.batch_execute_statement(
        resourceArn=aurora_arn,
        secretArn=get_secret_arn(),
        database=database_name,
        sql=sql,
        parameters=params
    )