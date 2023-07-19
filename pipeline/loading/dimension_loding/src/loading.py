import boto3

sts = boto3.client('sts')
account_id = sts.get_caller_identity()['Account']
region = sts.meta.region_name
aurora_arn = f'arn:aws:rds:{region}:{account_id}:cluster:aurora-cluster'
database_name = "postgres"
db = boto3.client('rds-data')



def get_secret_arn(secret_name = "aurora-secret"):
    client = boto3.client('secretsmanager')
    get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    return get_secret_value_response['ARN']


def load_sensor_data(data: list[dict]) -> None:
    """insert data in sensors table with columns sensor_id, sensor_type"""

    sql = f"INSERT INTO sensor (sensor_id, sensor_type) VALUES (:sensor_id, :sensor_type) ON CONFLICT DO NOTHING"
    params = []
    for item in data:
        params.append([
            {
                "name": "sensor_id",
                "value": {"longValue": int(item["sensor_id"])},
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
        parameterSets=params
    )


def load_location_data(data: list[dict]) -> None:
    """
    insert data in location table with columns
    location id, lat, lon, city, state, country, country_code, zipcode, timezone
    """
    sql = f"INSERT INTO location (location_id, latitude, longitude, city, state, country, zipcode, timezone) VALUES (:location_id, :latitude, :longitude, :city, :state, :country, :zipcode, :timezone) ON CONFLICT DO NOTHING"
    params = []
    for item in data:
        params.append([
            {
                "name": "location_id",
                "value": {"longValue": int(item["location_id"])},
            },
            {
                "name": "latitude",
                "value": {"doubleValue": float(item["latitude"])},
            },
            {
                "name": "longitude",
                "value": {"doubleValue": float(item["longitude"])},
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
        parameterSets=params
    )


def load_time_data(data: list[dict]) -> None:
    """
    insert data in time table with columns
    time_id, year, month, day_of_week, timestamp, isweekend
    """
    print(data)
    sql = f"INSERT INTO time (time_id, year, month, day_of_week, timestamp, isweekend) VALUES (:time_id, :year, :month, :day_of_week, :timestamp, :isweekend) ON CONFLICT DO NOTHING"
    params = []
    for item in data:
        params.append([
            {
                "name": "time_id",
                "value": {"longValue": int(item["time_id"])},
            },
            {
                "name": "year",
                "value": {"longValue": int(item["year"])},
            },
            {
                "name": "month",
                "value": {"longValue": int(item["month"])},
            },
            {
                "name": "day_of_week",
                "value": {"longValue": int(item["day_of_week"])},
            },
            {
                "name": "isweekend",
                "value": {"booleanValue": bool(item["day_of_week"] in ["5", "6"])},
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
        parameterSets=params
    )