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


def load_temperature(data: list[dict]):
    """
    insert data it temperature table with columns
    location_id, sensor_id, time_id, temperature, pressure, humidity
    """
    sql = f"INSERT INTO temperature (location_id, sensor_id, time_id, temperature, pressure, humidity) VALUES (:location_id, :sensor_id, :time_id, :temperature, :pressure, :humidity) ON CONFLICT DO NOTHING"
    params = []
    for item in data:
        params.append([
            {
                "name": "location_id",
                "value": {"longValue": int(item["location_id"])},
            },
            {
                "name": "sensor_id",
                "value": {"longValue": int(item["sensor_id"])},
            },
            {
                "name": "time_id",
                "value": {"longValue": int(item["time_id"])},
            },
            {
                "name": "temperature",
                "value": {"doubleValue": float(item["temperature"])},
            },
            {
                "name": "pressure",
                "value": {"doubleValue": float(item["pressure"])},
            },
            {
                "name": "humidity",
                "value": {"doubleValue": float(item["humidity"])},
            },
        ])
    db.batch_execute_statement(
        resourceArn=aurora_arn,
        secretArn=get_secret_arn(),
        database=database_name,
        sql=sql,
        parameterSets=params
    )


def load_concentration(data: list[dict]):
    """
    write data in concentration table with columns
    location_id, sensor_id, time_id, p_1, p_2
    """
    sql = f"INSERT INTO concentration (location_id, sensor_id, time_id, p_1, p_2) VALUES (:location_id, :sensor_id, :time_id, :p_1, :p_2) ON CONFLICT DO NOTHING"
    params = []
    for item in data:
        params.append([
            {
                "name": "location_id",
                "value": {"longValue": int(item["location_id"])},
            },
            {
                "name": "sensor_id",
                "value": {"longValue": int(item["sensor_id"])},
            },
            {
                "name": "time_id",
                "value": {"longValue": int(item["time_id"])},
            },
            {
                "name": "p_1",
                "value": {"doubleValue": float(item["p_1"])},
            },
            {
                "name": "p_2",
                "value": {"doubleValue": float(item["p_2"])},
            },
        ])
    db.batch_execute_statement(
        resourceArn=aurora_arn,
        secretArn=get_secret_arn(),
        database=database_name,
        sql=sql,
        parameterSets=params
    )