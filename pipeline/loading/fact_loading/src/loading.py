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


def load_temperature(data: list[dict]):
    """
    insert data it temperature table with columns
    location_id, sensor_id, time_id, temperature, pressure, humidity
    """
    sql = f"""INSERT INTO temperature (location_id, sensor_id, time_id, temperature, pressure, humidity)
    SELECT :location_id, :sensor_id, :time_id, :temperature, :pressure, :humidity
    WHERE EXISTS (SELECT * FROM sensor, location, time WHERE
    sensor.sensor_id = :sensor_id AND location.location_id = :location_id AND time.time_id = :time_id)
    ON CONFLICT DO NOTHING"""
    params = []
    for item in data:
        temp_cond = "temperature" in item and len(item["temperature"]) > 0 and  item["temperature"][0].isdigit()
        pressure_cond = "pressure" in item and len(item["pressure"]) > 0 and item["pressure"][0].isdigit()
        humidity_cond = "humidity" in item and len(item["humidity"]) > 0 and item["humidity"][0].isdigit()
        if not temp_cond and not pressure_cond and not humidity_cond:
            continue
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
                "value": {"doubleValue": float(item["temperature"])} if temp_cond else {"isNull": True},
            },
            {
                "name": "pressure",
                "value": {"doubleValue": float(item["pressure"])} if pressure_cond else {"isNull": True},
            },
            {
                "name": "humidity",
                "value": {"doubleValue": float(item["humidity"])} if humidity_cond else {"isNull": True},
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
    sql = f"""INSERT INTO concentration (location_id, sensor_id, time_id, p_1, p_2)
    SELECT :location_id, :sensor_id, :time_id, :p_1, :p_2
    WHERE EXISTS (SELECT * FROM sensor, location, time WHERE
    sensor.sensor_id = :sensor_id AND location.location_id = :location_id AND time.time_id = :time_id) 
    ON CONFLICT DO NOTHING"""
    params = []
    for item in data:
        p1_cond = "P1" in item and len(item["P1"]) > 0 and item["P1"][0].isdigit()
        p2_cond = "P2" in item and len(item["P2"]) > 0 and item["P2"][0].isdigit()
        if not p1_cond and not p2_cond:
            continue
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
                "value": {"doubleValue": float(item["P1"])} if p1_cond else {"isNull": True},
            },
            {
                "name": "p_2",
                "value": {"doubleValue": float(item["P2"])} if p2_cond else {"isNull": True},
            },
        ])
    db.batch_execute_statement(
        resourceArn=aurora_arn,
        secretArn=get_secret_arn(),
        database=database_name,
        sql=sql,
        parameterSets=params
    )