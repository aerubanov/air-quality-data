#!/bin/bash

set -e  # exit on error
set -x  # print commands

account_id=$(aws sts get-caller-identity --query Account --output text)
region=$(aws configure get region)
resource_arn='arn:aws:rds:'$region':'$account_id':cluster:aurora-cluster'

# get aurora-secret arn from aws secret manager
secret_arn=$(aws secretsmanager get-secret-value --secret-id aurora-secret --query 'ARN' --output text)

# get sensors counted by city in Germany
aws rds-data execute-statement --resource-arn $resource_arn --secret-arn $secret_arn --output table --database "postgres" \
    --sql "
    SELECT
    conc.p_1,
    conc.p_2,
    temp.temperature,
    temp.humidity,
    temp.pressure,
    conc.timestamp
    FROM
    (SELECT
    concentration.p_1,
    concentration.p_2,
    time.timestamp,
    location.location_id
    FROM
    concentration JOIN time ON concentration.time_id = time.time_id
    JOIN location ON concentration.location_id = location.location_id
    WHERE location.location_id = 66068
    AND time.timestamp >= '2023-04-08' AND time.timestamp < '2023-04-09') conc
    FULL OUTER JOIN
    (SELECT
    temperature.temperature,
    temperature.humidity,
    temperature.pressure,
    time.timestamp,
    location.location_id
    FROM
    temperature JOIN time ON temperature.time_id = time.time_id
    JOIN location ON temperature.location_id = location.location_id
    WHERE location.location_id = 66068
    AND time.timestamp >= '2023-04-08' AND time.timestamp < '2023-04-09') temp
    ON conc.timestamp = temp.timestamp AND conc.location_id = temp.location_id
    "