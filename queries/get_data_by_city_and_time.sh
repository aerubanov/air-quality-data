#!/bin/bash

set -e  # exit on error
set -x  # print commands

account_id=$(aws sts get-caller-identity --query Account --output text)
region=$(aws configure get region)
resource_arn='arn:aws:rds:'$region':'$account_id':cluster:aurora-cluster'

# get aurora-secret arn from aws secret manager
secret_arn=$(aws secretsmanager get-secret-value --secret-id aurora-secret --query 'ARN' --output text)

aws rds-data execute-statement --resource-arn $resource_arn --secret-arn $secret_arn --output table --database "postgres" \
    --sql "SELECT 
    concentration.p_1,
    concentration.p_2,
    location.latitude,
    location.longitude,
    time.timestamp 
    FROM
    concentration JOIN location ON concentration.location_id = location.location_id
    JOIN time ON concentration.time_id = time.time_id
    WHERE location.country = 'Germany'
    AND location.city = 'Leverkusen'
    AND time.timestamp >= '2023-03-20' AND time.timestamp < '2023-03-21'
    LIMIT 100"