#!/bin/bash
# bash script to create tables in Aurora DB using AWS CLI

set -e  # exit on error
set -x  # print commands

resource_arn='arn:aws:rds:eu-central-1:307660119800:cluster:aurora-cluster'

# get aurora-secret arn from aws secret manager
secret_arn=$(aws secretsmanager get-secret-value --secret-id aurora-secret --query 'ARN' --output text)

# list existing tables
aws rds-data execute-statement --resource-arn $resource_arn --secret-arn $secret_arn --database "postgres" \
   --sql "SELECT *FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'"

# create schema

# create dimension tables
# create sensor table
aws rds-data execute-statement --resource-arn $resource_arn --secret-arn $secret_arn --database "postgres" \
   --sql "CREATE TABLE IF NOT EXISTS sensor (sensor_id INT PRIMARY KEY, sensor_type TEXT)"

# create time table
aws rds-data execute-statement --resource-arn $resource_arn --secret-arn $secret_arn --database "postgres" \
   --sql "CREATE TABLE IF NOT EXISTS time (time_id INT PRIMARY KEY, timestamp TIMESTAMPTZ, year SMALLINT, month SMALLINT, day_of_week SMALLINT, isweekend BOOLEAN)"

# create location table
aws rds-data execute-statement --resource-arn $resource_arn --secret-arn $secret_arn --database "postgres" \
   --sql "CREATE TABLE IF NOT EXISTS location (location_id INT PRIMARY KEY, latitude FLOAT, longitude FLOAT, city TEXT, country TEXT, state TEXT, zipcode TEXT, country_code TEXT, timezone TEXT)"

# create fact tables
# create temperature table
aws rds-data execute-statement --resource-arn $resource_arn --secret-arn $secret_arn --database "postgres" \
   --sql "CREATE TABLE IF NOT EXISTS temperature (
      location_id INT  REFERENCES location(location_id),
      time_id INT  REFERENCES time(time_id),
      sensor_id INT  REFERENCES sensor(sensor_id),
      temperature FLOAT,
      humidity FLOAT,
      pressure FLOAT,
      PRIMARY KEY (location_id, time_id, sensor_id))"

# create concentration table
aws rds-data execute-statement --resource-arn $resource_arn --secret-arn $secret_arn --database "postgres" \
   --sql "CREATE TABLE IF NOT EXISTS concentration(
      location_id INT  REFERENCES location(location_id),
      time_id INT  REFERENCES time(time_id),
      sensor_id INT  REFERENCES sensor(sensor_id),
      p_1 FLOAT,
      p_2 FLOAT,
      PRIMARY KEY (location_id, time_id, sensor_id))"