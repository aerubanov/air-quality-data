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
   --sql "CREATE TABLE sensor IF NOT EXISTS (sensor_id INT PRIMARY KEY, sensor_type TEXT, sensor_name TEXT)"

# create time table
aws rds-data execute-statement --resource-arn $resource_arn --secret-arn $secret_arn --database "postgres" \
   --sql "CREATE TABLE time IF NOT EXISTS (time_id INT PRIMARY KEY, timestamp TIMESTAMPTZ, year SMALLINT, month SMALLINT, day_of_week SMALLINT, isweekend BOOLEAN)"

# create location table
aws rds-data execute-statement --resource-arn $resource_arn --secret-arn $secret_arn --database "postgres" \
   --sql "CREATE TABLE location IF NOT EXISTS (location_id INT PRIMARY KEY, latitude FLOAT, longitude FLOAT, city TEXT, country TEXT, state TEXT, zipcode TEXT, country TEXT, country_code TEXT, timezone TEXT)"

# create fact tables
# create temperature table
aws rds-data execute-statement --resource-arn $resource_arn --secret-arn $secret_arn --database "postgres" \
   --sql "CREATE TABLE temperature IF NOT EXISTS (
      CONSTRAINT location_fk FOREIGN KEY (location_id) REFERENCES location(location_id),
      CONSTRAINT time_fk FOREIGN KEY (time_id) REFERENCES time(time_id),
      CONSTRAINT sensor_fk FOREIGN KEY (sensor_id) REFERENCES sensor(sensor_id)
      temperature FLOAT,
      humidity FLOAT,
      pressure FLOAT,
      PRIMARY KEY (location_id, time_id, sensor_id))"

# create concentration table
aws rds-data execute-statement --resource-arn $resource_arn --secret-arn $secret_arn --database "postgres" \
   --sql "CREATE TABLE concentration IF NOT EXISTS (
      CONSTRAINT location_fk FOREIGN KEY (location_id) REFERENCES location(location_id),
      CONSTRAINT time_fk FOREIGN KEY (time_id) REFERENCES time(time_id),
      CONSTRAINT sensor_fk FOREIGN KEY (sensor_id) REFERENCES sensor(sensor_id)
      p_1 FLOAT,
      p_2 FLOAT,
      PRIMARY KEY (location_id, time_id, sensor_id))"