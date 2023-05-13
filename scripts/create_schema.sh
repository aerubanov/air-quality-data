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
