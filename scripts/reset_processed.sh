#!/bin/bash
# bash script to move all file from files/processed folder on s3 to files/new folder on s3

set -e  # exit on error
set -x  # print commands


aws s3 cp s3://staging-area-bucket/files/processed/ s3://staging-area-bucket/files/new/ --recursive --quiet
# remove processed files
aws s3 rm s3://staging-area-bucket/files/processed --recursive --quiet