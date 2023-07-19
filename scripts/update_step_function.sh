#!/bin/bash
# update AWS Step Function defenition using AWS CLI

set -e  # exit on error
set -x  # print commands

state_machine=$1
definition_file=$2
account_id=$(aws sts get-caller-identity --query Account --output text)
region=$(aws configure get region)


state_machine_arn=arn:aws:states:$region:$account_id:stateMachine:$state_machine

#print content of defenition_file
cat $definition_file


# update AWS Step Function definition
aws stepfunctions update-state-machine --state-machine-arn $state_machine_arn --definition """$(cat $definition_file)"""
