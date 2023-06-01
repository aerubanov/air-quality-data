# rebuild docker image and update lambda function code. Function will be deployed to AWS Lambda as container image. Function name passed as comand line argument.
#
# Example:
# ./update_lambda.sh <function name>

set -e  # exit on error
set -x  # print commands    

function_name=$1

aws ecr get-login-password --region eu-central-1 | docker login --username AWS --password-stdin 307660119800.dkr.ecr.eu-central-1.amazonaws.com
# aws ecr create-repository --repository-name $function_name --image-scanning-configuration scanOnPush=true --image-tag-mutability MUTABLE
docker build -t $function_name .
docker tag $function_name:latest  307660119800.dkr.ecr.eu-central-1.amazonaws.com/$function_name:latest
docker push 307660119800.dkr.ecr.eu-central-1.amazonaws.com/$function_name:latest
aws lambda update-function-code --function-name $function_name --image-uri 307660119800.dkr.ecr.eu-central-1.amazonaws.com/$function_name:latest