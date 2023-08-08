# Copy DynamoDB folder table schema in file schema.json. Then delete this table with all data. After that restore this table using schema.json and delet this file

set -e  # exit on error
set -x  # print commands   

table_name=folders

aws dynamodb describe-table --table-name $table_name | jq '.Table | del(.TableId, .TableArn, .ItemCount, .TableSizeBytes, .CreationDateTime, .TableStatus, .ProvisionedThroughput.NumberOfDecreasesToday)' > schema.json
# delete the table
#aws dynamodb delete-table --table-name $table_name
# wait 10 seconds
# sleep 10
# create table with the same schema
# aws dynamodb create-table --cli-input-json file://schema.json