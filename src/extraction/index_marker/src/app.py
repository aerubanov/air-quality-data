import boto3


def handler(event, context):
    print(f"Event: {event}")
    print(f"Context: {context}")
    folder = event["folder"]["S"]
    mark_folder(folder)
    print(f"Marked folder {folder} as processed")


def mark_folder(folder: str):
    """
    Set  DynamoDB item with key=folder processed field to True
    """
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('folders')
    response = table.update_item(
        Key={'folder': folder},
        UpdateExpression="set processed = :val",
        ExpressionAttributeValues={':val': True},
    )
    