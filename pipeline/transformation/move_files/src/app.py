import boto3


s3 = boto3.resource('s3')
source_folder = 'files/new/'
target_folder = 'files/processed/'



def handler(event, context):
    print(f"Event: {event}")
    print(f"Context: {context}")
    filename = event['Key'].split('/')[-1]
    move_file(filename)
    print(f"File: {filename} moved to processed folder")


def move_file(filename: str):
    """Move file from source folder to turget folder in S3 bucket"""
    s3.Object('staging-area-bucket', target_folder+filename).copy_from(
        source_folder+filename
    )
    s3.Object('staging-area-bucket', source_folder+filename).delete()
