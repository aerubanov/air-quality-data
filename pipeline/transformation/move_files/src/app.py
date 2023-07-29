import os
import boto3

backet_name = os.environ["S3_BUCKET"]
s3 = boto3.resource('s3')
source_folder = 'files/new/'
target_folder = 'files/processed/'



def handler(event, context):
    print(f"Event: {event}")
    print(f"Context: {context}")
    event1, event2 = event[0], event[1]
    if event1["Status"] == "Succes" and event2["Status"] == "Succes":
        filenames = [item['Key'].split('/')[-1] for item in event1["Items"]]
        for filename in filenames:
            move_file(filename)
        print(f"Folllowing files moved to processed folder: {filenames}")
    else:
        print(f"Files not processed: {filenames}")
        raise Exception("File not processed")
    return event1["Status"], event2["Status"]
    


def move_file(filename: str):
    """Move file from source folder to turget folder in S3 bucket"""
    s3.Object(backet_name, target_folder+filename).copy_from(
        CopySource={
            'Bucket': backet_name,
            'Key': source_folder+filename,
        },
    )
    s3.Object(backet_name, source_folder+filename).delete()