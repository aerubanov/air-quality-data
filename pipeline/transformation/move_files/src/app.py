import boto3


s3 = boto3.resource('s3')
source_folder = 'files/new/'
target_folder = 'files/processed/'



def handler(event, context):
    print(f"Event: {event}")
    print(f"Context: {context}")
    event1, event2 = event[0], event[1]
    if event1["Status"] == "Succes" and event2["Status"] == "Succes":
        filename = event1['Key'].split('/')[-1]
        move_file(filename)
        print(f"File: {filename} moved to processed folder")
    else:
        print(f"File: {event1['Key']} not processed")
        raise Exception("File not processed")
    return event1["Status"], event2["Status"]
    


def move_file(filename: str):
    """Move file from source folder to turget folder in S3 bucket"""
    s3.Object('staging-area-bucket', target_folder+filename).copy_from(
        CopySource={
            'Bucket': 'staging-area-bucket',
            'Key': source_folder+filename,
        },
    )
    s3.Object('staging-area-bucket', source_folder+filename).delete()


if __name__ == "__main__":
    handler([{'Status': 'Succes', 'Key': 'files/new/2017-08-05_bme280_sensor_1357.csv'}, {'Status': 'Succes', 'Key': 'files/new/2017-08-05_bme280_sensor_1357.csv'}], None)
