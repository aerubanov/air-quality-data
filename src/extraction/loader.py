import json
import os
import boto3
import botocore
import httpx



with open("config.json", "r") as cf:
    config = json.load(cf)

s3 = boto3.client(
    service_name="s3",
    aws_access_key_id=config["aws_access_key_id"],
    aws_secret_access_key=config["aws_secret_access_key"],
)
bucket = config["bucket_name"]


class FileLoader:
    def __init__(self) -> None:
        self.index_folder = "file_index/new/"
        self.processed_folder = "file_index/processed/"
        self.files_folder = "files/new/"
        self.data_dir = "data/"
        if not os.path.exists(self.data_dir):
            os.mkdir(self.data_dir)

    def load_file(self, link: str):
        """
        link like: https://archive.sensor.community/2015-10-01/2015-10-01_ppd42ns_sensor_27.csv
        """
        resp = httpx.get(link)
        data = resp.content.decode()
        path = self.data_dir + link.split("/").join()
        data_object = self.files_folder + link.split("/").join()
        print(path)
        with open(path, 'w') as f:
            f.write(data)
        try:
            s3.upload_file(path, bucket, data_object)
            return data_object        
        except botocore.exceptions.ClientError as e:
            print(e)
        finally:
            os.remove(path)