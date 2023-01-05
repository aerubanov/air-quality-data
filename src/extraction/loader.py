import httpx
from bs4 import BeautifulSoup
import re
import os
from multiprocessing.pool import ThreadPool
import boto3
import botocore
import json
from typing import Optional

with open("config.json", "r") as cf:
    config = json.load(cf)

s3 = boto3.client(
    service_name='s3',
	aws_access_key_id=config['aws_access_key_id'],
	aws_secret_access_key=config['aws_secret_access_key'],
    )
bucket = config['bucket_name']


class FTPLoader:

    def __init__(self) -> None:
        self.base_url = "https://archive.sensor.community/"
        self.link_pattern = re.compile('\d\d\d\d-\d\d-\d\d/')
        self.index_folder = 'file_index/'
        self.data_dir = 'data/'
        if not os.path.exists(self.data_dir):
            os.mkdir(self.data_dir)

    def build_index(self):
        """Collect file index and upload it on S3. Using multithreading"""
        resp = httpx.get(self.base_url)
        soup = BeautifulSoup(resp.text, 'lxml')
        links = [link.get('href') for link in soup.find_all('a')]
        pool = ThreadPool(processes=10)
        for result in map(self._check_and_scan_deeper, links):
            if result:
                print(f"{result} downloaded")
        print(len(self.index))

    def _check_and_scan_deeper(self, link: str) -> Optional[str]:
        if not self._check_link(link):
            print(f"skip {link}")
            return

        print(f"downloading {link}")
        url = self.base_url + link
        resp = httpx.get(url)
        soup = BeautifulSoup(resp.text, 'lxml')
        links = [item.get('href') for item in soup.find_all('a')]
        files = [url+item for item in links if '.csv' in item]
        data_file = self.data_dir + link[:-1] + '.txt'
        data_object = self.index_folder + link[:-1] + '.txt'
        with open(data_file, "w") as f:
            for line in files:
                f.write(line+'\n')

        try:
            s3.upload_file(data_file, bucket, data_object)
            return data_object        
        except botocore.exceptions.ClientError as e:
            print(e)
        finally:
            os.remove(data_file)
              

    def _check_link(self, link: str) -> bool:
        if not self.link_pattern.match(link):
            return False
        file = self.index_folder + link[:-1] + '.txt'
        try:
            s3.head_object(Bucket=bucket, Key=file)
            return False # file already exist
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == "404":
                # The object does not exist.
                pass
            else:
                # Something else has gone wrong.
                raise
        return True

    def load_data(self):
        raise NotImplementedError


if __name__ == "__main__":
    loader = FTPLoader()
    loader.build_index()
