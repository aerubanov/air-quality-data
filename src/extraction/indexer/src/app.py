import httpx
from bs4 import BeautifulSoup
import re
import os
import boto3
from typing import Optional
import time
import datetime

dynamodb = boto3.resource('dynamodb')
folders_table = dynamodb.Table('folders')

base_url = "https://archive.sensor.community/"
link_pattern = re.compile('\d\d\d\d-\d\d-\d\d/')
first_date = datetime.date(2015, 10, 1)

headers = {
    "Host": "archive.sensor.community",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/109.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

def handler(event, context):
    build_index()


def get_folders_list():
    response = folders_table.scan()
    data = response['Items']

    while 'LastEvaluatedKey' in response:
        response = folders_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])
    return [item["folder"] for item in data]


def build_index():
    """Collect file index and upload it on S3. Using multithreading"""
    resp = httpx.get(base_url, headers=headers)
    soup = BeautifulSoup(resp.text, 'lxml')
    links = [link.get('href') for link in soup.find_all('a')]
    checked_folders=set(get_folders_list())
    links = [i for i in links if i not in checked_folders]
    print(f"{len(links)} new links")
    if len(links) == 0:
        print("Index is up to date, nothing to download")
        return
    with folders_table.batch_writer() as batch:
        for link in links:
            if check_link(link):
                item = {
                    "folder": link,
                    "processed": False,
                }
                batch.put_item(Item=item)


def check_link(link: str) -> bool:
    "check link format"
    if not link_pattern.match(link):
        return False

    return True


if __name__ == "__main__":
    build_index()