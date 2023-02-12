import httpx
from bs4 import BeautifulSoup
import re
import os
import boto3
from typing import Optional
import time

dynamodb = boto3.resource('dynamodb')
folders_table = dynamodb.Table('folders')
files_table = dynamodb.Table('files')


def get_folders_list():
    response = folders_table.scan()
    data = response['Items']

    while 'LastEvaluatedKey' in response:
        response = folders_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])
    return [item["folder"] for item in data]


base_url = "https://archive.sensor.community/"
link_pattern = re.compile('\d\d\d\d-\d\d-\d\d/')

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

def build_index():
    """Collect file index and upload it on S3. Using multithreading"""
    resp = httpx.get(base_url, headers=headers)
    soup = BeautifulSoup(resp.text, 'lxml')
    links = [link.get('href') for link in soup.find_all('a')]
    checked_folders=set(get_folders_list())
    links = [i for i in links if i not in checked_folders]
    print(len(links))
    if len(links) == 0:
        print("Index is up to date, nothing to download")
        return
    cnt = 0
    for i in links:
        scan_deeper(i)
        cnt += 1
    print(f"Downloaded {cnt} items")

def scan_deeper(link: str) -> Optional[str]:
    """
    download list of links for single date, save it into file and upload to s3
    """
    if not check_link(link):
        return

    print(f"downloading {link}")
    url = base_url + link
    try:
        resp = httpx.get(url, headers=headers, timeout=60)
    except httpx.TimeoutException:
        print(f"can`t load {link}")
        time.sleep(10)
        try:
            resp = httpx.get(url, headers=headers, timeout=120)
        except httpx.TimeoutException:
            print (f"retry fail: {link}")
            return
        
    soup = BeautifulSoup(resp.text, 'lxml')
    links = [item.get('href') for item in soup.find_all('a')]
    files = [item for item in links if '.csv' in item]
    with files_table.batch_writer() as batch:
        for file in files:
            item = {
                "filename": file,
                "folder": link,
                "link": url+file,
                "processed": False,
            }
            batch.put_item(Item=item)

    item = {
        "folder": link,
        "processed": False,
        }
    folders_table.put_item(Item=item)
    print(f"compleated: {link}")
    

def check_link(link: str) -> bool:
    "check link format and if file already exist on s3"
    if not link_pattern.match(link):
        print(f"skip {link}")
        return False

    return True


if __name__ == "__main__":
    build_index()