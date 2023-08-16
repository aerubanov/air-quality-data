import httpx
from bs4 import BeautifulSoup
import re
import datetime
import boto3

dynamodb = boto3.resource('dynamodb')
folders_table = dynamodb.Table('folders')

base_url = "https://archive.sensor.community/"
link_pattern = re.compile('\d\d\d\d-\d\d-\d\d/')
nested_link_pattern = re.compile('\d\d\d\d/\d\d\d\d-\d\d-\d\d/')

headers = {
    "Host": "archive.sensor.community",
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/109.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
}

nested_folders = [
    "2015/",
    "2016/",
    "2017/",
    "2018/",
    "2019/",
    "2020/",
    "2021/",
    "2022/",
]


def handler(event, context):
    print(f"Event: {event}")
    print(f"Context: {context}")
    start_date = datetime.datetime.strptime(event["startDate"], "%Y-%m-%d")
    end_date = datetime.datetime.strptime(event["endDate"], "%Y-%m-%d")
    build_index(start_date, end_date)


def get_folders_list():
    """Get list of folders from DynamoDB"""
    response = folders_table.scan()
    data = response['Items']

    while 'LastEvaluatedKey' in response:
        response = folders_table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])
    return [item["folder"] for item in data]


def get_links():
    """Get list of folders avialible in data source"""
    resp = httpx.get(base_url, headers=headers, timeout=120)
    print(resp.status_code)
    soup = BeautifulSoup(resp.text, 'lxml')
    links = [link.get('href') for link in soup.find_all('a') if link_pattern.match(link.get('href'))]

    # get data for previous years
    for folder in nested_folders:
        resp = httpx.get(base_url + folder, headers=headers, timeout=120)
        print(folder)
        print(resp.status_code)
        soup = BeautifulSoup(resp.text, 'lxml')
        links.extend([folder+link.get('href') for link in soup.find_all('a') if link_pattern.match(link.get('href'))])
    return links


def build_index(start_date: datetime.datetime, end_date: datetime.datetime):
    """Collect file index and upload it on S3. Using multithreading"""
    resp = httpx.get(base_url, headers=headers, timeout=120)
    soup = BeautifulSoup(resp.text, 'lxml')
    links = get_links()
    checked_folders=set(get_folders_list())
    links = [i for i in links if i not in checked_folders]
    links = [i for i in links if check_link(i, start_date, end_date)]
    print(f"{len(links)} new links")
    if len(links) == 0:
        print("Index is up to date, nothing to download")
        return
    print(links)
    with folders_table.batch_writer() as batch:
        for link in links:
            item = {
                "folder": link,
                "processed": False,
            }
            batch.put_item(Item=item)


def check_link(link: str, start_date: datetime.datetime, end_date: datetime.datetime) -> bool:
    "check link format and filter by date"
    if link_pattern.match(link):
        date = datetime.datetime.strptime(link.replace('/', ''), "%Y-%m-%d")
    elif nested_link_pattern.match(link):
        date = datetime.datetime.strptime(link.split('/')[1], "%Y-%m-%d")
    else:
        return False
    if date < start_date or date > end_date:
        return False

    return True