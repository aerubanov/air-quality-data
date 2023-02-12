import httpx
from bs4 import BeautifulSoup
import re
import os
from threading import Thread
import boto3
import botocore
from typing import Optional
import time

#db = boto3.client(service_name='dynamodb')
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
index_folder = 'file_index/new/'
data_dir = '/tmp/data/'
if not os.path.exists(data_dir):
    os.mkdir(data_dir)

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
    # multiprocessing.ThreadPool is not working in AWS Lambda
    # https://aws.amazon.com/blogs/compute/parallel-processing-in-python-with-aws-lambda/
    #with ThreadPool(10) as p:
    #    booleans = p.map(check_link, links)
    #booleans = process_in_threads(links, check_link, n_threads=10)
    #links = [x for x, b in zip(links, booleans) if b]
    links = [i for i in links if i not in checked_folders]
    print(len(links))
    if len(links) == 0:
        print("Index is up to date, nothing to download")
        return
    cnt = 0
    #with ThreadPool(10) as pool:
    #    for result in pool.map(scan_deeper, links):
    #        if result:
    #            cnt += 1
    #for result in process_in_threads(links, scan_deeper, n_threads=min(1, len(links))):
    #    if result:
    #        cnt += 1
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
    data_file = data_dir + link[:-1] + '.txt'
    data_object = index_folder + link[:-1] + '.txt'
    #with open(data_file, "w") as f:
    #    for line in files:
    #        f.write(line+'\n')
    with files_table.batch_writer() as batch:
        for file in files:
            item = {
                "filename": file,
                "folder": link,
                "link": url+file,
                "processed": False,
            }
            batch.put_item(Item=item)

    #try:
    #    s3.upload_file(data_file, bucket, data_object)
    #    print(f"compleated: {link}")
    #    return data_object        
    #except botocore.exceptions.ClientError as e:
    #    print(e)
    #finally:
    #    os.remove(data_file)
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
    # file = index_folder + link[:-1] + '.txt'
    # try:
    #     s3.head_object(Bucket=bucket, Key=file)
    #     print(f"skip {link}")
    #     return False # file already exist
    # except botocore.exceptions.ClientError as e:
    #     if e.response['Error']['Code'] == "404":
    #         # The object does not exist.
    #         pass
    #     else:
    #         # Something else has gone wrong.
    #         raise
    # return True
    #resp = db.get_item(Key={"folder": {"S": link}}, TableName="folders")
    #if "Item" in resp:
    #    print(f"skip {link}")
    #    return False # We alredy added this folder to index
    
    return True


# def job(func, data):
#     for i, item in enumerate(data):
#         data[i] = func(item)


# def process_in_threads(data: list, func: callable, n_threads: int) -> list:
#     if n_threads > 1:
#         chunk_size = round(len(data)/n_threads)
#         chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
#     else:
#         chunks = [data]
#     threads = [None for i in range(n_threads)]

#     for i in range(n_threads):
#         threads[i] = Thread(target=job, args=(func, chunks[i]))
#         threads[i].start()

#     for i in range(n_threads):
#         threads[i].join()

#     return [item for sublist in chunks for item in sublist]



if __name__ == "__main__":
    build_index()
    #print(get_folders_list())
