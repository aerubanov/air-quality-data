import httpx
import bs4
import json
import os
import boto3

base_url = "https://archive.sensor.community/"
backet = boto3.resource("s3").Bucket("staging-area-bucket")
folder = "file_list/"
data_dir = "/tmp/data/"

def handler(event, context):
    print(f"Event: {event}")
    print(f"Context: {context}")
    link = event["folder"]["S"]
    print(link)
    data_object = link[:-1]+".json"
    files = list_files(link)
    if not os.path.exists(data_dir):
        os.mkdir(data_dir)
    with open(data_dir+data_object, 'w') as f:
        json.dump(files, f)
    backet.upload_file(data_dir+data_object, folder+data_object)
    os.remove(data_dir+data_object)
    return {
        "FileListFile": folder+data_object,
        "Bucket": "staging-area-bucket",
        }


def list_files(link):
    """
    Return list of hrefs from link
    """
    url = base_url + link
    # load url with retries
    try:
        response = httpx.get(url, timeout=20)
    except httpx.ReadTimeout:
        print("Timeout")
        response = httpx.get(url, timeout=120)

    soup = bs4.BeautifulSoup(response.text, 'lxml')
    links = [item.get('href') for item in soup.find_all('a')]
    files = [item for item in links if '.csv' in item]
    return [url+file for file in files]


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--folder', type=str, required=True)
    args = parser.parse_args()
    print(list_files(args.folder))