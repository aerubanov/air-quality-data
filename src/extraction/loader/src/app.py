import json
import os
import boto3
import botocore
import httpx

s3 = boto3.client("s3")
bucket = "staging-area-bucket" #TODO: get from env var

index_folder = "file_index/new/"
processed_folder = "file_index/processed/"
files_folder = "files/new/"
data_dir = "/tmp/data/"
if not os.path.exists(data_dir):
    os.mkdir(data_dir)


def handler(event, context):
    raise NotImplementedError


def process_index_file(filename: str):
    data_object = index_folder + filename
    print(f"processing {data_object}")
    path = data_dir + filename
    with open(path, 'wb') as fp:
        s3.download_fileobj(bucket, data_object, fp)
    
    with open(path, "r") as f:
        lines = f.read().split('\n')[:-1]
    
    os.remove(path)
    print(f"going to download {len(lines)} files")
    for link in lines:
        print(link)
        load_file(link)

    s3.copy_object(
        Bucket=bucket,
        CopySource=data_object,
        Key=processed_folder+filename,
    )
    print(f"{data_object} processed")
    

def load_file(link: str):
    """
    link like: https://archive.sensor.community/2015-10-01/2015-10-01_ppd42ns_sensor_27.csv
    """
    resp = httpx.get(link)
    data = resp.content.decode()
    path = data_dir + link.split("/")[-1]
    data_object = files_folder + link.split("/")[-1]
    with open(path, 'w') as f:
        f.write(data)
    try:
        s3.upload_file(path, bucket, data_object)
        return data_object        
    except botocore.exceptions.ClientError as e:
        print(e)
        raise e
    finally:
        os.remove(path)


def job(func, data):
    for i in data:
        func(i)


def process_in_threads(data: list, func: callable, n_threads: int) -> list:
    if n_threads > 1:
        chunk_size = round(len(data)/n_threads)
        chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
    else:
        chunks = [data]
    threads = [None for i in range(n_threads)]

    for i in range(n_threads):
        threads[i] = Thread(target=job, args=(func, chunks[i]))
        threads[i].start()

    for i in range(n_threads):
        threads[i].join()

    return [item for sublist in chunks for item in sublist]

if __name__ == "__main__":
    process_index_file("2022-05-18.txt")