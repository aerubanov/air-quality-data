import boto3

s3 = boto3.client(service_name='s3')
bucket = "staging-area-bucket" #TODO: get from env var


def handler(event, context):
    pass


def index_diff() -> list:
    new = get_objects_set("file_index/new/")
    processed = get_objects_set("file_index/processed/")
    diff = new - processed
    print(f"number of new items: {len(new)}")
    print(f" number of processed items: {len(processed)}")
    print(f"number of items to process: {len(diff)}")
    return list(diff)


def get_objects_set(preffix: str) -> set:
    result = []
    resp = s3.list_objects(Bucket=bucket, Prefix=preffix)
    while (resp["IsTruncated"]):
        result.extend(get_keys(resp))
        resp = s3.list_objects(Bucket=bucket, Prefix=preffix, Marker=preffix+result[-1])
    result.extend(get_keys(resp))
    return set(result)


def get_keys(resp: dict) -> list:
    try:
        keys = [item['Key'].split('/')[-1] for item in resp["Contents"]]
    except KeyError:
        keys = []
    return keys

if __name__ == "__main__":
    index_diff()