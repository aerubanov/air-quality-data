import os
import boto3
import pandas as pd


source_bucket = boto3.resource('s3').Bucket("staging-area-bucket")
target_bucket = boto3.resource('s3').Bucket("transformed-bucket")
data_dir = '/tmp/data'
prefix = 'files/new/'
data_colums = {'timestamp', 'temperature', 'humidity', 'pressure', 'P1', 'P2', 'sensor_id'}
if not os.path.exists(data_dir):
    os.makedirs(data_dir)


def handler(event, context):
    print(f"Event: {event}")
    print(f"Context: {context}")
    df_list = []
    for item in event["Items"]:
        filename = item['Key'].split('/')[-1]
        data = get_s3_file_data(filename)
        data = extract_data(data)
        df_list.append(data)
    df = pd.concat(df_list)
    result = write_data_to_s3(df)
    print(f"File: {result} uploaded to s3")
    return {"Status": "Succes", "Key": item["Key"]}


def get_s3_file_data(filename: str) -> pd.DataFrame:
    source_bucket.download_file(prefix+filename, os.path.join(data_dir, filename))
    df = pd.read_csv(os.path.join(data_dir, filename), parse_dates=['timestamp'], sep=';')
    os.remove(os.path.join(data_dir, filename))
    return df


def extract_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df[[col for col in df.columns if col in data_colums]]
    df.set_index('timestamp', inplace=True)
    df = df.resample('1H').mean()
    df.dropna(inplace=True)
    df['sensor_id'] = df['sensor_id'].astype(int)
    # data = {col: df[col].tolist() for col in df.columns}
    # data['timestamp'] = df.index.format()
    # return data
    return df
    

def write_data_to_s3(data: pd.DataFrame) -> str:
    prefix = 'concentration/' if 'P1' in data.columns else 'temperature/'
    date = data.index[0].date()
    sensor_id = data['sensor_id'].iloc[0]
    data.to_csv(os.path.join(data_dir, f'{date}_{sensor_id}.csv'))
    target_bucket.upload_file(os.path.join(data_dir, f'{date}_{sensor_id}.csv'), prefix+f'{date}_{sensor_id}.csv')
    os.remove(os.path.join(data_dir, f'{date}_{sensor_id}.csv'))
    return prefix+f'{date}_{sensor_id}.csv'


if __name__ == "__main__":
    data = get_s3_file_data('2017-08-05_bme280_sensor_1093.csv')
    print(data.head())
    df = extract_data(data)
    print(df)