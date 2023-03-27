import os
import boto3
import pandas as pd


bucket = boto3.resource('s3').Bucket("staging-area-bucket")
data_dir = 'tmp/data'
prefix = 'files/new/'
data_colums = {'timestamp', 'temperature', 'humidity', 'pressure', 'P1', 'P2'}
if not os.path.exists(data_dir):
    os.makedirs(data_dir)


def handler(event, context):
    print(f"Event: {event}")
    print(f"Context: {context}")
    filename = event['file']
    data = get_s3_file_data(filename)
    data = extract_data(data)
    return data


def get_s3_file_data(filename: str) -> pd.DataFrame:
    bucket.download_file(prefix+filename, os.path.join(data_dir, filename))
    df = pd.read_csv(os.path.join(data_dir, filename), parse_dates=['timestamp'], sep=';')
    os.remove(os.path.join(data_dir, filename))
    return df


def extract_data(df: pd.DataFrame) -> dict:
    df = df[[col for col in df.columns if col in data_colums]]
    df = df.set_index('timestamp')
    df = df.resample('1H').mean()
    data = {col: df[col].tolist() for col in df.columns}
    data['timestamp'] = df.index.format()
    return data
    

if __name__ == "__main__":
    data = get_s3_file_data('2017-08-05_bme280_sensor_1207.csv')
    print(data.head())
    print(extract_data(data))