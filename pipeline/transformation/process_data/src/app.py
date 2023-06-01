import os
import boto3
import pandas as pd
import datetime
from typing import Tuple
import botocore

s3 = boto3.resource('s3')
source_bucket = boto3.resource('s3').Bucket("staging-area-bucket")
target_bucket = boto3.resource('s3').Bucket("transformed-bucket")
start_date = datetime.datetime(2015, 1, 1)
data_dir = '/tmp/data'
prefix = 'files/new/'
data_colums = {'timestamp', 'temperature', 'humidity', 'pressure', 'P1', 'P2', 'sensor_id', 'location'}
if not os.path.exists(data_dir):
    os.makedirs(data_dir)


def handler(event, context):
    print(f"Event: {event}")
    print(f"Context: {context}")
    fact_list, time_list = [], []
    for item in event["Items"]:
        filename = item['Key'].split('/')[-1]
        data = get_s3_file_data(filename)
        fact_df, time_df = extract_data(data)
        fact_list.append(fact_df)
        time_list.append(time_df)
    fact_df = pd.concat(fact_list)
    time_df = pd.concat(time_list)
    result = write_data_to_s3(fact_df)
    print(f"File: {result} uploaded to s3")
    cnt = write_time_to_s3(time_df)
    print(f"{cnt} time files uploaded to s3")
    return {"Status": "Succes", "Items": event["Items"]}


def get_s3_file_data(filename: str) -> pd.DataFrame:
    source_bucket.download_file(prefix+filename, os.path.join(data_dir, filename))
    df = pd.read_csv(os.path.join(data_dir, filename), parse_dates=['timestamp'], sep=';')
    os.remove(os.path.join(data_dir, filename))
    return df


def filter_messurements(df: pd.DataFrame) -> pd.DataFrame:
    # filter temperature columns if present
    if 'temperature' in df.columns:
        df.drop(df[df['temperature'] > 60].index, inplace=True)
        df.drop(df[df['temperature'] < -60].index, inplace=True)
    
    #filter humidity if present
    if 'humidity' in df.columns:
        df.drop(df[df['humidity'] > 100].index, inplace=True)
        df.drop(df[df['humidity'] < 0].index, inplace=True)
    
    # filter pressure if present
    if 'pressure' in df.columns:
        df.drop(df[df['pressure'] > 120000].index, inplace=True)
        df.drop(df[df['pressure'] < 80000].index, inplace=True)

    # filter P1 if present:
    if 'P1' in df.columns:
        df.drop(df[df['P1'] > 1000].index, inplace=True)
        df.drop(df[df['P1'] < 0].index, inplace=True)

    # filter P2 if present:
    if 'P2' in df.columns:
        df.drop(df[df['P2'] > 1000].index, inplace=True)
        df.drop(df[df['P2'] < 0].index, inplace=True)

    return df



def extract_data(df: pd.DataFrame) -> Tuple[pd.DataFrame]:
    df = df[[col for col in df.columns if col in data_colums]]
    df = filter_messurements(df)
    df.set_index('timestamp', inplace=True)
    df = df.resample('1H').mean()
    df.dropna(inplace=True)
    df['sensor_id'] = df['sensor_id'].astype(int)
    df['location_id'] = df['location'].astype(int)
    df['time_id'] = (df.index-start_date)//pd.Timedelta('1 hour')

    df['year'] = df.index.year
    df['month'] = df.index.month
    df['day_of_week'] = df.index.dayofweek
    df['timestamp'] = df.index

    time_df = df[['time_id', 'year', 'month', 'day_of_week', 'timestamp']]
    fact_df = df[[col for col in df.columns if col in [
                    'temperature',
                    'humidity', 
                    'pressure', 
                    'P1', 
                    'P2',
                    'sensor_id',
                    'location_id',
                    'time_id']
                ]]
    return fact_df, time_df
    

def write_data_to_s3(data: pd.DataFrame) -> str:
    prefix = 'concentration/' if 'P1' in data.columns else 'temperature/'
    date = data.index[0].date()
    sensor_id = data['sensor_id'].iloc[0]
    data.to_csv(os.path.join(data_dir, f'{date}_{sensor_id}.csv'), index=False)
    target_bucket.upload_file(os.path.join(data_dir, f'{date}_{sensor_id}.csv'), prefix+f'{date}_{sensor_id}.csv')
    os.remove(os.path.join(data_dir, f'{date}_{sensor_id}.csv'))
    return prefix+f'{date}_{sensor_id}.csv'

def write_time_to_s3(data: pd.DataFrame) -> str:
    prefix = 'time/'
    data = data.drop_duplicates()
    columns = data.columns
    cnt = 0
    # iterate over rows in dataframe
    for _, row in data.iterrows():
        time_id = row['time_id']
        if check_s3_file_exist(f"{time_id}.csv", prefix):
            continue
        with open(os.path.join(data_dir, f'{time_id}.csv'), 'w') as f:
            f.write(','.join(columns)+"\n")
            f.write(','.join([str(row[col]) for col in columns])+"\n")
        target_bucket.upload_file(os.path.join(data_dir, f'{time_id}.csv'), prefix+f'{time_id}.csv')
        os.remove(os.path.join(data_dir, f'{time_id}.csv'))
        cnt += 1
    return cnt
    


def check_s3_file_exist(filename: str, prefix: str) -> bool:
    try:
        s3.Object("transformed-bucket", prefix+filename).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            # The object does not exist.
            return False
        else:
            # Something else has gone wrong.
            raise
    return True


if __name__ == "__main__":
    data = get_s3_file_data('2023-03-01_bme280_sensor_10006.csv')
    print(data.head())
    fact_df, time_df = extract_data(data)
    print(fact_df)
    print(time_df)
    #write_time_to_s3(time_df)
    #write_data_to_s3(fact_df)