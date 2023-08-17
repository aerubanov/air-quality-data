import os
import boto3
import pandas as pd
from pandas.util import hash_pandas_object
import hashlib
import datetime
from typing import Tuple
import botocore

s3 = boto3.resource('s3')
source_bucket_name = os.environ['SOURCE_BUCKET']
target_bucket_name = os.environ['TARGET_BUCKET']
source_bucket = boto3.resource('s3').Bucket(source_bucket_name)
target_bucket = boto3.resource('s3').Bucket(target_bucket_name)
start_date = datetime.datetime(2015, 1, 1)
data_dir = '/tmp/data'
prefix = 'files/new/'
data_colums = {'timestamp', 'temperature', 'humidity', 'pressure', 'P1', 'P2', 'sensor_id', 'location'}
numeric_columns = ['temperature', 'humidity', 'pressure', 'P1', 'P2']
if not os.path.exists(data_dir):
    os.makedirs(data_dir)


def handler(event, context):
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
    """Get data from s3 and return a dataframe"""
    source_bucket.download_file(prefix+filename, os.path.join(data_dir, filename))
    df = pd.read_csv(os.path.join(data_dir, filename), parse_dates=['timestamp'], sep=';')
    os.remove(os.path.join(data_dir, filename))
    return df


def filter_messurements(df: pd.DataFrame) -> pd.DataFrame:
    """Filter out measurements that are too cold or too hot"""
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
    """Select columns, filter, and resample data. Extract time data and fact data"""
    df = df[[col for col in df.columns if col in data_colums]]
    for col in df.columns:
        if col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    df.dropna(inplace=True)
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
    """Write csv files with fact data to s3. Use hash as filename."""
    prefix = 'concentration/' if 'P1' in data.columns else 'temperature/'
    filename = hashlib.sha256(pd.util.hash_pandas_object(data, index=False).values).hexdigest()
    data.to_csv(os.path.join(data_dir, f'{filename}.csv'), index=False)
    target_bucket.upload_file(os.path.join(data_dir, f'{filename}.csv'), prefix+f'{filename}.csv')
    os.remove(os.path.join(data_dir, f'{filename}.csv'))
    return prefix+f'{filename}.csv'

def write_time_to_s3(data: pd.DataFrame) -> str:
    """Write csv files with time data to s3. If file already exists, skip it."""
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
        s3.Object(target_bucket_name, prefix+filename).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            # The object does not exist.
            return False
        else:
            # Something else has gone wrong.
            raise
    return True