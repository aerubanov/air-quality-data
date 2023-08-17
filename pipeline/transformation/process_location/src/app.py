import os
import boto3
import geopy
from geopy.exc import GeocoderServiceError
import botocore
from timezonefinder import TimezoneFinder
import time

s3 = boto3.resource('s3')
source_bucket_name = os.environ['SOURCE_BUCKET']
target_bucket_name = os.environ['TARGET_BUCKET']
source_bucket = s3.Bucket(source_bucket_name)
target_bucket = s3.Bucket(target_bucket_name)
data_dir = '/tmp/data'
prefix = 'files/new/'
target_prefix_sensors = 'sensors/'
target_prefix_locations = 'locations/'
if not os.path.exists(data_dir):
    os.makedirs(data_dir)
tf = TimezoneFinder()


def handler(event, context):
    for item in event["Items"]:
        filename = item['Key'].split('/')[-1]
        *cord, location_id = get_coord(filename)
        result_file = f"{location_id}.csv"
        if check_s3_file_exist(result_file, target_prefix_locations):
            print(f"Location file: {result_file} already exists")
        else:
            location_info = get_location_info(*cord)
            if location_info is None:
                continue
            timezone = get_timezone(*cord)
            write_location_data_to_s3(location_id, timezone, location_info)
            print(f"Location file: {result_file} uploaded to s3")
        sensor_info = get_sensor_info(filename)
        result_file = f"{sensor_info['sensor_id']}.csv"
        if check_s3_file_exist(result_file, target_prefix_sensors):
            print(f"Sensor file: {result_file} already exists")
        else:
            write_sensor_data_to_s3(sensor_info)
            print(f"Sensor file: {result_file} uploaded to s3")
    return {"Status": "Succes", "Items": event["Items"]}


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


def get_coord(filename: str) -> tuple:
    """Get cordinate and location_id from first row of csv file based on header"""
    source_bucket.download_file(prefix+filename, os.path.join(data_dir, filename))
    # read first two lines from csv file
    with open(os.path.join(data_dir, filename), 'r') as f:
        data = f.readlines()[:2]
    os.remove(os.path.join(data_dir, filename))
    headers = data[0].split(';')
    values = data[1].split(';')
    # get position of lat and lon columns
    lat_col = headers.index('lat')
    lon_col = headers.index('lon')
    location_col = headers.index('location')
    # get latitude and longitude
    latitude = float(values[lat_col])
    longitude = float(values[lon_col])
    location_id = int(values[location_col])
    return (latitude, longitude, location_id)


def get_sensor_info(filename: str) -> dict:
    """Get sensor_id and sensor_type from first row of csv file based on header"""
    source_bucket.download_file(prefix+filename, os.path.join(data_dir, filename))
    with open(os.path.join(data_dir, filename), 'r') as f:
        data = f.readlines()
    os.remove(os.path.join(data_dir, filename))
    headers = data[0].split(';')
    values = data[1].split(';')
    senor_id_col = headers.index('sensor_id')
    sensor_type_col = headers.index('sensor_type')
    sensor_id = int(values[senor_id_col])
    sensor_type = values[sensor_type_col]
    is_indoor = 'indoor' in filename
    return {
        "sensor_id": sensor_id,
        "is_indoor": is_indoor,
        "sensor_type": sensor_type
    }



def get_location_info(latitude: float, longitude: float):
    """Get city, state, country, country_code, zipcode from geopy"""
    geolocator = geopy.geocoders.Nominatim(user_agent='air-data-pipline')
    error_count = 0
    while True:
        try:
            location = geolocator.reverse(f"{latitude}, {longitude}", language='en')
            break
        except GeocoderServiceError:
            error_count += 1
            if error_count > 3:
                raise
            time.sleep(4^error_count)
    if location is None:
        return None

    address = location.raw['address']
    city = address.get('city', '')
    state = address.get('state', '')
    country = address.get('country', '')
    country_code = address.get('country_code')
    zipcode = address.get('postcode')
    return {
        'city': city,
        'state': state,
        'country': country,
        'country_code': country_code,
        'zipcode': zipcode,
        'latitude': latitude,
        'longitude': longitude,
        }

def get_timezone(latitude: float, longitude: float):
    return tf.timezone_at(lng=longitude, lat=latitude)

def write_location_data_to_s3(location_id, timezone, data):
    if check_s3_file_exist(f"{location_id}.csv", target_prefix_locations):
        print(f"Location file: {location_id}.csv already exists")
        return
    with open(os.path.join(data_dir, f'{location_id}.csv'), 'w') as f:
        f.write('location_id,latitude,longitude,city,state,country,country_code,zipcode,timezone\n')
        f.write(f'{location_id},{data["latitude"]},{data["longitude"]},{data["city"]},{data["state"]},{data["country"]},{data["country_code"]},{data["zipcode"]},{timezone}\n')
    target_bucket.upload_file(os.path.join(data_dir, f'{location_id}.csv'), target_prefix_locations+f'{location_id}.csv')
    os.remove(os.path.join(data_dir, f'{location_id}.csv'))

def write_sensor_data_to_s3(sensor_info:dict):
    if check_s3_file_exist(f"{sensor_info['sensor_id']}.csv", target_prefix_sensors):
        print(f"Sensor file: {sensor_info['sensor_id']}.csv already exists")
        return
    with open(os.path.join(data_dir, f'{sensor_info["sensor_id"]}.csv'), 'w') as f:
        f.write('sensor_id,sensor_type,is_indoor\n')
        f.write(f'{sensor_info["sensor_id"]},{sensor_info["sensor_type"]},{sensor_info["is_indoor"]}\n')
    target_bucket.upload_file(os.path.join(data_dir, f'{sensor_info["sensor_id"]}.csv'), target_prefix_sensors+f'{sensor_info["sensor_id"]}.csv')
    os.remove(os.path.join(data_dir, f'{sensor_info["sensor_id"]}.csv'))