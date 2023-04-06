import os
import boto3
import geopy
import botocore


s3 = boto3.resource('s3')
source_bucket = s3.Bucket("staging-area-bucket")
target_bucket = s3.Bucket("transformed-bucket")
geolocator = geopy.geocoders.Nominatim(user_agent='air-data-pipline')
data_dir = '/tmp/data'
prefix = 'files/new/'
target_prefix = 'sensors/'
if not os.path.exists(data_dir):
    os.makedirs(data_dir)


def handler(event, context):
    print(f"Event: {event}")
    print(f"Context: {context}")
    filename = event['Key'].split('/')[-1]
    sensor_id = filename.split('.')[0].split('_')[-1]
    result_file = f"{sensor_id}.csv"
    if check_s3_file_exist(result_file):
        print(f"File: {result_file} already exists")
        return
    cord = get_coord(filename)
    location_info = get_location_info(*cord)
    write_data_to_s3(sensor_id, location_info)
    print(f"File: {result_file} uploaded to s3")


def check_s3_file_exist(filename: str) -> bool:
    try:
        s3.Object("transformed-bucket", target_prefix+filename).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            # The object does not exist.
            return False
        else:
            # Something else has gone wrong.
            raise
    return True


def get_coord(filename: str) -> tuple:
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
    # get latitude and longitude
    latitude = float(values[lat_col])
    longitude = float(values[lon_col])
    return (latitude, longitude)


def get_location_info(latitude: float, longitude: float):
    location = geolocator.reverse(f"{latitude}, {longitude}", language='en')
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

def write_data_to_s3(sensor_id, data):
    with open(os.path.join(data_dir, f'{sensor_id}.csv'), 'w') as f:
        f.write('sensor_id,latitude,longitude,city,state,country,country_code,zipcode\n')
        f.write(f'{sensor_id},{data["latitude"]},{data["longitude"]},{data["city"]},{data["state"]},{data["country"]},{data["country_code"]},{data["zipcode"]}\n')
    target_bucket.upload_file(os.path.join(data_dir, f'{sensor_id}.csv'), target_prefix+f'{sensor_id}.csv')
    os.remove(os.path.join(data_dir, f'{sensor_id}.csv'))


if __name__ == "__main__":
    coord = get_coord('2017-08-05_bme280_sensor_1207.csv')
    print(coord)
    print(get_location_info(*coord))