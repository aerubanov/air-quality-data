import os
import boto3
import geopy


bucket = boto3.resource('s3').Bucket("staging-area-bucket")
geolocator = geopy.geocoders.Nominatim(user_agent='air-data-pipline')
data_dir = '/tmp/data'
prefix = 'files/new/'
if not os.path.exists(data_dir):
    os.makedirs(data_dir)


def handler(event, context):
    print(f"Event: {event}")
    print(f"Context: {context}")
    filename = event['file']
    cord = get_coord(filename)
    location_info = get_location_info(*cord)
    return location_info


def get_coord(filename: str) -> tuple:
    bucket.download_file(prefix+filename, os.path.join(data_dir, filename))
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
    city = location.raw['address']['city']
    country = location.raw['address']['country']
    country_code = location.raw['address']['country_code']
    return {
        'city': city,
        'country': country,
        'country_code': country_code,
        'latitude': latitude,
        'longitude': longitude,
        }



if __name__ == "__main__":
    coord = get_coord('2017-08-05_bme280_sensor_1207.csv')
    print(coord)
    print(get_location_info(*coord))