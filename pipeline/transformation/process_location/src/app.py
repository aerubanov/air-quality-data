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
    print(f"Event: {event}")
    print(f"Context: {context}")
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
            #write_location_data_to_s3(location_id, timezone, location_info)
            print(f"Location file: {result_file} uploaded to s3")
        sensor_info = get_sensor_info(filename)
        result_file = f"{sensor_info['sensor_id']}.csv"
        if check_s3_file_exist(result_file, target_prefix_sensors):
            print(f"Sensor file: {result_file} already exists")
        else:
            #write_sensor_data_to_s3(sensor_info)
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
    return {
        "sensor_id": sensor_id,
        "sensor_type": sensor_type
    }



def get_location_info(latitude: float, longitude: float):
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
    with open(os.path.join(data_dir, f'{location_id}.csv'), 'w') as f:
        f.write('location_id,latitude,longitude,city,state,country,country_code,zipcode,timezone\n')
        f.write(f'{location_id},{data["latitude"]},{data["longitude"]},{data["city"]},{data["state"]},{data["country"]},{data["country_code"]},{data["zipcode"]},{timezone}\n')
    target_bucket.upload_file(os.path.join(data_dir, f'{location_id}.csv'), target_prefix_locations+f'{location_id}.csv')
    os.remove(os.path.join(data_dir, f'{location_id}.csv'))

def write_sensor_data_to_s3(sensor_info:dict):
    with open(os.path.join(data_dir, f'{sensor_info["sensor_id"]}.csv'), 'w') as f:
        f.write('sensor_id,sensor_type\n')
        f.write(f'{sensor_info["sensor_id"]},{sensor_info["sensor_type"]}\n')
    target_bucket.upload_file(os.path.join(data_dir, f'{sensor_info["sensor_id"]}.csv'), target_prefix_sensors+f'{sensor_info["sensor_id"]}.csv')
    os.remove(os.path.join(data_dir, f'{sensor_info["sensor_id"]}.csv'))


if __name__ == "__main__":
    event = {
    "Items": [
    {
      "Etag": "\"f286931bc6cd2edb6c8fb21ddb2fa5d8\"",
      "Key": "files/new/2023-04-09_pms7003_sensor_79683.csv",
      "LastModified": 1690631451,
      "Size": 25097,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"8deb7fa7bed6cc3770647f6ce5642c0c\"",
      "Key": "files/new/2023-04-09_ppd42ns_sensor_107.csv",
      "LastModified": 1690631452,
      "Size": 15424,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"8bebee410455b10e9e36432f140d4644\"",
      "Key": "files/new/2023-04-09_radiation_sbm-19_sensor_46386.csv",
      "LastModified": 1690631452,
      "Size": 115271,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"d325f3ce5c42d6d05221321ef9c411d3\"",
      "Key": "files/new/2023-04-09_radiation_sbm-19_sensor_71364.csv",
      "LastModified": 1690631453,
      "Size": 45543,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"04bb29d368f0d9c692b18e1a79469048\"",
      "Key": "files/new/2023-04-09_radiation_sbm-19_sensor_73604.csv",
      "LastModified": 1690631453,
      "Size": 46378,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"b6d797d55034d2b9894ce60f54ad0da8\"",
      "Key": "files/new/2023-04-09_radiation_sbm-20_sensor_71217.csv",
      "LastModified": 1690631450,
      "Size": 45055,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"bd08404d825e6a858a91efff86958cee\"",
      "Key": "files/new/2023-04-09_radiation_sbm-20_sensor_76518.csv",
      "LastModified": 1690631451,
      "Size": 45624,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"e688df259cafd8f0457f22ea11e9ccc1\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_31122.csv",
      "LastModified": 1690631452,
      "Size": 41234,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"c8587e90f4e24814840cd8fb258c0367\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_36347.csv",
      "LastModified": 1690631452,
      "Size": 38660,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"0386de5b8341823c108a6e04da85e950\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_36953.csv",
      "LastModified": 1690631453,
      "Size": 39000,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"992a61d1cc7cdfce74b341b628819bce\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_37761.csv",
      "LastModified": 1690631453,
      "Size": 41444,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"60c35d4f0b1c35ba623d780ccafc1fdb\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_37773.csv",
      "LastModified": 1690631450,
      "Size": 44564,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"f144831d677517995ee75deb7d94fb10\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_38695.csv",
      "LastModified": 1690631451,
      "Size": 38797,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"0cf2f7e0ed90429ab7a210881dcfbf21\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_39280.csv",
      "LastModified": 1690631452,
      "Size": 44664,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"1d332acf3339a63ccec922002ecce032\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_39976.csv",
      "LastModified": 1690631452,
      "Size": 18947,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"96ff5a3b02cec585e524624b82a3d8d7\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_40369.csv",
      "LastModified": 1690631453,
      "Size": 37778,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"2d89fddac0e2f1c6b2a21be59afcc969\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_41675.csv",
      "LastModified": 1690631453,
      "Size": 44937,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"40f394f07f568373f4c381c80cba5723\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_42048.csv",
      "LastModified": 1690631450,
      "Size": 51680,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"3db5cbb9eaec6004db4a92ffd297a387\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_43128.csv",
      "LastModified": 1690631451,
      "Size": 44105,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"7f482e627668828cc828d794af79c399\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_43947.csv",
      "LastModified": 1690631452,
      "Size": 52139,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"f1af030664f72635bbf3fed84f067d14\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_44590.csv",
      "LastModified": 1690631452,
      "Size": 45944,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"3af9d5e475eb34d3bb9b654735cb437a\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_45738.csv",
      "LastModified": 1690631453,
      "Size": 35564,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"dadd6ec0722b5e02d333500c6a44ed46\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_45879.csv",
      "LastModified": 1690631453,
      "Size": 51510,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"6898f1029d86a055001565ccd87035fc\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_46258.csv",
      "LastModified": 1690631450,
      "Size": 51401,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"97275d32a2965a1e8b73960cbd84f4fa\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_46599.csv",
      "LastModified": 1690631451,
      "Size": 40795,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"169f51eb13b28ffc3e3f66b7158f5644\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_46973.csv",
      "LastModified": 1690631452,
      "Size": 49728,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"44117695d420cda8e07ba1e1cfa5048b\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_50358.csv",
      "LastModified": 1690631452,
      "Size": 34684,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"21a95dea824d7d746f081621244e1a31\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_57613.csv",
      "LastModified": 1690631453,
      "Size": 51324,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"b3008fbaac928cc9a6d6eb4a2c48b700\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_57776.csv",
      "LastModified": 1690631453,
      "Size": 50439,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"683759ad45d7e0dede4ba14ad1200696\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_59328.csv",
      "LastModified": 1690631451,
      "Size": 43001,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"5434ec32d8db5758825a67a164772c31\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_59914.csv",
      "LastModified": 1690631451,
      "Size": 7847,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"a3a58b756e14a4fa0831162861d64be5\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_68488.csv",
      "LastModified": 1690631452,
      "Size": 656,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"2a9c52e2b14accdde7169f7fc024577f\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_68627.csv",
      "LastModified": 1690631452,
      "Size": 747,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"a1c53629ccb48d06f858abead2e72394\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_68890.csv",
      "LastModified": 1690631453,
      "Size": 656,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"c603d9a4e044b93dfd57bc9f678a5142\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_68964.csv",
      "LastModified": 1690631453,
      "Size": 471,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"b99aa809680b09030aae20649a8b3100\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_69398.csv",
      "LastModified": 1690631451,
      "Size": 44798,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"d366fb1c3e26429c560188e110d77fbf\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_69441.csv",
      "LastModified": 1690631451,
      "Size": 44739,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"506d4426535605e4e55e258651e4bd77\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_70784.csv",
      "LastModified": 1690631452,
      "Size": 22068,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"ff97ca112d14c3f27676421d71c1dd08\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_70948.csv",
      "LastModified": 1690631452,
      "Size": 50225,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"02f7b3e768956f02fcb049b3928f22ce\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_71180.csv",
      "LastModified": 1690631453,
      "Size": 45669,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"160d59b322b7017b1b77749fad84e77d\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_71284.csv",
      "LastModified": 1690631453,
      "Size": 15384,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"b58729ff023619ee62705ae1206f41e8\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_71576.csv",
      "LastModified": 1690631451,
      "Size": 50234,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"0c6d93512a3a77cbc64a953c5f569616\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_72145.csv",
      "LastModified": 1690631451,
      "Size": 45784,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"fa7e3097109ba3ca70e5f128ac71f934\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_73274.csv",
      "LastModified": 1690631452,
      "Size": 749,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"e4ded2078fa07e7d3eb87d036c071540\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_73622.csv",
      "LastModified": 1690631452,
      "Size": 51406,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"46d1921d35df5f8b7fd1c7314ff1f4db\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_73668.csv",
      "LastModified": 1690631453,
      "Size": 45798,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"8195bcaaa445b2ce114401d3040c4c82\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_73785.csv",
      "LastModified": 1690631453,
      "Size": 50594,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"ea28d8c3d824c3efafec7bd6dfde34ea\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_73797.csv",
      "LastModified": 1690631450,
      "Size": 50834,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"4861c6a410f29b293db87841442acabb\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_73887.csv",
      "LastModified": 1690631451,
      "Size": 26294,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"caa438e2d24a9c7b6c40ba175661a2b8\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_73891.csv",
      "LastModified": 1690631452,
      "Size": 42825,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"ac01737519222857bae6e9ab13e25a9d\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_74110.csv",
      "LastModified": 1690631452,
      "Size": 45144,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"d6de25daf9ffe5e4b6d6fade50d46279\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_74198.csv",
      "LastModified": 1690631453,
      "Size": 50834,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"d7f40d39b49ac18560c717b1f2424080\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_74332.csv",
      "LastModified": 1690631453,
      "Size": 51314,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"fb822a81b99a87363635a2589c59d8b2\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_74797.csv",
      "LastModified": 1690631450,
      "Size": 44008,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"59a2e65a9bd6a4f20c56004f1954d6b5\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_74837.csv",
      "LastModified": 1690631451,
      "Size": 45246,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"1464df4c32e17dab608a1914312d9c94\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_74844.csv",
      "LastModified": 1690631452,
      "Size": 45721,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"0480d257ac12dfa86f76f4e4fceb27b8\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_75158.csv",
      "LastModified": 1690631452,
      "Size": 45384,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"d6f36c64722120372117a1b23503f9ae\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_75270.csv",
      "LastModified": 1690631453,
      "Size": 46286,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"72cbc27d45532b1f9a95cb31a1b2b975\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_75411.csv",
      "LastModified": 1690631453,
      "Size": 45133,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"28ecddafedeeab687b5a58b4ad1f3c40\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_75432.csv",
      "LastModified": 1690631450,
      "Size": 45304,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"a3c3185bf1617afd8105548afac293b5\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_75443.csv",
      "LastModified": 1690631451,
      "Size": 28492,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"e437a5be88c74bbb3fb57dba1a370344\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_75841.csv",
      "LastModified": 1690631452,
      "Size": 43544,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"6deda1e85a32e9b2a59fdd93d3a7c80c\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_77193.csv",
      "LastModified": 1690631452,
      "Size": 45464,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"72c40a8c5f55e3077f35dd93157e7487\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_77506.csv",
      "LastModified": 1690631453,
      "Size": 45545,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"2cf27358776f883f13db0647b3578eb3\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_78047.csv",
      "LastModified": 1690631453,
      "Size": 46648,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"a1e80ae4960b627796f7c1a0b74a9476\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_79332.csv",
      "LastModified": 1690631451,
      "Size": 31245,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"116441306065eae10e1da749c2338ce6\"",
      "Key": "files/new/2023-04-09_radiation_si22g_sensor_79704.csv",
      "LastModified": 1690631451,
      "Size": 5162,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"28c21fb683a9687ea8c312b93762278d\"",
      "Key": "files/new/2023-04-09_scd30_sensor_79336.csv",
      "LastModified": 1690631451,
      "Size": 38040,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"bc2e61125459d2983a904ff95235468b\"",
      "Key": "files/new/2023-04-09_sds011_sensor_1000.csv",
      "LastModified": 1690631454,
      "Size": 45321,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"e563cceec2a699783211e068d28ce8d2\"",
      "Key": "files/new/2023-04-09_sds011_sensor_10001.csv",
      "LastModified": 1690631466,
      "Size": 45705,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"e6bb562fadcd774e5b2ee88c0c23749c\"",
      "Key": "files/new/2023-04-09_sds011_sensor_10005.csv",
      "LastModified": 1690631466,
      "Size": 39015,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"c6a2f9b0714c6399f8587d52b1ed4bb4\"",
      "Key": "files/new/2023-04-09_sds011_sensor_10007.csv",
      "LastModified": 1690631467,
      "Size": 38273,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"e45b97d5b3fd993dac04a2a84c96cc81\"",
      "Key": "files/new/2023-04-09_sds011_sensor_10029.csv",
      "LastModified": 1690631463,
      "Size": 37653,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"c2c22de6e49a0e0a77c0268a3327476b\"",
      "Key": "files/new/2023-04-09_sds011_sensor_10031.csv",
      "LastModified": 1690631463,
      "Size": 38912,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"4685d78f589902d75633e26ad11ac312\"",
      "Key": "files/new/2023-04-09_sds011_sensor_1004.csv",
      "LastModified": 1690631452,
      "Size": 37674,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"89bae6fa521dafb154e50056297a3f8e\"",
      "Key": "files/new/2023-04-09_sds011_sensor_10041.csv",
      "LastModified": 1690631464,
      "Size": 28357,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"d8c2eeda81554da4dc4279bffae9155e\"",
      "Key": "files/new/2023-04-09_sds011_sensor_10045.csv",
      "LastModified": 1690631465,
      "Size": 38207,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"3e3f6b45d283b2b3f2dc18a88a1f1855\"",
      "Key": "files/new/2023-04-09_sds011_sensor_10056.csv",
      "LastModified": 1690631466,
      "Size": 58934,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"a8e96ce6f04d9ab925e5883e90563aae\"",
      "Key": "files/new/2023-04-09_sds011_sensor_10059.csv",
      "LastModified": 1690631467,
      "Size": 30405,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"7fa11dfabdb547e79de4c739f8f399f7\"",
      "Key": "files/new/2023-04-09_sds011_sensor_10061.csv",
      "LastModified": 1690631463,
      "Size": 37510,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"03200e441025e2bf8e9a5a8fec04f0cd\"",
      "Key": "files/new/2023-04-09_sds011_sensor_10063.csv",
      "LastModified": 1690631463,
      "Size": 19310,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"9d6d512aeb2a2ad457ed96b699125455\"",
      "Key": "files/new/2023-04-09_sds011_sensor_10078.csv",
      "LastModified": 1690631464,
      "Size": 38496,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"e62661528db237e2d50cb8a3ab1b655f\"",
      "Key": "files/new/2023-04-09_sds011_sensor_10082.csv",
      "LastModified": 1690631465,
      "Size": 37978,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"b3c7a5f7b001e74a3b1950ff67fe1b3f\"",
      "Key": "files/new/2023-04-09_sds011_sensor_10088.csv",
      "LastModified": 1690631466,
      "Size": 31458,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"e121ef6cd3ef429e54e8c45229ab9293\"",
      "Key": "files/new/2023-04-09_sds011_sensor_10090.csv",
      "LastModified": 1690631467,
      "Size": 38193,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"12e194fb1c9c54d350a72441fa09cf7e\"",
      "Key": "files/new/2023-04-09_sds011_sensor_10094.csv",
      "LastModified": 1690631463,
      "Size": 34278,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"d06ebcbd9904bfc6ac33ba24111efd80\"",
      "Key": "files/new/2023-04-09_sds011_sensor_10096.csv",
      "LastModified": 1690631463,
      "Size": 38429,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"56c336aab7aa6ea94c1ede9fc07c526e\"",
      "Key": "files/new/2023-04-09_sds011_sensor_1010.csv",
      "LastModified": 1690631452,
      "Size": 38075,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"5fae3bfb64971a2481ce146725a5b89e\"",
      "Key": "files/new/2023-04-09_sds011_sensor_10100.csv",
      "LastModified": 1690631464,
      "Size": 38674,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"3bfb552f955ab8de1d9abe8e129914e5\"",
      "Key": "files/new/2023-04-09_sds011_sensor_10110.csv",
      "LastModified": 1690631465,
      "Size": 38709,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"63634552103fbc55e49419cfb226c73a\"",
      "Key": "files/new/2023-04-09_sds011_sensor_10112.csv",
      "LastModified": 1690631466,
      "Size": 37724,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"f50ceba2a1dfe1af6c37a20dbdb28a2e\"",
      "Key": "files/new/2023-04-09_sds011_sensor_10114.csv",
      "LastModified": 1690631467,
      "Size": 34963,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"3688af5ce23265315834cb3ec437596a\"",
      "Key": "files/new/2023-04-09_sds011_sensor_10116.csv",
      "LastModified": 1690631463,
      "Size": 38241,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"536853135db1e88e07b5ccf8265de42a\"",
      "Key": "files/new/2023-04-09_sds011_sensor_10120.csv",
      "LastModified": 1690631463,
      "Size": 37398,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"19f824f8cd4f1f957ae739e28cdc4dc8\"",
      "Key": "files/new/2023-04-09_sds011_sensor_10122.csv",
      "LastModified": 1690631464,
      "Size": 37861,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"9b41b6768bb5b4687497a051278534bb\"",
      "Key": "files/new/2023-04-09_sds011_sensor_10126.csv",
      "LastModified": 1690631465,
      "Size": 39788,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"efe8f1fa5c681eb603811750b12431e8\"",
      "Key": "files/new/2023-04-09_sds011_sensor_10128.csv",
      "LastModified": 1690631466,
      "Size": 37978,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"7a8614d82dbf5784ce533b1c7460f7f0\"",
      "Key": "files/new/2023-04-09_sds011_sensor_10132.csv",
      "LastModified": 1690631467,
      "Size": 37807,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"5721b0cc2eba1210aa91716d4a496129\"",
      "Key": "files/new/2023-04-09_sds011_sensor_10134.csv",
      "LastModified": 1690631463,
      "Size": 38525,
      "StorageClass": "STANDARD"
    },
    {
      "Etag": "\"ca61fee43fcbd8593c984f5b8672a9bd\"",
      "Key": "files/new/2023-04-09_sds011_sensor_1014.csv",
      "LastModified": 1690631453,
      "Size": 81313,
      "StorageClass": "STANDARD"
    }
    ] 
    }
    handler(event, None)
    # *coord, location_id = get_coord('2023-03-01_bme280_sensor_10006.csv')
    # print(coord)
    # print(location_id)
    # print(get_location_info(*coord))
    # print(get_timezone(*coord))