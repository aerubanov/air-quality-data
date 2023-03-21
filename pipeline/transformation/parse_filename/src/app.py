import os

def handler(event, context):
    print(f'Event: {event}')
    print(f'Context: {context}')
    pass


def parse_filename(filename: str) -> dict:
    indoor = "indoor" in filename
    parts = filename.split('.')[0].split('_')
    if indoor:
        sensor_id = parts[-2]
        sensor_type = parts[-4]
    else:
        sensor_id = parts[-1]
        sensor_type = parts[-3]
    return {
        'sensor_id': sensor_id,
        'sensor_type': sensor_type,
        'indoor': indoor
    }