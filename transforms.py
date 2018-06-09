import re, csv, json
import pandas as pd
from datetime import datetime
from tqdm import tqdm


def read_gtfs_into_df(filename):
    with open(f'./seattle-gtfs-20180511/{filename}.txt', 'r+') as csvfile:
        reader = csv.reader(csvfile)
        headers = next(reader)
        rows = []
        for line in reader:
            row = {}
            for h, v in zip(headers, line):
                row[h] = v
            rows.append(row)
    return pd.DataFrame(rows)


raw_id_re = re.compile('[0-9]_([0-9]*)')
def parse_raw_id(id):
    match = raw_id_re.match(str(id))
    if match:
        return match.group(1)
    else:
        return None


def parse_timestamp(timestamp):
    return datetime.fromtimestamp(timestamp / 1000)


def parse_latlng(location):
    return (location['location']['lat'], location['location']['lon'])


def parse_location(location):
    lat, lng = parse_latlng(location)
    trip_id = parse_raw_id(location['tripId'])
    vehicle_id = parse_raw_id(location['vehicleId'])
    location_time = parse_timestamp(location['lastLocationUpdateTime'])

    return {
        'lat': lat,
        'lng': lng,
        'trip_id': trip_id,
        'vehicle_id': vehicle_id,
        'location_time':  location_time
    }


def load_locations_from_files(filenames):
    locations = []

    for filename in tqdm(filenames):
        with open(f'data/{filename}', 'r') as f:
            json_locations = json.loads(f.read())

            try:
                json_locations['data']['list']
            except:
                print(f'error parsing locations from {filename}')
                continue

            for location in json_locations['data']['list']:
                try:
                    loc = parse_location(location)
                    locations.append(loc)
                except Exception as e:
                    print(f'error parsing location with trip_id {location["tripId"]} from file {filename}')
                    print(e)

    return pd.DataFrame(locations)
