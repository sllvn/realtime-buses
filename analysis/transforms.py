import re, csv, json
import pandas as pd
import geopandas as gpd
from datetime import datetime
from tqdm import tqdm
import pickle
from shapely.geometry import Point
from shapely.ops import nearest_points


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
        'time':  location_time
    }


def load_locations_from_files(filenames):
    locations = []

    for filename in tqdm(filenames, desc='loading locations from files'):
        with open(f'data/locations/{filename}', 'r') as f:
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


def make_find_closest(trip_geometries, stops_df):
    def process(loc):
        point = Point(loc['lng'], loc['lat'])

        try:
            trip_stop_locations = trip_geometries[loc['trip_id']]
            closest_stop = nearest_points(trip_stop_locations, point)[0]
            stop_lon, stop_lat = closest_stop.x, closest_stop.y
            closest_stop = stops_df[(stops_df['stop_lon'] == stop_lon) & (stops_df['stop_lat'] == stop_lat)]
            return closest_stop['stop_id'].iloc[0]
        except Exception as e:
            return None
    return process


def get_stops_for_trip(trip_id, stops_df, stop_times_df):
    stop_times_for_trip = stop_times_df[stop_times_df.trip_id == trip_id]
    stops_for_trip = stops_df[stops_df.stop_id.isin(stop_times_for_trip.stop_id)]
    return stops_for_trip


def load_trip_geometries(gtfs_dfs, reload=False, date=None):
    # takes ~6 min to reload
    # TODO: update this to auto-reload if pickled version does not exist
    if reload:
        trip_geometries = {}
        crs = {'init': 'epsg:4326'}
        trip_ids = gtfs_dfs['trips']['trip_id'].unique()

        for trip_id in tqdm(trip_ids, desc='creating trip geometry hash table'):
            stops_for_trip = get_stops_for_trip(trip_id, gtfs_dfs['stops'], gtfs_dfs['stop_times'])
            stops_for_trip = stops_for_trip[['stop_id', 'stop_lat', 'stop_lon']]
            # stops_for_trip['stop_lat'] = stops_for_trip['stop_lat'].apply(pd.to_numeric)
            # stops_for_trip['stop_lon'] = stops_for_trip['stop_lon'].apply(pd.to_numeric)

            geometry = [Point(xy) for xy in zip(stops_for_trip.stop_lon, stops_for_trip.stop_lat)]
            stops_for_trip = stops_for_trip.drop(['stop_lon', 'stop_lat'], axis=1)
            stops_for_trip = gpd.GeoDataFrame(stops_for_trip, crs=crs, geometry=geometry)
            trip_geometries[trip_id] = stops_for_trip.geometry.unary_union

        pickle.dump(trip_geometries, open(f'data/trip_geometries_{date}.p', 'wb'))
    else:
        trip_geometries = pickle.load(open(f'data/trip_geometries_{date}.p', 'rb'))

    return trip_geometries
