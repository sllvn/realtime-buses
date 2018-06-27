import re
import pandas as pd
from os import listdir
from os.path import isfile, join
from datetime import datetime

from geopy.distance import geodesic
from tqdm import tqdm
import dask.dataframe as dd
from dask.multiprocessing import get

from transforms import load_locations_from_files

tqdm.pandas()

NUMBER_OF_CORES = 8


def get_stops_for_trip(trip_id, stops_df, stop_times_df):
    stop_times_for_trip = stop_times_df[stop_times_df.trip_id == trip_id]
    stops_for_trip = stops_df[stops_df.stop_id.isin(stop_times_for_trip.stop_id)]
    return stops_for_trip


def find_closest_stop_id(location, stops_df, stop_times_df):
    vehicle_latlng = (location['lat'], location['lng'])
    closest_stop = (None, 100_000)

    stops_for_trip = get_stops_for_trip(location['trip_id'], stops_df, stop_times_df)

    for _, stop in stops_for_trip.iterrows():
        stop_latlng = (stop['stop_lat'], stop['stop_lon'])
        distance = geodesic(vehicle_latlng, stop_latlng).m
        if distance < closest_stop[1]:
            closest_stop = (stop['stop_id'], distance)

    return closest_stop[0]


def apply_find_closest_stop_id(partition_df, stops_df, stop_times_df):
    return partition_df.apply(lambda loc: find_closest_stop_id(loc, stops_df, stop_times_df), axis=1)


def load_location_data_for_date(date_string):
    filenames = [f for f in listdir('./data/locations') if isfile(join('./data/locations', f))]
    filenames = [f for f in filenames if re.search(date_string, f)]

    locations_df = load_locations_from_files(filenames)
    locations_df.dropna(subset=['trip_id'], inplace=True)
    locations_df.drop_duplicates(inplace=True)

    tqdm.pandas(desc='casting trip_id to numeric')
    locations_df['trip_id'] = locations_df['trip_id'].progress_apply(pd.to_numeric)

    tqdm.pandas(desc='casting vehicle_id to numeric')
    locations_df['vehicle_id'] = locations_df['vehicle_id'].progress_apply(pd.to_numeric)
    tqdm.pandas()

    return locations_df


def transform_locations(locations_df, gtfs_dfs, parallelize=True):
    print(f'transforming locations (parallelize={parallelize})')
    trip_route_lookup = gtfs_dfs['trip_route_lookup']
    stops_df = gtfs_dfs['stops_df']
    stop_times_df = gtfs_dfs['stop_times_df']

    locations_df = pd.merge(locations_df, trip_route_lookup, how='left', on='trip_id')

    if parallelize:
        current_time = datetime.now().isoformat()
        print(f'beginning parallel transforms, this might take a while... (started at {current_time})')
        ddata = dd.from_pandas(locations_df, npartitions=NUMBER_OF_CORES)
        locations_df['closest_stop_id'] = ddata.map_partitions(lambda loc: apply_find_closest_stop_id(loc, stops_df, stop_times_df)).compute(get=get)
    else:
        locations_df['closest_stop_id'] = locations_df.progress_apply(lambda loc: find_closest_stop_id(loc, stops_df, stop_times_df), axis=1)

    locations_df['closest_stop_id'] = locations_df['closest_stop_id'].fillna(0.0).astype(int)

    return locations_df
