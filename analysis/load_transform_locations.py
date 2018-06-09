import re
import pandas as pd
from os import listdir
from os.path import isfile, join
from datetime import datetime

from geopy.distance import geodesic
from tqdm import tqdm
import dask.dataframe as dd
from dask.multiprocessing import get

from transforms import load_locations_from_files, read_gtfs_into_df

pd.options.mode.chained_assignment = None
tqdm.pandas()

NUMBER_OF_CORES = 8


def get_stops_for_trip(trip_id, stops_df, stop_times_df):
    stop_times_for_trip = stop_times_df[stop_times_df['trip_id'] == trip_id]
    stops_for_trip = stops_df[stops_df['stop_id'].isin(stop_times_for_trip['stop_id'])]
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
    print('loading locations')
    filenames = [f for f in listdir('./data') if isfile(join('./data', f))]
    filenames = [f for f in filenames if re.search(date_string, f)]
    locations_df = load_locations_from_files(filenames)
    locations_df.dropna(subset=['trip_id'], inplace=True)
    locations_df['trip_id'] = locations_df['trip_id'].progress_apply(pd.to_numeric)
    locations_df['vehicle_id'] = locations_df['vehicle_id'].progress_apply(pd.to_numeric)

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
        closest_stop_ids = ddata.map_partitions(lambda loc: apply_find_closest_stop_id(loc, stops_df, stop_times_df)).compute(get=get)
    else:
        closest_stop_ids = locations_df.progress_apply(lambda loc: find_closest_stop_id(loc, stops_df, stop_times_df), axis=1)

    locations_df['closest_stop_ids'] = closest_stop_ids

    return locations_df


def load_gtfs(reload=False, filename='./gtfs.h5'):
    store = pd.HDFStore(filename)

    if reload:
        print('loading trips')
        trips_df = read_gtfs_into_df('trips')
        trip_route_lookup = trips_df[['trip_id', 'route_id']]
        trip_route_lookup['trip_id'] = trip_route_lookup['trip_id'].progress_apply(pd.to_numeric)
        trip_route_lookup['route_id'] = trip_route_lookup['route_id'].progress_apply(pd.to_numeric)

        print('loading stops')
        stops_df = read_gtfs_into_df('stops')
        stops_df['stop_id'] = stops_df['stop_id'].progress_apply(pd.to_numeric)

        print('loading stop_times')
        stop_times_df = read_gtfs_into_df('stop_times')
        stop_times_df['trip_id'] = stop_times_df['trip_id'].progress_apply(pd.to_numeric)
        stop_times_df['stop_id'] = stop_times_df['stop_id'].progress_apply(pd.to_numeric)

        store['trip_route_lookup'] = trip_route_lookup
        store['stops_df'] = stops_df
        store['stop_times_df'] = stop_times_df
    else:
        print('loading gtfs from store')
        trip_route_lookup = store['trip_route_lookup']
        stops_df = store['stops_df']
        stop_times_df = store['stop_times_df']

    store.close()

    return {
        'trip_route_lookup': trip_route_lookup,
        'stops_df': stops_df,
        'stop_times_df': stop_times_df
    }


if __name__ == '__main__':
    date_string = '20180525'

    gtfs_dfs = load_gtfs()
    locations_df = load_location_data_for_date(date_string)
    locations_df = transform_locations(locations_df, gtfs_dfs)

    print(f'loaded and transformed {len(locations_df)} locations')

    store = pd.HDFStore(f'./transformed_locations_{date_string}.h5')
    store['locations'] = locations_df
    store.close()
