from datetime import date, timedelta
import pandas as pd
from time import sleep

from load_transform_locations import load_location_data_for_date, load_gtfs
from transforms import read_gtfs_into_df, load_trip_geometries, make_find_closest
from tqdm import tqdm

tqdm.pandas()

from dask.diagnostics import ProgressBar
import dask.dataframe as dd
from dask.multiprocessing import get

NUMBER_OF_CORES = 8


def generate_dates(start, end):
    delta = end - start
    for i in range(delta.days + 1):
        yield str(start + timedelta(i)).replace('-', '')


if __name__ == '__main__':
    gtfs_dfs = load_gtfs()
    gtfs_dfs['stops']['stop_lon'] = gtfs_dfs['stops']['stop_lon'].progress_apply(pd.to_numeric)
    gtfs_dfs['stops']['stop_lat'] = gtfs_dfs['stops']['stop_lat'].progress_apply(pd.to_numeric)

    trip_geometries = load_trip_geometries(gtfs_dfs)

    start_date = date(2018, 6, 8)
    end_date = date(2018, 6, 16)

    for date in generate_dates(start_date, end_date):
        print(f'processing locations for date: {date}')
        locations_df = load_location_data_for_date(date)
        locations_df = pd.merge(locations_df, gtfs_dfs['trip_route_lookup'], how='left', on='trip_id')

        find_closest = make_find_closest(trip_geometries, gtfs_dfs['stops'])
        def apply_find_closest_stop_id(partition_df):
            return partition_df.apply(find_closest, axis=1)

        ddata = dd.from_pandas(locations_df, npartitions=NUMBER_OF_CORES)
        with ProgressBar():
            locations_df['closest_stop_id'] = ddata.map_partitions(apply_find_closest_stop_id).compute(get=get)

        store = pd.HDFStore(f'./new_{date}.h5')
        store['locations'] = locations_df
        store.close()

        sleep(10)
