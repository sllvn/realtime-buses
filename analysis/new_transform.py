from datetime import date
import pandas as pd
from time import sleep
from dask.diagnostics import ProgressBar
import dask.dataframe as dd
from dask.multiprocessing import get
from tqdm import tqdm

from load_transform_locations import load_location_data_for_date
from gtfs import load_gtfs_for_date, pick_latest_gtfs_for_date
from transforms import load_trip_geometries, make_find_closest
from utils import generate_dates

tqdm.pandas()


NUMBER_OF_CORES = 8


if __name__ == '__main__':
    start_date = date(2018, 5, 23)
    end_date = date(2018, 6, 26)

    for date in generate_dates(start_date, end_date):
        print(f'processing locations for date: {date}')

        latest_gtfs_date = pick_latest_gtfs_for_date(date)
        gtfs_dfs = load_gtfs_for_date(date)
        trip_geometries = load_trip_geometries(gtfs_dfs, date=latest_gtfs_date)

        locations_df = load_location_data_for_date(date)
        locations_df = pd.merge(locations_df, gtfs_dfs['trips'][['trip_id', 'route_id']], how='left', on='trip_id')

        find_closest = make_find_closest(trip_geometries, gtfs_dfs['stops'])
        def apply_find_closest_stop_id(partition_df):
            return partition_df.apply(find_closest, axis=1)

        ddata = dd.from_pandas(locations_df, npartitions=NUMBER_OF_CORES)
        with ProgressBar():
            locations_df['closest_stop_id'] = ddata.map_partitions(apply_find_closest_stop_id).compute(get=get)

        trips_df = gtfs_dfs['trips']
        trips_df = trips_df.set_index('trip_id')
        trips_df.sort_index(inplace=True)
        locations_df['trip_direction'] = locations_df.progress_apply(lambda x: trips_df.loc[x.trip_id, 'direction_id'], axis=1)

        locations_df = locations_df.set_index('time')
        locations_df.sort_index(inplace=True)

        store = pd.HDFStore(f'./data/transformed/{date}.h5')
        store['locations'] = locations_df
        store.close()

        print('')
        print('###')
        print('')

        sleep(10)
