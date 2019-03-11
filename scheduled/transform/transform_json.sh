#!/usr/bin/env bash

datestr=$(date -d'yesterday' +%Y%m%d)

echo "syncing json files"
aws s3 sync s3://realtime-buses/ . --exclude="*" --include="locations_${datestr}*.json"

echo "processing into csv"
./process_raw_json_locations.sh -i '*.json' -o locations_${datestr}.csv

echo "cleaning csv"
sed 's/"1_\([0-9]*\)"/\1/g' locations_${datestr}.csv | sed 's/""//g' > locations_${datestr}_clean.csv
mv locations_${datestr}_clean.csv locations_${datestr}.csv

echo "compressing"
zip locations_${datestr}.csv.zip locations_${datestr}.csv
7z a locations_${datestr}_raw.7z locations_${datestr}*.json

echo "uploading to s3"
aws s3 cp locations_${datestr}.csv.zip s3://realtime-buses/processed/
aws s3 cp locations_${datestr}_raw.7z s3://realtime-buses/archive/

echo "cleaning up"
rm *json *zip *csv *7z

echo "done"
