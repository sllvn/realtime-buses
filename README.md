# OBA realtime bus location scraper

Scrapes bus location data from the OneBusAway API every 1 minute using AWS Lambda and S3, then transforms (via AWS Batch) scraped JSON into de-duped CSV files every night.

## Data

### De-duped CSVs

Transformed CSV files are available at http://realtime-buses.s3.amazonaws.com/processed/index.html (or via AWS S3 CLI at s3://realtime-buses/processed). The CSVs are headerless, but the columns are `location_time`, `lat`, `lon`, `trip_id`, and `vehicle_id`. Additional info, e.g., `route_id`, can be inferred by joining a GTFS feed (available at https://transitfeeds.com/p/king-county-metro/73) to the `trip_id` column.

Sample data:

```csv
1552089545000,47.408241271972656,-122.25468444824219,39168942,7043
1552089545000,47.59035110473633,-122.14629364013672,34746303,7317
1552089546000,47.305641174316406,-122.22965240478516,39168842,7010
1552089546000,47.521026611328125,-122.36727905273438,40954821,8299
1552089546000,47.57124710083008,-122.1317367553711,34746128,7318
```

### Raw JSON responses

The raw JSON response from OneBusAway has been archived (via 7zip) and is available at s3://realtime-buses/archive/ (the bucket is public). This data contains quite a bit more information than the transformed CSV files, but much of it is redundant and not useful for analysis.

Sample data:

```json
{
  "code": 200,
  "currentTime": 1552161643360,
  "data": {
    "limitExceeded": false,
    "list": [
      {
        "lastLocationUpdateTime": 1552161606000,
        "lastUpdateTime": 1552161606000,
        "location": {
          "lat": 47.6016731262207,
          "lon": -122.3065414428711
        },
        "phase": "in_progress",
        "status": "SCHEDULED",
        "tripId": "1_40572947",
        "tripStatus": {
          "activeTripId": "1_40572947",
          "blockTripSequence": 5,
          "closestStop": "1_27550",
          "closestStopTimeOffset": 0,
          "distanceAlongTrip": 3410.424146645899,
          "frequency": null,
          "lastKnownDistanceAlongTrip": 0,
          "lastKnownLocation": {
            "lat": 47.6016731262207,
            "lon": -122.3065414428711
          },
          "lastKnownOrientation": 0,
          "lastLocationUpdateTime": 1552161606000,
          "lastUpdateTime": 1552161606000,
          "nextStop": "1_27550",
          "nextStopTimeOffset": 0,
          "orientation": 359.7927828945337,
          "phase": "in_progress",
          "position": {
            "lat": 47.601676989666956,
            "lon": -122.30654614291264
          },
          "predicted": true,
          "scheduleDeviation": 88,
          "scheduledDistanceAlongTrip": 3410.424146645899,
          "serviceDate": 1552118400000,
          "situationIds": [],
          "status": "SCHEDULED",
          "totalDistanceAlongTrip": 7441.26272215484,
          "vehicleId": "1_3624"
        },
        "vehicleId": "1_3624"
      },
      ...
    ],
  },
  "text": "OK",
  "version": 2
}
```


## Ideas for exploration

1. Join with street widths and show if there's a relationship between # of lanes and arrival time variance
2. Clustering
3. Health of realtime reporting
4. Probablistic travel time from given point (e.g. Wallingford to Capitol Hill, google maps = 38 mins, actual 55)
