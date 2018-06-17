# OBA realtime bus location scraper

Scrapes buses from the OneBusAway API every 1 minute using AWS Lambda and S3. Data to be analyzed at a later time.

### Ideas for explanation

1. Join with street widths and show if there's a relationship between # of lanes and arrival time variance
2. Clustering
3. Health of realtime reporting
4. Probablistic travel time from given point (e.g. Wallingford to Capitol Hill, google maps = 38 mins, actual 55)
