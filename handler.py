import requests, boto3, os
from datetime import datetime

AWS_CONFIG = {
    'S3_BUCKET': os.environ['AWS_CONFIG_S3_BUCKET'],
    'ACCESS_KEY': os.environ['AWS_CONFIG_ACCESS_KEY'],
    'SECRET_KEY': os.environ['AWS_CONFIG_SECRET_KEY']
}
OBA_API_KEY = os.environ['OBA_API_KEY']

session = boto3.Session(
    aws_access_key_id=AWS_CONFIG['ACCESS_KEY'],
    aws_secret_access_key=AWS_CONFIG['SECRET_KEY'],
)
s3 = session.resource('s3')

def scrape_locations(event, context):
    agency_id = 1 # TODO: pull from context
    url = f'http://api.pugetsound.onebusaway.org/api/where/vehicles-for-agency/{agency_id}.json?key={OBA_API_KEY}'
    res = requests.get(url)

    date = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
    key = f'locations_{date}.json'
    s3.Bucket(AWS_CONFIG['S3_BUCKET']).put_object(Key=key, Body=res.text)
    print(f'successfully uploaded to {key}')
