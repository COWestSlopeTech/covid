import boto3
import csv
import urllib2
from io import StringIO

print('Loading function')

s3 = boto3.client('s3')

COUNTY_URL = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv"
STATE_URL = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-states.csv"

BUCKET = 'rft-covid-csv'

STATE_KEY = 'colorado.csv'
GARFIELD_KEY = 'garfield.csv'
EAGLE_KEY = 'eagle.csv'
PITKIN_KEY = 'pitkin.csv'

STATE_POPULATION = 5758736
PITKIN_POPULATION = 17767
GARFIELD_POPULATION = 60061
EAGLE_POPULATION = 55127


def lambda_handler(event, context):
    # State-level block
    with StringIO() as output_buffer, csv.reader(urllib2.urlopen(STATE_URL)) as reader, csv.writer(output_buffer) as writer:
        writer.writerows(filter(lambda row: row[1] == 'Colorado', reader))
        s3.put_object(Bucket=BUCKET, Key=STATE_KEY, Body=output_buffer)
        print('Wrote things')
