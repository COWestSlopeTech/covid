import boto3
from csv import DictReader, DictWriter
from urllib.request import urlopen
from io import BytesIO
from codecs import iterdecode, getwriter

print('Loading function')

s3 = boto3.client('s3')

COUNTY_URL = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-counties.csv"
STATE_URL = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us-states.csv"

BUCKET = 'rft-covid-csv'

# These are the filenames for S3
STATE_KEY = 'colorado.csv'
GARFIELD_KEY = 'garfield.csv'
EAGLE_KEY = 'eagle.csv'
PITKIN_KEY = 'pitkin.csv'

# Estimates pulled from census.gov for 2019
STATE_POPULATION = 5758736
PITKIN_POPULATION = 17767
GARFIELD_POPULATION = 60061
EAGLE_POPULATION = 55127

# Values in the source files
COLORADO = 'Colorado'
GARFIELD = 'Garfield'
EAGLE = 'Eagle'
PITKIN = 'Pitkin'

# Field names
CASES = 'cases'
DEATHS = 'deaths'
STATE = 'state'
COUNTY = 'county'
POPULATION = "population"
CASES_PER_1000 = "cases_per_1000"
DEATHS_PER_1000 = "deaths_per_1000"


def augment_row(population):
    def inner(row):
        # Ewww mutation
        row[POPULATION] = population
        row[DEATHS_PER_1000] = int(row[DEATHS]) / population * 1000
        row[CASES_PER_1000] = int(row[CASES]) / population * 1000
        return row
    return inner


def state_stream(stream):
    return map(
        augment_row(STATE_POPULATION),
        filter(
            lambda row: row[STATE] == COLORADO,
            stream,
        ),
    )


def county_stream(county_name, county_population):
    def inner(stream):
        return map(
            augment_row(county_population),
            filter(
                lambda row: (row[STATE] == COLORADO and row[COUNTY] == county_name),
                stream,
            )
        )
    return inner


COUNTIES = [
    # source, dest, stream
    (COUNTY_URL, GARFIELD_KEY, county_stream(GARFIELD, GARFIELD_POPULATION)),
    (COUNTY_URL, PITKIN_KEY, county_stream(PITKIN, PITKIN_POPULATION)),
    (COUNTY_URL, EAGLE_KEY, county_stream(EAGLE, EAGLE_POPULATION)),
]


# TODO: A lot of the weirdness in the way that I wrote this function
# was because I wanted to play with python streams and keep memory
# overhead to a minimum. Lambda is still reportin ~70mb in memory usage,
# though, so it may not be working as desired.
def read_and_write(source, destination, manipulate_stream):
    # Note the BytesIO here - S3 wants a byte-based output, so we can't use
    # a string buffer like StringIO. This also necessitates the the translation
    # in the DictWriter construction below.
    with BytesIO() as output_buffer, urlopen(source) as input_stream:
        read_stream = DictReader(iterdecode(input_stream, 'utf-8'))
        output_fieldnames = read_stream.fieldnames + [
            POPULATION,
            CASES_PER_1000,
            DEATHS_PER_1000,
        ]
        # The getwriter call here is necessary because we need to go from a text stream to a
        # byte stream. See above.
        write_stream = DictWriter(getwriter('utf-8')(output_buffer), fieldnames=output_fieldnames)
        write_stream.writeheader()
        write_stream.writerows(manipulate_stream(read_stream))
        # This is required to reset the cursor on the buffer back to the start - otherwise
        # boto will upload a 0b file.
        # TODO: figure out if there's some way to stream directly (i.e. without needing to hold
        # the entire results set in memory)
        output_buffer.seek(0)
        s3.put_object(
            Bucket=BUCKET,
            Key=destination,
            Body=output_buffer,
            ACL='public-read',
            ContentType='text/plain',
        )


def lambda_handler(event, context):
    read_and_write(STATE_URL, STATE_KEY, state_stream)
    for source, dest, manipulate_stream in COUNTIES:
        read_and_write(source, dest, manipulate_stream)

    print('All done')
