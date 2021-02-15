import requests
from apache_beam.io.gcp.bigquery import parse_table_schema_from_json
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients import bigquery
from datetime import datetime, timedelta
import dateutil.parser
import apache_beam as beam
import pandas as pd
import json
import copy
import os
import urllib
import pprint
import argparse
import logging
import re
import traceback


AUTHENTICATION_TOKEN = os.getenv("AUTHENTICATION_TOKEN")
BASE_URL = os.getenv("BASE_URL", "https://my.15five.com/api/public")
ENDPOINT = os.getenv("ENDPOINT")
DATE_FIELDS=os.getenv("DATE_FIELDS", "")
DATE_TIME_FIELDS=os.getenv("DATE_TIME_FIELDS", "")
SYNC_BUCKET=os.getenv("SYNC_BUCKET")

def main(argv=None):
    """
    The main function which creates the pipeline and runs it.
    """
    parser = argparse.ArgumentParser()
    # Here we add some specific command line arguments we expect.
    # This defaults the output table in your BigQuery you'll have
    # to create the example_data dataset yourself using bq mk temp
    parser.add_argument('--output',
                        dest='output',
                        required=False)

    # Parse arguments from the command line.
    known_args, pipeline_args = parser.parse_known_args(argv)

    table_spec = bigquery.TableReference(
        projectId=os.getenv('GOOGLE_PROJECT_ID'),
        datasetId=os.getenv('GOOGLE_DATASET_ID'),
        tableId=os.getenv('GOOGLE_TABLE_ID')
    )

    pipeline_options = PipelineOptions(
        flags=pipeline_args,
    )
    pipeline = beam.Pipeline(options=pipeline_options)

    (pipeline
     | 'Get Data From 15Five ' >> beam.Create(get_data_from_15five())
     | 'Format Dates' >> beam.ParDo(FormatElements())
     | 'Write To BigQuery' >> beam.io.WriteToBigQuery(
         table_spec,
         write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
         create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
         custom_gcs_temp_location=SYNC_BUCKET)
     )
    pipeline.run().wait_until_finish()



class FormatElements(beam.DoFn):
    def __init__(self):
        pass

    def process(self, element):
        element_copy = copy.deepcopy(element)
        element = change_keys(element)
        for key in element:
            if key in get_date_fields():
                value = element.get(key)
                element[key] = datetime.fromisoformat(value)
            if key in get_datetime_fields():
                value = element.get(key)
                element[key] = dateutil.parser.parse(value)
                element[key] = element[key].replace(tzinfo=None)
                element[key] = element[key].replace(microsecond=0)
        return [element]

def change_keys(data):
    remove_list = []
    data_copy = copy.deepcopy(data)
    for key in data_copy:
        if "_" in key:
            remove_list.append(key)
            new_key = underscore_to_camel(key)
            data[new_key] = data[key]
            if isinstance(data[new_key], list):
                tmp_list = []
                for item in data[new_key]:
                    tmp_list.append(change_keys(item))
                data[new_key] = tmp_list
        if isinstance(data[key], list):
            tmp_list = []
            for item in data[key]:
                tmp_list.append(change_keys(item))
            data[key] = tmp_list
    for key in remove_list:
        del data[key]
    return data


def underscore_to_camel(name):
    under_pat = re.compile(r'_([a-z])')
    return under_pat.sub(lambda x: x.group(1).upper(), name)

def get_data_from_15five():
    data = []
    next = True
    url = get_url()
    while next:
        res = requests.get(
            url,
            headers={
                "Authorization": f"Bearer {AUTHENTICATION_TOKEN}",
                "Accept": "application/json",
                "Content-Type": "application/json; charset=utf-8"
            }
        )
        res_data = res.json().get("results")
        if res.json().get("next"):
            url = res.json().get("next")
        else:
            next = False
        data = data + flatten_data(res_data)
    return data

def get_url():
    return f"{BASE_URL}/{ENDPOINT}/?order_by=create_ts&created_on_start={get_previous_day_timestamp()}"

def get_date_fields():
    return DATE_FIELDS.split(",")

def get_datetime_fields():
    return DATE_TIME_FIELDS.split(",")

def flatten_data(data):
    resp_data = []
    for item in data:
        df = pd.json_normalize(item, sep='_')
        resp_data.append(df.to_dict(orient='records')[0])
    return resp_data

def get_previous_day_timestamp():
    yesterday = datetime.now() - timedelta(1)
    yesterday = yesterday.replace(hour=0, minute=1)
    return urllib.parse.quote(yesterday.strftime("%Y-%m-%d %H:%M:%S"))


def run(data=None, ctx=None):
    logging.getLogger().setLevel(logging.INFO)
    main()

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    main()
