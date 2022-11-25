#Author Pascal Schoch
#Date 25.11.22

from entsoe import EntsoePandasClient
import pandas as pd
from dotenv import load_dotenv
import os
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
from datetime import timedelta

import logging
import boto3
from botocore.exceptions import ClientError
import os

import os
import sys
import threading

from airflow import DAG

from airflow.operators.python import PythonOperator
import logging


basepath = r"/home/ubuntu/airflow/dags/project/"

load_dotenv()

API_KEY = os.environ.get('API_KEY_entsoe')
client = EntsoePandasClient(api_key=API_KEY)
timestamp_from = (datetime.now() - timedelta(days=1, hours=6)).strftime("%Y%m%d")#'20221109'
timestamp_to = (datetime.now()- timedelta(hours=6)).strftime("%Y%m%d") #'20221110'
start = pd.Timestamp(timestamp_from, tz='UTC')
end = pd.Timestamp(timestamp_to, tz='UTC')
country_code = 'CH'


"""
dict_callbles = {
    "wind_and_solar_forecast_" + country_code: client.query_wind_and_solar_forecast,

}
"""
dict_callbles = {"load_actual_forecast_"+country_code:client.query_load_and_forecast,                  # sucess 2020-01-01 - 2022-11-05
                #"net_position_actual_"+country_code: client.query_net_position,                                # not available
                 #"installed_generation_capacity_"+country_code:client.query_installed_generation_capacity,      # sucess 2020,2021, 2022
                 "generation_actual_"+country_code:client.query_generation,                                     # sucess 2020-01-01 - 2022-11-05
                 "prices_day_ahead_"+country_code:client.query_day_ahead_prices,                                # sucess 2020-01-01 - 2022-11-05
                 "generation_forecast_"+country_code:client.query_generation_forecast,                          # sucess 2020-01-01 - 2022-11-05
                 "wind_and_solar_forecast_"+country_code:client.query_wind_and_solar_forecast,                    # sucess 2020-01-01 - 2022-11-05
                 #"aggregate_water_reservoirs_and_hydro_storage_"+country_code:client.query_aggregate_water_reservoirs_and_hydro_storage, #sucess 2019-12-29 - 2022-10-30
                 "cross_border_flows":client.query_crossborder_flows,                                             # sucess 2020-01-01 - 2022-11-05
                 "scheduled_exchanges_day_ahead":client.query_scheduled_exchanges                                # sucess 2020-01-01 - 2022-11-05
}



#https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html
class ProgressPercentage(object):

    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify, assume this is hooked up to a single filename
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()


def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def pq_to_df(filepath):
    """
    Read Parquet file and convert to pandas dataframe
    """
    return pq.read_table(filepath).to_pandas()

def df_to_pq(df,filepath):
    """
    Read Parquet file and convert to pandas dataframe
    """
    pq.write_table(pa.Table.from_pandas(df), filepath)


def load_csv(filepath):
    """
    This Function imports a csv File and returns a Pandas Dataframe
    :return:
    """
    return pd.read_csv(filepath, sep='[;,]', engine='python')


def create_df_from_entsoe(data_generator:callable, country_code, **kwargs):
    """

    :param data_generator:
    :return:
    """
    timestamp_from = (datetime.now() - timedelta(days=1, hours=6)).strftime("%Y%m%d")  # '20221109'
    timestamp_to = (datetime.now() - timedelta(hours=6)).strftime("%Y%m%d")  # '20221110'
    start = pd.Timestamp(timestamp_from, tz='UTC')
    end = pd.Timestamp(timestamp_to, tz='UTC')
    """
    start = kwargs['start']
    end = kwargs['end']
    """
    if data_generator == client.query_day_ahead_prices:
        df = data_generator(country_code, start=start, end=end, resolution = '60T')

    elif data_generator == client.query_net_position:
        df = data_generator(country_code, start=start, end=end, dayahead= True)

    elif data_generator == client.query_crossborder_flows:
        df = pd.DataFrame()
        for country_to, country_from in zip(["DE", "IT", "AT", "FR", "CH", "CH", "CH", "CH"], ["CH", "CH", "CH", "CH", "DE", "IT", "AT", "FR"]):
            df_single = data_generator(country_code_from = country_from,country_code_to=country_to , start=start, end=end)
            df_single = df_single.to_frame()
            df_single.columns = ["cross_boarder_flow_"+country_from+"_to_"+country_to]
            df = pd.concat([df, df_single], axis = 1)
            print(df)

    elif data_generator == client.query_scheduled_exchanges:
        df = pd.DataFrame()
        for country_to in ["DE", "IT", "AT", "FR"]:
            df_single = data_generator(country_code_from = country_code,country_code_to=country_to , start=start, end=end, dayahead = True)
            df_single = df_single.to_frame()
            df_single.columns = ["scheduled_exchange_day_ahead_"+country_code+"_to_"+country_to]
            df = pd.concat([df, df_single], axis = 1)
            print(df)

    else:
        df = data_generator(country_code, start=start, end=end)

    return df


def load_entsoe_to_parquet():
    for values, keys in zip(dict_callbles.values(), dict_callbles.keys()):
        print(values)
        df = create_df_from_entsoe(values, country_code)


        if isinstance(df, pd.Series):
            df = df.to_frame()
            df.columns = [keys]
        df.columns = df.columns.str.replace(r"[][()\n\t= ]", "_", regex=True)
        print(df)
        if not os.path.exists(os.path.join(basepath, r"data/"+keys+r"/")):
            # Create a new directory because it does not exist
            os.makedirs(os.path.join(basepath, r"data/"+keys+r"/"))
            print("The new directory is created!")
        df_to_pq(df, os.path.join(basepath, r"data/"+keys+r"/"+keys+"_"+timestamp_from+"_to_"+timestamp_to+".parquet"))
    logging.info("all the parquet files have been created")

def upload_parquet_to_s3(**kwargs):
    #https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html
    timestamp_from = (datetime.now() - timedelta(days=1, hours=6)).strftime("%Y%m%d")  # '20221109'
    timestamp_to = (datetime.now() - timedelta(hours=6)).strftime("%Y%m%d")  # '20221110'
    start = pd.Timestamp(timestamp_from, tz='UTC')
    end = pd.Timestamp(timestamp_to, tz='UTC')
    """
    timestamp_from = kwargs['ts_from']
    timestamp_to = kwargs['timestamp_to']
    start = kwargs['start']
    end = kwargs['end']
    """
    for keys in dict_callbles:
        #print('aws_access_key_id', os.environ.get('aws_access_key_id'))
        #print('aws_session_token',os.environ.get('aws_session_token'))
        #print('aws_secret_access_key',os.environ.get('aws_secret_access_key'))
        s3 = boto3.client(
        's3',
        aws_access_key_id=os.environ.get('aws_access_key_id'),
        aws_secret_access_key=os.environ.get('aws_secret_access_key'),
        aws_session_token=os.environ.get('aws_session_token')
        )
        print("s3", s3)
        s3.upload_file(os.path.join(basepath, r"data/"+keys+r"/"+keys+"_"+timestamp_from+"_to_"+timestamp_to+".parquet"),  #filename
                       'entsoebucket4', #bucketname
                       keys+r"/"+keys+"_"+timestamp_from+"_to_"+timestamp_to+".parquet", #object_name
        Callback=ProgressPercentage(os.path.join(basepath, r"data/"+keys+r"/"+keys+"_"+timestamp_from+"_to_"+timestamp_to+".parquet"))
        )
    logging.info("scucessful upload of parquet files to s3 bucket")


def start():
    logging.info('Starting the DAG')

dag = DAG(
        'datapipeline_API_entsoe_to_S3',
        schedule_interval='@daily',
        start_date=pd.Timestamp("20221120",tz='UTC'),
        end_date = pd.Timestamp("20221230", tz = "UTC")#datetime(2022,11,16) #- timedelta(days=1))    #datetime.datetime.now() - datetime.timedelta(days=1))
)

greet_task = PythonOperator(
   task_id="start_task",
   python_callable=start,
   dag=dag
)

fetch_task = PythonOperator(
            task_id='Fetch_entsoe_API_to_Parquet',
            python_callable=load_entsoe_to_parquet,
            #op_kwargs= { "end": end,"start": start, "ts_from": timestamp_from, "ts_to": timestamp_to},
            dag=dag


)


upload_task = PythonOperator(
            task_id='UPLOAD_S3',
            python_callable=upload_parquet_to_s3,
            #op_kwargs= {"start": start, "end": end, "ts_from": timestamp_from, "ts_to": timestamp_to},
            dag=dag

)
"""

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    load_entsoe_to_parquet()
    upload_parquet_to_s3()


"""

greet_task >> fetch_task >> upload_task




