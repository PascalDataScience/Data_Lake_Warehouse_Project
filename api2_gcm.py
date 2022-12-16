import boto3
import datetime
import json
import matplotlib as mpl
import matplotlib.pyplot as plt
import meteomatics.api as api
import os

user = os.environ.get('USERNAME_API')
pwd = os.environ.get('PASSWORD_API')
aws_access_key_id = os.environ.get('ACCESS_KEY_ID')
aws_secret_access_key = os.environ.get('SECRET_ACCESS_KEY')
aws_session_token = os.environ.get('SESSION_TOKEN')
AWS_STORAGE_BUCKET_NAME = 'entsoebucket4'


def lambda_handler(event, context):
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

def analysis(df_ts):
    mpl.rcParams['figure.dpi'] = 300
    plt.style.use('ggplot')
    df_index = df_ts.reset_index()
    fig, ax = plt.subplots()
    ax.set_title('Temperature in the last 365 days')
    ax.set_ylabel('Temperature [°C]')
    ax.set_xlabel('Date')
    ax.plot(df_index['validdate'], df_index['temp'], label='Temp')
    ax.legend()
    plt.show()

def saveS3(df_ts):
    # print(df_ts)
    df_ts.to_csv('/tmp/test.csv')  # saving dataframe to csv file
    s3 = boto3.client('s3',
                      aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key,
                      aws_session_token=aws_session_token)
    with open("/tmp/test.csv", "rb") as f:
        s3.upload_fileobj(f, AWS_STORAGE_BUCKET_NAME,
                          f"weather_data/temperature4_{datetime.datetime.now().date().strftime('%y%m%d')}.csv")


def renameCol(df_ts):
    df_new = df_ts.rename(columns={'t_2m:C': 'temp'})
    return df_new


def timestamp():
    now = datetime.datetime(datetime.datetime.now().year,
                            datetime.datetime.now().month,
                            datetime.datetime.now().day,
                            14, 0, 0)
    start = now - datetime.timedelta(days=365)
    return start, now


def time_series_example(username: str, password: str, run_analysis:bool):
    start, end = timestamp()
    interval_ts = datetime.timedelta(hours=1)
    coordinates_ts = [(47.377275, 8.539653)]
    parameters_ts = ['t_2m:C']  # list of parameters https://www.meteomatics.com/en/api/available-parameters/
    model = 'mix'
    ens_select = None  # e.g. 'median'
    cluster_select = None  # e.g. "cluster:1", see https://www.meteomatics.com/en/api/available-parameters/alphabetic-list/
    interp_select = 'gradient_interpolation'
    try:
        df_ts = api.query_time_series(coordinates_ts, start, end, interval_ts, parameters_ts,
                                      username, password, model, ens_select, interp_select,
                                      cluster_select=cluster_select)
        df_ts = renameCol(df_ts)
        if run_analysis:
            analysis(df_ts)

        try:
            saveS3(df_ts)
        except Exception as e:
            print(e)

    except Exception as e:
        print(e)
        return False
    return True


if __name__ == "__main__":
    run_analysis = True # set to True to see a timeseries of the temperature
    time_series_example(user, pwd, run_analysis)

