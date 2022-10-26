
# https://github.com/meteomatics/python-connector-api

import numpy
import pandas
import requests

import datetime as dt
import meteomatics.api as api

import os



user = os.environ.get('USERNAME_API')
pwd = os.environ.get('PASSWORD_API')

# print(user)
# print(pwd)

def time_series_example(username: str, password: str):
    startdate_ts = dt.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    enddate_ts = startdate_ts + dt.timedelta(days=1)
    interval_ts = dt.timedelta(hours=1)
    coordinates_ts = [(47.249297, 9.342854), (50., 10.)]
    parameters_ts = ['t_2m:C', 'rr_1h:mm']
    model = 'mix'
    ens_select = None  # e.g. 'median'
    cluster_select = None  # e.g. "cluster:1", see http://api.meteomatics.com/API-Request.html#cluster-selection
    interp_select = 'gradient_interpolation'

    # _logger.info("\ntime series:")

    print(123)
    try:
        df_ts = api.query_time_series(coordinates_ts, startdate_ts, enddate_ts, interval_ts, parameters_ts,
                                      username, password, model, ens_select, interp_select,
                                      cluster_select=cluster_select)
        print(df_ts)

        # _logger.info("Dataframe head \n" + df_ts.head().to_string())
    except Exception as e:
        print(e)
        # _logger.info("Failed, the exception is {}".format(e))
        return False
    return True

def grid_example(username: str, password: str):
    lat_N = 50
    lon_W = -15
    lat_S = 20
    lon_E = 10
    res_lat = 3
    res_lon = 3
    startdate_grid = dt.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    parameter_grid = 'evapotranspiration_1h:mm'  # 't_2m:C'

    # _logger.info("\ngrid:")

    try:
        df_grid = api.query_grid(startdate_grid, parameter_grid, lat_N, lon_W, lat_S, lon_E, res_lat, res_lon,
                                 username, password)
        print(df_grid)
        # _logger.info("Dataframe head \n" + df_grid.head().to_string())
    except Exception as e:
        # _logger.error("Failed, the exception is {}".format(e))
        return False
    return True

def grid_png_example(username: str, password: str):
    lat_N = 50
    lon_W = -15
    lat_S = 20
    lon_E = 10
    res_lat = 3
    res_lon = 3
    filename_png = "grid_target.png"
    startdate_png = dt.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    parameter_png = 't_2m:C'

    # _logger.info("\ngrid as a png:")
    try:
        api.query_grid_png(filename_png, startdate_png, parameter_png, lat_N, lon_W, lat_S, lon_E, res_lat, res_lon,
                           username, password)
        # _logger.info("filename = {}".format(filename_png))
    except Exception as e:
        # _logger.error("Failed, the exception is {}".format(e))
        return False
    return True


if __name__ == "__main__":
    # run_example(time_series_example)
    time_series_example(user, pwd)
    # grid_example(user, pwd)
    # grid_png_example(user, pwd)