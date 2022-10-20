
# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    from entsoe import EntsoePandasClient
    import pandas as pd
    from dotenv import load_dotenv
    import os

    load_dotenv()

    API_KEY =os.environ.get('API_KEY')
    client = EntsoePandasClient(api_key=API_KEY)

    start = pd.Timestamp('20211126', tz='Europe/Zurich')
    end = pd.Timestamp('20211127', tz='Europe/Zurich')
    country_code = 'CH'

    print(client.query_load(country_code, start=start, end=end))
    print(client.query_generation(country_code, start=start, end=end))
    # print(client.query_load_and_forecast(country_code, start=start, end=end)
    df = client.query_generation(country_code, start=start, end=end)
    df2 = client.query_generation_forecast(country_code, start=start, end=end)

    #df_prices =
