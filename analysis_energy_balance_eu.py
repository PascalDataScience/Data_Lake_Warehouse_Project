import pyarrow as pa
import pyarrow.parquet as pq
import matplotlib.pyplot as plt
import glob
import pandas as pd
import seaborn as sns
import numpy as np
import os

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

if __name__ == "__main__":
    # set the Path where your files are stored from https://www.kaggle.com/datasets/gpreda/energy-balance-in-europe
    basepath = "The path to your FIles"
    df_energy_balance_eu = pd.read_csv(os.path.join(basepath,"energy_balance_eu.csv"), sep =",")
    df_to_pq(df_energy_balance_eu, os.path.join(basepath, "energy_balance_eu.parquet"))
    df_energy_balance_eu = pq_to_df(os.path.join(basepath, "energy_balance_eu.parquet"))
    print(df_energy_balance_eu)

    df_nrg_bal_dict = pd.read_csv(os.path.join(basepath,"nrg_bal_dict.csv"), sep = ",")
    df_siec_dict = pd.read_csv(os.path.join(basepath,"siec_dict.csv"), sep = ",")


    #merge the whole data together to one dataset
    df_merged = df_energy_balance_eu.merge(df_nrg_bal_dict,
                                           how= "inner",
                                           on= "nrg_bal"
                                           ).merge(df_siec_dict,
                                                   how= "inner",
                                                   on = "siec"
                                                   )

    df_FR = df_merged.where(df_merged["geo"] == "FR").dropna()
    df_DE = df_merged.where(df_merged["geo"] == "DE").dropna()

    df_FR_exports = df_FR[(df_FR["unit"] == "GWH") & (df_FR['nrg_bal_name'] == "Exports" ) & (df_FR['siec_name'] == "Electricity" ) ][["TIME_PERIOD", "OBS_VALUE", "siec_name"]]
    df_FR_exports.index = df_FR_exports["TIME_PERIOD"]
    df_DE_exports = df_DE[(df_DE["unit"] == "GWH") & (df_DE['nrg_bal_name'] == "Exports") & (df_DE['siec_name'] == "Electricity" ) ][["TIME_PERIOD", "OBS_VALUE", "siec_name"]]
    df_DE_exports.index = df_DE_exports["TIME_PERIOD"]

    """
    Compare Export DE and FR
    """
    plt.title("Comparison Electricity Export DE and FR [GWH]")
    plt.plot(df_FR_exports["OBS_VALUE"], color = "blue", label= "Export FR")
    plt.plot(df_DE_exports["OBS_VALUE"], color = "red", label= "Export DE")
    plt.legend(loc="upper left")
    plt.xticks(rotation=45)
    plt.subplots_adjust(bottom=0.15)
    plt.show()
