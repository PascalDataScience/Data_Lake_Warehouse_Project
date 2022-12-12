import pyarrow as pa
import pyarrow.parquet as pq
import matplotlib.pyplot as plt
import glob
import pandas as pd
import seaborn as sns
import numpy as np
from sklearn import linear_model
import os

def mape(actual, pred):
    """
    Calculation of a MAPE = Mean Absolut Percentage Error
    """
    actual, pred = actual, pred
    return np.mean(np.abs((actual - pred) / actual)) * 100


def lin_model(X_train, y_train):
    """
    Train a linear model and return a y_pred as predictions
    """
    #Initialize Model
    lr_model = linear_model.LinearRegression()

    #Train Linear Regression Model
    lr_model.fit(X_train, y_train)

    #Predict Datapoints
    y_pred = lr_model.predict(X_test)
    return y_pred

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
    #Where your files are stored in subfolders
    basepath = "your path"
    lst_filenames = glob.glob(basepath+'\*\*.parquet")

    df_all = pd.DataFrame()
    #---------------------------------------------------------------------------
    """
    Concatenate all Files together
    """
    for filename in lst_filenames:
        df_single = pq_to_df(filename)
        df_all = pd.concat([df_all, df_single], axis = 1)
        print((df_all))

    #---------------------------------------------------------------------------
    """
    Plot Correlation Matrix as a Heatmap
    """
    df_dropped = df_all.dropna(axis=1,how='all')
    df_cleaned = df_dropped.iloc[:, ~df_dropped.columns.duplicated()]
    #df_dropped = df_all.drop(columns=["Nuclear",])
    correlations = df_cleaned.corr()

    fig, ax = plt.subplots(figsize=(15, 15))  # Sample figsize in inches
    sns.heatmap(correlations,
                xticklabels=correlations.columns,
                yticklabels=correlations.columns)
    plt.subplots_adjust(left=0.25, bottom=0.25)
    ax.tick_params(labelsize=12)
    plt.show()

    #-----------------------------------------------------------------------------
    """
    Compare Actual Load with Forecasted Load
    """
    df_cleaned = df_cleaned.dropna()
    plt.title("Comparison Forecasted and Actual Load [MW]")
    plt.plot(df_cleaned["Forecasted_Load"], color = "blue", label= "Forecasted Load")
    plt.plot(df_cleaned["Actual_Load"], color = "red", label= "Actual Load")
    plt.legend(loc="upper left")
    plt.xticks(rotation=45)
    plt.subplots_adjust(bottom=0.15)
    plt.show()

    """
    Train a Linear Model and Compare the MAPE of the Model prediction with the Forecasted Load from ENTSOE
    """
    #Drop NaN because LinReg from Sklearn doesnt allow NaN

    y = df_cleaned["Actual_Load"]
    X = df_cleaned.drop(columns =["Forecasted_Load", "Actual_Load"])

    #train test split
    train_until = "2021-12-31 00:00:00+00:00"
    validate_from = "2022-01-01 00:00:05+00:00"
    X_train = X.loc[:train_until]
    X_test =X.loc[validate_from:]
    y_train =y.loc[:train_until]
    y_test =y.loc[validate_from:]

    #Predict Load with a simple Linear Model
    y_pred =lin_model(X_train, y_train)

    #Plot MAPE
    mape_entsoe = mape(df_cleaned["Actual_Load"].loc[validate_from:],
                    df_cleaned["Forecasted_Load"].loc[validate_from:]
                    )

    mape_lin = mape(y_test, y_pred)
    print("Comparison of MAPE [%]from Load Forecast ENTSOE and self defined Linear Regression: ")
    print("MAPE ENTSOE [%]: ",mape_entsoe.round(2))
    print("MAPE LinReg [%]: ",mape_lin.round(2))

