import pandas as pd
import cx_Oracle
import numpy as np

IO_ID = int(input("Enter the IO:"))
conn = cx_Oracle.connect("TFR_REP/welcome@10.29.20.76/tfrdb")
writer = pd.ExcelWriter("Daily Peformance({}).xlsx".format(IO_ID), engine="xlsxwriter", datetime_format="MM-DD")

def common_columns():
    read_common_columns = pd.read_csv("Eociocommoncolumn.csv")
    data_common_columns_new = read_common_columns.loc[read_common_columns.IOID == IO_ID, :]
    data_common_columns = data_common_columns_new.loc[:, ["Columns-IO", "Values-IO", "Columns-AM-Sales",
                                                          "Values-AM-Sales", "Columns-Campaign-Info",
                                                          "Values-Campaign-Info"]]

    return read_common_columns, data_common_columns


def connect_TFR():

        sql_summary = "select * from TFR_REP.SUMMARY_MV where IO_ID = {}".format(IO_ID)
        sql_KM = "select * from TFR_REP.KEY_METRIC_MV where IO_ID = {}".format(IO_ID)
        sql_Daily_sales = "select * from TFR_REP.DAILY_SALES_MV where IO_ID = {}".format(IO_ID)
        return sql_summary, sql_KM, sql_Daily_sales

def read_query():
    sql_summary, sql_KM, sql_Daily_sales = connect_TFR()
    read_sql_summary = pd.read_sql(sql_summary, conn)
    read_sql_KM = pd.read_sql(sql_KM, conn)
    read_sql_Daily_sales = pd.read_sql(sql_Daily_sales, conn)
    return read_sql_summary, read_sql_KM, read_sql_Daily_sales