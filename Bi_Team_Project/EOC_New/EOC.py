import pandas as pd
import cx_Oracle
import numpy as np

IO_ID = int(input("Enter the IO:"))
conn = cx_Oracle.connect("TFR_REP/welcome@10.29.20.76/tfrdb")

def common_columns():
    read_common_columns = pd.read_csv("Eociocommoncolumn.csv")
    data_common_columns = read_common_columns.loc[read_common_columns.IOID == IO_ID, :]

def connect_TFR():
    sql_summary = "select * from TFR_REP.SUMMARY_MV where IO_ID = {}".format(IO_ID)
    sql_KM = "select * from TFR_REP.KEY_METRIC_MV where IO_ID = {}".format(IO_ID)
    sql_VD = "select * from TFR_REP.VIDEO_DETAIL_MV where IO_ID = {}".format(IO_ID)
    sql_ID = "select * from TFR_REP.INTERACTION_DETAIL_MV where IO_ID = {}".format(IO_ID)
    sql_Daily_sales = "select * from TFR_REP.DAILY_SALES_MV where IO_ID = {}".format(IO_ID)
    return sql_summary, sql_KM, sql_VD, sql_ID, sql_Daily_sales

def read_query():
    sql_summary, sql_KM, sql_VD, sql_ID, sql_Daily_sales = connect_TFR()
    read_sql_summary = pd.read_sql(sql_summary, conn)
    read_sql_KM = pd.read_sql(sql_KM, conn)
    read_sql_VD = pd.read_sql(sql_VD, conn)
    read_sql_ID = pd.read_sql(sql_ID, conn)
    read_sql_Daily_sales = pd.read_sql(sql_Daily_sales, conn)
    return read_sql_summary, read_sql_KM, read_sql_VD, read_sql_ID, read_sql_Daily_sales

def access_data_summary():
    read_sql_summary, read_sql_KM, read_sql_VD, read_sql_ID, read_sql_Daily_sales = read_query()

    summary_pivot_first = pd.pivot_table(read_sql_summary, values=["BUDGET"], index=["PLACEMENT_ID", "PLACEMENT_DESC",
                                                                                     "SDATE",
                                                                                     "EDATE", "CREATIVE_DESC",
                                                                                     "METRIC_DESC", "COST_TYPE_DESC",
                                                                                     "UNIT_COST", "BOOKED_QTY"],
                                         aggfunc=np.sum)

    summary_data_summary_new = summary_pivot_first.reset_index()

    summary_data_summary = summary_data_summary_new[[ "PLACEMENT_ID", "PLACEMENT_DESC", "SDATE", "EDATE",
                                                      "CREATIVE_DESC", "METRIC_DESC", "COST_TYPE_DESC",
                                                      "UNIT_COST", "BUDGET","BOOKED_QTY"]]

    KM_pivot_first = pd.pivot_table(read_sql_KM, values=["IMPRESSIONS", "ENGAGEMENTS"], index=["PLACEMENT_ID",
                                                                                               "PLACEMENT_DESC"],
                                    aggfunc=np.sum)
    KM_data_summary_new = KM_pivot_first.reset_index()

    KM_data_summary = KM_data_summary_new[["PLACEMENT_ID", "PLACEMENT_DESC", "IMPRESSIONS", "ENGAGEMENTS"]]

    daily_sales_pivot_first = pd.pivot_table(read_sql_Daily_sales, values=["VIEWS", "CLICKS"], index=["PLACEMENT_ID",
                                                                                                      "PLACEMENT_DESC"],
                                             aggfunc=np.sum)
    daily_sales_pivot_first_new = daily_sales_pivot_first.reset_index()

    daily_sales_data_summary = daily_sales_pivot_first_new[["PLACEMENT_ID", "PLACEMENT_DESC", "VIEWS", "CLICKS"]]
    return summary_data_summary, KM_data_summary, daily_sales_data_summary

def summary_creation():
    summary_data_summary, KM_data_summary, daily_sales_data_summary = access_data_summary()
    summary_data_summary["PLACEMENT_DESC"] = summary_data_summary["PLACEMENT_DESC"].astype(str)
    KM_data_summary["PLACEMENT_DESC"] = KM_data_summary["PLACEMENT_DESC"].astype(str)
    daily_sales_data_summary["PLACEMENT_DESC"] = daily_sales_data_summary["PLACEMENT_DESC"].astype(str)
    summary = summary_data_summary.merge(pd.concat([KM_data_summary, daily_sales_data_summary]), on=["PLACEMENT_ID"],
                                         suffixes=('_right', '_left'))
    summary_new = summary.loc[:, ["PLACEMENT_ID", "PLACEMENT_DESC_right", "SDATE", "EDATE", "CREATIVE_DESC",
                                  "METRIC_DESC", "COST_TYPE_DESC", "UNIT_COST", "BUDGET", "BOOKED_QTY", "CLICKS",
                                  "ENGAGEMENTS", "IMPRESSIONS", "VIEWS"]]

    print summary_new

def main():
    common_columns()
    connect_TFR()
    read_query()
    access_data_summary()
    summary_creation()

if __name__ == "__main__":
    main ()
