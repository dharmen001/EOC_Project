import pandas as pd
import cx_Oracle
import numpy as np
from xlsxwriter.utility import xl_rowcol_to_cell

IO_ID = int(input("Enter the IO:"))
conn = cx_Oracle.connect("TFR_REP/welcome@10.29.20.76/tfrdb")
writer = pd.ExcelWriter("Daily Peformance({}).xlsx".format(IO_ID), engine="xlsxwriter", datetime_format="MM-DD-YYYY")

def common_Columns():
    read_common_columns = pd.read_csv("Eociocommoncolumn.csv")
    data_common_columns_new = read_common_columns.loc[read_common_columns.IOID == IO_ID, :]
    data_common_columns = data_common_columns_new.loc[:, ["Columns-IO", "Values-IO", "Columns-AM-Sales",
                                                          "Values-AM-Sales", "Columns-Campaign-Info",
                                                          "Values-Campaign-Info"]]

    return  read_common_columns, data_common_columns


def connect_TFR():
        sql_KM = "select * from TFR_REP.KEY_METRIC_MV where IO_ID = {}".format(IO_ID)
        sql_Daily_sales = "select * from TFR_REP.DAILY_SALES_MV where IO_ID = {}".format(IO_ID)
        return sql_KM, sql_Daily_sales

def read_Query():
    sql_KM, sql_Daily_sales = connect_TFR()
    read_sql_KM = pd.read_sql(sql_KM, conn)
    read_sql_Daily_sales = pd.read_sql(sql_Daily_sales, conn)
    return read_sql_KM, read_sql_Daily_sales

def access_Data_KM_Sales():
    read_sql_KM, read_sql_Daily_sales = read_Query()
    """KM_Data = pd.pivot_table(read_sql_KM, index=["PLACEMENT_ID", "PLACEMENT_DESC", "METRIC_DESC", "DAY_DESC"],
                             values=["IMPRESSIONS", "ENGAGEMENTS", "VWR_CLICK_THROUGHS",
                                        "ENG_CLICK_THROUGHS", "DPE_CLICK_THROUGHS",
                                        "VWR_VIDEO_VIEW_100_PC_COUNT",
                                        "ENG_VIDEO_VIEW_100_PC_COUNT",
                                        "DPE_VIDEO_VIEW_100_PC_COUNT",
                                        "ENG_TOTAL_TIME_SPENT",
                                        "DPE_TOTAL_TIME_SPENT",
                                        "ENG_INTERACTIVE_ENGAGEMENTS",
                                        "DPE_INTERACTIVE_ENGAGEMENTS",
                                        "CPCV_COUNT",
                                        "DPE_ENGAGEMENTS"],aggfunc=np.sum, fill_value= 0)
    

    """KM_reset = KM_Data.reset_index()
  KM_Data_New = KM_reset[["PLACEMENT_ID","PLACEMENT_DESC","METRIC_DESC", "DAY_DESC", "IMPRESSIONS",
                          "ENGAGEMENTS", "VWR_CLICK_THROUGHS", "ENG_CLICK_THROUGHS", "DPE_CLICK_THROUGHS",
                          "VWR_VIDEO_VIEW_100_PC_COUNT", "ENG_VIDEO_VIEW_100_PC_COUNT", "DPE_VIDEO_VIEW_100_PC_COUNT",
                          "ENG_TOTAL_TIME_SPENT", "DPE_TOTAL_TIME_SPENT", "ENG_INTERACTIVE_ENGAGEMENTS",
                          "DPE_INTERACTIVE_ENGAGEMENTS", "CPCV_COUNT", "DPE_ENGAGEMENTS"]]

  daily_Sales_Data = pd.pivot_table(read_sql_Daily_sales, values=["VIEWS", "CLICKS", "CONVERSIONS"],
                                           index=["PLACEMENT_ID", "PLACEMENT_DESC", "METRIC_DESC", "DAY_DESC"],
                                           aggfunc=np.sum)
  sales_reset = daily_Sales_Data.reset_index()
  daily_Sales_Data_new = sales_reset[["PLACEMENT_ID", "PLACEMENT_DESC", "METRIC_DESC", "DAY_DESC",
                                      "VIEWS", "CLICKS", "CONVERSIONS"]]

  return KM_Data_New, daily_Sales_Data_new

def KM_Sales():
  KM_Data_New, daily_Sales_Data_new = access_Data_KM_Sales()

  accessing_KM_columns = KM_Data_New.loc[:, ["PLACEMENT_ID","PLACEMENT_DESC","METRIC_DESC", "DAY_DESC",
                                             "IMPRESSIONS","ENGAGEMENTS", "VWR_CLICK_THROUGHS", "ENG_CLICK_THROUGHS",
                                             "DPE_CLICK_THROUGHS","VWR_VIDEO_VIEW_100_PC_COUNT",
                                             "ENG_VIDEO_VIEW_100_PC_COUNT", "DPE_VIDEO_VIEW_100_PC_COUNT",
                                             "ENG_TOTAL_TIME_SPENT", "DPE_TOTAL_TIME_SPENT",
                                             "ENG_INTERACTIVE_ENGAGEMENTS","DPE_INTERACTIVE_ENGAGEMENTS",
                                             "CPCV_COUNT", "DPE_ENGAGEMENTS"]]

  accessing_sales_columns = daily_Sales_Data_new.loc[:, ["PLACEMENT_ID", "PLACEMENT_DESC", "METRIC_DESC", "DAY_DESC",
                                                         "VIEWS", "CLICKS", "CONVERSIONS"]]

  return accessing_KM_columns, accessing_sales_columns

def rename_KM_Sales():
  accessing_KM_columns, accessing_sales_columns = KM_Sales()
  rename_KM_columns = accessing_KM_columns.rename(columns={"PLACEMENT_ID": "Placement ID",
                                                           "PLACEMENT_DESC": "Placement#",
                                                           "METRIC_DESC": "Metric",
                                                           "DAY_DESC": "Day",
                                                           "IMPRESSIONS": "KM_Impressions",
                                                           "ENGAGEMENTS": "Delivered Engagements",
                                                           "VWR_CLICK_THROUGHS": "VWR click through",
                                                           "ENG_CLICK_THROUGHS": "Eng click through",
                                                           "DPE_CLICK_THROUGHS": "Deep click through",
                                                           "VWR_VIDEO_VIEW_100_PC_COUNT": "VWR video 100 pc",
                                                           "ENG_VIDEO_VIEW_100_PC_COUNT": "ENG video 100 pc",
                                                           "DPE_VIDEO_VIEW_100_PC_COUNT": "Deep video 100 pc",
                                                           "ENG_TOTAL_TIME_SPENT": "Eng total time spent",
                                                           "DPE_TOTAL_TIME_SPENT": "Deep total time spent",
                                                           "ENG_INTERACTIVE_ENGAGEMENTS": "Eng intractive engagement",
                                                           "DPE_INTERACTIVE_ENGAGEMENTS": "DPE intractive engagement",
                                                           "CPCV_COUNT": "Completions",
                                                           "DPE_ENGAGEMENTS": "Deep Engagements"}, inplace=True)
  rename_sales_column = accessing_sales_columns.rename(columns={"PLACEMENT_ID": "Placement ID",
                                                                "PLACEMENT_DESC": "Placement#",
                                                                "METRIC_DESC": "Metric",
                                                                "DAY_DESC": "Day",
                                                                "VIEWS": "Delivered Impressions",
                                                                "CLICKS": "Sales_Clicks",
                                                                "CONVERSIONS": "Conversions"}, inplace=True)
  return accessing_KM_columns, accessing_sales_columns


def write_KM_Sales():
  data_common_columns=common_Columns()
  accessing_KM_columns,accessing_sales_columns=rename_KM_Sales()
  replace_blank_with_zero_KM=accessing_KM_columns.fillna(0)
  replace_blank_with_zero_sales=accessing_sales_columns.fillna(0)
  print replace_blank_with_zero_KM"""

def main():
    common_Columns()
    connect_TFR()
    read_Query()
    access_Data_KM_Sales()
    #KM_Sales()
    #rename_KM_Sales()
    #write_KM_Sales()
    #formatting()
    writer.close()

if __name__ == "__main__":
    main()