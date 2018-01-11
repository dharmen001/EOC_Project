import pandas as pd
import  cx_Oracle
import numpy as np

IO_ID = int(input("Enter the IO:"))
conn = cx_Oracle.connect("TFR_REP/welcome@10.29.20.76/tfrdb")
writer = pd.ExcelWriter("Daily Performance({}).xlsx".format(IO_ID), engine="xlsxwriter", datetime_format="YYYY-MM-DD")

def common_columns():
    read_common_columns = pd.read_csv("Eociocommoncolumn.csv")
    data_common_columns_new = read_common_columns.loc[read_common_columns.IOID == IO_ID, :]
    data_common_columns = data_common_columns_new.loc[:, ["Columns-IO", "Values-IO", "Columns-AM-Sales",
                                                          "Values-AM-Sales", "Columns-Campaign-Info",
                                                          "Values-Campaign-Info"]]

    return read_common_columns, data_common_columns

def connect_TFR():
        sql_KM="select * from TFR_REP.KEY_METRIC_MV where IO_ID = {}".format(IO_ID)
        sql_Daily_sales="select * from TFR_REP.DAILY_SALES_MV where IO_ID = {}".format(IO_ID)
        return sql_KM, sql_Daily_sales
def read_query():
    sql_KM, sql_Daily_sales = connect_TFR()
    read_sql_KM = pd.read_sql(sql_KM, conn)
    read_sql_Daily_sales = pd.read_sql(sql_Daily_sales, conn)
    return read_sql_KM, read_sql_Daily_sales

def access_data_summary():
    read_sql_KM,read_sql_Daily_sales=read_query()
    KM_pivot_first = pd.pivot_table(read_sql_KM, values=["IMPRESSIONS","ENGAGEMENTS","VWR_CLICK_THROUGHS",
                                                         "ENG_CLICK_THROUGHS" ,"DPE_CLICK_THROUGHS",
                                                         "VWR_VIDEO_VIEW_100_PC_COUNT","ENG_VIDEO_VIEW_100_PC_COUNT",
                                                         "DPE_VIDEO_VIEW_100_PC_COUNT","ENG_TOTAL_TIME_SPENT",
                                                         "DPE_TOTAL_TIME_SPENT","ENG_INTERACTIVE_ENGAGEMENTS",
                                                          "DPE_INTERACTIVE_ENGAGEMENTS","CPCV_COUNT","DPE_ENGAGEMENTS"],
                                    index=["PLACEMENT_ID","PLACEMENT_DESC","METRIC_DESC", "DAY_DESC"],
                                    aggfunc=np.sum)
    KM_data_daily_new=KM_pivot_first.reset_index()
    KM_data_daily = KM_data_daily_new[["PLACEMENT_ID","PLACEMENT_DESC","METRIC_DESC", "DAY_DESC",
                                       "IMPRESSIONS","ENGAGEMENTS",
                                       "VWR_CLICK_THROUGHS",
                                       "ENG_CLICK_THROUGHS","DPE_CLICK_THROUGHS",
                                       "VWR_VIDEO_VIEW_100_PC_COUNT","ENG_VIDEO_VIEW_100_PC_COUNT",
                                       "DPE_VIDEO_VIEW_100_PC_COUNT","ENG_TOTAL_TIME_SPENT",
                                       "DPE_TOTAL_TIME_SPENT","ENG_INTERACTIVE_ENGAGEMENTS",
                                       "DPE_INTERACTIVE_ENGAGEMENTS","CPCV_COUNT","DPE_ENGAGEMENTS"
                                       ]]

    final_summary = KM_data_daily.to_excel(writer, sheet_name="Daily Performance({})".format(IO_ID),  startcol=0,
                                            startrow=7,
                                            header=True, index=False)
    return final_summary

def format_summary():
    final_summary = access_data_summary()
    workbook=writer.book
    worksheet=writer.sheets["Daily Performance({})".format(IO_ID)]
    workbook.close()
    writer.close()

def main():
    common_columns()
    connect_TFR()
    read_query()
    access_data_summary()
    format_summary()

if __name__ == "__main__":
    main ()


