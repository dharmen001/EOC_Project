import pandas as pd
import cx_Oracle
import numpy as np
import datetime

IO_ID = int(input("Enter the IO:"))
conn = cx_Oracle.connect("TFR_REP/welcome@10.29.20.76/tfrdb")
writer = pd.ExcelWriter("Summary({}).xlsx".format(IO_ID), engine="xlsxwriter", datetime_format="YYYY-MM-DD")

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

    summary_data_summary = summary_data_summary_new[["PLACEMENT_ID", "PLACEMENT_DESC", "SDATE", "EDATE",
                                                     "CREATIVE_DESC", "METRIC_DESC", "COST_TYPE_DESC",
                                                     "UNIT_COST", "BUDGET", "BOOKED_QTY"]]

    KM_pivot_first = pd.pivot_table(read_sql_KM, values=["IMPRESSIONS", "ENGAGEMENTS", "CPCV_COUNT", "DPE_ENGAGEMENTS"],
                                    index=["PLACEMENT_ID","PLACEMENT_DESC"],
                                    aggfunc=np.sum)
    KM_data_summary_new = KM_pivot_first.reset_index()

    KM_data_summary = KM_data_summary_new[["PLACEMENT_ID", "PLACEMENT_DESC", "IMPRESSIONS", "ENGAGEMENTS", "CPCV_COUNT",
                                           "DPE_ENGAGEMENTS"]]

    daily_sales_pivot_first = pd.pivot_table(read_sql_Daily_sales, values=["VIEWS", "CLICKS", "CONVERSIONS"],
                                             index=["PLACEMENT_ID", "PLACEMENT_DESC"],
                                             aggfunc=np.sum)
    daily_sales_pivot_first_new = daily_sales_pivot_first.reset_index()

    daily_sales_data_summary = daily_sales_pivot_first_new[["PLACEMENT_ID", "PLACEMENT_DESC", "VIEWS", "CLICKS",
                                                            "CONVERSIONS"]]
    return summary_data_summary, KM_data_summary, daily_sales_data_summary

def summary_creation():
    summary_data_summary, KM_data_summary, daily_sales_data_summary = access_data_summary()
    summary_all = summary_data_summary.merge(pd.concat([KM_data_summary, daily_sales_data_summary]), on=["PLACEMENT_ID"]
                                             , suffixes=('_right', '_left'))
    summary_new = summary_all.loc[:, ["PLACEMENT_ID", "PLACEMENT_DESC_right", "SDATE", "EDATE", "CREATIVE_DESC",
                                      "METRIC_DESC", "COST_TYPE_DESC", "UNIT_COST", "BUDGET", "BOOKED_QTY",
                                      "ENGAGEMENTS", "VIEWS", "IMPRESSIONS", "CLICKS", "CONVERSIONS", "CPCV_COUNT",
                                      "DPE_ENGAGEMENTS"]]

    return summary_new

def rename_cols():
    summary_new = summary_creation()
    summary_cols_rename = summary_new.rename(columns={"PLACEMENT_ID": "Placement ID", "PLACEMENT_DESC_right":
                                                      "Placement#",
                                                      "SDATE": "Start Date", "EDATE": "End Date",
                                                      "CREATIVE_DESC": "Creative Name", "METRIC_DESC": "Metric",
                                                      "COST_TYPE_DESC": "COST TYPE", "UNIT_COST": "Unit Cost",
                                                      "BUDGET": "Planned Cost", "BOOKED_QTY": "Booked Eng#"
                                                                                              "Booked Imp",
                                                      "ENGAGEMENTS": "Delivered Engagements", "VIEWS":
                                                                        "Delivered Impressions", "IMPRESSIONS":
                                                          "KM_Impressions", "CLICKS": "Sales_Clicks",
                                                      "CONVERSIONS": "Conversions", "CPCV_COUNT": "Completions",
                                                      "DPE_ENGAGEMENTS": "Deep Engagements"},
                                             inplace=True)

    return summary_new
def adding_column_Delivery():
    summary_new = rename_cols()

    summary_new["Delivery% ENG"] = summary_new["Delivered Engagements"]/summary_new["Booked Eng#"
                                                                                    "Booked Imp"]
    summary_new["Delivery% Impression"] = summary_new["Delivered Impressions"]/summary_new["Booked Eng#"
                                                                                           "Booked Imp"]
    summary_new["Delivery% KM"] = summary_new["KM_Impressions"]/summary_new["Booked Eng#"
                                                                            "Booked Imp"]
    summary_new["Delivery% Clicks"] = summary_new["Sales_Clicks"]/summary_new["Booked Eng#"
                                                                              "Booked Imp"]
    summary_new["Delivery% Conversion"] = summary_new["Conversions"]/summary_new["Booked Eng#"
                                                                                 "Booked Imp"]
    summary_new["Delivery% Completions"] = summary_new["Completions"]/summary_new["Booked Eng#"
                                                                                  "Booked Imp"]
    summary_new["Delivery% DeepEng"] = summary_new["Deep Engagements"]/summary_new["Booked Eng#"
                                                                                   "Booked Imp"]
    return summary_new

def adding_column_Spend():
    summary_new = adding_column_Delivery()
    summary_new["Spend Eng"] = summary_new["Delivered Engagements"]*summary_new["Unit Cost"]
    summary_new["Spend Impression"] = summary_new["Delivered Impressions"]/1000*summary_new["Unit Cost"]
    summary_new["Spend KM"] = summary_new["KM_Impressions"]/1000*summary_new["Unit Cost"]
    summary_new["Spend Clicks"] = summary_new["Sales_Clicks"]*summary_new["Unit Cost"]
    summary_new["Spend Conversion"] = summary_new["Conversions"]*summary_new["Unit Cost"]
    summary_new["Spend Completions"] = summary_new["Completions"]*summary_new["Unit Cost"]
    summary_new["Spend DeepEng"] = summary_new["Deep Engagements"]*summary_new["Unit Cost"]
    return summary_new

def write_summary():
    summary_old = adding_column_Spend()
    data_common_columns = common_columns()

    summary_new = summary_old.fillna(0)
    summary = data_common_columns[1].to_excel(writer, sheet_name="Summary({})".format(IO_ID), startcol=0,
                                              startrow=1, index=False, header=False)

    final_summary = summary_new.to_excel(writer, sheet_name="Summary({})".format(IO_ID),  startcol=0, startrow=6,
                                         header=True, index=False, )


    return summary, final_summary

def format_summary():
    summary, final_summary = write_summary()
    workbook = writer.book
    worksheet = writer.sheets["Summary({})".format(IO_ID)]
    worksheet.hide_gridlines(2)
    format1 = workbook.add_format({"num_format": "$#,###0.00", "align": "center"})
    format2 = workbook.add_format({"num_format": "0.00%", "align": "center"})
    format3 = workbook.add_format({"bold": True, "font_color": '#FFFFFF', "align": "left", "fg_color": "#87CEFA"})
    format4 = workbook.add_format({"align": "center"})
    format5 = workbook.add_format({"bold": True, "font_color": '#000000', "fg_color": "#87CEFA"})
    worksheet.set_column("A:A", 30, format4)
    worksheet.set_column("B:B", 78, format4)
    worksheet.set_column("C:D", 30, format4)
    worksheet.set_column("E:E", 40, format4)
    worksheet.set_column("F:G", 20, format4)
    worksheet.set_column("H:I", 20, format1)
    worksheet.set_column("J:J", 22, format4)
    worksheet.set_column("K:Q", 20, format4)
    worksheet.set_column("R:X", 20, format2)
    worksheet.set_column("Y:AE", 20, format1)
    worksheet.merge_range("A6:AE6", "Campaign Summary", format3)
    worksheet.freeze_panes(7, 2)
    writer.save
    writer.close()

def main():
    common_columns()
    connect_TFR()
    read_query()
    access_data_summary()
    summary_creation()
    rename_cols()
    adding_column_Delivery()
    adding_column_Spend()
    write_summary()
    format_summary()

if __name__ == "__main__":
    main ()
