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
    read_sql_KM, read_sql_Daily_sales = read_query()

    KM_pivot_first = pd.pivot_table(read_sql_KM, values=["IMPRESSIONS","ENGAGEMENTS","VWR_CLICK_THROUGHS",
                                                         "ENG_CLICK_THROUGHS" ,"DPE_CLICK_THROUGHS",
                                                         "VWR_VIDEO_VIEW_100_PC_COUNT","ENG_VIDEO_VIEW_100_PC_COUNT",
                                                         "DPE_VIDEO_VIEW_100_PC_COUNT","ENG_TOTAL_TIME_SPENT",
                                                         "DPE_TOTAL_TIME_SPENT","ENG_INTERACTIVE_ENGAGEMENTS",
                                                         "DPE_INTERACTIVE_ENGAGEMENTS","CPCV_COUNT","DPE_ENGAGEMENTS"],
                                    index=["PLACEMENT_ID", "PLACEMENT_DESC","METRIC_DESC","DAY_DESC"],
                                    aggfunc=np.sum)

    KM_data_daily_new = KM_pivot_first.reset_index()

    KM_data_daily = KM_data_daily_new[["PLACEMENT_ID","PLACEMENT_DESC","METRIC_DESC", "DAY_DESC",
                                       "IMPRESSIONS","ENGAGEMENTS",
                                       "VWR_CLICK_THROUGHS",
                                       "ENG_CLICK_THROUGHS","DPE_CLICK_THROUGHS",
                                       "VWR_VIDEO_VIEW_100_PC_COUNT","ENG_VIDEO_VIEW_100_PC_COUNT",
                                       "DPE_VIDEO_VIEW_100_PC_COUNT","ENG_TOTAL_TIME_SPENT",
                                       "DPE_TOTAL_TIME_SPENT","ENG_INTERACTIVE_ENGAGEMENTS",
                                       "DPE_INTERACTIVE_ENGAGEMENTS","CPCV_COUNT","DPE_ENGAGEMENTS"
                                       ]]
    daily_sales_pivot_first = pd.pivot_table(read_sql_Daily_sales, values=["VIEWS", "CLICKS", "CONVERSIONS"],
                                             index=["PLACEMENT_ID", "PLACEMENT_DESC", "METRIC_DESC", "DAY_DESC"],
                                             aggfunc=np.sum)

    daily_sales_pivot_first_new = daily_sales_pivot_first.reset_index()

    daily_sales_data_daily = daily_sales_pivot_first_new[["PLACEMENT_ID", "PLACEMENT_DESC", "METRIC_DESC", "DAY_DESC",
                                                          "VIEWS", "CLICKS", "CONVERSIONS"]]
    return KM_data_daily, daily_sales_data_daily

def access_KM_sales_daily():

    KM_data_daily, daily_sales_data_daily = rename_col()

    KM_data_daily_access_data = KM_data_daily.loc[:, ["PLACEMENT_ID","PLACEMENT_DESC","METRIC_DESC", "DAY_DESC",
                                       "IMPRESSIONS","ENGAGEMENTS",
                                       "VWR_CLICK_THROUGHS",
                                       "ENG_CLICK_THROUGHS","DPE_CLICK_THROUGHS",
                                       "VWR_VIDEO_VIEW_100_PC_COUNT","ENG_VIDEO_VIEW_100_PC_COUNT",
                                       "DPE_VIDEO_VIEW_100_PC_COUNT","ENG_TOTAL_TIME_SPENT",
                                       "DPE_TOTAL_TIME_SPENT","ENG_INTERACTIVE_ENGAGEMENTS",
                                       "DPE_INTERACTIVE_ENGAGEMENTS","CPCV_COUNT","DPE_ENGAGEMENTS"]]

    daily_sales_data_daily_data = daily_sales_data_daily.loc[:, ["PLACEMENT_ID", "PLACEMENT_DESC", "METRIC_DESC", "DAY_DESC",
                                                          "VIEWS", "CLICKS", "CONVERSIONS"]]
    return KM_data_daily_access_data, daily_sales_data_daily_data

def rename_col():
    KM_data_daily_access_data, daily_sales_data_daily_data = access_data_summary()

    KM_data_daily_rename = KM_data_daily_access_data.rename(columns={"PLACEMENT_ID": "Placement ID", "PLACEMENT_DESC": "Placement#",
                                                         "METRIC_DESC": "Metric", "DAY_DESC": "Day",
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
    sales_data_Daily_rename = daily_sales_data_daily_data.rename(columns={"PLACEMENT_ID": "Placement ID",
                                                                     "PLACEMENT_DESC": "Placement#",
                                                                     "METRIC_DESC": "Metric",
                                                                     "DAY_DESC": "Day",
                                                                     "VIEWS": "Delivered Impressions",
                                                                     "CLICKS": "Sales_Clicks",
                                                                     "CONVERSIONS": "Conversions"}, inplace=True)
    return KM_data_daily_access_data, daily_sales_data_daily_data



def adding_vcr_ctr_IR_ATS():

    KM_data_daily_access_data, daily_sales_data_daily_data = access_KM_sales_daily()

    KM_data_daily_access_data["ENG VCR%"] = KM_data_daily_access_data["ENG video 100 pc"]/KM_data_daily_access_data["Delivered Engagements"]
    KM_data_daily_access_data["Instream VCR%"] = KM_data_daily_access_data["VWR video 100 pc"]/KM_data_daily_access_data["KM_Impressions"]
    KM_data_daily_access_data["CPCV VCR%"] = KM_data_daily_access_data["Completions"]/KM_data_daily_access_data["KM_Impressions"]
    KM_data_daily_access_data["ENG CTR%"] = KM_data_daily_access_data["Eng click through"]/KM_data_daily_access_data["Delivered Engagements"]
    KM_data_daily_access_data["VWR CTR%"] = KM_data_daily_access_data["Eng click through"]/KM_data_daily_access_data["KM_Impressions"]
    KM_data_daily_access_data["ENG Interaction Rate%"] = KM_data_daily_access_data["Eng intractive engagement"]/KM_data_daily_access_data["Delivered Engagements"]
    KM_data_daily_access_data["DPE Interaction Rate%"] = KM_data_daily_access_data["DPE intractive engagement"]/KM_data_daily_access_data["Deep Engagements"]
    KM_data_daily_access_data["ENG ATS"] = KM_data_daily_access_data["Eng total time spent"]/KM_data_daily_access_data["Delivered Engagements"]
    KM_data_daily_access_data["Deep ATS"] = KM_data_daily_access_data["Deep total time spent"]/KM_data_daily_access_data["Deep Engagements"]

    daily_sales_data_daily_data["CTR%"] = daily_sales_data_daily_data["Sales_Clicks"]/daily_sales_data_daily_data["Delivered Impressions"]

    return KM_data_daily_access_data, daily_sales_data_daily_data

def write_daily():
    data_common_columns = common_columns()
    KM_data_daily_access_data, daily_sales_data_daily_data = adding_vcr_ctr_IR_ATS()
    daily = data_common_columns[1].to_excel(writer, sheet_name="Daily Performance({})".format(IO_ID), startcol=0,
                                            startrow=7, index=False, header=False)
    KM_data_daily_final = KM_data_daily_access_data.to_excel(writer, sheet_name="Daily Performance({})".format(IO_ID), startcol=0,
                                                             startrow=12, index=False, header=True)
    daily_sales_data_daily_final = daily_sales_data_daily_data.to_excel(writer, sheet_name="Daily Performance({})".format(IO_ID),
                                                                        startcol=0, startrow=len(KM_data_daily_access_data)+16,
                                                                        index=False, header=True)
    return daily, KM_data_daily_access_data, daily_sales_data_daily_data

def format_daily():
    KM_data_daily_access_data = write_daily()
    workbook=writer.book
    worksheet= writer.sheets["Daily Performance({})".format(IO_ID)]
    percent_fmt=workbook.add_format({"num_format":"0.00%","align":"center"})
    alignment=workbook.add_format({"align":"center"})
    worksheet.hide_gridlines(2)
    worksheet.insert_image("A1", "Exponential.png")
    format_common_column={"header_row":False,"style":"Table Style Medium 2",'autofilter':False}
    worksheet.add_table("A8:F10",format_common_column)
    format_merge_row=workbook.add_format({"bold":True,"font_color":'#FFFFFF',"align":"left",
                                          "fg_color":"#8EE5EE"})

    worksheet.merge_range("A12:AA12", "Placement & Daily level Performance - Brand Engagement", format_merge_row)
    worksheet.merge_range("A{}:H{}".format(len(KM_data_daily_access_data)+14, len(KM_data_daily_access_data)+14),
                          "Placement & Daily level Performance - Brand Performance", format_merge_row)

    worksheet.set_column("A:A", 15, alignment)
    worksheet.set_column("B:B", 78, alignment)
    worksheet.set_column("C:C", 20, alignment)
    worksheet.set_column("D:D", 12, alignment)
    worksheet.set_column("E:E", 19, alignment)
    worksheet.set_column("F:F", 21, alignment)
    worksheet.set_column("G:G", 16, alignment)
    worksheet.set_column("H:H", 16, alignment)
    worksheet.set_column("I:I", 16, alignment)
    worksheet.set_column("J:J", 16, alignment)
    worksheet.set_column("K:K", 16, alignment)
    worksheet.set_column("L:L", 17, alignment)
    worksheet.set_column("M:M", 16, alignment)
    worksheet.set_column("N:N", 16, alignment)
    worksheet.set_column("O:O", 23, alignment)
    worksheet.set_column("P:P", 24, alignment)
    worksheet.set_column("Q:Q", 11, alignment)
    worksheet.set_column("R:R", 17, alignment)
    worksheet.set_column("S:S", 10, percent_fmt)
    worksheet.set_column("T:T", 14, percent_fmt)
    worksheet.set_column("U:U", 10, percent_fmt)
    worksheet.set_column("V:V", 10, percent_fmt)
    worksheet.set_column("W:W", 10, percent_fmt)
    worksheet.set_column("X:X", 20, percent_fmt)
    worksheet.set_column("Y:Y", 20, percent_fmt)
    worksheet.set_column("Z:Z", 12, alignment)
    worksheet.set_column("AA:AA",12, alignment)
    writer.save
    writer.close()

def main():
    common_columns()
    connect_TFR()
    read_query()
    access_data_summary()
    access_KM_sales_daily()
    rename_col()
    adding_vcr_ctr_IR_ATS()
    write_daily()
    format_daily()

if __name__ == "__main__":
    main ()


