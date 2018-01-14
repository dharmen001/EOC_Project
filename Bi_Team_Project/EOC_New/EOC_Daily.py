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
    KM_Data = pd.pivot_table(read_sql_KM, values=["IMPRESSIONS", "ENGAGEMENTS", "VWR_CLICK_THROUGHS",
                                                  "ENG_CLICK_THROUGHS", "DPE_CLICK_THROUGHS",
                                                  "VWR_VIDEO_VIEW_100_PC_COUNT",
                                                  "ENG_VIDEO_VIEW_100_PC_COUNT",
                                                  "DPE_VIDEO_VIEW_100_PC_COUNT",
                                                  "ENG_TOTAL_TIME_SPENT",
                                                  "DPE_TOTAL_TIME_SPENT",
                                                  "ENG_INTERACTIVE_ENGAGEMENTS",
                                                  "DPE_INTERACTIVE_ENGAGEMENTS",
                                                  "CPCV_COUNT",
                                                  "DPE_ENGAGEMENTS"],
                                    index=["PLACEMENT_ID", "PLACEMENT_DESC", "METRIC_DESC", "DAY_DESC"],
                                    aggfunc=np.sum)
    KM_reset = KM_Data.reset_index()
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

    accessing_KM_columns = KM_Data_New.loc[:, ["PLACEMENT_ID","PLACEMENT_DESC","METRIC_DESC", "DAY_DESC", "IMPRESSIONS",
                            "ENGAGEMENTS", "VWR_CLICK_THROUGHS", "ENG_CLICK_THROUGHS", "DPE_CLICK_THROUGHS",
                            "VWR_VIDEO_VIEW_100_PC_COUNT", "ENG_VIDEO_VIEW_100_PC_COUNT", "DPE_VIDEO_VIEW_100_PC_COUNT",
                            "ENG_TOTAL_TIME_SPENT", "DPE_TOTAL_TIME_SPENT", "ENG_INTERACTIVE_ENGAGEMENTS",
                            "DPE_INTERACTIVE_ENGAGEMENTS", "CPCV_COUNT", "DPE_ENGAGEMENTS"]]

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

def adding_vcr_ctr_IR_ATS():
    accessing_KM_columns, accessing_sales_columns = rename_KM_Sales()

    accessing_KM_columns["ENG VCR%"] = accessing_KM_columns["ENG video 100 pc"]/accessing_KM_columns["Delivered Engagements"]
    accessing_KM_columns["Instream VCR%"] = accessing_KM_columns ["VWR video 100 pc"]/accessing_KM_columns["KM_Impressions"]
    accessing_KM_columns["CPCV VCR%"] = accessing_KM_columns["Completions"]/accessing_KM_columns["KM_Impressions"]
    accessing_KM_columns["ENG CTR%"] = accessing_KM_columns["Eng click through"]/accessing_KM_columns["Delivered Engagements"]
    accessing_KM_columns["VWR CTR%"] = accessing_KM_columns["Eng click through"]/accessing_KM_columns["KM_Impressions"]
    accessing_KM_columns["ENG Interaction Rate%"] = accessing_KM_columns["Eng intractive engagement"]/accessing_KM_columns["Delivered Engagements"]
    accessing_KM_columns["DPE Interaction Rate%"] = accessing_KM_columns["DPE intractive engagement"]/accessing_KM_columns["Deep Engagements"]
    accessing_KM_columns["ENG ATS"] = accessing_KM_columns["Eng total time spent"]/accessing_KM_columns["Delivered Engagements"]
    accessing_KM_columns["Deep ATS"]= accessing_KM_columns["Deep total time spent"]/accessing_KM_columns["Deep Engagements"]

    accessing_sales_columns["CTR%"] = ((accessing_sales_columns["Sales_Clicks"]/accessing_sales_columns
                                        ["Delivered Impressions"])*100).apply('{0:.2f}%'.format)

    return accessing_KM_columns, accessing_sales_columns

def write_KM_Sales():
    data_common_columns = common_Columns()
    accessing_KM_columns, accessing_sales_columns=adding_vcr_ctr_IR_ATS()
    replace_blank_with_zero_KM = accessing_KM_columns.fillna(0)
    replace_blank_with_zero_sales = accessing_sales_columns.fillna(0)

    writing_data_common_columns = data_common_columns[1].to_excel(writer, sheet_name="Daily Performance({})".format(IO_ID), startcol=0, startrow=7, index=False, header=False)

    writing_KM_columns = replace_blank_with_zero_KM.to_excel(writer, sheet_name="Daily Performance({})".format(IO_ID), startcol=0, startrow=12, index=False, header=True)

    writing_sales_columns = replace_blank_with_zero_sales.to_excel(writer, sheet_name="Daily Performance({})".format(IO_ID),
                                                             startcol=0, startrow=len(accessing_KM_columns)+16,
                                                             index=False, header=True)

    return accessing_KM_columns, accessing_sales_columns, replace_blank_with_zero_KM, replace_blank_with_zero_sales

def formatting():
    accessing_KM_columns,accessing_sales_columns=adding_vcr_ctr_IR_ATS()
    number_rows_KM = accessing_KM_columns.shape[0]
    number_cols_KM = accessing_KM_columns.shape[1]
    number_rows_sales = accessing_sales_columns.shape[0]
    number_cols_sales = accessing_sales_columns.shape[1]
    workbook=writer.book
    worksheet=writer.sheets["Daily Performance({})".format(IO_ID)]
    percent_fmt=workbook.add_format({"num_format": "0.00%", "align": "center"})
    alignment=workbook.add_format({"align": "center"})
    worksheet.hide_gridlines(2)
    worksheet.insert_image("A1", "Exponential.png")
    format_common_column={"header_row": False, "style": "Table Style Medium 2", 'autofilter': False}
    worksheet.add_table("A8:F10",format_common_column)
    format_merge_row=workbook.add_format({"bold":True,"font_color":'#FFFFFF',"align":"left",
                                          "fg_color":"#8EE5EE"})
    worksheet.merge_range("A12:AA12","Placement & Daily level Performance - Brand Engagement",format_merge_row)
    worksheet.merge_range("A{}:H{}".format(number_rows_KM+16,number_rows_KM+16),
                          "Placement & Daily level Performance - Brand Performance", format_merge_row)

    full_border=workbook.add_format({"border": 1, "border_color": "#8EE5EE", "align":"center",
                                     "fg_color": "#8EE5EE", "bold": True})


    data_border_style= workbook.add_format({"border": 1, "border_color": "#8EE5EE"})


    worksheet.freeze_panes(13, 4)

    for col in range(4, 18):
        cell_location = xl_rowcol_to_cell(number_rows_KM+13, col)
        start_range = xl_rowcol_to_cell(13, col)
        end_range = xl_rowcol_to_cell(number_rows_KM+12, col)
        formula = "=SUM({:s}:{:s})".format(start_range, end_range)
        worksheet.write_formula(cell_location, formula, full_border)

    worksheet.write_string(number_rows_KM+13, 0, "Total", full_border)

    for col in range(4, 7):
        cell_location = xl_rowcol_to_cell(number_rows_KM + number_rows_sales+ 17, col)
        start_range = xl_rowcol_to_cell(number_rows_KM + 17, col)
        end_range = xl_rowcol_to_cell(number_rows_KM + number_rows_sales + 16, col)
        formula = "=SUM({:s}:{:s})".format(start_range, end_range)
        worksheet.write_formula(cell_location, formula, full_border)

    worksheet.write_string(number_rows_KM + number_rows_sales + 17, 0, "Total", full_border)
    worksheet.set_zoom(80)
    worksheet.set_column("A:A", 15, alignment)
    worksheet.set_column("B:B", 78, alignment)
    worksheet.set_column("C:C", 20, alignment)
    worksheet.set_column("D:D", 12, alignment)
    worksheet.set_column("E:E", 19, alignment)
    worksheet.set_column("F:F", 21, alignment)
    worksheet.set_column("G:G", 16, alignment)
    worksheet.set_column("H:H", 16, alignment)
    worksheet.set_column("I:K", 16, alignment)
    worksheet.set_column("L:L", 17, alignment)
    worksheet.set_column("M:N", 16, alignment)
    worksheet.set_column("O:O", 23, alignment)
    worksheet.set_column("P:P", 24, alignment)
    worksheet.set_column("Q:Q", 11, alignment)
    worksheet.set_column("R:R", 17, alignment)
    worksheet.set_column("S:S", 10, percent_fmt, {'level': 1,'hidden': True})
    worksheet.set_column("T:T", 14, percent_fmt, {'level': 1,'hidden': True})
    worksheet.set_column("U:W", 10, percent_fmt, {'level': 1,'hidden': True})
    worksheet.set_column("X:Y", 20, percent_fmt, {'level': 1,'hidden': True})
    worksheet.set_column("Z:Z", 12, alignment, {'level': 1,'hidden': True})
    worksheet.set_column("AA:AA", 12, alignment, {'level': 1,'hidden': True})
    worksheet.set_row(number_rows_KM)
    worksheet.conditional_format("A14:AA{}".format(number_rows_KM+13),
                                 {"type": "no_blanks", "format": data_border_style})
    worksheet.conditional_format("A{}:H{}".format(18+number_rows_KM, number_rows_sales+number_rows_KM+17),
                                 {"type": "no_blanks", "format": data_border_style})

    worksheet.set_row("M:N", 20, None, {'level': 1,'hidden': True})

def main():
    common_Columns()
    connect_TFR()
    read_Query()
    access_Data_KM_Sales()
    KM_Sales()
    rename_KM_Sales()
    adding_vcr_ctr_IR_ATS()
    write_KM_Sales()
    formatting()
    writer.close()

if __name__ == "__main__":
    main()
