import pandas as pd
import numpy as np
import config
from xlsxwriter.utility import xl_rowcol_to_cell

class Daily():
    def __init__(self, config):
        self.config = config

    def connect_TFR_daily(self):
            sql_KM = "select * from TFR_REP.KEY_METRIC_MV where IO_ID = {}".format(self.config.IO_ID)
            sql_Daily_sales = "select * from TFR_REP.DAILY_SALES_MV where IO_ID = {}".format(self.config.IO_ID)
            return sql_KM, sql_Daily_sales

    def read_Query_daily(self):
        sql_KM, sql_Daily_sales = self.connect_TFR_daily()
        read_sql_KM = pd.read_sql(sql_KM, self.config.conn)
        read_sql_Daily_sales = pd.read_sql(sql_Daily_sales, self.config.conn)
        return read_sql_KM, read_sql_Daily_sales

    def access_Data_KM_Sales_daily(self):
        read_sql_KM, read_sql_Daily_sales = self.read_Query_daily()
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

        try:
            KM_Data_New = KM_reset[["PLACEMENT_ID","PLACEMENT_DESC","METRIC_DESC", "DAY_DESC", "IMPRESSIONS",
                                "ENGAGEMENTS", "VWR_CLICK_THROUGHS", "ENG_CLICK_THROUGHS", "DPE_CLICK_THROUGHS",
                                "VWR_VIDEO_VIEW_100_PC_COUNT", "ENG_VIDEO_VIEW_100_PC_COUNT", "DPE_VIDEO_VIEW_100_PC_COUNT",
                                "ENG_TOTAL_TIME_SPENT", "DPE_TOTAL_TIME_SPENT", "ENG_INTERACTIVE_ENGAGEMENTS",
                                "DPE_INTERACTIVE_ENGAGEMENTS", "CPCV_COUNT", "DPE_ENGAGEMENTS"]]
        except KeyError:
            KM_Data_New = KM_reset[[]]

        daily_Sales_Data = pd.pivot_table(read_sql_Daily_sales, values=["VIEWS", "CLICKS", "CONVERSIONS"],
                                                 index=["PLACEMENT_ID", "PLACEMENT_DESC", "METRIC_DESC", "DAY_DESC"],
                                                 aggfunc=np.sum)
        sales_reset = daily_Sales_Data.reset_index()
        try:
            daily_Sales_Data_new = sales_reset[["PLACEMENT_ID", "PLACEMENT_DESC", "METRIC_DESC", "DAY_DESC",
                                            "VIEWS", "CLICKS", "CONVERSIONS"]]
        except KeyError:
            daily_Sales_Data_new = sales_reset[[]]

        return KM_Data_New, daily_Sales_Data_new

    def KM_Sales_daily(self):
        KM_Data_New, daily_Sales_Data_new = self.access_Data_KM_Sales_daily()

        try:

            accessing_KM_columns = KM_Data_New[["PLACEMENT_ID","PLACEMENT_DESC","METRIC_DESC", "DAY_DESC", "IMPRESSIONS",
                                "ENGAGEMENTS", "VWR_CLICK_THROUGHS", "ENG_CLICK_THROUGHS", "DPE_CLICK_THROUGHS",
                                "VWR_VIDEO_VIEW_100_PC_COUNT", "ENG_VIDEO_VIEW_100_PC_COUNT", "DPE_VIDEO_VIEW_100_PC_COUNT",
                                "ENG_TOTAL_TIME_SPENT", "DPE_TOTAL_TIME_SPENT", "ENG_INTERACTIVE_ENGAGEMENTS",
                                "DPE_INTERACTIVE_ENGAGEMENTS", "CPCV_COUNT", "DPE_ENGAGEMENTS"]]
        except KeyError:

            accessing_KM_columns = KM_Data_New[[]]

        try:
           accessing_sales_columns = daily_Sales_Data_new[["PLACEMENT_ID", "PLACEMENT_DESC", "METRIC_DESC", "DAY_DESC",
                                                               "VIEWS", "CLICKS", "CONVERSIONS"]]
        except KeyError:
            accessing_sales_columns=daily_Sales_Data_new[[]]

        return accessing_KM_columns, accessing_sales_columns

    def rename_KM_Sales_daily(self):
        accessing_KM_columns, accessing_sales_columns = self.KM_Sales_daily()
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

    def adding_vcr_ctr_IR_ATS_daily(self):
        accessing_KM_columns, accessing_sales_columns = self.rename_KM_Sales_daily()
        read_query_summary_results=self.read_Query_daily()
        read_sql_KM=read_query_summary_results[0]
        read_sql_Daily_sales=read_query_summary_results[1]

        try:
            if read_sql_KM.iloc[0]["IO_ID"]==self.config.IO_ID:
                accessing_KM_columns["ENG VCR%"] = accessing_KM_columns["ENG video 100 pc"]/accessing_KM_columns["Delivered Engagements"]
        except KeyError as e:
            accessing_KM_columns["ENG VCR%"] = 0
        except IndexError as e:
            pass
        try:
            if read_sql_KM.iloc[0]["IO_ID"]==self.config.IO_ID:
                accessing_KM_columns["Instream VCR%"] = accessing_KM_columns ["VWR video 100 pc"]/accessing_KM_columns["KM_Impressions"]
        except KeyError as e:
            accessing_KM_columns["Instream VCR%"] = 0
        except IndexError as e:
            pass
        try:
            if read_sql_KM.iloc[0]["IO_ID"]==self.config.IO_ID:
                accessing_KM_columns["CPCV VCR%"] = accessing_KM_columns["Completions"]/accessing_KM_columns["KM_Impressions"]
        except KeyError as e:
            accessing_KM_columns["CPCV VCR%"] = 0
        except IndexError as e:
            pass
        try:
            if read_sql_KM.iloc[0]["IO_ID"]==self.config.IO_ID:
                accessing_KM_columns["ENG CTR%"] = accessing_KM_columns["Eng click through"]/accessing_KM_columns["Delivered Engagements"]
        except KeyError as e:
            accessing_KM_columns["ENG CTR%"] = 0
        except IndexError as e:
            pass
        try:
            if read_sql_KM.iloc[0]["IO_ID"]==self.config.IO_ID:
                accessing_KM_columns["VWR CTR%"] = accessing_KM_columns["Eng click through"]/accessing_KM_columns["KM_Impressions"]
        except KeyError as e:
            accessing_KM_columns["VWR CTR%"] = 0
        except IndexError as e:
            pass
        try:
            if read_sql_KM.iloc[0]["IO_ID"]==self.config.IO_ID:
                accessing_KM_columns["ENG Interaction Rate%"] = accessing_KM_columns["Eng intractive engagement"]/accessing_KM_columns["Delivered Engagements"]
        except KeyError as e:
            accessing_KM_columns["ENG Interaction Rate%"] = 0
        except IndexError as e:
            pass
        try:
            if read_sql_KM.iloc[0]["IO_ID"]==self.config.IO_ID:
                accessing_KM_columns["DPE Interaction Rate%"] = accessing_KM_columns["DPE intractive engagement"]/accessing_KM_columns["Deep Engagements"]
        except KeyError as e:
            accessing_KM_columns["DPE Interaction Rate%"] = 0
        except IndexError as e:
            pass
        try:
            if read_sql_KM.iloc[0]["IO_ID"]==self.config.IO_ID:
                accessing_KM_columns["ENG ATS"] = ((accessing_KM_columns["Eng total time spent"]/accessing_KM_columns["Delivered Engagements"])/1000).apply('{0:.2f}'.format)
        except KeyError as e:
            accessing_KM_columns["ENG ATS"] = 0
        except IndexError as e:
            pass
        try:
            if read_sql_KM.iloc[0]["IO_ID"]==self.config.IO_ID:
                accessing_KM_columns["Deep ATS"] = ((accessing_KM_columns["Deep total time spent"]/accessing_KM_columns["Deep Engagements"])/1000).apply('{0:.2f}'.format)
        except KeyError as e:
            accessing_KM_columns["Deep ATS"] = 0
        except IndexError as e:
            pass
        try:
            if read_sql_Daily_sales.iloc[0]["IO_ID"]==self.config.IO_ID:
                accessing_sales_columns["CTR%"] = accessing_sales_columns["Sales_Clicks"]/accessing_sales_columns["Delivered Impressions"]
        except KeyError as e:
            accessing_sales_columns["CTR%"] = 0
        except IndexError as e:
            pass

        return accessing_KM_columns, accessing_sales_columns

    def write_KM_Sales_summary(self):
        data_common_columns = self.config.common_columns_summary()
        accessing_KM_columns, accessing_sales_columns = self.adding_vcr_ctr_IR_ATS_daily()
        replace_blank_with_zero_KM = accessing_KM_columns.fillna(0)
        replace_blank_with_zero_sales = accessing_sales_columns.fillna(0)

        writing_data_common_columns = data_common_columns[1].to_excel(self.config.writer, sheet_name="Daily Performance({})".format(self.config.IO_ID), startcol=0, startrow=7, index=False, header=False)

        writing_KM_columns = replace_blank_with_zero_KM.to_excel(self.config.writer, sheet_name="Daily Performance({})".format(self.config.IO_ID), startcol=0, startrow=12, index=False, header=True)

        writing_sales_columns = replace_blank_with_zero_sales.to_excel(self.config.writer, sheet_name="Daily Performance({})".format(self.config.IO_ID),
                                                                 startcol=0, startrow=len(accessing_KM_columns)+16,
                                                                 index=False, header=True)

        return accessing_KM_columns, accessing_sales_columns, replace_blank_with_zero_KM, replace_blank_with_zero_sales

    def formatting_daily(self):
        accessing_KM_columns, accessing_sales_columns = self.adding_vcr_ctr_IR_ATS_daily()
        number_rows_KM = accessing_KM_columns.shape[0]
        #number_cols_KM = accessing_KM_columns.shape[1]
        number_rows_sales = accessing_sales_columns.shape[0]
        #number_cols_sales = accessing_sales_columns.shape[1]
        read_query_summary_results = self.read_Query_daily()
        read_sql_KM=read_query_summary_results[0]
        read_sql_Daily_sales=read_query_summary_results[1]
        workbook= self.config.writer.book
        worksheet=self.config.writer.sheets["Daily Performance({})".format(self.config.IO_ID)]
        percent_fmt=workbook.add_format({"num_format": "0.00%", "align": "center"})
        alignment=workbook.add_format({"align": "center"})
        worksheet.hide_gridlines(2)
        worksheet.insert_image("A1", "Exponential.png")
        #format_common_column={"header_row": False, "style": "Table Style Medium 2", 'autofilter': False}
        #worksheet.add_table("A8:F10",format_common_column)
        format_merge_row=workbook.add_format({"bold":True,"font_color":'#FFFFFF',"align":"left",
                                              "fg_color":"#6495ED"})
        worksheet.merge_range("A7:F7", "Daily Performance", format_merge_row)
        try:
            if read_sql_KM.iloc[0]["IO_ID"] == self.config.IO_ID:
                worksheet.merge_range("A12:AA12","Placement & Daily level Performance - Brand Engagement",
                                      format_merge_row)
        except IndexError as e:
            pass

        try:
            if read_sql_Daily_sales.iloc[0]["IO_ID"] == self.config.IO_ID:
                worksheet.merge_range("A{}:H{}".format(number_rows_KM+16,number_rows_KM+16),
                              "Placement & Daily level Performance - Brand Performance", format_merge_row)
        except IndexError as e:
            pass

        full_border=workbook.add_format({"border": 1, "border_color": "#000000", "align":"center",
                                         "fg_color": "#6495ED", "bold": True})
        worksheet.conditional_format("A8:F10",{"type":"no_blanks","format":full_border})


        data_border_style= workbook.add_format({"border": 1, "border_color": "#000000"})


        worksheet.freeze_panes(13, 0)

        for col in range(4, 18):
            cell_location = xl_rowcol_to_cell(number_rows_KM+13, col)
            start_range = xl_rowcol_to_cell(13, col)
            end_range = xl_rowcol_to_cell(number_rows_KM+12, col)
            formula = "=SUM({:s}:{:s})".format(start_range, end_range)
            try:
                if read_sql_KM.iloc[0]["IO_ID"]==self.config.IO_ID:
                    worksheet.write_formula(cell_location, formula, full_border)
            except IndexError as e:
                pass
        try:
            if read_sql_KM.iloc[0]["IO_ID"]==self.config.IO_ID:
                worksheet.write_string(number_rows_KM+13, 0, "Total", full_border)
        except IndexError as e:
            pass

        for col in range(4, 7):
            cell_location = xl_rowcol_to_cell(number_rows_KM + number_rows_sales+ 17, col)
            start_range = xl_rowcol_to_cell(number_rows_KM + 17, col)
            end_range = xl_rowcol_to_cell(number_rows_KM + number_rows_sales + 16, col)
            formula = "=SUM({:s}:{:s})".format(start_range, end_range)
            try:
                if read_sql_Daily_sales.iloc[0]["IO_ID"]==self.config.IO_ID:
                    worksheet.write_formula(cell_location, formula, full_border)
            except IndexError as e:
                pass

        try:
            if read_sql_Daily_sales.iloc[0]["IO_ID"]==self.config.IO_ID:
                worksheet.write_string(number_rows_KM + number_rows_sales + 17, 0, "Total", full_border)
        except IndexError as e:
            pass

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
        #worksheet.set_row(number_rows_KM)
        worksheet.conditional_format("A14:AA{}".format(number_rows_KM+13),
                                     {"type": "no_blanks", "format": data_border_style})
        worksheet.conditional_format("A{}:H{}".format(18+number_rows_KM, number_rows_sales+number_rows_KM+17),
                                     {"type": "no_blanks", "format": data_border_style})
        worksheet.conditional_format("H{}:H{}".format(18+number_rows_KM, number_rows_sales+number_rows_KM+17),
                                     {"type": "no_blanks", "format": percent_fmt})

        #VDX Performance

        column_chart_KM=workbook.add_chart({'type':'column'})
        column_chart_KM.add_series({
                'name':"='Daily Performance({})'!E13".format(self.config.IO_ID),
                'categories':"='Daily Performance({})'!D14:D{}".format(self.config.IO_ID,number_rows_KM+13),
                'values':"='Daily Performance({})'!E14:E{}".format(self.config.IO_ID,number_rows_KM+13),
                'fill': {'color': '#6495ED'}
            })
        line_chart_KM = workbook.add_chart({'type': 'line'})
        line_chart_KM.add_series({
            'name':"='Daily Performance({})'!F13".format(self.config.IO_ID),
            'categories':"='Daily Performance({})'!D14:D{}".format(self.config.IO_ID,number_rows_KM),
            'values':"='Daily Performance({})'!F14:F{}".format(self.config.IO_ID,number_rows_KM+13),
            'y2_axis':True
        })

        column_chart_KM.combine(line_chart_KM)
        column_chart_KM.set_title({'name': 'VDX Performance'})
        column_chart_KM.set_x_axis({'name':'Date'})
        column_chart_KM.set_y_axis({'name':'Impression'})
        line_chart_KM.set_y2_axis({'name':'Engegements'})
        column_chart_KM.set_size({'width':1000,'height':500})


        #Sales Performance
        column_chart_sales=workbook.add_chart({'type':'column'})
        column_chart_sales.add_series({
            'name':"='Daily Performance({})'!E{}".format(self.config.IO_ID,number_rows_KM+17),
            'categories':"='Daily Performance({})'!D{}:D{}".format(self.config.IO_ID,number_rows_KM+18,number_rows_KM+number_rows_sales+17),
            'values':"='Daily Performance({})'!E{}:E{}".format(self.config.IO_ID,number_rows_KM+18,number_rows_KM+number_rows_sales+17),
            'fill':{'color':'#6495ED'}
        })
        line_chart_sales=workbook.add_chart({'type':'line'})
        line_chart_sales.add_series({
            'name':"='Daily Performance({})'!F{}".format(self.config.IO_ID,number_rows_KM+17),
            'categories':"='Daily Performance({})'!D{}:D{}".format(self.config.IO_ID,number_rows_KM+18,number_rows_KM +number_rows_sales+17),
            'values':"='Daily Performance({})'!F{}:F{}".format(self.config.IO_ID,number_rows_KM+18,number_rows_KM + number_rows_sales+17),
            'y2_axis':True
        })

        column_chart_sales.combine(line_chart_sales)
        column_chart_sales.set_title({'name':'Sales Performance'})
        column_chart_sales.set_x_axis({'name':'Date'})
        column_chart_sales.set_y_axis({'name':'Impression'})
        line_chart_sales.set_y2_axis({'name':'Clicks'})
        column_chart_sales.set_size({'width':1000,'height':500})
        #column_chart_sales.set_plotarea({'layout':{'x':0.20,'y':0.25,'width':0.75,'height':0.60,}})"""

        try:
            if read_sql_KM.iloc[0]["IO_ID"]==self.config.IO_ID:
                worksheet.insert_chart("AC14", column_chart_KM)
        except IndexError as e:
            pass

        try:
            if read_sql_Daily_sales.iloc[0]["IO_ID"]==self.config.IO_ID:
                worksheet.insert_chart("I{}".format(number_rows_KM+18), column_chart_sales)
        except IndexError as e:
            pass

    def main(self):
        self.config.common_columns_summary()
        self.connect_TFR_daily()
        self.read_Query_daily()
        self.access_Data_KM_Sales_daily()
        self.KM_Sales_daily()
        self.rename_KM_Sales_daily()
        self.adding_vcr_ctr_IR_ATS_daily()
        self.write_KM_Sales_summary()
        self.formatting_daily()

if __name__ == "__main__":
    pass

#enable it when running for individual file
    #c=config.Config('Dial',565337)
    #o=Daily(c)
    #o.main()
    #c.saveAndCloseWriter()
