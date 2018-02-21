import pandas as pd
import numpy as np
from xlsxwriter.utility import xl_rowcol_to_cell
import xlsxwriter

class ad_Size():
    def __init__(self, config):
        self.config = config

    def connect_TFR_adSize(self):
            sql_KM = "select * from TFR_REP.ADSIZE_KM_MV where IO_ID = {}".format(self.config.IO_ID)
            sql_Daily_sales = "select * from TFR_REP.ADSIZE_SALES_MV where IO_ID = {}".format(self.config.IO_ID)
            return sql_KM, sql_Daily_sales

    def read_Query_adSize(self):
        sql_KM, sql_Daily_sales = self.connect_TFR_adSize()
        read_sql_KM = pd.read_sql(sql_KM, self.config.conn)
        read_sql_Daily_sales = pd.read_sql(sql_Daily_sales, self.config.conn)
        return read_sql_KM, read_sql_Daily_sales

    def access_Data_KM_Sales_ad_Size(self):
        read_sql_KM,read_sql_Daily_sales=self.read_Query_adSize()
        KM_Data = pd.pivot_table(read_sql_KM, index=["MEDIA_SIZE_DESC"], values=["IMPRESSIONS", "ENGAGEMENTS",
                                                                                 "DPE_ENGAGEMENTS", "ENG_CLICK_THROUGHS",
                                                                                 "VWR_CLICK_THROUGHS",
                                                                                 "ENG_VIDEO_VIEW_100_PC_COUNT",
                                                                                 "VWR_VIDEO_VIEW_100_PC_COUNT",
                                                                                 "DPE_VIDEO_VIEW_100_PC_COUNT",
                                                                                 "CPCV_COUNT", "DPE_CLICK_THROUGHS"],
                                 aggfunc=np.sum)
        KM_reset= KM_Data.reset_index()

        try:
            KM_Data_New = KM_reset[["MEDIA_SIZE_DESC", "IMPRESSIONS", "ENGAGEMENTS", "DPE_ENGAGEMENTS",
                                    "ENG_CLICK_THROUGHS", "VWR_CLICK_THROUGHS", "ENG_VIDEO_VIEW_100_PC_COUNT",
                                    "VWR_VIDEO_VIEW_100_PC_COUNT", "DPE_VIDEO_VIEW_100_PC_COUNT", "CPCV_COUNT",
                                    "DPE_CLICK_THROUGHS"]]
        except KeyError:
            KM_Data_New=KM_reset[[]]

        daily_Sales_Data=pd.pivot_table(read_sql_Daily_sales, values=["VIEWS", "CLICKS", "CONVERSIONS"],
                                        index=["MEDIA_SIZE_DESC"],
                                        aggfunc=np.sum)
        sales_reset=daily_Sales_Data.reset_index()

        try:
            daily_Sales_Data_new=sales_reset[["MEDIA_SIZE_DESC", "VIEWS", "CLICKS", "CONVERSIONS"]]
        except KeyError:
            daily_Sales_Data_new=sales_reset[[]]

        return KM_Data_New, daily_Sales_Data_new

    def KM_Sales_ad_Size(self):

        KM_Data_New, daily_Sales_Data_new = self.access_Data_KM_Sales_ad_Size()

        try:
         accessing_KM_columns = KM_Data_New[["MEDIA_SIZE_DESC", "IMPRESSIONS", "ENGAGEMENTS", "DPE_ENGAGEMENTS",
                                                   "ENG_CLICK_THROUGHS", "VWR_CLICK_THROUGHS",
                                                   "ENG_VIDEO_VIEW_100_PC_COUNT", "VWR_VIDEO_VIEW_100_PC_COUNT",
                                                   "DPE_VIDEO_VIEW_100_PC_COUNT", "CPCV_COUNT", "DPE_CLICK_THROUGHS"]]
        except KeyError:
            accessing_KM_columns=KM_Data_New[[]]

        try:

            accessing_sales_columns = daily_Sales_Data_new[["MEDIA_SIZE_DESC", "VIEWS", "CLICKS", "CONVERSIONS"]]

        except KeyError:
            accessing_sales_columns=daily_Sales_Data_new[[]]

        return accessing_KM_columns, accessing_sales_columns

    def rename_KM_Sales_ad_Size(self):

        accessing_KM_columns, accessing_sales_columns = self.KM_Sales_ad_Size()
        rename_KM_columns = accessing_KM_columns.rename(columns={"MEDIA_SIZE_DESC": "Ad Size",
                                                                 "IMPRESSIONS": "KM_Impressions",
                                                                 "ENGAGEMENTS": "Delivered Engagements",
                                                                 "DPE_ENGAGEMENTS": "Deep Engagements",
                                                                 "ENG_CLICK_THROUGHS": "Eng click through",
                                                                 "VWR_CLICK_THROUGHS": "VWR click through",
                                                                 "ENG_VIDEO_VIEW_100_PC_COUNT": "ENG video 100 pc",
                                                                 "VWR_VIDEO_VIEW_100_PC_COUNT": "VWR video 100 pc",
                                                                 "DPE_VIDEO_VIEW_100_PC_COUNT": "Deep video 100 pc",
                                                                 "CPCV_COUNT":"Completions",
                                                                 "DPE_CLICK_THROUGHS": "Deep click through"
                                                                 }, inplace=True)
        rename_sales_column = accessing_sales_columns.rename(columns={"MEDIA_SIZE_DESC": "Ad Size",
                                                                      "VIEWS":"Delivered Impressions",
                                                                      "CLICKS":"Sales_Clicks",
                                                                      "CONVERSIONS":"Conversions"}, inplace=True)
        return accessing_KM_columns, accessing_sales_columns

    def adding_vcr_ctr_ad_Size(self):
        accessing_KM_columns, accessing_sales_columns = self.rename_KM_Sales_ad_Size()
        read_query_summary_results=self.read_Query_adSize()
        read_sql_KM=read_query_summary_results[0]
        read_sql_Daily_sales=read_query_summary_results[1]
        try:
            if read_sql_KM.iloc[0]["IO_ID"]==self.config.IO_ID:
                accessing_KM_columns["ENG RATE %"] = accessing_KM_columns["Delivered Engagements"]/accessing_KM_columns["KM_Impressions"]
        except KeyError as e:
            accessing_KM_columns["ENG RATE %"] = 0
        except IndexError as e:
            pass
        try:
            if read_sql_KM.iloc[0]["IO_ID"]==self.config.IO_ID:
                accessing_KM_columns["DPE Rate %"] = accessing_KM_columns ["Deep Engagements"]/accessing_KM_columns["KM_Impressions"]
        except KeyError as e:
            accessing_KM_columns["DPE Rate %"] = 0
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
                accessing_KM_columns["Eng VCR %"] = accessing_KM_columns["ENG video 100 pc"]/accessing_KM_columns["Delivered Engagements"]
        except KeyError as e:
            accessing_KM_columns["Eng VCR %"] = 0
        except IndexError as e:
            pass
        try:
            if read_sql_KM.iloc[0]["IO_ID"]==self.config.IO_ID:
                accessing_KM_columns["VWR VCR %"] = accessing_KM_columns["VWR video 100 pc"]/accessing_KM_columns["Delivered Engagements"]
        except KeyError as e:
            accessing_KM_columns["VWR VCR %"] = 0
        except IndexError as e:
            pass
        try:
            if read_sql_KM.iloc[0]["IO_ID"]==self.config.IO_ID:
                accessing_KM_columns["Deep VCR %"] = accessing_KM_columns["Deep video 100 pc"]/accessing_KM_columns["Delivered Engagements"]
        except KeyError as e:
            accessing_KM_columns["Deep VCR %"] = 0
        except IndexError as e:
            pass
        try:
            if read_sql_Daily_sales.iloc[0]["IO_ID"]==self.config.IO_ID:
                accessing_sales_columns["CTR%"] = accessing_sales_columns["Sales_Clicks"]/accessing_sales_columns["Delivered Impressions"]
        except KeyError as e:
            accessing_sales_columns["CTR%"]= 0
        except IndexError as e:
            pass

        return accessing_KM_columns, accessing_sales_columns

    def write_KM_Sales_ad_Size(self):

        data_common_columns = self.config.common_columns_summary()
        accessing_KM_columns, accessing_sales_columns = self.adding_vcr_ctr_ad_Size()
        replace_blank_with_zero_KM = accessing_KM_columns.fillna(0)
        replace_blank_with_zero_sales = accessing_sales_columns.fillna(0)

        writing_data_common_columns = data_common_columns[1].to_excel(self.config.writer,
                                                                      sheet_name="Ad-Size Performance({})".format(self.config.IO_ID),
                                                                      startcol=0, startrow=7, index=False, header=False)

        writing_KM_columns = replace_blank_with_zero_KM.to_excel(self.config.writer,
                                                                 sheet_name="Ad-Size Performance({})".format(self.config.IO_ID),
                                                                 startcol=0, startrow=12, index=False, header=True)

        writing_sales_columns = replace_blank_with_zero_sales.to_excel(self.config.writer,
                                                                       sheet_name="Ad-Size Performance({})".format(self.config.IO_ID),
                                                                       startcol=0, startrow=len(accessing_KM_columns)+16,
                                                                       index=False, header=True)

        return accessing_KM_columns, accessing_sales_columns, replace_blank_with_zero_KM, replace_blank_with_zero_sales

    def formatting_ad_Size(self):
        accessing_KM_columns, accessing_sales_columns = self.adding_vcr_ctr_ad_Size()
        number_rows_KM = accessing_KM_columns.shape[0]
        number_rows_sales = accessing_sales_columns.shape[0]
        read_query_summary_results=self.read_Query_adSize()
        read_sql_KM=read_query_summary_results[0]
        read_sql_Daily_sales=read_query_summary_results[1]
        workbook=self.config.writer.book
        worksheet=self.config.writer.sheets["Ad-Size Performance({})".format(self.config.IO_ID)]
        alignment=workbook.add_format({"align":"center"})
        worksheet.hide_gridlines(2)
        worksheet.insert_image("A1","Exponential.png")
        #format_common_column={"header_row":False,"style": "Table Style Medium 2", 'autofilter': False}
        #worksheet.add_table("A8:F10", format_common_column)
        format_merge_row=workbook.add_format({"bold": True, "font_color": '#FFFFFF',"align": "left",
                                              "fg_color": "#6495ED"})
        percent_fmt=workbook.add_format({"num_format":"0.00%","align":"center"})
        worksheet.merge_range("A7:F7","Ad-Size Performance", format_merge_row)
        try:
            if read_sql_KM.iloc[0]["IO_ID"]==self.config.IO_ID:
                worksheet.merge_range("A12:R12", "VDX Performance - Ad Size Summary", format_merge_row)
        except IndexError as e:
            pass
        try:
            if read_sql_Daily_sales.iloc[0]["IO_ID"]==self.config.IO_ID:
                worksheet.merge_range("A{}:E{}".format(number_rows_KM+16, number_rows_KM+16),
                              "Standard Banner Performance - Ad Size ", format_merge_row)
        except IndexError as e:
            pass

        full_border=workbook.add_format({"border": 1, "border_color": "#000000","align": "center",
                                         "fg_color": "#6495ED", "bold":True})
        worksheet.conditional_format("A8:F10",{"type":"no_blanks","format":full_border})

        data_border_style = workbook.add_format({"border": 1, "border_color": "#000000"})

        worksheet.freeze_panes(13, 0)

        for col in range(1, 11):
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

        for col in range(1, 4):
            cell_location = xl_rowcol_to_cell(number_rows_KM + number_rows_sales + 17, col)
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
        worksheet.set_column("B:B", 25, alignment)
        worksheet.set_column("C:C", 21, alignment)
        worksheet.set_column("D:D", 17, alignment)
        worksheet.set_column("E:E", 19, alignment)
        worksheet.set_column("F:F", 21, alignment)
        worksheet.set_column("G:G", 16, alignment)
        worksheet.set_column("H:H", 16, alignment)
        worksheet.set_column("I:K", 16, alignment)
        worksheet.set_column("L:L", 17, alignment)
        worksheet.set_column("M:M", 16, alignment, {'level': 1, 'hidden': True})
        worksheet.set_column("N:N", 16, alignment)
        worksheet.set_column("O:O", 23, alignment, {'level': 1, 'hidden': True})
        worksheet.set_column("P:P", 24, alignment)
        worksheet.set_column("Q:Q", 11, alignment, {'level': 1, 'hidden': True})
        worksheet.set_column("R:R", 17, alignment, {'level': 1, 'hidden': True})

        worksheet.conditional_format("A14:R{}".format(number_rows_KM+13),
                                     {"type": "no_blanks", "format": data_border_style})
        worksheet.conditional_format("A{}:E{}".format(18+number_rows_KM,number_rows_sales+number_rows_KM+17),
                                     {"type": "no_blanks", "format": data_border_style})

        worksheet.conditional_format("L14:R{}".format(number_rows_KM+13),
                                     {"type":"no_blanks","format":percent_fmt})
        worksheet.conditional_format("E{}:E{}".format(18+number_rows_KM,number_rows_sales+number_rows_KM+17),
                                     {"type":"no_blanks","format":percent_fmt})

    def main(self):
        self.config.common_columns_summary()
        self.connect_TFR_adSize()
        self.read_Query_adSize()
        self.access_Data_KM_Sales_ad_Size()
        self.KM_Sales_ad_Size()
        self.rename_KM_Sales_ad_Size()
        self.adding_vcr_ctr_ad_Size()
        self.write_KM_Sales_ad_Size()
        self.formatting_ad_Size()

    if __name__ == "__main__":
        pass
        #enable it when running for individual file
        #c=config.Config('dial',565337)
        #o = ad_Size(c)
        #o.main()
        #c.saveAndCloseWriter()


