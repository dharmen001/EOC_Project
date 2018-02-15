import pandas as pd
import numpy as np
from xlsxwriter.utility import xl_rowcol_to_cell
import config


class Summary():
    def __init__(self,config):
        self.config=config

    def connect_TFR_summary(self):

        sql_summary="select * from TFR_REP.SUMMARY_MV where IO_ID = {}".format(self.config.IO_ID)
        sql_KM="select * from TFR_REP.KEY_METRIC_MV where IO_ID = {}".format(self.config.IO_ID)
        sql_Daily_sales="select * from TFR_REP.DAILY_SALES_MV where IO_ID = {}".format(self.config.IO_ID)
        return sql_summary,sql_KM,sql_Daily_sales

    def read_query_summary(self):
        sql_summary,sql_KM,sql_Daily_sales=self.connect_TFR_summary()
        read_sql_summary=pd.read_sql(sql_summary,self.config.conn)
        read_sql_KM=pd.read_sql(sql_KM,self.config.conn)
        read_sql_Daily_sales=pd.read_sql(sql_Daily_sales,self.config.conn)
        return read_sql_summary,read_sql_KM,read_sql_Daily_sales

    def access_data_summary(self):
        read_sql_summary,read_sql_KM,read_sql_Daily_sales=self.read_query_summary()

        summary_pivot_first=pd.pivot_table(read_sql_summary,values=["BUDGET"],index=["PLACEMENT_ID",
                                                                                     "PLACEMENT_DESC","SDATE",
                                                                                     "EDATE","CREATIVE_DESC",
                                                                                     "METRIC_DESC","COST_TYPE_DESC",
                                                                                     "UNIT_COST","BOOKED_QTY"],
                                           aggfunc=np.sum)

        summary_data_summary_new=summary_pivot_first.reset_index()

        try:
            summary_data_summary=summary_data_summary_new[["PLACEMENT_ID","PLACEMENT_DESC","SDATE","EDATE",
                                                           "CREATIVE_DESC","METRIC_DESC","COST_TYPE_DESC",
                                                           "UNIT_COST","BUDGET","BOOKED_QTY"]]
        except KeyError:
            summary_data_summary=summary_data_summary_new[[]]

        KM_pivot_first=pd.pivot_table(read_sql_KM,values=["IMPRESSIONS","ENGAGEMENTS","CPCV_COUNT",
                                                          "DPE_ENGAGEMENTS"],index=["PLACEMENT_ID","PLACEMENT_DESC"]
                                      ,aggfunc=np.sum)
        KM_data_summary_new=KM_pivot_first.reset_index()
        try:
            KM_data_summary=KM_data_summary_new[["PLACEMENT_ID","PLACEMENT_DESC","IMPRESSIONS","ENGAGEMENTS",
                                                 "CPCV_COUNT","DPE_ENGAGEMENTS"]]
        except KeyError:
            KM_data_summary=KM_data_summary_new[[]]

        daily_sales_pivot_first=pd.pivot_table(read_sql_Daily_sales,values=["VIEWS","CLICKS","CONVERSIONS"],
                                               index=["PLACEMENT_ID","PLACEMENT_DESC"],
                                               aggfunc=np.sum)
        daily_sales_pivot_first_new=daily_sales_pivot_first.reset_index()

        try:
            daily_sales_data_summary=daily_sales_pivot_first_new[["PLACEMENT_ID","PLACEMENT_DESC","VIEWS","CLICKS",
                                                                  "CONVERSIONS"]]
        except KeyError:
            daily_sales_data_summary=daily_sales_pivot_first_new[[]]

        return summary_data_summary,KM_data_summary,daily_sales_data_summary

    def summary_creation(self):
        summary_data_summary,KM_data_summary,daily_sales_data_summary=self.access_data_summary()

        summary_all=summary_data_summary.merge(pd.concat([KM_data_summary,daily_sales_data_summary]),
                                               on=["PLACEMENT_ID"],suffixes=('_right','_left'))

        try:
            summary_new=summary_all[["PLACEMENT_ID","PLACEMENT_DESC_right","SDATE","EDATE","CREATIVE_DESC",
                                     "METRIC_DESC","COST_TYPE_DESC","UNIT_COST","BUDGET","BOOKED_QTY",
                                     "ENGAGEMENTS","VIEWS","IMPRESSIONS","CLICKS","CONVERSIONS","CPCV_COUNT",
                                     "DPE_ENGAGEMENTS"]]
        except KeyError:
            summary_new=summary_all[[]]

        return summary_new

    def rename_cols_sumary(self):
        summary_new=self.summary_creation()
        summary_cols_rename=summary_new.rename(columns={"PLACEMENT_ID":"Placement ID","PLACEMENT_DESC_right":
            "Placement#",
                                                        "SDATE":"Start Date","EDATE":"End Date",
                                                        "CREATIVE_DESC":"Creative Name","METRIC_DESC":"Metric",
                                                        "COST_TYPE_DESC":"COST TYPE","UNIT_COST":"Unit Cost",
                                                        "BUDGET":"Planned Cost","BOOKED_QTY":"Booked Eng#"
                                                                                             "Booked Imp",
                                                        "ENGAGEMENTS":"Delivered Engagements","VIEWS":
                                                            "Delivered Impressions","IMPRESSIONS":
                                                            "KM_Impressions","CLICKS":"Sales_Clicks",
                                                        "CONVERSIONS":"Conversions","CPCV_COUNT":"Completions",
                                                        "DPE_ENGAGEMENTS":"Deep Engagements"},
                                               inplace=True)

        return summary_new

    def adding_column_Delivery_summary(self):
        summary_new=self.rename_cols_sumary()
        try:
            summary_new["Delivery% ENG"]=summary_new["Delivered Engagements"]/summary_new["Booked Eng#Booked Imp"]
        except KeyError as e:
            pass
            #summary_new["Delivery% ENG"]= 0
        try:
            summary_new["Delivery% Impression"]=summary_new["Delivered Impressions"]/summary_new[
                "Booked Eng#""Booked Imp"]
        except KeyError as e:
            #summary_new["Delivery% Impression"] = 0
            pass
        try:
            summary_new["Delivery% KM"]=summary_new["KM_Impressions"]/summary_new["Booked Eng#""Booked Imp"]
        except KeyError as e:
            #summary_new["Delivery% KM"] = 0
            pass
        try:
            summary_new["Delivery% Clicks"]=summary_new["Sales_Clicks"]/summary_new["Booked Eng#""Booked Imp"]
        except KeyError as e:
            #summary_new["Delivery% Clicks"] = 0
            pass
        try:
            summary_new["Delivery% Conversion"]=summary_new["Conversions"]/summary_new["Booked Eng#""Booked Imp"]
        except KeyError as e:
            #summary_new["Delivery% Conversion"] = 0
            pass
        try:
            summary_new["Delivery% Conversion"]=summary_new["Conversions"]/summary_new["Booked Eng#""Booked Imp"]
        except KeyError as e:
            #summary_new["Delivery% Conversion"] = 0
            pass
        try:
            summary_new["Delivery% Completions"]=summary_new["Completions"]/summary_new["Booked Eng#""Booked Imp"]
        except KeyError as e:
            #summary_new["Delivery% Completions"] = 0
            pass
        try:
            summary_new["Delivery% DeepEng"]=summary_new["Deep Engagements"]/summary_new["Booked Eng#""Booked Imp"]
        except KeyError as e:
            #summary_new["Delivery% DeepEng"] = 0
            pass

        return summary_new

    def adding_column_Spend(self):

        summary_new=self.adding_column_Delivery_summary()

        try:
            summary_new["Spend Eng"]=summary_new["Delivered Engagements"]*summary_new["Unit Cost"]
        except KeyError as e:
            #summary_new["Spend Eng"] = 0
            pass
        try:
            summary_new["Spend Impression"]=summary_new["Delivered Impressions"]/1000*summary_new["Unit Cost"]
        except KeyError as e:
            #summary_new["Spend Impression"] = 0
            pass
        try:
            summary_new["Spend KM"]=summary_new["KM_Impressions"]/1000*summary_new["Unit Cost"]
        except KeyError as e:
            #summary_new["Spend KM"] = 0
            pass
        try:
            summary_new["Spend Clicks"]=summary_new["Sales_Clicks"]*summary_new["Unit Cost"]
        except KeyError as e:
            #summary_new["Spend Clicks"] = 0
            pass
        try:
            summary_new["Spend Conversion"]=summary_new["Conversions"]*summary_new["Unit Cost"]
        except KeyError as e:
            #summary_new["Spend Conversion"] = 0
            pass
        try:
            summary_new["Spend Completions"]=summary_new["Completions"]*summary_new["Unit Cost"]
        except KeyError as e:
            #summary_new["Spend Completions"] = 0
            pass
        try:
            summary_new["Spend DeepEng"]=summary_new["Deep Engagements"]*summary_new["Unit Cost"]
        except KeyError as e:
            #summary_new["Spend DeepEng"] = 0
            pass
        return summary_new

    def write_summary(self):

        summary_new=self.adding_column_Spend()

        data_common_columns=self.config.common_columns_summary()

        summary_new_new=summary_new.fillna(0)

        summary=data_common_columns[1].to_excel(self.config.writer,sheet_name="Summary({})".format(self.config.IO_ID),
                                                startcol=0,
                                                startrow=7,index=False,header=False)

        final_summary=summary_new_new.to_excel(self.config.writer,sheet_name="Summary({})".format(self.config.IO_ID),
                                               startcol=0,startrow=12,
                                               header=True,index=False)

        return summary,final_summary #summary_old

    def common_summary(self):

        data_common_columns=self.config.common_columns_summary()
        summary_new=self.adding_column_Spend()
        workbook=self.config.writer.book
        worksheet=self.config.writer.sheets["Summary({})".format(self.config.IO_ID)]
        number_rows=summary_new.shape[0]
        number_cols=summary_new.shape[1]

        money_fmt=workbook.add_format({"num_format":"$#,###0.00","align":"center"})

        percent_fmt=workbook.add_format({"num_format":"0.00%","align":"center"})

        full_border=workbook.add_format({"num_format":"$#,###0.00",
                                         "border":1,"border_color":"#000000","align":"center",
                                         "fg_color":"#6495ED","bold":True})
        full_border_total_format=workbook.add_format({"border":1,"border_color":"#000000","align":"center",
                                                      "fg_color":"#6495ED","bold":True})

        border_style=workbook.add_format({"border":1,"border_color":"#000000","fg_color":"#8EE5EE"})

        data_border_style=workbook.add_format({"border":1,"border_color":"#000000"})

        alignment=workbook.add_format({"align":"center"})

        worksheet.hide_gridlines(2)
        worksheet.insert_image("A1","Exponential.png")
        worksheet.freeze_panes(13,2)

        #format_common_column = {"header_row": False, "style": "Table Style Medium 2", 'autofilter': False}

        #worksheet.add_table("A8:F10", format_common_column)
        worksheet.conditional_format("A8:F10",{"type":"no_blanks","format":full_border_total_format})

        format_merge_row=workbook.add_format({"bold":True,"font_color":'#FFFFFF',"align":"left",
                                              "fg_color":"#6495ED"})

        worksheet.merge_range("A7:F7","Campaign Summary",format_merge_row)
        worksheet.set_column("M:N",20,None,{'level':1,'hidden':True})
        worksheet.set_column("T:X",20,None,{'level':1,'hidden':True})
        worksheet.set_column("AA:AE",20,None,{'level':1,'hidden':True})

        for col in range(7,9):
            cell_location=xl_rowcol_to_cell(number_rows+13,col)
            start_range=xl_rowcol_to_cell(13,col)
            end_range=xl_rowcol_to_cell(number_rows+12,col)
            formula="=SUM({:s}:{:s})".format(start_range,end_range)
            worksheet.write_formula(cell_location,formula,full_border)

        for col in range(9,17):
            cell_location=xl_rowcol_to_cell(number_rows+13,col)
            start_range=xl_rowcol_to_cell(13,col)
            end_range=xl_rowcol_to_cell(number_rows+12,col)
            formula="=SUM({:s}:{:s})".format(start_range,end_range)
            worksheet.write_formula(cell_location,formula,full_border_total_format)

        for col in range(24,31):
            cell_location=xl_rowcol_to_cell(number_rows+13,col)
            start_range=xl_rowcol_to_cell(13,col)
            end_range=xl_rowcol_to_cell(number_rows+12,col)
            formula="=SUM({:s}:{:s})".format(start_range,end_range)
            worksheet.write_formula(cell_location,formula,full_border)

        worksheet.write_string(number_rows+13,0,"Total",full_border_total_format)
        worksheet.set_zoom(100)
        worksheet.set_column("A:AE",None,alignment)
        worksheet.set_column("A:A",30)
        worksheet.set_column("B:B",78)
        worksheet.set_column("C:D",30)
        worksheet.set_column("E:E",40)
        worksheet.set_column("F:G",20)
        worksheet.set_column("H:I",20,money_fmt)
        worksheet.set_column("J:J",22)
        worksheet.set_column("K:Q",20)
        worksheet.set_column("R:X",20,percent_fmt)
        worksheet.set_column("Y:AE",20,money_fmt)
        worksheet.conditional_format("A14:AE{}".format(number_cols),{"type":"no_blanks","format":data_border_style})
        worksheet.conditional_format("A13:AE13",{"type":"no_blanks","format":border_style})

    def main(self):
        self.config.common_columns_summary()
        self.connect_TFR_summary()
        self.read_query_summary()
        self.access_data_summary()
        self.summary_creation()
        self.rename_cols_sumary()
        self.adding_column_Delivery_summary()
        self.adding_column_Spend()
        self.write_summary()
        self.common_summary()


if __name__=="__main__":
    pass

    #enable it when running for individual file
    #c = config.Config('COE', 600447)
    #o = Summary(c)
    #o.main()
    #c.saveAndCloseWriter()