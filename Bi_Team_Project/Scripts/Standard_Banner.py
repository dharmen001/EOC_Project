import pandas as pd
import numpy as np
import config
from xlsxwriter.utility import xl_rowcol_to_cell

class Daily():
    def __init__(self, config):
        self.config = config

    def connect_TFR_daily(self):
        sql_summary="select * from TFR_REP.SUMMARY_MV where IO_ID = {}".format(self.config.IO_ID)
        sql_Daily = "select * from TFR_REP.DAILY_SALES_MV where IO_ID = {}".format(self.config.IO_ID)
        sql_adsize="select * from TFR_REP.ADSIZE_SALES_MV where IO_ID = {}".format(self.config.IO_ID)
        return sql_Daily,sql_summary,sql_adsize

    def read_Query_daily(self):
        sql_Daily,sql_summary,sql_adsize = self.connect_TFR_daily()
        read_Daily_sales = pd.read_sql(sql_Daily, self.config.conn)
        read_sql_summary = pd.read_sql(sql_summary,self.config.conn)
        read_sql_adsize = pd.read_sql(sql_adsize,self.config.conn)
        return read_Daily_sales, read_sql_summary, read_sql_adsize

    def access_Data_KM_Sales_daily(self):
        read_Daily_sales,read_sql_summary,read_sql_adsize = self.read_Query_daily()

        read_summary=pd.pivot_table(read_sql_summary,values=["BUDGET"],
                                    index=["PLACEMENT_ID","PLACEMENT_DESC","CREATIVE_DESC","COST_TYPE_DESC","UNIT_COST",
                                           "BOOKED_QTY"],aggfunc=np.sum)

        reset_data_summary=read_summary.reset_index()

        read_sales=pd.pivot_table(read_Daily_sales,values=["VIEWS","CLICKS","CONVERSIONS"],
                                  index=["PLACEMENT_ID","PLACEMENT_DESC","METRIC_DESC","DAY_DESC"],aggfunc=np.sum)

        reset_data_sales=read_sales.reset_index()

        read_adsize=pd.pivot_table(read_sql_adsize,values=["VIEWS","CLICKS","CONVERSIONS"],
                                   index=["PLACEMENT_ID","PLACEMENT_DESC","MEDIA_SIZE_DESC"],aggfunc=np.sum)

        reset_data_adsize=read_adsize.reset_index()

        summary_columns=reset_data_summary[
            ["PLACEMENT_ID","PLACEMENT_DESC","CREATIVE_DESC","COST_TYPE_DESC","UNIT_COST","BOOKED_QTY"]]

        sales_columns=reset_data_sales[
            ["PLACEMENT_ID","PLACEMENT_DESC","METRIC_DESC","DAY_DESC","VIEWS","CLICKS","CONVERSIONS"]]

        ad_size_columns=reset_data_adsize[
            ["PLACEMENT_ID","PLACEMENT_DESC","MEDIA_SIZE_DESC","VIEWS","CLICKS","CONVERSIONS"]]

        return summary_columns, sales_columns, ad_size_columns

    def standard_banner(self):
        summary_columns, sales_columns,ad_size_columns = self.access_Data_KM_Sales_daily()
        #merge_summary_sales = (pd.concat[summary_columns,sales_columns],on= ["PLACEMENT_ID"] ,axis=1)



    def main(self):
        self.config.common_columns_summary()
        self.connect_TFR_daily()
        self.read_Query_daily()
        self.access_Data_KM_Sales_daily()
        self.standard_banner()
        #self.KM_Sales_daily()
        #self.rename_KM_Sales_daily()
        #self.adding_vcr_ctr_IR_ATS_daily()
        #self.write_KM_Sales_summary()
        #self.formatting_daily()

if __name__=="__main__":
    #pass

    #enable it when running for individual file
    c=config.Config('Dial',565337)
    o=Daily(c)
    o.main()
    c.saveAndCloseWriter()



