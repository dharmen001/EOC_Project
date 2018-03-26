import pandas as pd
import numpy as np
from xlsxwriter.utility import xl_rowcol_to_cell
import config


class AdSize_Sales():
    def __init__(self,config):
        self.config=config

    def connect_TFR_DailySales(self):
        sql_qry = "SELECT PLACEMENT_ID, MEDIA_SIZE_DESC, VIEWS, CLICKS, CONVERSIONS FROM TFR_REP.ADSIZE_SALES_MV where IO_ID = {} order by PLACEMENT_ID, MEDIA_SIZE_DESC".format(self.config.IO_ID)
        read_sql_Daily_sales = pd.read_sql(sql_qry,self.config.conn)
        return read_sql_Daily_sales

    def read_query_summary(self):
        read_sql_adsize_sales = self.connect_TFR_DailySales()
        #print(read_sql_adsize_sales)
        return read_sql_adsize_sales

    def read_Adsize_Sales(self, pl = 2182878):
        df = self.read_query_summary()
        df_by_placement = df[(df["PLACEMENT_ID"] == pl)]
        print(df_by_placement)
        print(df["PLACEMENT_ID"].count())
        return df


if __name__=="__main__":
    pass

    #enable it when running for individual file
    c = config.Config('Origin', 565337)
    o = AdSize_Sales(c)
    o.read_query_summary()
    #o.read_cumulative_Sales()
