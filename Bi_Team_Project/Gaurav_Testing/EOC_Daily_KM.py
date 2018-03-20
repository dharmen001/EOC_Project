import pandas as pd
import numpy as np
from xlsxwriter.utility import xl_rowcol_to_cell
import config


class Daily_KeyMetric():
    def __init__(self,config):
        self.config=config

    def connect_TFR_DailySales(self):
        #sql_qry = "select PLACEMENT_ID, DAY_DESC, IMPRESSIONS, CLICKS, CONVERSIONS from TFR_REP.DAILY_SALES_MV where IO_ID = {} order by PLACEMENT_ID, DAY_DESC".format(self.config.IO_ID)
        sql_qry = "select * from TFR_REP.KEY_METRIC_MV where IO_ID = {} order by PLACEMENT_ID, DAY_DESC".format(self.config.IO_ID)
        read_sql_Daily_sales = pd.read_sql(sql_qry,self.config.conn)
        return read_sql_Daily_sales

    def read_query_summary(self):
        read_sql_Daily_sales = self.connect_TFR_DailySales()
        read_sql_Daily_sales.sort_values(by="DAY_DESC", ascending=True)
        read_sql_Daily_sales = read_sql_Daily_sales[read_sql_Daily_sales['IMPRESSIONS'] != 0]
        #print(read_sql_Daily_sales)
        return read_sql_Daily_sales

    def read_cumulative_DF(self, pl = 2182848):
        df = self.read_query_summary()
        df_by_placement = df[(df["PLACEMENT_ID"] == pl) & (df['IMPRESSIONS'] != 0)]
        print(df_by_placement)
        print(df['IMPRESSIONS'].sum())
        print(df["PLACEMENT_ID"].count())
        return df


if __name__=="__main__":
    pass

    #enable it when running for individual file
    c = config.Config('Origin', 565337)
    o = Daily_KeyMetric(c)
    #o.printData()
    #o.read_query_summary()
    o.read_cumulative_DF()
