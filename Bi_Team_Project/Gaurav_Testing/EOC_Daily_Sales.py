import pandas as pd
import numpy as np
from xlsxwriter.utility import xl_rowcol_to_cell
import config


class Daily_Sales():
    def __init__(self,config):
        self.config=config

    def connect_TFR_DailySales(self):
        sql_qry = "select PLACEMENT_ID, TO_DATE(DAY_DESC, 'MM-DD-YYYY') AS DAY_DESC, VIEWS, CLICKS, CONVERSIONS from TFR_REP.DAILY_SALES_MV where IO_ID = {} order by PLACEMENT_ID, DAY_DESC".format(self.config.IO_ID)
        read_sql_Daily_sales = pd.read_sql(sql_qry,self.config.conn)
        return read_sql_Daily_sales

    def read_query_summary(self):
        read_sql_Daily_sales = self.connect_TFR_DailySales()
        read_sql_Daily_sales.sort_values(by="DAY_DESC", ascending=True)
        read_sql_Daily_sales = read_sql_Daily_sales[read_sql_Daily_sales['VIEWS'] != 0]
        return read_sql_Daily_sales

    def read_cumulative_Sales(self, pl = 2182878):
        df = self.read_query_summary()
        df_by_placement = df[(df["PLACEMENT_ID"] == pl) & (df['VIEWS'] != 0)]
        print(df_by_placement)
        print(df['VIEWS'].sum())
        print(df["PLACEMENT_ID"].count())
        return df


if __name__=="__main__":
    pass

    #enable it when running for individual file
    c = config.Config('Origin', 565337)
    o = Daily_Sales(c)
    o.read_query_summary()
    #o.read_cumulative_Sales()
