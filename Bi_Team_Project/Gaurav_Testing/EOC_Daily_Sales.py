import pandas as pd
import numpy as np
from xlsxwriter.utility import xl_rowcol_to_cell
import config


class Daily_Sales():
    def __init__(self,config):
        self.config=config

    def connect_TFR_DailySales(self):
        sql_qry = "select PLACEMENT_ID, DAY_DESC, VIEWS, CLICKS, CONVERSIONS from TFR_REP.DAILY_SALES_MV where IO_ID = {} order by PLACEMENT_ID, DAY_DESC".format(self.config.IO_ID)
        read_sql_Daily_sales=pd.read_sql(sql_qry,self.config.conn)
        return read_sql_Daily_sales


    def read_query_summary(self,pl):
        read_sql_Daily_sales = self.connect_TFR_DailySales()
        #read_sql_Daily_sales.sort_values(by="DAY_DESC", ascending=True)
        df_by_placement = read_sql_Daily_sales[read_sql_Daily_sales["PLACEMENT_ID"] == pl]
        #print(df_by_placement)
        return df_by_placement


    def main(self):
        self.config.common_columns_summary()
        df=self.read_query_summary(2182898)
        print(df)
        recordCount = df["PLACEMENT_ID"].count()
        print(recordCount)


if __name__=="__main__":
    pass

    #enable it when running for individual file
    c = config.Config('Origin', 565337)
    o = Daily_Sales(c)
    o.main()
    c.saveAndCloseWriter()