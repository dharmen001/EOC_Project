import pandas as pd
import numpy as np
from xlsxwriter.utility import xl_rowcol_to_cell
import config


class Summary_Detail():
    def __init__(self,config):
        self.config=config

    def connect_TFR_Summary(self):
        sql_qry = "select PLACEMENT_ID, CREATIVE_DESC, SDATE, LDATE, BUDGET, UNIT_COST, BOOKED_QTY, COST_TYPE_DESC, DATA_SOURCE from TFR_REP.SUMMARY_MV where IO_ID = {} order by PLACEMENT_ID".format(self.config.IO_ID)
        read_sql_Summary=pd.read_sql(sql_qry,self.config.conn)
        return read_sql_Summary

    def read_query_summary(self):
        read_sql_Summary = self.connect_TFR_Summary()
        return read_sql_Summary

    def read_summary_KM(self):
        df_KM = self.read_query_summary()
        df_KM = df_KM[df_KM["DATA_SOURCE"]=='KM']
        return df_KM

    def read_summary_Sales(self):
        df_Sales = self.read_query_summary()
        df_Sales = df_Sales[df_Sales["DATA_SOURCE"]=='SalesFile']
        return df_Sales

    def main(self):
        df=self.read_summary_KM()
        print(df)
        df=self.read_summary_Sales()
        print(df)


if __name__=="__main__":
    #pass

    #enable it when running for individual file
    c = config.Config('Origin', 565337)
    o = Summary_Detail(c)
    o.main()
    c.saveAndCloseWriter()