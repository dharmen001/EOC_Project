import pandas as pd
import config


class Summary_Header:
    def __init__(self, config):
        self.config=config

    def connect_TFR_Summary(self):
        sql_qry = "select min(SDATE), (CASE WHEN max(EDATE) < trunc(sysdate-1) THEN max(EDATE) ELSE trunc(sysdate-1) END) AS LASTDATE, IO_DESC, CLIENT_DESC, ACCOUNT_MGR, SALES_REP from TFR_REP.SUMMARY_MV where IO_ID = {} group by IO_DESC, CLIENT_DESC, ACCOUNT_MGR, SALES_REP".format(self.config.IO_ID)
        read_sql_Summary=pd.read_sql(sql_qry,self.config.conn)
        print (read_sql_Summary)
        exit()
        return read_sql_Summary

    def read_query_summary(self):
        read_sql_Summary = self.connect_TFR_Summary()
        return read_sql_Summary

    def printData(self):
        df = self.read_query_summary()
        print(df)


if __name__ == "__main__":
    pass

    #enable it when running for individual file
    c = config.Config('Origin', 565337)
    o = Summary_Header(c)
    o.printData()
