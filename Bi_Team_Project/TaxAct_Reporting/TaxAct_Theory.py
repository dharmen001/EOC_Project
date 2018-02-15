import pandas as pd
from pandas.io import gbq
import cx_Oracle
import numpy as np
conn = cx_Oracle.connect("TFR_REP/welcome@10.29.20.76/tfrdb")
#projectid = "tribalfusion.com:expobidder"
query_adspace = "Select C.DAY_DESC,B.IO_ID,sum(A.NET_REVENUE)/100000 as Net_revenue from TFR_ADSPACE_BUY_V1_FACT A\
                    JOIN(select X.buy_ID,X.IO_ID,Y.IO_DESC from TFR_BUY_DIM X JOIN TFR_IO_DIM Y on X.IO_ID=Y.IO_ID) B on A.buy_ID=B.buy_ID\
                    JOIN TFR_DAY_DIM C on A.DAY_ID= C.DAY_ID WHERE B.IO_ID=597297 and c.day_desc>='25-dec-2017'\
                group by C.DAY_DESC,B.IO_ID"

query_pixel = "select c.day_desc,A.client_id,F.Client_desc,E.pixel_type_ID,E.pixel_type_Desc,A.event_type_id,F.Acct_mgr_Desc,F.Division_Desc,F.User_Desc,sum(a.Pixel_fired)as pixel_fired,sum(a.unique_users) as unique_users from TF_EVENT_OP_Fct A " \
              "join (select X.DAY_ID,X.DAY_DESC,X.MONTH_ID,Y.MONTH_DESC from tfr_day_dim X join tfr_month_dim Y on X.month_id=Y.month_id) C on A.DAY_ID=C.day_id " \
              "join (SELECT A.Client_ID,A.Client_Desc,A.User_ID,B.User_Desc,A.Division_ID,C.Division_Desc,A.Acct_Mgr_ID,D.Acct_Mgr_Desc FROM TFR_CLIENT_DIM A " \
              "JOIN TFR_USER_DIM B ON A.USER_ID=B.USER_ID JOIN TFR_DIVISION_DIM C ON A.DIVISION_ID=C.DIVISION_ID " \
              "JOIN TFR_ACCT_MGR_DIM D ON A.ACCT_MGR_ID=D.ACCT_MGR_ID)F on A.Client_id=F.CLient_id " \
              "join TFR_pixel_type_dim E on A.PIXEL_TYPE_ID=E.pixel_type_id where A.client_id=748773 and A.EVENT_TYPE_ID =2 and A.PIXEL_TYPE_ID = 32 " \
              "group by c.day_desc,E.pixel_type_ID,E.pixel_type_Desc,A.client_id,F.client_Desc,A.event_type_id,F.Acct_mgr_Desc,F.Division_Desc,F.User_Desc"
#path=("C://BiTeam-New-ProjectPython//Bi_Team_Project//Reports//TaxAct.xlsx")
#writer=pd.ExcelWriter(path, engine="xlsxwriter", datetime_format="YYYY-MM-DD")

def read_files():
    read_tp_impclick_conv = pd.read_csv("744234_TaxACTFY18_DailyReporting.csv", skiprows=14, low_memory=True)
    read_tp_impclick_conv.drop(read_tp_impclick_conv.tail(1).index, inplace=True)

    read_tp_first_touch = pd.read_csv("744234_TaxACTFY18_DailyReporting_FIRSTTOUCH.csv", skiprows=14, low_memory=True)
    read_tp_first_touch.drop(read_tp_first_touch.tail(1).index, inplace=True)

    read_tp_linear = pd.read_csv("744234_TaxACTFY18_DailyReporting_LINEAR.csv",skiprows=14, low_memory=True)
    read_tp_linear.drop(read_tp_linear.tail(1).index, inplace=True)

    read_tp_last_touch = pd.read_csv("744234_TaxACTFY18_DailyReporting_LASTTOUCH.csv", skiprows=14, low_memory=True)
    read_tp_last_touch.drop(read_tp_last_touch.tail(1).index, inplace=True)

    read_DBM_impclick_conv = pd.read_csv("744234_DBM_Competitive_Report_Position-Based.csv",skiprows=11,low_memory=True)
    read_DBM_impclick_conv.drop(read_DBM_impclick_conv.tail(1).index, inplace=True)

    read_DBM_first_touch = pd.read_csv("744234_DBM_Competitive_Report_First-Touch.csv", skiprows=11, low_memory=True)
    read_DBM_first_touch.drop(read_DBM_first_touch.tail(1).index, inplace=True)

    read_DBM_last_touch = pd.read_csv("744234_DBM_Competitive_Report_Last-Touch.csv", skiprows=11, low_memory=True)
    read_DBM_last_touch.drop(read_DBM_last_touch.tail(1).index, inplace=True)

    #read_bidder = gbq.read_gbq("select * from bidder_reports_5.stats_client_exchange_daily_2018_01 where client_id = 748773",
                               #projectid)

    #read_adspace = pd.read_sql(query_adspace, conn)

    read_pixel = pd.read_sql(query_pixel, conn)

    return read_tp_impclick_conv, read_tp_first_touch,read_tp_linear, read_tp_last_touch, read_DBM_impclick_conv,\
           read_DBM_first_touch, read_DBM_last_touch,  read_pixel

#read_bidder
#read_adspace


def check_files():

    read_tp_impclick_conv, read_tp_first_touch, read_tp_linear, read_tp_last_touch, read_DBM_impclick_conv,\
    read_DBM_first_touch, read_DBM_last_touch, read_pixel = read_files()

    #read_adspace,
    #,read_bidder

    read_tp_impclick_conv_pv = pd.pivot_table(read_tp_impclick_conv, index=["Date"], values = ["Impressions", "Clicks",
                                                                                               "Starts : Conversion - Start - Consumer: Total Conversions"],aggfunc=np.sum)
    read_tp_impclick_conv_pv_reset = read_tp_impclick_conv_pv.reset_index()

    read_tp_first_touch_pv = pd.pivot_table(read_tp_first_touch, index = ["Date"],
                                            values= ["Starts : Conversion - Start - Consumer: Total Conversions"], aggfunc=np.sum)
    read_tp_first_touch_pv_reset = read_tp_first_touch_pv.reset_index()

    read_tp_linear_pv = pd.pivot_table(read_tp_linear,index=["Date"],
                                       values=["Starts : Conversion - Start - Consumer: Total Conversions"],aggfunc=np.sum)
    read_tp_linear_pv_reset = read_tp_linear_pv.reset_index()

    read_tp_last_touch_pv = pd.pivot_table(read_tp_last_touch, index=["Date"],
                                           values=["Starts : Conversion - Start - Consumer: Total Conversions"], aggfunc=np.sum)
    read_tp_last_touch_pv_reset = read_tp_last_touch_pv.reset_index()

    read_DBM_impclick_conv_pv = pd.pivot_table(read_DBM_impclick_conv, index=["Date"],
                                               values=["Impressions", "Starts : Conversion - Start - Consumer: Total Conversions"],aggfunc=np.sum)
    read_DBM_impclick_conv_pv_reset = read_DBM_impclick_conv_pv.reset_index()

    read_DBM_first_touch_pv = pd.pivot_table(read_DBM_first_touch, index=["Date"], values=["Starts : Conversion - Start - Consumer: Total Conversions"], aggfunc=np.sum)
    read_DBM_first_touch_pv_reset = read_DBM_first_touch_pv.reset_index()

    read_DBM_last_touch_pv = pd.pivot_table(read_DBM_last_touch, index=["Date"], values=["Starts : Conversion - Start - Consumer: Total Conversions"], aggfunc=np.sum)
    read_DBM_last_touch_pv_reset = read_DBM_last_touch_pv.reset_index()

    #read_bidder_pv = pd.pivot_table(read_bidder, index=["time_id"], values=["total_win_price"], columns=["client_mode"],aggfunc=np.sum)
    #read_bidder_pv_reset = read_bidder_pv.reset_index()

    #read_adspace_pv = pd.pivot_table(read_adspace, index=["DAY_DESC"], values=["NET_REVENUE"], aggfunc=np.sum, fill_value=0)
    #read_adspace_pv_reset = read_adspace_pv.reset_index()

    read_pixel_pv = pd.pivot_table(read_pixel, index=["DAY_DESC"], values=["PIXEL_FIRED"], aggfunc=np.sum, fill_value=0)
    read_pixel_pv_reset = read_pixel_pv.reset_index()


def start_merging():
    read_tp_impclick_conv_pv_reset,read_tp_first_touch_pv_reset,\
    read_tp_linear_pv_reset,read_tp_last_touch_pv_reset,read_DBM_impclick_conv_pv_reset,\
    read_DBM_first_touch_pv_reset,read_DBM_last_touch_pv_reset,\
    read_pixel_pv_reset = check_files()
    #read_bidder_pv_reset,
    #read_adspace_pv_reset,

    print read_tp_impclick_conv_pv_reset

def main():
    read_files()
    check_files()
    start_merging()


if __name__ == "__main__":
    main()

