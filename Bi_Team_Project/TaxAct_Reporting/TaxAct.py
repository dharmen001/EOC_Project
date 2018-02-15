import pandas as pd
import numpy as np
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

    read_adspace = pd.read_csv("Adspace.csv", low_memory=True)
    read_pixel = pd.read_csv("pixel_event_taxact.csv", low_memory=True)
    read_sales = pd.read_csv("Sales_TaxAct.csv", low_memory=True)
    read_bidder = pd.read_csv("Bidder.csv", low_memory=True)

    return read_tp_impclick_conv, read_tp_first_touch,read_tp_linear, read_tp_last_touch, read_DBM_impclick_conv,\
           read_DBM_first_touch, read_DBM_last_touch, read_adspace, read_pixel, read_sales, read_bidder




def check_files():
    read_tp_impclick_conv = read_files()
    print read_tp_impclick_conv


def main():
    read_files()
    check_files()


if __name__ == "__main__":
    main()

