import pandas as pd
import cx_Oracle
import numpy as np

IO_ID = int(input("Enter the IO:"))
conn = cx_Oracle.connect("TFR_REP/welcome@10.29.20.76/tfrdb")
writer = pd.ExcelWriter("Summary({})".format(IO_ID), engine="xlsxwriter")

def common_columns():
    read_common_columns = pd.read_csv("Eociocommoncolumn.csv")
    data_common_columns = read_common_columns.loc[read_common_columns.IOID == IO_ID, :]
    return read_common_columns, data_common_columns

def connect_TFR():
    sql_summary = "select * from TFR_REP.SUMMARY_MV where IO_ID = {}".format(IO_ID)
    sql_KM = "select * from TFR_REP.KEY_METRIC_MV where IO_ID = {}".format(IO_ID)
    sql_VD = "select * from TFR_REP.VIDEO_DETAIL_MV where IO_ID = {}".format(IO_ID)
    sql_ID = "select * from TFR_REP.INTERACTION_DETAIL_MV where IO_ID = {}".format(IO_ID)
    sql_Daily_sales = "select * from TFR_REP.DAILY_SALES_MV where IO_ID = {}".format(IO_ID)
    return sql_summary, sql_KM, sql_VD, sql_ID, sql_Daily_sales

def read_query():
    sql_summary, sql_KM, sql_VD, sql_ID, sql_Daily_sales = connect_TFR()
    read_sql_summary = pd.read_sql(sql_summary, conn)
    read_sql_KM = pd.read_sql(sql_KM, conn)
    read_sql_VD = pd.read_sql(sql_VD, conn)
    read_sql_ID = pd.read_sql(sql_ID, conn)
    read_sql_Daily_sales = pd.read_sql(sql_Daily_sales, conn)
    return read_sql_summary, read_sql_KM, read_sql_VD, read_sql_ID, read_sql_Daily_sales

def access_data_summary():
    read_sql_summary, read_sql_KM, read_sql_VD, read_sql_ID, read_sql_Daily_sales = read_query()

    summary_pivot_first = pd.pivot_table(read_sql_summary, values=["BUDGET"], index=["PLACEMENT_ID", "PLACEMENT_DESC",
                                                                                     "SDATE",
                                                                                     "EDATE", "CREATIVE_DESC",
                                                                                     "METRIC_DESC", "COST_TYPE_DESC",
                                                                                     "UNIT_COST", "BOOKED_QTY"],
                                         aggfunc=np.sum)

    summary_data_summary_new = summary_pivot_first.reset_index()

    summary_data_summary = summary_data_summary_new[["PLACEMENT_ID", "PLACEMENT_DESC", "SDATE", "EDATE",
                                                     "CREATIVE_DESC", "METRIC_DESC", "COST_TYPE_DESC",
                                                     "UNIT_COST", "BUDGET", "BOOKED_QTY"]]

    KM_pivot_first = pd.pivot_table(read_sql_KM, values=["IMPRESSIONS", "ENGAGEMENTS", "CPCV_COUNT", "DPE_ENGAGEMENTS"],
                                    index=["PLACEMENT_ID","PLACEMENT_DESC"],
                                    aggfunc=np.sum)
    KM_data_summary_new = KM_pivot_first.reset_index()

    KM_data_summary = KM_data_summary_new[["PLACEMENT_ID", "PLACEMENT_DESC", "IMPRESSIONS", "ENGAGEMENTS", "CPCV_COUNT",
                                           "DPE_ENGAGEMENTS"]]

    daily_sales_pivot_first = pd.pivot_table(read_sql_Daily_sales, values=["VIEWS", "CLICKS", "CONVERSIONS"],
                                             index=["PLACEMENT_ID", "PLACEMENT_DESC"],
                                             aggfunc=np.sum)
    daily_sales_pivot_first_new = daily_sales_pivot_first.reset_index()

    daily_sales_data_summary = daily_sales_pivot_first_new[["PLACEMENT_ID", "PLACEMENT_DESC", "VIEWS", "CLICKS",
                                                            "CONVERSIONS"]]
    return summary_data_summary, KM_data_summary, daily_sales_data_summary

def summary_creation():
    summary_data_summary, KM_data_summary, daily_sales_data_summary = access_data_summary()
    summary_all = summary_data_summary.merge(pd.concat([KM_data_summary, daily_sales_data_summary]), on=["PLACEMENT_ID"]
                                             , suffixes=('_right', '_left'))
    summary_new = summary_all.loc[:, ["PLACEMENT_ID", "PLACEMENT_DESC_right", "SDATE", "EDATE", "CREATIVE_DESC",
                                      "METRIC_DESC", "COST_TYPE_DESC", "UNIT_COST", "BUDGET", "BOOKED_QTY",
                                      "ENGAGEMENTS", "VIEWS", "IMPRESSIONS", "CLICKS", "CONVERSIONS", "CPCV_COUNT",
                                      "DPE_ENGAGEMENTS"]]
    return summary_new

def rename_cols():
    summary_new = summary_creation()
    summary_cols_rename = summary_new.rename(columns={"PLACEMENT_ID": "Placement ID", "PLACEMENT_DESC_right":
                                                      "Placement#",
                                                      "SDATE": "Start Date", "EDATE": "End Date",
                                                      "CREATIVE_DESC": "Creative Name", "METRIC_DESC": "Metric",
                                                      "COST_TYPE_DESC": "COST TYPE", "UNIT_COST": "Unit Cost",
                                                      "BUDGET": "Planned Cost", "BOOKED_QTY": "Booked Engagements#"
                                                                                              "Booked Impressions",
                                                      "ENGAGEMENTS": "Delivered Engagements", "VIEWS":
                                                                        "Delivered Impressions", "IMPRESSIONS":
                                                          "KM_Impressions", "CLICKS": "Sales_Clicks",
                                                      "CONVERSIONS": "Conversions", "CPCV_COUNT": "Completions",
                                                      "DPE_ENGAGEMENTS": "Deep Engagements"},
                                             inplace=True)
    return summary_new
def adding_column_Delivery():
    summary_new = rename_cols()
    conditions = [(summary_new["Creative Name"] == "Display & Mobile Performance Blend") &
                  (summary_new["Metric"] == "CPM w/ CPA goal") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Display & Mobile Performance Blend") &
                  (summary_new["Metric"] == "CPC") & (summary_new["Unit Cost"] == "CPC"),
                  (summary_new["Creative Name"] == "Display & Mobile Performance Blend") &
                  (summary_new["Metric"] == "View-based CPA") & (summary_new["Unit Cost"] == "CPA"),
                  (summary_new["Creative Name"] == "Display & Mobile Performance Blend") &
                  (summary_new["Metric"] == "CPC") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Display & Mobile Performance Blend") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Display & Mobile Performance Blend") &
                  (summary_new["Metric"] == "Click-based CPA") & (summary_new["Unit Cost"] == "CPA"),
                  (summary_new["Creative Name"] == "Display & Mobile Performance Blend") &
                  (summary_new["Metric"] == "Click-based CPA") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Display & Mobile Performance Blend") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Display & Mobile Performance Blend") &
                  (summary_new["Metric"] == "CPM w/ CPA goal") & (summary_new[ "Unit Cost"] == "CPC"),
                  (summary_new["Creative Name"] == "Expandable Adhesion/IAB Blend (Video)") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Expandable Adhesion/IAB Blend (Video)") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units - Billboard") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units - Desktop + Mobile") &
                  (summary_new["Metric"] == "CPM w/ CPA goal") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units - Desktop + Mobile") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units - Desktop + Mobile") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units - Desktop + Mobile") &
                  (summary_new["Metric"] == "CPC") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units - Desktop + Mobile") &
                  (summary_new["Metric"] == "View-based CPA") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units - Desktop + Mobile") &
                  (summary_new["Metric"] == "View-based CPA") & (summary_new["Unit Cost"] == "CPA"),
                  (summary_new["Creative Name"] == "IAB Units - Desktop + Mobile") &
                  (summary_new["Metric"] == "CPM w/ CPA goal") & (summary_new[ "Unit Cost"] == "CPA"),
                  (summary_new["Creative Name"] == "IAB Units - Desktop + Mobile") &
                  (summary_new["Metric"] == "Click-based CPA") & (summary_new["Unit Cost"] == "CPA"),
                  (summary_new["Creative Name"] == "IAB Units - Desktop + Mobile") &
                  (summary_new["Metric"] == "CPC") & (summary_new[ "Unit Cost"] == "CPC"),
                  (summary_new["Creative Name"] == "IAB Units - Desktop + Mobile") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPC"),
                  (summary_new["Creative Name"] == "IAB Units - Desktop + Mobile") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "IAB Units - Desktop + Mobile") &
                  (summary_new["Metric"] == "Click-based CPA") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units - Desktop + Mobile") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPA"),
                  (summary_new["Creative Name"] == "IAB Units - Desktop + Mobile") &
                  (summary_new["Metric"] == "CPM w/ CPA goal") & (summary_new[ "Unit Cost"] == "CPC"),
                  (summary_new["Creative Name"] == "IAB Units - Desktop + Mobile") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "IAB Units - Half Page") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units - Half Page") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units - Mobile Only") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units - Mobile Only") &
                  (summary_new["Metric"] == "CPM w/ CPA goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units - Mobile Only") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units - Mobile Only") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPCV"),
                  (summary_new["Creative Name"] == "IAB Units (Desktop + Mobile) + Expandable Adhesion (Non-Video)") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units (Desktop + Mobile) + Expandable Adhesion (Non-Video)") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units (Desktop + Mobile) + Expandable Adhesion (Non-Video)") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "IAB Units (Desktop + Mobile) + Expandable Adhesion (Non-Video)") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "IAB Units (Desktop + Mobile) + Expandable Adhesion (Video)") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units (Desktop + Mobile) + Expandable Adhesion (Video)") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units (Desktop + Mobile) + Non-Expanding Adhesion") &
                  (summary_new["Metric"] == "CPM w/ CPA goal") & (summary_new[ "Unit Cost" ] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units (Desktop + Mobile) + Non-Expanding Adhesion") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units (Desktop + Mobile) + Non-Expanding Adhesion") &
                  (summary_new["Metric"] == "CPM w/ CPA goal") & (summary_new[ "Unit Cost"] == "CPA"),
                  (summary_new["Creative Name"] == "IAB Units (Desktop + Mobile) + Non-Expanding Adhesion") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPC"),
                  (summary_new["Creative Name"] == "IAB Units (Desktop + Mobile) + Non-Expanding Adhesion") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPC"),
                  (summary_new["Creative Name"] == "IAB Units (Desktop + Mobile) + Non-Expanding Adhesion") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units (Desktop + Mobile) + Non-Expanding Adhesion") &
                  (summary_new["Metric"] == "CPM w/ CPA goal") & (summary_new["Unit Cost"] == "CPC"),
                  (summary_new["Creative Name"] == "IAB Units (Desktop + Mobile) + Non-Expanding Adhesion") &
                  (summary_new["Metric"] == "CPC") & (summary_new[ "Unit Cost"] == "CPC"),
                  (summary_new["Creative Name"] == "IAB Units (Mobile Only) + Non-Expanding Adhesion") &
                  (summary_new["Metric"] == "CPM w/ CPA goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units (Mobile Only) + Non-Expanding Adhesion") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPC"),
                  (summary_new["Creative Name"] == "IAB Units (Mobile Only) + Non-Expanding Adhesion") &
                  (summary_new["Metric" ] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPC"),
                  (summary_new["Creative Name" ] == "IAB Units (Mobile Only) + Non-Expanding Adhesion") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units (Mobile Only) + Non-Expanding Adhesion") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Non-Expanding Adhesion/IAB Blend") &
                  (summary_new["Metric"] == "CPM w/ CPA goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Non-Expanding Adhesion/IAB Blend") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Non-Expanding Adhesion/IAB Blend") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Non-Expanding Adhesion/IAB Blend") &
                  (summary_new["Metric"] == "CPM w/ CPA goal") & (summary_new[ "Unit Cost"] == "CPA"),
                  (summary_new["Creative Name"] == "Pre-roll - Desktop") & (summary_new[ "Metric"] == "CPCV") &
                  (summary_new["Unit Cost"] == "CPCV"),
                  (summary_new["Creative Name"] == "Pre-roll - Desktop") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Pre-roll - Desktop") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Pre-roll - Desktop") & (summary_new[ "Metric"] == "CPCV") &
                  (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "Pre-roll - Desktop") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPCV"),
                  (summary_new["Creative Name"] == "Pre-roll - Desktop") &
                  (summary_new["Metric"] == "CPM w/ CPA goal") & (summary_new[ "Unit Cost"] == "CPCV"),
                  (summary_new["Creative Name"] == "Pre-roll - Desktop") &
                  (summary_new["Metric"] == "CPM w/ CPA goal") & (summary_new[ "Unit Cost" ] == "CPM"),
                  (summary_new["Creative Name"] == "Pre-roll - Desktop" ) & (summary_new[ "Metric"] == "vCPM") &
                  (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Pre-roll - Desktop + Mobile") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Pre-roll - Desktop + Mobile") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Pre-roll - Desktop + Mobile") &
                  (summary_new["Metric"] == "vCPM") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Pre-roll - Mobile") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Pre-roll - Mobile") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPCV"),
                  (summary_new["Creative Name"] == "Pre-roll - Mobile") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Pre-roll - Mobile") & (summary_new[ "Metric"] == "CPCV") &
                  (summary_new["Unit Cost"] == "CPCV"),
                  (summary_new["Creative Name"] == "Pre-roll -Desktop") & (summary_new[ "Metric"] == "CPCV") &
                  (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Pre-roll -Desktop") &
                  (summary_new["Metric"] == "CPM w/ CPA goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Pre-roll -Desktop") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Pre-roll -Desktop") & (summary_new[ "Metric"] == "CPCV") &
                  (summary_new["Unit Cost"] == "CPCV"),
                  (summary_new["Creative Name"] == "Pre-roll -Desktop") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Pre-roll -Desktop") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPCV"),
                  (summary_new["Creative Name"] == "Blend of VDX Rectangle/VDX Leaderboard/VDX Skyscraper/VDX Half"
                                                   "page/VDX Billboard") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost" ] == "CPM"),
                  (summary_new["Creative Name"] == "Blend of VDX Rectangle/VDX Leaderboard/VDX Skyscraper/VDX Half"
                                                   "page/VDX Billboard") &
                  (summary_new["Metric"] == "CPE") & (summary_new[ "Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "Blend of VDX Rectangle/VDX Leaderboard/VDX Skyscraper/VDX Half"
                                                   "page/VDX Billboard") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Blend of VDX Rectangle/VDX Leaderboard/VDX Skyscraper/VDX Half"
                                                   "page/VDX Billboard") &
                  (summary_new["Metric"] == "CPE+") & (summary_new[ "Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "Blend of VDX Rectangle/VDX Leaderboard/VDX Skyscraper/VDX Half"
                                                   "page/VDX Billboard") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost" ] == "CPE"),
                  (summary_new["Creative Name"] == "Blend of VDX Rectangle/VDX Leaderboard/VDX Skyscraper/VDX Half "
                                                   "page/VDX Billboard") &
                  (summary_new["Metric"] == "vCPM") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Billboard") & (summary_new[ "Metric"] == "CPE") &
                  (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Billboard") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Billboard") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Billboard") & (summary_new[ "Metric"] == "CPE+") &
                  (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Billboard") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Billboard") & (summary_new["Metric"] == "vCPM") &
                  (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Billboard") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Display Blend") & (summary_new["Metric"] == "CPE") &
                  (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Display Blend") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Display Blend") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Display Blend") & (summary_new[ "Metric"] == "CPE+") &
                  (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Display Blend") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Halfpage") & (summary_new[ "Metric"] == "CPE") &
                  (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Halfpage") & (summary_new[ "Metric"] == "CPE+") &
                  (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Halfpage") & (summary_new[ "Metric"] == "CPM Branding") &
                  (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Halfpage") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Halfpage") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Halfpage") & (summary_new["Metric"] == "CPM Branding") &
                  (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX In-Stream") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX In-Stream") & (summary_new["Metric"] == "CPE") &
                  (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX In-Stream") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX In-Stream") & (summary_new["Metric"] == "CPCV") &
                  (summary_new["Unit Cost"] == "CPCV"),
                  (summary_new["Creative Name"] == "VDX In-Stream") & (summary_new["Metric"] == "CPE+") &
                  (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX In-Stream" ) & (summary_new[ "Metric" ] == "CPE") &
                  (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX In-Stream" ) & (summary_new[ "Metric" ] == "CPCV") &
                  (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX In-Stream") & (summary_new[ "Metric"] == "vCPM") &
                  (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX In-Stream") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX In-Stream") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX In-Stream") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new["Unit Cost"] == "CPCV"),
                  (summary_new["Creative Name"] == "VDX Leaderboard") & (summary_new["Metric"] == "CPE") &
                  (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Leaderboard") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Mobile Adhesion Banner") &
                  (summary_new["Metric"] == "CPE") & (summary_new[ "Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Mobile Adhesion Banner") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Mobile Adhesion Banner") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Mobile Adhesion Banner") &
                  (summary_new["Metric"] == "CPE+") & (summary_new[ "Unit Cost" ] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Mobile Adhesion Banner" ) &
                  (summary_new["Metric"] == "CPE") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Mobile Adhesion Banner") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Mobile Adhesion Banner") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Mobile IAB") & (summary_new["Metric"] == "CPE") &
                  (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Mobile IAB") & (summary_new["Metric"] == "CPE+") &
                  (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Mobile IAB") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Mobile IAB") & (summary_new["Metric"] == "vCPM") &
                  (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Mobile IAB") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name" ] == "VDX Mobile Leaderboard") &
                  (summary_new["Metric"] == "CPE") & (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Mobile Rectangle") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Mobile Rectangle") & (summary_new["Metric"] == "CPE") &
                  (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Rectangle") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Rectangle") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Rectangle") & (summary_new["Metric"] == "CPE") &
                  (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Rectangle") & (summary_new["Metric"] == "CPE+") &
                  (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Skyscraper") & (summary_new["Metric"] == "CPE") &
                  (summary_new["Unit Cost"] == "CPE")]
    choices = [ "Delivered Impressions/Booked Engagements#Booked Impressions",
                "Sales_Clicks/Booked Engagements#Booked Impressions",
                "Conversions/Booked Engagements#Booked Impressions",
                "Delivered Impressions/Booked Engagements#Booked Impressions",
                "Delivered Impressions/Booked Engagements#Booked Impressions",
                "Conversions/Booked Engagements#Booked Impressions",
                "Delivered Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Sales_Clicks/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Delivered Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Delivered Impressions/Booked Engagements#Booked Impressions",
                "Delivered Impressions/Booked Engagements#Booked Impressions",
                "Conversions/Booked Engagements#Booked Impressions",
                "Conversions/Booked Engagements#Booked Impressions",
                "Conversions/Booked Engagements#Booked Impressions",
                "Sales_Clicks/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "Delivered Impressions/Booked Engagements#Booked Impressions",
                "Conversions/Booked Engagements#Booked Impressions",
                "Sales_Clicks/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Delivered Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Completions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Delivered Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Conversions/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Sales_Clicks/Booked Engagements#Booked Impressions",
                "Sales_Clicks/Booked Engagements#Booked Impressions",
                "Delivered Impressions/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Delivered Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Conversions/Booked Engagements#Booked Impressions",
                "Completions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "Completions/Booked Engagements#Booked Impressions",
                "Completions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Completions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Completions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Completions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Completions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Deep Engagements/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Deep Engagements/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "Deep Engagements/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "Deep Engagements/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Completions/Booked Engagements#Booked Impressions",
                "Deep Engagements/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "Completions/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Deep Engagements/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "Deep Engagements/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "Deep Engagements/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions"]

    summary_new["Delivery"] = np.select(conditions, choices)
    return summary_new

def adding_column_Spend():
    summary_new = adding_column_Delivery()
    conditions_Spend = [
                  (summary_new["Creative Name"] == "Display & Mobile Performance Blend") &
                  (summary_new["Metric"] == "CPM w/ CPA goal") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Display & Mobile Performance Blend") &
                  (summary_new["Metric"] == "CPC") & (summary_new["Unit Cost"] == "CPC"),
                  (summary_new["Creative Name"] == "Display & Mobile Performance Blend") &
                  (summary_new["Metric"] == "View-based CPA") & (summary_new["Unit Cost"] == "CPA"),
                  (summary_new["Creative Name"] == "Display & Mobile Performance Blend") &
                  (summary_new["Metric"] == "CPC") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Display & Mobile Performance Blend") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Display & Mobile Performance Blend") &
                  (summary_new["Metric"] == "Click-based CPA") & (summary_new["Unit Cost"] == "CPA"),
                  (summary_new["Creative Name"] == "Display & Mobile Performance Blend") &
                  (summary_new["Metric"] == "Click-based CPA") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Display & Mobile Performance Blend") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Display & Mobile Performance Blend") &
                  (summary_new["Metric"] == "CPM w/ CPA goal") & (summary_new[ "Unit Cost"] == "CPC"),
                  (summary_new["Creative Name"] == "Expandable Adhesion/IAB Blend (Video)") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Expandable Adhesion/IAB Blend (Video)") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units - Billboard") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units - Desktop + Mobile") &
                  (summary_new["Metric"] == "CPM w/ CPA goal") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units - Desktop + Mobile") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units - Desktop + Mobile") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units - Desktop + Mobile") &
                  (summary_new["Metric"] == "CPC") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units - Desktop + Mobile") &
                  (summary_new["Metric"] == "View-based CPA") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units - Desktop + Mobile") &
                  (summary_new["Metric"] == "View-based CPA") & (summary_new["Unit Cost"] == "CPA"),
                  (summary_new["Creative Name"] == "IAB Units - Desktop + Mobile") &
                  (summary_new["Metric"] == "CPM w/ CPA goal") & (summary_new[ "Unit Cost"] == "CPA"),
                  (summary_new["Creative Name"] == "IAB Units - Desktop + Mobile") &
                  (summary_new["Metric"] == "Click-based CPA") & (summary_new["Unit Cost"] == "CPA"),
                  (summary_new["Creative Name"] == "IAB Units - Desktop + Mobile") &
                  (summary_new["Metric"] == "CPC") & (summary_new[ "Unit Cost"] == "CPC"),
                  (summary_new["Creative Name"] == "IAB Units - Desktop + Mobile") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPC"),
                  (summary_new["Creative Name"] == "IAB Units - Desktop + Mobile") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "IAB Units - Desktop + Mobile") &
                  (summary_new["Metric"] == "Click-based CPA") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units - Desktop + Mobile") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPA"),
                  (summary_new["Creative Name"] == "IAB Units - Desktop + Mobile") &
                  (summary_new["Metric"] == "CPM w/ CPA goal") & (summary_new[ "Unit Cost"] == "CPC"),
                  (summary_new["Creative Name"] == "IAB Units - Desktop + Mobile") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "IAB Units - Half Page") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units - Half Page") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units - Mobile Only") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units - Mobile Only") &
                  (summary_new["Metric"] == "CPM w/ CPA goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units - Mobile Only") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units - Mobile Only") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPCV"),
                  (summary_new["Creative Name"] == "IAB Units (Desktop + Mobile) + Expandable Adhesion (Non-Video)") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units (Desktop + Mobile) + Expandable Adhesion (Non-Video)") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units (Desktop + Mobile) + Expandable Adhesion (Non-Video)") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "IAB Units (Desktop + Mobile) + Expandable Adhesion (Non-Video)") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "IAB Units (Desktop + Mobile) + Expandable Adhesion (Video)") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units (Desktop + Mobile) + Expandable Adhesion (Video)") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units (Desktop + Mobile) + Non-Expanding Adhesion") &
                  (summary_new["Metric"] == "CPM w/ CPA goal") & (summary_new[ "Unit Cost" ] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units (Desktop + Mobile) + Non-Expanding Adhesion") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units (Desktop + Mobile) + Non-Expanding Adhesion") &
                  (summary_new["Metric"] == "CPM w/ CPA goal") & (summary_new[ "Unit Cost"] == "CPA"),
                  (summary_new["Creative Name"] == "IAB Units (Desktop + Mobile) + Non-Expanding Adhesion") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPC"),
                  (summary_new["Creative Name"] == "IAB Units (Desktop + Mobile) + Non-Expanding Adhesion") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPC"),
                  (summary_new["Creative Name"] == "IAB Units (Desktop + Mobile) + Non-Expanding Adhesion") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units (Desktop + Mobile) + Non-Expanding Adhesion") &
                  (summary_new["Metric"] == "CPM w/ CPA goal") & (summary_new["Unit Cost"] == "CPC"),
                  (summary_new["Creative Name"] == "IAB Units (Desktop + Mobile) + Non-Expanding Adhesion") &
                  (summary_new["Metric"] == "CPC") & (summary_new[ "Unit Cost"] == "CPC"),
                  (summary_new["Creative Name"] == "IAB Units (Mobile Only) + Non-Expanding Adhesion") &
                  (summary_new["Metric"] == "CPM w/ CPA goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units (Mobile Only) + Non-Expanding Adhesion") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPC"),
                  (summary_new["Creative Name"] == "IAB Units (Mobile Only) + Non-Expanding Adhesion") &
                  (summary_new["Metric" ] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPC"),
                  (summary_new["Creative Name" ] == "IAB Units (Mobile Only) + Non-Expanding Adhesion") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units (Mobile Only) + Non-Expanding Adhesion") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Non-Expanding Adhesion/IAB Blend") &
                  (summary_new["Metric"] == "CPM w/ CPA goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Non-Expanding Adhesion/IAB Blend") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Non-Expanding Adhesion/IAB Blend") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Non-Expanding Adhesion/IAB Blend") &
                  (summary_new["Metric"] == "CPM w/ CPA goal") & (summary_new[ "Unit Cost"] == "CPA"),
                  (summary_new["Creative Name"] == "Pre-roll - Desktop") & (summary_new[ "Metric"] == "CPCV") &
                  (summary_new["Unit Cost"] == "CPCV"),
                  (summary_new["Creative Name"] == "Pre-roll - Desktop") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Pre-roll - Desktop") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Pre-roll - Desktop") & (summary_new[ "Metric"] == "CPCV") &
                  (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "Pre-roll - Desktop") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPCV"),
                  (summary_new["Creative Name"] == "Pre-roll - Desktop") &
                  (summary_new["Metric"] == "CPM w/ CPA goal") & (summary_new[ "Unit Cost"] == "CPCV"),
                  (summary_new["Creative Name"] == "Pre-roll - Desktop") &
                  (summary_new["Metric"] == "CPM w/ CPA goal") & (summary_new[ "Unit Cost" ] == "CPM"),
                  (summary_new["Creative Name"] == "Pre-roll - Desktop" ) & (summary_new[ "Metric"] == "vCPM") &
                  (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Pre-roll - Desktop + Mobile") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Pre-roll - Desktop + Mobile") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Pre-roll - Desktop + Mobile") &
                  (summary_new["Metric"] == "vCPM") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Pre-roll - Mobile") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Pre-roll - Mobile") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPCV"),
                  (summary_new["Creative Name"] == "Pre-roll - Mobile") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Pre-roll - Mobile") & (summary_new[ "Metric"] == "CPCV") &
                  (summary_new["Unit Cost"] == "CPCV"),
                  (summary_new["Creative Name"] == "Pre-roll -Desktop") & (summary_new[ "Metric"] == "CPCV") &
                  (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Pre-roll -Desktop") &
                  (summary_new["Metric"] == "CPM w/ CPA goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Pre-roll -Desktop") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Pre-roll -Desktop") & (summary_new[ "Metric"] == "CPCV") &
                  (summary_new["Unit Cost"] == "CPCV"),
                  (summary_new["Creative Name"] == "Pre-roll -Desktop") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Pre-roll -Desktop") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPCV"),
                  (summary_new["Creative Name"] == "Blend of VDX Rectangle/VDX Leaderboard/VDX Skyscraper/VDX Half"
                                                   "page/VDX Billboard") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost" ] == "CPM"),
                  (summary_new["Creative Name"] == "Blend of VDX Rectangle/VDX Leaderboard/VDX Skyscraper/VDX Half"
                                                   "page/VDX Billboard") &
                  (summary_new["Metric"] == "CPE") & (summary_new[ "Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "Blend of VDX Rectangle/VDX Leaderboard/VDX Skyscraper/VDX Half"
                                                   "page/VDX Billboard") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Blend of VDX Rectangle/VDX Leaderboard/VDX Skyscraper/VDX Half"
                                                   "page/VDX Billboard") &
                  (summary_new["Metric"] == "CPE+") & (summary_new[ "Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "Blend of VDX Rectangle/VDX Leaderboard/VDX Skyscraper/VDX Half"
                                                   "page/VDX Billboard") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost" ] == "CPE"),
                  (summary_new["Creative Name"] == "Blend of VDX Rectangle/VDX Leaderboard/VDX Skyscraper/VDX Half "
                                                   "page/VDX Billboard") &
                  (summary_new["Metric"] == "vCPM") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Billboard") & (summary_new[ "Metric"] == "CPE") &
                  (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Billboard") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Billboard") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Billboard") & (summary_new[ "Metric"] == "CPE+") &
                  (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Billboard") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Billboard") & (summary_new["Metric"] == "vCPM") &
                  (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Billboard") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Display Blend") & (summary_new["Metric"] == "CPE") &
                  (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Display Blend") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Display Blend") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Display Blend") & (summary_new[ "Metric"] == "CPE+") &
                  (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Display Blend") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Halfpage") & (summary_new[ "Metric"] == "CPE") &
                  (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Halfpage") & (summary_new[ "Metric"] == "CPE+") &
                  (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Halfpage") & (summary_new[ "Metric"] == "CPM Branding") &
                  (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Halfpage") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Halfpage") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Halfpage") & (summary_new["Metric"] == "CPM Branding") &
                  (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX In-Stream") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX In-Stream") & (summary_new["Metric"] == "CPE") &
                  (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX In-Stream") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX In-Stream") & (summary_new["Metric"] == "CPCV") &
                  (summary_new["Unit Cost"] == "CPCV"),
                  (summary_new["Creative Name"] == "VDX In-Stream") & (summary_new["Metric"] == "CPE+") &
                  (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX In-Stream" ) & (summary_new[ "Metric" ] == "CPE") &
                  (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX In-Stream" ) & (summary_new[ "Metric" ] == "CPCV") &
                  (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX In-Stream") & (summary_new[ "Metric"] == "vCPM") &
                  (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX In-Stream") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX In-Stream") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX In-Stream") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new["Unit Cost"] == "CPCV"),
                  (summary_new["Creative Name"] == "VDX Leaderboard") & (summary_new["Metric"] == "CPE") &
                  (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Leaderboard") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Mobile Adhesion Banner") &
                  (summary_new["Metric"] == "CPE") & (summary_new[ "Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Mobile Adhesion Banner") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Mobile Adhesion Banner") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Mobile Adhesion Banner") &
                  (summary_new["Metric"] == "CPE+") & (summary_new[ "Unit Cost" ] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Mobile Adhesion Banner" ) &
                  (summary_new["Metric"] == "CPE") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Mobile Adhesion Banner") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Mobile Adhesion Banner") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Mobile IAB") & (summary_new["Metric"] == "CPE") &
                  (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Mobile IAB") & (summary_new["Metric"] == "CPE+") &
                  (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Mobile IAB") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Mobile IAB") & (summary_new["Metric"] == "vCPM") &
                  (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Mobile IAB") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name" ] == "VDX Mobile Leaderboard") &
                  (summary_new["Metric"] == "CPE") & (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Mobile Rectangle") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Mobile Rectangle") & (summary_new["Metric"] == "CPE") &
                  (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Rectangle") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Rectangle") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Rectangle") & (summary_new["Metric"] == "CPE") &
                  (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Rectangle") & (summary_new["Metric"] == "CPE+") &
                  (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Skyscraper") & (summary_new["Metric"] == "CPE") &
                  (summary_new["Unit Cost"] == "CPE")
    ]
    choices_new = [
            "Delivered Impressions/1000*Unit Cost",
            "Sales_Clicks*Unit Cost",
            "Conversions*Unit Cost",
            "Delivered Impressions/1000*Unit Cost",
            "Delivered Impressions/1000*Unit Cost",
            "Conversions*Unit Cost",
            "Delivered Impressions/1000*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "Sales_Clicks*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "Delivered Impressions/1000*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "Delivered Impressions/1000*Unit Cost",
            "Delivered Impressions/1000*Unit Cost",
            "Conversions*Unit Cost",
            "Conversions*Unit Cost",
            "Conversions*Unit Cost",
            "Sales_Clicks*Unit Cost",
            "Delivered Engagements*Unit Cost",
            "Delivered Engagements*Unit Cost",
            "Delivered Impressions/1000*Unit Cost",
            "Conversions*Unit Cost",
            "Sales_Clicks*Unit Cost",
            "Delivered Engagements*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "Delivered Impressions/1000*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "Completions*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "Delivered Engagements*Unit Cost",
            "Delivered Engagements*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "Delivered Impressions/1000*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "Conversions*Unit Cost",
            "Delivered Engagements*Unit Cost",
            "Delivered Engagements*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "Sales_Clicks*Unit Cost",
            "Sales_Clicks*Unit Cost",
            "Delivered Impressions/1000*Unit Cost",
            "Delivered Engagements*Unit Cost",
            "Delivered Engagements*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "Delivered Impressions/1000*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "Conversions*Unit Cost",
            "Completions*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "Delivered Engagements*Unit Cost",
            "Completions*Unit Cost",
            "Completions*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "Completions*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "Completions*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "Completions*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "Completions*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "Delivered Engagements*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "Deep Engagements*Unit Cost",
            "Delivered Engagements*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "Delivered Engagements*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "Deep Engagements*Unit Cost",
            "Delivered Engagements*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "Delivered Engagements*Unit Cost",
            "Delivered Engagements*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "Delivered Engagements*Unit Cost",
            "Deep Engagements*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "Delivered Engagements*Unit Cost",
            "Deep Engagements*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "Delivered Engagements*Unit Cost",
            "Delivered Engagements*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "Delivered Engagements*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "Completions*Unit Cost",
            "Deep Engagements*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "Delivered Engagements*Unit Cost",
            "Delivered Engagements*Unit Cost",
            "Completions*Unit Cost",
            "Delivered Engagements*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "Delivered Engagements*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "Deep Engagements*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "Delivered Engagements*Unit Cost",
            "Delivered Engagements*Unit Cost",
            "Delivered Engagements*Unit Cost",
            "Deep Engagements*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "Delivered Engagements*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "Delivered Engagements*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "KM_Impressions/1000*Unit Cost",
            "Delivered Engagements*Unit Cost",
            "Deep Engagements*Unit Cost",
            "Delivered Engagements*Unit Cost",
    ]
    summary_new["Spend"] = np.select(conditions_Spend, choices_new)
def write_summary():
    summary_new=adding_column_Spend()
    read_common_columns = common_columns()
    data_common_columns = common_columns()
    summary = data_common_columns.to_excel(writer, sheet_name="Summary({})".format(IO_ID), startcol=3,
                                           startrow=7, index=False)
    df1 = pd.DataFrame({"Campaign Summary": []})
    add_df1 = df1.to_excel(writer, sheet_name="Summary({})".format(IO_ID),startcol=3, startrow=10,index=False)
    offset = len(summary)+3
    for df in summary:
        df.to_excel(writer, sheet_name="Summary({})".format(IO_ID), startrow=offset, startcol=3, header=True, index=False)
    writer.save
    return summary
def main():
    common_columns()
    connect_TFR()
    read_query()
    access_data_summary()
    summary_creation()
    rename_cols()
    adding_column_Delivery()
    adding_column_Spend()
    write_summary()

if __name__ == "__main__":
    main ()
