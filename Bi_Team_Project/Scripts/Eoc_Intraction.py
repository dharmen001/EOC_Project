# coding=utf-8
import pandas as pd
import numpy as np
from xlsxwriter.utility import xl_rowcol_to_cell
import xlsxwriter
import config


class Intraction():
	def __init__(self, config):
		self.config = config
	
	def connect_TFR_Intraction(self):
		sql_preroll_summary = "select * from (select substr(PLACEMENT_DESC, 1, INSTR(PLACEMENT_DESC, '.', 1)-1) as "'Placement#'", SDATE as "'Start_Date'", EDATE as "'End_Date'", initcap(CREATIVE_DESC)  as "'Placement_Name'", COST_TYPE_DESC as "'Cost_type'", UNIT_COST as "'Unit_Cost'", BUDGET as "'Planned_Cost'", BOOKED_QTY as "'Booked_Imp#Booked_Eng'" from  TFR_REP.SUMMARY_MV where (IO_ID = {}) AND (DATA_SOURCE = 'KM') AND CREATIVE_DESC IN(SELECT DISTINCT CREATIVE_DESC FROM TFR_REP.SUMMARY_MV) ORDER BY PLACEMENT_ID) WHERE Placement_Name IN ('Pre-Roll - Desktop','Pre-Roll - Desktop + Mobile','Pre-Roll – Desktop + Mobile','Pre-Roll - In-Stream/Mobile Blend','Pre-Roll - Mobile','Pre-Roll -Desktop','Pre-Roll - In-Stream')".format(self.config.IO_ID)
		sql_preroll_mv = "select substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as "'Placement#'", sum(IMPRESSIONS) as "'Impression'", sum(CPCV_COUNT) as "'Completions'" from TFR_REP.KEY_METRIC_MV WHERE IO_ID = {} GROUP BY PLACEMENT_ID, PLACEMENT_DESC ORDER BY PLACEMENT_ID".format(self.config.IO_ID)
		
		return sql_preroll_summary, sql_preroll_mv
	
	def read_query_preroll(self):
		sql_preroll_summary, sql_preroll_mv = self.connect_TFR_Intraction()
		
		read_sql_preroll_summary = pd.read_sql(sql_preroll_summary,self.config.conn)
		read_sql_preroll_mv = pd.read_sql(sql_preroll_mv,self.config.conn)
	
		return read_sql_preroll_summary, read_sql_preroll_mv
	
	def accessing_preroll_columns(self):
		read_sql_preroll_summary, read_sql_preroll_mv = self.read_query_preroll()
		
		print ("Dharmendra",read_sql_preroll_summary)
		print ("Harsh", read_sql_preroll_mv)
	
	
	def main(self):
		self.config.common_columns_summary()
		self.connect_TFR_Intraction()
		self.read_query_preroll()
		self.accessing_preroll_columns()

if __name__=="__main__":
	# pass
	
	# enable it when running for individual file
	c = config.Config( 'Dial', 582047 )
	o = Intraction( c )
	o.main()
	c.saveAndCloseWriter()
