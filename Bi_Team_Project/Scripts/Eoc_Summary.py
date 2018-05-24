# coding=utf-8
"""
Created by:Dharmendra
Date:2018-03-23
"""
from __future__ import print_function
import pandas as pd
import numpy as np
from xlsxwriter.utility import xl_range, xl_rowcol_to_cell

import config
import logging
import pandas.io.formats.excel

pandas.io.formats.excel.header_style = None


class Summary (object):
	"""This class in for creating summary sheet"""
	
	def __init__(self, config):
		"""
		:type config: Config
		"""
		self.config = config
		self.logger = self.config.logger
	
	def connect_TFR_summary(self):
		"""
	    
	    :param self:Query Reading
	    :return:Query
	    """
		sqlvdxsummary = "select * from (select substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as Placement#, TO_CHAR(SDATE, 'YYYY-MM-DD') as Start_Date, TO_CHAR(EDATE, 'YYYY-MM-DD') as End_Date, initcap(CREATIVE_DESC)  as Placement_Name, COST_TYPE_DESC as Cost_type, UNIT_COST as Unit_Cost, BUDGET as Planned_Cost, BOOKED_QTY as Booked_Imp#Booked_Eng FROM TFR_REP.SUMMARY_MV where IO_ID = {} AND DATA_SOURCE = 'KM' AND (TO_CHAR(SDATE, 'YYYY-MM-DD') >= '{}' AND TO_CHAR(EDATE, 'YYYY-MM-DD') <= '{}') AND CREATIVE_DESC IN(SELECT DISTINCT CREATIVE_DESC FROM TFR_REP.SUMMARY_MV) ORDER BY PLACEMENT_ID) WHERE Placement_Name Not LIKE '%Pre-Roll%' or Placement_Name LIKE '%Pre–Roll%'".format(self.config.ioid,self.config.start_date,self.config.end_date)
	
		sqlvdxmv = "select substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as Placement#, sum(IMPRESSIONS) as "'Impression'", sum(ENGAGEMENTS) as "'Eng'", sum(DPE_ENGAGEMENTS) as Deep,sum(CPCV_COUNT) as Completions from TFR_REP.KEY_METRIC_MV WHERE IO_ID = {} AND TO_CHAR(TO_DATE(DAY_DESC, 'MM/DD/YYYY'),'YYYY-MM-DD') BETWEEN '{}' AND '{}' GROUP BY PLACEMENT_ID, PLACEMENT_DESC ORDER BY PLACEMENT_ID".format(self.config.ioid,self.config.start_date,self.config.end_date)
	
		sqldisplaysummary = "select substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as Placement#, TO_CHAR(SDATE, 'YYYY-MM-DD') as Start_Date, TO_CHAR(EDATE, 'YYYY-MM-DD') as End_Date, CREATIVE_DESC  as Placement_Name, COST_TYPE_DESC as Cost_type,UNIT_COST as Unit_Cost,BUDGET as Planned_Cost,BOOKED_QTY as Booked_Imp#Booked_Eng FROM TFR_REP.SUMMARY_MV where IO_ID = {} AND DATA_SOURCE = 'SalesFile' AND (TO_CHAR(SDATE, 'YYYY-MM-DD') >= '{}' AND TO_CHAR(EDATE, 'YYYY-MM-DD') <= '{}') ORDER BY PLACEMENT_ID".format (self.config.ioid,self.config.start_date, self.config.end_date)
	
		sqldisplaymv = "select substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as Placement#, sum(VIEWS) as Delivered_Impresion, sum(CLICKS) as Clicks, sum(CONVERSIONS) as Conversion from TFR_REP.DAILY_SALES_MV WHERE IO_ID = {} AND TO_CHAR(TO_DATE(DAY_DESC, 'MM/DD/YYYY'),'YYYY-MM-DD') BETWEEN '{}' AND '{}' GROUP BY PLACEMENT_ID, PLACEMENT_DESC ORDER BY PLACEMENT_ID".format(self.config.ioid,self.config.start_date,self.config.end_date)
	
		sqlprerollsummary = "SELECT * FROM (select substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as Placement#, TO_CHAR(SDATE, 'YYYY-MM-DD') as Start_Date, TO_CHAR(EDATE, 'YYYY-MM-DD') as End_Date, initcap(CREATIVE_DESC)  as Placement_Name, COST_TYPE_DESC as Cost_type, UNIT_COST as Unit_Cost, BUDGET as Planned_Cost, BOOKED_QTY as Booked_Imp#Booked_Eng FROM TFR_REP.SUMMARY_MV where (IO_ID = {}) AND (DATA_SOURCE = 'KM') AND (TO_CHAR(SDATE, 'YYYY-MM-DD') >= '{}' AND TO_CHAR(EDATE, 'YYYY-MM-DD') <= '{}') AND CREATIVE_DESC IN(SELECT DISTINCT CREATIVE_DESC FROM TFR_REP.SUMMARY_MV) ORDER BY PLACEMENT_ID) WHERE Placement_Name LIKE '%Pre-Roll%' or Placement_Name LIKE '%Pre–Roll%'".format (self.config.ioid,self.config.start_date,self.config.end_date)
	
		sqlprerollmv = "select substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as Placement#,sum(IMPRESSIONS) as Impression, sum(CPCV_COUNT) as Completions from TFR_REP.KEY_METRIC_MV WHERE IO_ID = {} AND TO_CHAR(TO_DATE(DAY_DESC, 'MM/DD/YYYY'),'YYYY-MM-DD') BETWEEN '{}' AND '{}' GROUP BY PLACEMENT_ID, PLACEMENT_DESC ORDER BY PLACEMENT_ID".format (self.config.ioid,self.config.start_date,self.config.end_date)
	
		#return sqlvdxsummary, sqldisplaysummary, sqlprerollsummary, sqldisplaymv, sqlvdxmv, sqlprerollmv
		
		self.sqlvdxsummary = sqlvdxsummary
		self.sqldisplaysummary = sqldisplaysummary
		self.sqlprerollsummary = sqlprerollsummary
		self.sqldisplaymv = sqldisplaymv
		self.sqlvdxmv = sqlvdxmv
		self.sqlprerollmv = sqlprerollmv
	
	def read_query_summary(self):
		"""
	Connecting with TFR and query
		:param self:
		:return:
		"""
		#sqlvdxsummary, sqldisplaysummary, sqlprerollsummary, sqldisplaymv, sqlvdxmv, sqlprerollmv = self.connect_TFR_summary ()
		
		read_sql__v_d_x = None
		read_sql__display = None
		read_sql_preroll = None
		read_sql__display_mv = None
		read_sql__v_d_x_mv = None
		read_sql_preroll_mv = None
		try:
			self.logger.info (
				'Running Query on 10.29.20.76 in Summary MV for VDX placements for IO - {}'.format (self.config.ioid))
			read_sql__v_d_x = pd.read_sql (self.sqlvdxsummary, self.config.conn)
			#print(read_sql__v_d_x)
			
			self.logger.info (
				'Running Query on 10.29.20.76 in Summary MV for Display placements for IO - {}'.format (self.config.ioid))
			read_sql__display= pd.read_sql (self.sqldisplaysummary, self.config.conn)
			#print (read_sql__display)
			
			self.logger.info (
				'Running Query on 10.29.20.76 in Summary MV for Preroll placements for IO - {}'.format (self.config.ioid))
			read_sql_preroll = pd.read_sql (self.sqlprerollsummary, self.config.conn)
			print (read_sql_preroll)
			exit()
			
			self.logger.info (
				'Running Query on 10.29.20.76 in DailySales MV for Display placements for IO - {}'.format (self.config.ioid))
			read_sql__display_mv = pd.read_sql (self.sqldisplaymv, self.config.conn)
			#print (read_sql__display_mv)
			
			self.logger.info(
				'Running Query on 10.29.20.76 in KeyMetric MV for VDX placements for IO - {}'.format(self.config.ioid))
			read_sql__v_d_x_mv = pd.read_sql (self.sqlvdxmv, self.config.conn)
			#print (read_sql__v_d_x_mv)
			
			self.logger.info(
				'Running Query on 10.29.20.76 in KeyMetric MV for Preroll placements for IO - {}'.format (self.config.ioid))
			read_sql_preroll_mv = pd.read_sql (self.sqlprerollmv, self.config.conn)
			#print(read_sql_preroll_mv)
			#exit()
			
		except AttributeError as e:
			self.logger.error(str(e)+' Connection Not established please rerun for IO - {}'.format(self.config.ioid))
			pass
		except Exception as e:
			self.logger.error(str(e)+' Table or view does not exist')
			pass
		#return read_sql__v_d_x, read_sql__display, read_sql_preroll, read_sql__display_mv, read_sql__v_d_x_mv, read_sql_preroll_mv
		
		self.read_sql__v_d_x = read_sql__v_d_x
		self.read_sql__display = read_sql__display
		self.read_sql_preroll = read_sql_preroll
		self.read_sql__display_mv = read_sql__display_mv
		self.read_sql__v_d_x_mv = read_sql__v_d_x_mv
		self.read_sql_preroll_mv = read_sql_preroll_mv
	
	def access_data_summary(self):
		"""
	merging columns
		:param self:
		:return:
		"""
		
		self.logger.info('Query Stored for further processing for IO - {}'.format(self.config.ioid))
		
		#read_sql__v_d_x, read_sql__display, read_sql_preroll, read_sql__display__m_v, read_sql__v_d_x_mv, read_sql_preroll_mv = \
			#self.read_query_summary ()
		
		self.logger.info('Building Display placements for IO - {}'.format(self.config.ioid))
		displayfirsttable = None
		try:
			if self.read_sql__display.empty:
				self.logger.info("No Display placements for IO - {}".format(self.config.ioid))
				pass
			else:
				self.logger.info ("Display placements found for IO - {}".format (self.config.ioid))
				display_first__summary = self.read_sql__display.merge (self.read_sql__display_mv, on="PLACEMENT#", how="inner",
				                                                suffixes=('_1', '_2'))
				
				displayfirsttable = display_first__summary[["PLACEMENT#", "START_DATE", "END_DATE", "PLACEMENT_NAME",
				                                             "COST_TYPE", "UNIT_COST", "PLANNED_COST",
				                                             "BOOKED_IMP#BOOKED_ENG", "DELIVERED_IMPRESION"]]
		except (KeyError,ValueError,AttributeError,TypeError) as e:
				self.logger.error(str(e))
				pass
			
		self.logger.info ('Buliding VDX placements for IO - {}'.format(self.config.ioid))
		vdx_access_table = None
		try:
			if self.read_sql__v_d_x.empty:
				self.logger.info ("No VDX placements for IO - {}".format (self.config.ioid))
				pass
			else:
				self.logger.info ("VDX placements found for IO - {}".format (self.config.ioid))
				vdx_second_summary = self.read_sql__v_d_x.merge (self.read_sql__v_d_x_mv, on="PLACEMENT#", how="inner", suffixes=('_1', '_2'))
				vdx_second__table = vdx_second_summary[["PLACEMENT#", "START_DATE", "END_DATE", "PLACEMENT_NAME",
				                                       "COST_TYPE", "UNIT_COST", "PLANNED_COST", "BOOKED_IMP#BOOKED_ENG",
				                                       "IMPRESSION", "ENG", "DEEP", "COMPLETIONS"]]
				
				#print (vdx_second__table)
				
				conditionseng = [(vdx_second__table.loc[:, ['COST_TYPE']]=='CPE'),
				                  (vdx_second__table.loc[:, ['COST_TYPE']]=='CPE+')]
				choiceseng = [vdx_second__table.loc[:, ["ENG"]],
				               vdx_second__table.loc[:, ["DEEP"]]]
				
				vdx_second__table['Delivered_Engagements'] = np.select (conditionseng, choiceseng, default=0)
			
				conditionsimp = [(vdx_second__table.loc[:, ['COST_TYPE']]=='CPCV'),
			                      (vdx_second__table.loc[:, ['COST_TYPE']]=='CPM')]
				choiceimp = [vdx_second__table.loc[:, ["COMPLETIONS"]],
			                   vdx_second__table.loc[:, ["IMPRESSION"]]]
			
				vdx_second__table['Delivered_Impressions'] = np.select (conditionsimp, choiceimp, default=0)
				vdx_access_table = vdx_second__table[["PLACEMENT#", "START_DATE", "END_DATE", "PLACEMENT_NAME","COST_TYPE",
				                                      "UNIT_COST", "PLANNED_COST","BOOKED_IMP#BOOKED_ENG",
				                                      "Delivered_Engagements","Delivered_Impressions"]]
		except (KeyError,ValueError,AttributeError,TypeError) as e:
			self.logger.error(str(e))
			pass
			

		self.logger.info('Buliding Preroll Placements for IO - {}'.format(self.config.ioid))
		preroll_access_table = None
		try:
			if self.read_sql_preroll.empty:
				self.logger.info ("No Preroll placements for IO - {}".format (self.config.ioid))
				pass
			else:
				self.logger.info ("Preroll placements found for IO - {}".format (self.config.ioid))
				preroll_third_summary = self.read_sql_preroll.merge (self.read_sql_preroll_mv, on="PLACEMENT#", how="inner",
			                                                    suffixes=('_1', '_2'))
				preroll_third_table = preroll_third_summary[
					["PLACEMENT#", "START_DATE", "END_DATE", "PLACEMENT_NAME", "COST_TYPE",
				    "UNIT_COST", "PLANNED_COST", "BOOKED_IMP#BOOKED_ENG", "IMPRESSION",
				    "COMPLETIONS"]]
				
				conditionscpcv = [(preroll_third_table.loc[:, ['COST_TYPE']]=='CPCV')]
				choicescpcv = [preroll_third_table.loc[:, ['COMPLETIONS']]]
				
				preroll_third_table['Delivered_Impressions'] = np.select (conditionscpcv, choicescpcv,
				                                                          default=preroll_third_table.loc[:,
				                                                                  ["IMPRESSION"]])
				preroll_access_table = preroll_third_table[["PLACEMENT#", "START_DATE", "END_DATE", "PLACEMENT_NAME",
				                                            "COST_TYPE","UNIT_COST", "PLANNED_COST",
				                                            "BOOKED_IMP#BOOKED_ENG","Delivered_Impressions"]]
		
		except (KeyError,ValueError,AttributeError, TypeError) as e:
			self.logger.error(str(e))
			pass
		
		#return displayfirsttable, vdx_access_table, preroll_access_table
		self.displayfirsttable = displayfirsttable
		self.vdx_access_table = vdx_access_table
		self.preroll_access_table = preroll_access_table
	
	def summary_creation(self):
		"""
	Creating Summary Sheet
		:param self:
		:return:
		"""
		#displayfirsttable, vdx_access_table, preroll_access_table = self.access_data_summary ()
		
		#print (self.displayfirsttable)
		#print(self.vdx_access_table)
		#print (self.preroll_access_table)
		self.logger.info('Adding Delivery Metrices on Display Placements for IO - {}'.format(self.config.ioid))
		try:
			if self.read_sql__display.empty:
				self.logger.info('No Display Placement for IO {}'.format(self.config.ioid))
				pass
			else:
				self.logger.info ('Display Placement found for IO {}'.format (self.config.ioid))
				self.displayfirsttable['Delivery%'] = self.displayfirsttable['DELIVERED_IMPRESION']/self.displayfirsttable[
					'BOOKED_IMP#BOOKED_ENG']
				self.displayfirsttable['Spend'] = self.displayfirsttable['DELIVERED_IMPRESION']/1000*self.displayfirsttable[
					'UNIT_COST']
				self.displayfirsttable["PLACEMENT#"] = self.displayfirsttable["PLACEMENT#"].astype (int)
		except (KeyError,TypeError,AttributeError,ValueError) as e:
			self.logger.error(str(e))
			pass
		
		self.logger.info ('Adding Delivery Metrices on VDX Placements for IO - {}'.format(self.config.ioid))
		try:
			if self.read_sql__v_d_x.empty:
				self.logger.info ('No VDX Placement for IO {}'.format (self.config.ioid))
				pass
			else:
				self.logger.info ('VDX Placement found for IO {}'.format (self.config.ioid))
				self.vdx_access_table["Delivered_Engagements"] = self.vdx_access_table["Delivered_Engagements"].astype (int)
				self.vdx_access_table["Delivered_Impressions"] = self.vdx_access_table["Delivered_Impressions"].astype (int)
				self.vdx_access_table["PLACEMENT#"] = self.vdx_access_table["PLACEMENT#"].astype (int)
				
				choices_vdx_eng = self.vdx_access_table["Delivered_Engagements"]/self.vdx_access_table["BOOKED_IMP#BOOKED_ENG"]
				choices_vdx_cpcv = self.vdx_access_table["Delivered_Impressions"]/self.vdx_access_table["BOOKED_IMP#BOOKED_ENG"]
				
				choices_vdx_eng_spend = self.vdx_access_table["Delivered_Engagements"]*self.vdx_access_table["UNIT_COST"]
				choices_vdx_cpcv_spend = self.vdx_access_table["Delivered_Impressions"] * self.vdx_access_table["UNIT_COST"]
				choices_vdx_cpm_spend = self.vdx_access_table["Delivered_Impressions"]/1000*self.vdx_access_table["UNIT_COST"]
				
				mask1 = self.vdx_access_table["COST_TYPE"].isin (['CPE', 'CPE+'])
				mask2 = self.vdx_access_table["COST_TYPE"].isin (['CPM', 'CPCV'])
				mask3 = self.vdx_access_table["COST_TYPE"].isin (['CPCV'])
				mask4 = self.vdx_access_table["COST_TYPE"].isin (['CPM'])
				
				self.vdx_access_table['Delivery%'] = np.select ([mask1, mask2], [choices_vdx_eng, choices_vdx_cpcv],
				                                           default=0.00)
				
				self.vdx_access_table['Spend'] = np.select ([mask1, mask3, mask4], [choices_vdx_eng_spend,
				                                                                    choices_vdx_cpcv_spend,choices_vdx_cpm_spend],
				                                       default=0.00)
				self.vdx_access_table['Delivery%'] = self.vdx_access_table['Delivery%'].replace (np.inf, 0.00)
				self.vdx_access_table['Spend'] = self.vdx_access_table['Spend'].replace (np.inf, 0.00)
		except (KeyError,TypeError,ValueError,AttributeError) as e:
			self.logger.error (str (e))
			pass
			
		
		self.logger.info ('Adding Delivery Metrices on Preroll Placements for IO - {}'.format (self.config.ioid))
		try:
			if self.read_sql_preroll.empty:
				self.logger.info ('No Preroll Placement for IO {}'.format (self.config.ioid))
				pass
			else:
				self.logger.info ('Preroll Placement found for IO {}'.format (self.config.ioid))
				self.preroll_access_table["PLACEMENT#"] = self.preroll_access_table["PLACEMENT#"].astype (int)
				self.preroll_access_table['Delivery%'] = self.preroll_access_table["Delivered_Impressions"]/self.preroll_access_table[
					"BOOKED_IMP#BOOKED_ENG"]
				self.preroll_access_table['Spend'] = self.preroll_access_table["Delivered_Impressions"]/1000*self.preroll_access_table[
					"UNIT_COST"]
				self.preroll_access_table['Delivery%'] = self.preroll_access_table['Delivery%'].replace (np.inf, 0.00)
				self.preroll_access_table['Spend'] = self.preroll_access_table['Spend'].replace (np.inf, 0.00)
		except (KeyError,TypeError,AttributeError) as e:
			self.logger.error(str(e))
			pass
		
		"""try:
			rename_display = self.displayfirsttable.rename (columns={"PLACEMENT#":"Placement#",
			                                                         "START_DATE":"Start Date", "END_DATE":"End Date",
			                                                         "PLACEMENT_NAME":"Placement Name",
			                                                         "COST_TYPE":"Cost Type", "UNIT_COST":"Unit Cost",
			                                                         "PLANNED_COST":"Planned Cost",
			                                                         "BOOKED_IMP#BOOKED_ENG":"Booked",
			                                                         "DELIVERED_IMPRESION":"Delivered_Impressions"},
			                                                inplace=True)
			
			rename_vdx = self.vdx_access_table.rename(columns={"PLACEMENT#":"Placement#", "START_DATE":"Start Date",
			                                               "END_DATE":"End Date","PLACEMENT_NAME":"Placement Name",
			                                               "COST_TYPE":"Cost Type", "UNIT_COST":"Unit Cost",
			                                               "PLANNED_COST":"Planned Cost", "BOOKED_IMP#BOOKED_ENG":"Booked"},
			                                      inplace=True)
			
			rename_preroll = self.preroll_access_table.rename(columns = {"PLACEMENT#":"Placement#", "START_DATE":"Start Date",
			                                                      "END_DATE":"End Date","PLACEMENT_NAME":"Placement Name",
			                                                      "COST_TYPE":"Cost Type", "UNIT_COST":"Unit Cost",
			                                                      "PLANNED_COST":"Planned Cost",
			                                                      "BOOKED_IMP#BOOKED_ENG":"Booked"},inplace=True)
		except (TypeError, KeyError, ValueError, AttributeError):
			pass"""
	
		self.logger.info ("Writing data into Summary Sheet for IO - {}".format (self.config.ioid))
	
	def rename_display(self):
		
		"""
		Display Placements Rename Column
		:return:
		"""
		rename_display = self.displayfirsttable.rename (columns={"PLACEMENT#":"Placement#","START_DATE":"Start Date",
		                                                         "END_DATE":"End Date","PLACEMENT_NAME":"Placement Name",
		                                                         "COST_TYPE":"Cost Type", "UNIT_COST":"Unit Cost",
		                                                         "PLANNED_COST":"Planned Cost",
		                                                         "BOOKED_IMP#BOOKED_ENG":"Booked",
		                                                         "DELIVERED_IMPRESION":"Delivered_Impressions"},inplace=True)
	
	def rename_vdx(self):
		"""
		VDX Placements Rename Columsn
		:return:
		"""
		rename_vdx = self.vdx_access_table.rename(columns={"PLACEMENT#":"Placement#", "START_DATE":"Start Date",
		                                                   "END_DATE":"End Date","PLACEMENT_NAME":"Placement Name",
		                                                   "COST_TYPE":"Cost Type", "UNIT_COST":"Unit Cost",
		                                                   "PLANNED_COST":"Planned Cost", "BOOKED_IMP#BOOKED_ENG":"Booked"},
			                                      inplace=True)
		
	def rename_preroll(self):
		"""
		:return:
		"""
		rename_preroll = self.preroll_access_table.rename (columns={"PLACEMENT#":"Placement#", "START_DATE":"Start Date","END_DATE":"End Date", "PLACEMENT_NAME":"Placement Name",
		                                                            "COST_TYPE":"Cost Type", "UNIT_COST":"Unit Cost",
		                                                            "PLANNED_COST":"Planned Cost","BOOKED_IMP#BOOKED_ENG":"Booked"},
		                                                   inplace=True)
	
	
	def  write_campaign_info(self):
		"""
		Writing Campaign_information to File
		:return:
		"""
		
		data_common_columns = self.config.common_columns_summary ()
		
		campaign_info = data_common_columns[1].to_excel (self.config.writer, sheet_name="Delivery Summary", startcol=1,
		                                                 startrow=1, index=False, header=False)
	
	def write_summary_display(self):
		
		"""
		Writing Display_Data to File
		:return:
		"""
		
		
		display_info = self.displayfirsttable.to_excel(self.config.writer,sheet_name = "Delivery Summary",
	                                                   startcol=2,startrow=8,header=True,index=False)
		
	def write_summary_vdx(self):
		"""
		Writing VDX_Data to File
		
		:return:
		"""
		display_length = 0
		if self.displayfirsttable is not None:
			display_length = len(self.displayfirsttable)
		
		vdx_info = self.vdx_access_table.to_excel(self.config.writer,sheet_name = "Delivery Summary",
	                                              startcol=2,startrow = 8+display_length+5,header = True,index=False)
		
	
	def write_summary_preroll(self):
		"""
		
		Writing Preroll_Data to File
		:return:
		"""
		display_length = 0
		if self.displayfirsttable is not None:
			display_length = len (self.displayfirsttable)
		vdx_length = 0
		if self.vdx_access_table is not None:
			vdx_length = len (self.vdx_access_table)
			
		preroll_info = self.preroll_access_table.to_excel(self.config.writer,sheet_name = "Delivery Summary",
		                                         startcol =2, startrow = 8+display_length+5+vdx_length+5, header = True, index=False)
	
	
	def format_campaign_info(self):
		"""
		formatting campaign info
		:return:
		"""
		workbook = self.config.writer.book
		worksheet = self.config.writer.sheets["Delivery Summary".format (self.config.ioid)]
		worksheet.set_zoom (75)
		worksheet.hide_gridlines (2)
		worksheet.set_row (0, 6)
		worksheet.set_column ("A:A", 2)
		format_campaign_info = workbook.add_format({"bold":True,"bg_color":'#00B0F0', "align":"left"})
		worksheet.insert_image ("O6", "Exponential.png", {"url":"https://www.tribalfusion.com"})
		worksheet.insert_image ("O2", "Client_Logo.png")
		worksheet.conditional_format ("A1:R5", {"type":"blanks", "format":format_campaign_info})
		worksheet.conditional_format ("A1:R5", {"type":"no_blanks", "format":format_campaign_info})
		format_write = workbook.add_format ({"bold":True, "bg_color":"#00B0F0", "align":"left"})
		format_header = workbook.add_format ({"bold":True, "bg_color":"#00B0F0"})
		format_colour = workbook.add_format({"bg_color":"#00B0F0"})
		format_subtotal = workbook.add_format({"bg_color":"#A5A5A5","bold":True,"align":"center"})
		format_subtotal_row = workbook.add_format({"bg_color":"#A5A5A5","bold":True})
		number_fmt = workbook.add_format({"num_format":"#,##0","bg_color":"#A5A5A5","bold":True})
		number_fmt_new = workbook.add_format ({"num_format":"#,##0"})
		percent_fmt = workbook.add_format({"num_format":"0.00%","bg_color":"#A5A5A5","bold":True})
		percent_fmt_new = workbook.add_format ({"num_format":"0.00%"})
		money_fmt_total = workbook.add_format ({"num_format":"$#,###0.00","bg_color":"#A5A5A5","bold":True})
		money_fmt = workbook.add_format ({"num_format":"$#,###0.00"})
		
		try:
			if self.read_sql__display.empty:
				pass
			else:
				worksheet.write_string(7,2,"Standard Banners (Performance/Brand)",format_write)
				
				for row in range(7,8):
					for col in range(3,13):
						cell_reference = xl_rowcol_to_cell (row, col, row_abs=True, col_abs=True)
						worksheet.write ('{}'.format (cell_reference), None, format_colour)
				
				worksheet.set_row(8,29)
				
				"""for row in range(8,9):
					for col in range(2,13):
						cell_reference = xl_rowcol_to_cell (row, col, row_abs=True, col_abs=True)
						worksheet.write ('{}'.format (cell_reference), None, format_header)
						#worksheet.set_column(col,None,format_header)
						worksheet.set_row(row,29,format_header)"""
				
				worksheet.conditional_format(8,2,8,12,{"type":"no_blanks","format":format_header})
				
				worksheet.write_string(9+self.displayfirsttable.shape[0],2,"Subtotal",format_subtotal)
				
				for row in range (9+self.displayfirsttable.shape[0], 9+self.displayfirsttable.shape[0]+1):
					for col in range(3,8):
							cell_reference = xl_rowcol_to_cell(row,col,row_abs=True, col_abs=True)
							worksheet.write('{}'.format(cell_reference),None,format_subtotal_row)
				
				
				for col in range(8,9):
					cell_location = xl_rowcol_to_cell (9+self.displayfirsttable.shape[0], col)
					start_range = xl_rowcol_to_cell (9, col)
					end_range = xl_rowcol_to_cell (8+self.displayfirsttable.shape[0], col)
					formula = '=sum({:s}:{:s})'.format (start_range, end_range)
					worksheet.write_formula (cell_location, formula, money_fmt_total)
					
				for col in range(9,11):
					cell_location = xl_rowcol_to_cell(9+self.displayfirsttable.shape[0],col)
					start_range = xl_rowcol_to_cell(9,col)
					end_range = xl_rowcol_to_cell(8+self.displayfirsttable.shape[0],col)
					formula = '=sum({:s}:{:s})'.format (start_range, end_range)
					worksheet.write_formula(cell_location,formula,number_fmt)
				
				worksheet.write_formula(9+self.displayfirsttable.shape[0],self.displayfirsttable.shape[1],
				                        '=IFERROR(K{}/J{},0)'.format(9+self.displayfirsttable.shape[0]+1,
				                                                     9+self.displayfirsttable.shape[0]+1),percent_fmt)
				
				for col in range(12,13):
					cell_location = xl_rowcol_to_cell (9+self.displayfirsttable.shape[0], col)
					start_range = xl_rowcol_to_cell (9, col)
					end_range = xl_rowcol_to_cell (8+self.displayfirsttable.shape[0], col)
					formula = '=sum({:s}:{:s})'.format (start_range, end_range)
					worksheet.write_formula (cell_location, formula, money_fmt_total)
				
				"""worksheet.conditional_format(9+self.displayfirsttable.shape[0],3,9+self.displayfirsttable.shape[0],
				                             self.displayfirsttable.shape[1]+1,{"type":"blanks","format":format_subtotal_row})
				
				worksheet.conditional_format(9+self.displayfirsttable.shape[0],3,9+self.displayfirsttable.shape[0],
				                             self.displayfirsttable.shape[1]+1,{"type":"no_blanks",
				                                                              "format":format_subtotal_row})"""
				
				worksheet.conditional_format(9,7,8+self.displayfirsttable.shape[0],8,{"type":"no_blanks","format":money_fmt})
				
				worksheet.conditional_format(9,self.displayfirsttable.shape[1]+1,8+self.displayfirsttable.shape[0],
				                             self.displayfirsttable.shape[1]+1,{"type":"no_blanks","format":money_fmt})
				
				worksheet.conditional_format(9,self.displayfirsttable.shape[1],8+self.displayfirsttable.shape[0],
				                             self.displayfirsttable.shape[1],{"type":"no_blanks","format":percent_fmt_new})
				
				worksheet.conditional_format(9,9,8+self.displayfirsttable.shape[0],10,
				                             {"type":"no_blanks", "format":number_fmt_new})
				
		except AttributeError:
			pass
		
		try:
			if self.read_sql__v_d_x.empty:
				pass
			else:
				display_row = 0
				if self.displayfirsttable is not None:
					display_row = self.displayfirsttable.shape[0]
				
				worksheet.write_string (9+display_row+3, 2, "VDX (Display, Mobile and Instream)", format_write)
				
				for row in range (9+display_row+3, 9+display_row+4):
					for col in range(3,14):
						cell_reference = xl_rowcol_to_cell (row, col, row_abs=True, col_abs=True)
						worksheet.write ('{}'.format (cell_reference), None, format_colour)
				
				worksheet.set_row (9+display_row+4, 29)
				worksheet.conditional_format (9+display_row+4, 2, 9+display_row+4, 14, {"type":"no_blanks", "format":format_header})
				
				worksheet.write_string (9+display_row+self.vdx_access_table.shape[0]+5, 2, "Subtotal", format_subtotal)
				
				for row in range (9+display_row+self.vdx_access_table.shape[0]+5, 9+display_row+self.vdx_access_table.shape[0]+6):
					for col in range(3,8):
						cell_reference = xl_rowcol_to_cell(row,col,row_abs=True, col_abs=True)
						worksheet.write('{}'.format(cell_reference),None,format_subtotal_row)
					for col in range(12,13):
						cell_reference = xl_rowcol_to_cell (row, col, row_abs=True, col_abs=True)
						worksheet.write ('{}'.format (cell_reference), None, format_subtotal_row)
						
				for col in range(8,9):
					cell_location = xl_rowcol_to_cell (9+display_row+self.vdx_access_table.shape[0]+5, col)
					start_range = xl_rowcol_to_cell (9+display_row+5, col)
					end_range = xl_rowcol_to_cell (8+display_row+self.vdx_access_table.shape[0]+5, col)
					formula = '=sum({:s}:{:s})'.format (start_range, end_range)
					worksheet.write_formula (cell_location, formula, money_fmt_total)
				
				
				for col in range(9,12):
					cell_location = xl_rowcol_to_cell (9+display_row+self.vdx_access_table.shape[0]+5, col)
					start_range = xl_rowcol_to_cell (9+display_row+5, col)
					end_range = xl_rowcol_to_cell (8+display_row+self.vdx_access_table.shape[0]+5, col)
					formula = '=sum({:s}:{:s})'.format (start_range, end_range)
					worksheet.write_formula (cell_location, formula, number_fmt)
					
					
				#worksheet.write_formula(9+display_row+self.vdx_access_table.shape[0]+5,12,'')
				
				for col in range(13,14):
					cell_location = xl_rowcol_to_cell (9+display_row+self.vdx_access_table.shape[0]+5, col)
					start_range = xl_rowcol_to_cell (9+display_row+5, col)
					end_range = xl_rowcol_to_cell (8+display_row+self.vdx_access_table.shape[0]+5, col)
					formula = '=sum({:s}:{:s})'.format (start_range, end_range)
					worksheet.write_formula (cell_location, formula, money_fmt_total)
				
				worksheet.conditional_format (9+display_row+5, 7, 8+display_row+self.vdx_access_table.shape[0]+5, 8,
				                              {"type":"no_blanks", "format":money_fmt})
				
				worksheet.conditional_format (9+display_row+5, self.vdx_access_table.shape[1]+1,
				                              8+display_row+self.vdx_access_table.shape[0]+5, self.vdx_access_table.shape[1]+1,
				                              {"type":"no_blanks", "format":money_fmt})
				
				worksheet.conditional_format (9+display_row+5, 9,
				                              8+display_row+self.vdx_access_table.shape[0]+5,
				                              11,{"type":"no_blanks", "format":number_fmt_new})
				
				worksheet.conditional_format(9+display_row+5,self.vdx_access_table.shape[1],
				                             8+display_row+self.vdx_access_table.shape[0]+5,self.vdx_access_table.shape[1],
				                             {"type":"no_blanks", "format":percent_fmt_new})
			
		except AttributeError:
			pass
		
		try:
			if self.read_sql_preroll.empty:
				pass
			else:
				display_row =0
				vdx_row =0
				if self.displayfirsttable is not None:
					display_row = self.displayfirsttable.shape[0]
				if self.vdx_access_table is not None:
					vdx_row = self.vdx_access_table.shape[0]
				
				worksheet.write_string (9+display_row+3+vdx_row+5, 2, "Standard Pre Roll", format_write)
				
				for row in range (9+display_row+3+vdx_row+5, 9+display_row+3+vdx_row+6):
					for col in range(3,13):
						cell_reference = xl_rowcol_to_cell (row, col, row_abs=True, col_abs=True)
						worksheet.write ('{}'.format (cell_reference), None, format_colour)
				
				worksheet.set_row (9+display_row+3+vdx_row+6, 29)
				worksheet.conditional_format (9+display_row+3+vdx_row+6, 2, 9+display_row+3+vdx_row+6, 13,
				                              {"type":"no_blanks", "format":format_header})
				
				worksheet.write_string (9+display_row+5+vdx_row+5+self.preroll_access_table.shape[0], 2, "Subtotal", format_subtotal)
				
				for row in range(9+display_row+5+vdx_row+5+self.preroll_access_table.shape[0],9+display_row+5+vdx_row+5+self.preroll_access_table.shape[0]+1):
					for col in range(3,8):
						cell_reference = xl_rowcol_to_cell (row, col, row_abs=True, col_abs=True)
						worksheet.write ('{}'.format (cell_reference), None, format_subtotal_row)
						
				
				for col in range(8,9):
					cell_location = xl_rowcol_to_cell (9+display_row+5+vdx_row+5+self.preroll_access_table.shape[0], col)
					start_range = xl_rowcol_to_cell (9+display_row+5+vdx_row+5, col)
					end_range = xl_rowcol_to_cell (8+display_row+5+vdx_row+5+self.preroll_access_table.shape[0], col)
					formula = '=sum({:s}:{:s})'.format (start_range, end_range)
					worksheet.write_formula (cell_location, formula, money_fmt_total)
				
				
				for col in range(9,11):
					cell_location = xl_rowcol_to_cell (9+display_row+5+vdx_row+5+self.preroll_access_table.shape[0], col)
					start_range = xl_rowcol_to_cell (9+display_row+5+vdx_row+5, col)
					end_range = xl_rowcol_to_cell (8+display_row+5+vdx_row+5+self.preroll_access_table.shape[0], col)
					formula = '=sum({:s}:{:s})'.format (start_range, end_range)
					worksheet.write_formula (cell_location, formula, number_fmt)
					
					
				worksheet.write_formula (9+display_row+5+vdx_row+5+self.preroll_access_table.shape[0],
				                         self.preroll_access_table.shape[1],
				                         '=IFERROR(K{}/J{},0)'.format (9+display_row+5+vdx_row+5+self.preroll_access_table.shape[0]+1,
				                                                       9+display_row+5+vdx_row+5+
				                                                       self.preroll_access_table.shape[0]+1),
				                         percent_fmt)
					
				for col in range(12,13):
					cell_location = xl_rowcol_to_cell (9+display_row+5+vdx_row+5+self.preroll_access_table.shape[0],
					                                   col)
					start_range = xl_rowcol_to_cell (9+display_row+5+vdx_row+5, col)
					end_range = xl_rowcol_to_cell (8+display_row+5+vdx_row+5+self.preroll_access_table.shape[0], col)
					formula = '=sum({:s}:{:s})'.format (start_range, end_range)
					worksheet.write_formula (cell_location, formula, money_fmt_total)
					
				
				worksheet.conditional_format (9+display_row+5+vdx_row+5, 7,
				                              8+display_row+5+vdx_row+5+self.preroll_access_table.shape[0], 8,
				                              {"type":"no_blanks", "format":money_fmt})
				
				
				worksheet.conditional_format (9+display_row+5+vdx_row+5,
				                              self.preroll_access_table.shape[1]+1, 8+display_row+5+vdx_row+5
				                              +self.preroll_access_table.shape[0],
				                              self.preroll_access_table.shape[1]+1,
				                              {"type":"no_blanks", "format":money_fmt})
				
				
				worksheet.conditional_format (9+display_row+5+vdx_row+5,
				                              self.preroll_access_table.shape[1],
				                              8+display_row+5+vdx_row+5+self.preroll_access_table.shape[0],
				                              self.preroll_access_table.shape[1],
				                              {"type":"no_blanks", "format":percent_fmt_new})
				
				worksheet.conditional_format (9+display_row+5+vdx_row+5, 9,
				                              8+display_row+5+vdx_row+5+self.preroll_access_table.shape[0], 10,
				                              {"type":"no_blanks", "format":number_fmt_new})
				
				
					
		except AttributeError:
			pass
			
		aligment_left = workbook.add_format ({"align":"left"})
		aligment_right = workbook.add_format({"align":"right"})
		aligment_center = workbook.add_format ({"align":"center"})
		worksheet.set_column("B:B",30,aligment_left)
		worksheet.set_column("C:C",36,aligment_center)
		worksheet.set_column("D:E",17,aligment_right)
		worksheet.set_column("F:F",47,aligment_right)
		worksheet.set_column("G:R",23,aligment_right)
		try:
			for row in range (5, 10):
				if self.read_sql__display.empty:
					worksheet.set_row (row, None, None, {'hidden':True})
		except(TypeError, KeyError, ValueError, AttributeError):
			pass
			
		try:
			for row in range (5, 15):
				if self.read_sql__display.empty  & self.read_sql__v_d_x.empty:
					worksheet.set_row (row, None, None, {'hidden':True})
		except (TypeError, KeyError, ValueError, AttributeError):
			pass
			
		try:
			for row in range (3+self.displayfirsttable.shape[0]+8, 3+self.displayfirsttable.shape[0]+13):
				if self.read_sql_preroll.empty:
					worksheet.set_row (row, None, None, {'hidden':True})
		except (TypeError, KeyError, ValueError, AttributeError):
				pass
		
	def main(self):
		"""
	This is main function.
		:param self:
		"""
		self.config.common_columns_summary()
		self.connect_TFR_summary()
		self.read_query_summary()
		self.access_data_summary()
		self.summary_creation()
		try:
			if self.read_sql__display.empty:
				self.logger.info ("No Display Placement found to rename columns for IO - {}".format (self.config.ioid))
				pass
			else:
				self.logger.info ("Display Placements found found to rename columns for IO - {}".format (self.config.ioid))
				self.rename_display ()
			
			if self.read_sql__v_d_x.empty:
				self.logger.info ("No VDX Placement found to rename columns for IO - {}".format (self.config.ioid))
				pass
			else:
				self.logger.info ("VDX Placements found to rename columns for IO - {}".format (self.config.ioid))
				self.rename_vdx ()
				
			if self.read_sql_preroll.empty:
				self.logger.info ("No Preroll Placement found to rename columns for IO - {}".format (self.config.ioid))
				pass
			else:
				self.logger.info ("Preroll Placements found to rename columns for IO - {}".format (self.config.ioid))
				self.rename_preroll()
		except AttributeError:
			pass
		
		self.write_campaign_info()
		try:
			if self.read_sql__display.empty:
				self.logger.info("No Display Placement for IO - {}".format(self.config.ioid))
				pass
			else:
				self.logger.info("Display Placements found for IO - {}".format(self.config.ioid))
				self.write_summary_display ()
			
			if self.read_sql__v_d_x.empty:
				self.logger.info("No VDX Placement for IO - {}".format(self.config.ioid))
				pass
			else:
				self.logger.info("VDX Placements found for IO - {}".format(self.config.ioid))
				self.write_summary_vdx()
			
			if self.read_sql_preroll.empty:
				self.logger.info("No Preroll Placement for IO - {}".format(self.config.ioid))
				pass
			else:
				self.logger.info("Preroll Placements found for IO - {}".format(self.config.ioid))
				self.write_summary_preroll()
		except AttributeError:
			pass
		self.format_campaign_info ()
		self.logger.info("Summary Sheet Created for IO - {}".format(self.config.ioid))

if __name__=="__main__":
	#pass
	# enable it when running for individual file
	c=config.Config("test", 608607,'2018-04-16','2018-04-23')
	o=Summary(c)
	o.main()
	c.saveAndCloseWriter()
