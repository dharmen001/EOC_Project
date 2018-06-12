# coding=utf-8
# !/usr/bin/env python
"""
Created by:Dharmendra
Date:2018-03-23
"""
from __future__ import print_function

import datetime
import pandas as pd
import numpy as np
from xlsxwriter.utility import xl_range, xl_rowcol_to_cell
import pandas.io.formats.excel
pandas.io.formats.excel.header_style = None
from functools import reduce

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
		self.logger.info ('Starting to create Summary Sheet for IO - {}'.format (self.config.ioid))
		
		self.logger.info ("Start executing: "+'VDX_Summary.sql'+" at "+str (datetime.datetime.now ().strftime ("%Y-%m-%d %H:%M")))
		read_vdx_summary = open("VDX_Summary.sql")
		sqlvdxsummary = read_vdx_summary.read().format(self.config.ioid,self.config.start_date,self.config.end_date)
		
		self.logger.info ("Start executing: "+'VDX_MV.sql'+" at "+str (datetime.datetime.now ().strftime ("%Y-%m-%d %H:%M")))
		read_vdx_mv = open("VDX_MV.sql")
		sqlvdxmv = read_vdx_mv.read().format(self.config.ioid,self.config.start_date,self.config.end_date)
		#sqlvdxmv  = "select substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as Placement#,sum(IMPRESSIONS) as Impression, sum(ENGAGEMENTS) as Eng, sum(DPE_ENGAGEMENTS) as Deep,sum(CPCV_COUNT) as Completions from TFR_REP.KEY_METRIC_MV WHERE IO_ID = {0} AND TO_CHAR(TO_DATE(DAY_DESC, 'MM/DD/YYYY'),'YYYY-MM-DD') BETWEEN '{1}' AND '{2}' GROUP BY PLACEMENT_ID, PLACEMENT_DESC ORDER BY PLACEMENT_ID".format(self.config.ioid,self.config.start_date,self.config.end_date)
		
		self.logger.info ("Start executing: "+'Display_Summary.sql'+" at "+str (datetime.datetime.now ().strftime ("%Y-%m-%d %H:%M")))
		read_display_summary = open("Display_Summary.sql")
		sqldisplaysummary = read_display_summary.read().format(self.config.ioid,self.config.start_date,self.config.end_date)
		
		self.logger.info ("Start executing: "+'Display_MV.sql'+" at "+str (datetime.datetime.now ().strftime ("%Y-%m-%d %H:%M")))
		read_display_mv = open("Display_MV.sql")
		sqldisplaymv = read_display_mv.read().format(self.config.ioid,self.config.start_date,self.config.end_date)
		
		self.logger.info ("Start executing: "+'Preroll_Summary.sql'+" at "+str (datetime.datetime.now ().strftime ("%Y-%m-%d %H:%M")))
		read_preroll_summary = open("Preroll_Summary.sql")
		sqlprerollsummary = read_preroll_summary.read().format(self.config.ioid,self.config.start_date,self.config.end_date)
		
		self.logger.info ("Start executing: "+'Preroll_MV.sql'+" at "+str (datetime.datetime.now ().strftime ("%Y-%m-%d %H:%M")))
		read_preroll_mv = open("Preroll_MV.sql")
		sqlprerollmv = read_preroll_mv.read().format(self.config.ioid,self.config.start_date,self.config.end_date)
		
		
		
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
		
		self.logger.info (
			'Running Query on 10.29.20.76 in Summary MV for VDX placements for IO - {}'.format (self.config.ioid))
		read_sql__v_d_x = pd.read_sql (self.sqlvdxsummary, self.config.conn)
		
		self.logger.info (
			'Running Query on 10.29.20.76 in Summary MV for Display placements for IO - {}'.format (self.config.ioid))
		read_sql__display= pd.read_sql (self.sqldisplaysummary, self.config.conn)
		
		
		self.logger.info (
			'Running Query on 10.29.20.76 in Summary MV for Preroll placements for IO - {}'.format (self.config.ioid))
		read_sql_preroll = pd.read_sql (self.sqlprerollsummary, self.config.conn)
		
		self.logger.info (
			'Running Query on 10.29.20.76 in DailySales MV for Display placements for IO - {}'.format (self.config.ioid))
		read_sql__display_mv = pd.read_sql (self.sqldisplaymv, self.config.conn)
		
		
		self.logger.info(
			'Running Query on 10.29.20.76 in KeyMetric MV for VDX placements for IO - {}'.format(self.config.ioid))
		read_sql__v_d_x_mv = pd.read_sql (self.sqlvdxmv, self.config.conn)
		
		self.logger.info(
			'Running Query on 10.29.20.76 in KeyMetric MV for Preroll placements for IO - {}'.format (self.config.ioid))
		read_sql_preroll_mv = pd.read_sql (self.sqlprerollmv, self.config.conn)
		
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
		
		self.logger.info('Building Display placements for IO - {}'.format(self.config.ioid))
		displayfirsttable = None
		try:
			if self.read_sql__display.empty:
				self.logger.info("No Display placements for IO - {}".format(self.config.ioid))
				pass
			else:
				self.logger.info ("Display placements found for IO - {}".format (self.config.ioid))
				display_first_exchange = [self.read_sql__display, self.read_sql__display_mv]
				#display_first__summary = self.read_sql__display.merge (self.read_sql__display_mv, on="PLACEMENT#")
				display_table_info = reduce (lambda left, right:pd.merge (left, right, on='PLACEMENT#'), display_first_exchange)
				
				display_table = display_table_info[["IO_ID","PLACEMENT#", "START_DATE", "END_DATE", "PLACEMENT_NAME",
				                                   "COST_TYPE", "UNIT_COST", "PLANNED_COST","BOOKED_IMP#BOOKED_ENG", "DELIVERED_IMPRESION","CLICKS"]]
				
				
				mask_display_imp = display_table["COST_TYPE"].isin(['CPM'])
				mask_display_click = display_table["COST_TYPE"].isin(['CPC'])
				
				choice_display_imp = display_table["DELIVERED_IMPRESION"]
				choice_display_click = display_table["CLICKS"]
				
				display_table["Delivered_Impressions"] = np.select([mask_display_imp,mask_display_click],
				                                                   [choice_display_imp,choice_display_click])
				
				display_merge = [display_table, self.config.cdb_io_exchange]
				
				display_table_info = reduce (lambda left, right:pd.merge (left, right, on='IO_ID'), display_merge)
				
				display_table_info["UNIT_COST"] = display_table_info["UNIT_COST"]*display_table_info["Currency Exchange Rate"]
				
				display_table_info["PLANNED_COST"] = display_table_info["PLANNED_COST"]*display_table_info["Currency Exchange Rate"]
				
				displayfirsttable = display_table_info[["PLACEMENT#", "START_DATE", "END_DATE", "PLACEMENT_NAME",
				                                             "COST_TYPE", "UNIT_COST", "PLANNED_COST",
				                                             "BOOKED_IMP#BOOKED_ENG", "Delivered_Impressions"]]
				
		except (AttributeError,KeyError,TypeError,IOError, ValueError) as e:
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
				vdx_second_summary = self.read_sql__v_d_x.merge (self.read_sql__v_d_x_mv, on="PLACEMENT#")
				vdx_second__table = vdx_second_summary[["IO_ID","PLACEMENT#", "START_DATE", "END_DATE", "PLACEMENT_NAME",
				                                        "COST_TYPE", "UNIT_COST", "PLANNED_COST",
				                                        "BOOKED_IMP#BOOKED_ENG",
				                                        "IMPRESSION", "ENG", "DEEP", "COMPLETIONS"]]
				
				conditionseng = [(vdx_second__table.loc[:, ['COST_TYPE']]=='CPE'),
				                 (vdx_second__table.loc[:, ['COST_TYPE']]=='CPE+')]
				choiceseng = [vdx_second__table.loc[:, ["ENG"]],
				              vdx_second__table.loc[:, ["DEEP"]]]
				
				vdx_second__table['Delivered_Engagements'] = np.select (conditionseng, choiceseng)
				
				conditionsimp = [(vdx_second__table.loc[:, ['COST_TYPE']]=='CPCV'),
				                 (vdx_second__table.loc[:, ['COST_TYPE']]=='CPM')]
				choiceimp = [vdx_second__table.loc[:, ["COMPLETIONS"]],
				             vdx_second__table.loc[:, ["IMPRESSION"]]]
				
				vdx_second__table['Delivered_Impressions'] = np.select (conditionsimp, choiceimp)
				
				vdx_exchange_table = [vdx_second__table,self.config.cdb_io_exchange]
				vdx_table = reduce (lambda left, right:pd.merge (left, right, on='IO_ID'), vdx_exchange_table)
				
				vdx_table['UNIT_COST'] = vdx_table['UNIT_COST']*vdx_table['Currency Exchange Rate']
				
				vdx_table['PLANNED_COST'] = vdx_table['PLANNED_COST']*vdx_table['Currency Exchange Rate']
				
				vdx_access_table = vdx_table[
					["PLACEMENT#", "START_DATE", "END_DATE", "PLACEMENT_NAME", "COST_TYPE",
					 "UNIT_COST", "PLANNED_COST", "BOOKED_IMP#BOOKED_ENG",
					 "Delivered_Engagements", "Delivered_Impressions"]]
				
				
		except (AttributeError,KeyError,TypeError,IOError, ValueError) as e:
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
				preroll_third_summary = self.read_sql_preroll.merge (self.read_sql_preroll_mv, on="PLACEMENT#")
				
				preroll_table = preroll_third_summary[["IO_ID","PLACEMENT#", "START_DATE", "END_DATE", "PLACEMENT_NAME",
				                                                "COST_TYPE","UNIT_COST", "PLANNED_COST", "BOOKED_IMP#BOOKED_ENG",
				                                                "IMPRESSION","COMPLETIONS"]]
				
				preroll_exchange_table = [preroll_table,self.config.cdb_io_exchange]
				
				preroll_final_table = reduce(lambda left, right:pd.merge (left, right, on='IO_ID'), preroll_exchange_table)
				
				preroll_final_table["UNIT_COST"] = preroll_final_table["UNIT_COST"] * preroll_final_table["Currency Exchange Rate"]
				
				preroll_final_table["PLANNED_COST"] = preroll_final_table["PLANNED_COST"]*preroll_final_table["Currency Exchange Rate"]
				
				preroll_third_table = preroll_final_table[
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
		
		except (KeyError,AttributeError,TypeError,IOError, ValueError) as e:
			self.logger.error(str(e))
			pass
		
		self.displayfirsttable = displayfirsttable
		self.vdx_access_table = vdx_access_table
		self.preroll_access_table = preroll_access_table
	
	def summary_creation(self):
		"""
	Creating Summary Sheet
		:param self:
		:return:
		"""
		self.logger.info('Adding Delivery Metrices on Display Placements for IO - {}'.format(self.config.ioid))
		try:
			if self.read_sql__display.empty:
				self.logger.info('No Display Placement for IO {}'.format(self.config.ioid))
				pass
			else:
				self.logger.info ('Display Placement found for IO {}'.format (self.config.ioid))
				
				mask_display_spend_cpm = self.displayfirsttable["COST_TYPE"].isin(['CPM'])
				mask_display_spend_cpc = self.displayfirsttable["COST_TYPE"].isin(['CPC'])
				
				choice_display_spend_cpm = self.displayfirsttable['Delivered_Impressions']/1000*self.displayfirsttable['UNIT_COST']
				choice_display_spend_cpc = self.displayfirsttable['Delivered_Impressions']*self.displayfirsttable['UNIT_COST']
				
				self.displayfirsttable['Delivery%'] = self.displayfirsttable['Delivered_Impressions']/self.displayfirsttable[
					'BOOKED_IMP#BOOKED_ENG']
				
				self.displayfirsttable['Spend'] = np.select([mask_display_spend_cpm,mask_display_spend_cpc],
				                                            [choice_display_spend_cpm,choice_display_spend_cpc])
				
				self.displayfirsttable["PLACEMENT#"] = self.displayfirsttable["PLACEMENT#"].astype (int)
				
		except (KeyError,AttributeError,TypeError,IOError, ValueError) as e:
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
		except (KeyError,AttributeError,TypeError,IOError, ValueError) as e:
			self.logger.error (str (e))
			pass
			
		
		self.logger.info ('Adding Delivery Metrices on Preroll Placements for IO - {}'.format (self.config.ioid))
		try:
			if self.read_sql_preroll.empty:
				self.logger.info ('No Preroll Placement for IO {}'.format (self.config.ioid))
				pass
			else:
				self.logger.info ('Preroll Placement found for IO {}'.format (self.config.ioid))
				mask5 = self.preroll_access_table["COST_TYPE"].isin (['CPCV'])
				mask6 = self.preroll_access_table["COST_TYPE"].isin (['CPM'])
				
				choice_preroll_cpcv = self.preroll_access_table["Delivered_Impressions"]*self.preroll_access_table["UNIT_COST"]
				choice_preroll_cpm = self.preroll_access_table["Delivered_Impressions"]/1000*self.preroll_access_table["UNIT_COST"]
				
				self.preroll_access_table["PLACEMENT#"] = self.preroll_access_table["PLACEMENT#"].astype (int)
				
				self.preroll_access_table['Delivery%'] = self.preroll_access_table["Delivered_Impressions"]/self.preroll_access_table[
					"BOOKED_IMP#BOOKED_ENG"]
				
				self.preroll_access_table['Spend'] = np.select([mask5,mask6],[choice_preroll_cpcv,choice_preroll_cpm])
				self.preroll_access_table['Delivery%'] = self.preroll_access_table['Delivery%'].replace (np.inf, 0.00)
				self.preroll_access_table['Spend'] = self.preroll_access_table['Spend'].replace (np.inf, 0.00)
				
		except (KeyError,AttributeError,TypeError,IOError, ValueError) as e:
			self.logger.error(str(e))
			pass
	
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
		                                                         "Delivered_Impressions":"Delivered_Impressions"},inplace=True)
	
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
		try:
			info_client = self.config.client_info.to_excel(self.config.writer, sheet_name="Delivery Summary", startcol=1, startrow=1, index=True, header=False)
			info_campaign = self.config.campaign_info.to_excel(self.config.writer, sheet_name="Delivery Summary", startcol=1, startrow=2, index=True, header=False)
			info_ac_mgr = self.config.ac_mgr.to_excel(self.config.writer, sheet_name="Delivery Summary", startcol=4, startrow=1, index=True, header=False)
			info_sales_rep = self.config.sales_rep.to_excel(self.config.writer, sheet_name="Delivery Summary", startcol=4, startrow=2, index=True, header=False)
			info_campaign_date = self.config.sdate_edate_final.to_excel(self.config.writer, sheet_name="Delivery Summary", startcol=7, startrow=1, index=True, header=False)
			info_agency = self.config.agency_info.to_excel(self.config.writer,sheet_name="Delivery Summary",startcol=1,startrow=3,index=True,header=False)
			info_currency = self.config.currency_info.to_excel(self.config.writer,sheet_name="Delivery Summary",startcol=7,startrow=3,index=True,header=False)
		except (KeyError, AttributeError, TypeError, IOError, ValueError) as e:
			self.logger.error(str(e))
			pass
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
			display_length = len(self.displayfirsttable)+4
		
		vdx_info = self.vdx_access_table.to_excel(self.config.writer,sheet_name = "Delivery Summary",
	                                              startcol=2,startrow = 8+display_length,header = True,index=False)
		
	
	def write_summary_preroll(self):
		"""
		
		Writing Preroll_Data to File
		:return:
		"""
		display_length = 0
		if self.displayfirsttable is not None:
			display_length = len (self.displayfirsttable)+4
		vdx_length = 0
		if self.vdx_access_table is not None:
			vdx_length = len (self.vdx_access_table)+4
			
		preroll_info = self.preroll_access_table.to_excel(self.config.writer,sheet_name = "Delivery Summary",
		                                         startcol =2, startrow = 8+display_length+vdx_length, header = True, index=False)
	
	
	def format_campaign_info(self):
		"""
		formatting campaign info
		:return:
		"""
		workbook = self.config.writer.book
		worksheet = self.config.writer.sheets["Delivery Summary"]
		worksheet.set_zoom(75)
		worksheet.hide_gridlines (2)
		worksheet.set_row (0, 6)
		worksheet.set_column ("A:A", 2)
		format_campaign_info = workbook.add_format({"bold":True,"bg_color":'#00B0F0', "align":"left"})
		worksheet.insert_image ("O7", "Exponential.png", {"url":"https://www.tribalfusion.com"})
		worksheet.insert_image ("M2", "Client_Logo.png")
		format_write = workbook.add_format ({"bold":True, "bg_color":"#00B0F0", "align":"left"})
		format_header = workbook.add_format ({"bold":True, "bg_color":"#00B0F0",'align': 'center'})
		format_subtotal = workbook.add_format({"bg_color":"#A5A5A5","bold":True,"align":"center"})
		format_subtotal_row = workbook.add_format({"bg_color":"#A5A5A5","bold":True})
		number_fmt = workbook.add_format({"num_format":"#,##0","bg_color":"#A5A5A5","bold":True})
		number_fmt_new = workbook.add_format ({"num_format":'#,##0'})
		percent_fmt = workbook.add_format({"num_format":"0.00%","bg_color":"#A5A5A5","bold":True})
		percent_fmt_new = workbook.add_format ({"num_format":"0.00%"})
		money_fmt_total = workbook.add_format ({"num_format":"$#,###0.00","bg_color":"#A5A5A5","bold":True})
		money_fmt = workbook.add_format ({"num_format":"$#,###0.00"})
		worksheet.write_string(2,8,self.config.status)
		worksheet.write_string(2,7,"Campaign Status")
		#worksheet.write_string (3, 8, "Agency Name")
		#worksheet.write_string (3, 7, "Currency")
		start_row = 7
		start_col = 2
		end_row =2
		
		try:
			if self.read_sql__display.empty:
				pass
			else:
				worksheet.write_string(start_row,start_col,"Standard Banners (Performance/Brand)",format_write)
				worksheet.set_row(start_row+1,29)
				
				worksheet.conditional_format (start_row, start_col, start_row, self.displayfirsttable.shape[1]+1, {"type":"no_blanks", "format":format_campaign_info})
				
				worksheet.conditional_format (start_row, start_col, start_row, self.displayfirsttable.shape[1]+1, {"type":"blanks", "format":format_campaign_info})
				
				worksheet.conditional_format (start_row+1, start_col, start_row+1, self.displayfirsttable.shape[1]+1, {"type":"no_blanks","format":format_header})
				
				worksheet.conditional_format (start_row+1, start_col, start_row+1, self.displayfirsttable.shape[1]+1, {"type":"blanks", "format":format_header})
				
				worksheet.write_string(start_row+self.displayfirsttable.shape[0]+end_row,start_col,"Subtotal",format_subtotal)

				
				for col in range(2,7):
					startrowformat = start_row+self.displayfirsttable.shape[0]+end_row
					worksheet.conditional_format(startrowformat,col,startrowformat,col,{"type":"no_blanks","format":format_subtotal_row})
					worksheet.conditional_format (startrowformat, col, startrowformat, col,
					                              {"type":"blanks", "format":format_subtotal_row})
				
				for col in range(7,8):
					startrowmoney = start_row+end_row
					endrowmoney = start_row+self.displayfirsttable.shape[0]+1
					worksheet.conditional_format (startrowmoney, col, endrowmoney, col,
					                              {"type":"no_blanks", "format":money_fmt})
					startrowformat = start_row+self.displayfirsttable.shape[0]+end_row
					worksheet.conditional_format(startrowformat,col,startrowformat,col,{"type":"no_blanks","format":format_subtotal_row})
					worksheet.conditional_format (startrowformat, col, startrowformat, col,
					                              {"type":"blanks", "format":format_subtotal_row})
					
					
				for col in range(8,9):
					cell_location = xl_rowcol_to_cell (start_row+self.displayfirsttable.shape[0]+end_row, col)
					start_range = xl_rowcol_to_cell (start_row+end_row, col)
					end_range = xl_rowcol_to_cell (start_row+self.displayfirsttable.shape[0]+1, col)
					formula = '=sum({:s}:{:s})'.format (start_range, end_range)
					worksheet.write_formula (cell_location, formula, money_fmt_total)
					startrowmoney = start_row+end_row
					endrowmoney = start_row+self.displayfirsttable.shape[0]+1
					worksheet.conditional_format(startrowmoney,col,endrowmoney,col,{"type":"no_blanks","format":money_fmt})

				for col in range(9,11):
					cell_location = xl_rowcol_to_cell(start_row+end_row+self.displayfirsttable.shape[0],col)
					start_range = xl_rowcol_to_cell(start_row+end_row,col)
					end_range = xl_rowcol_to_cell(start_row+self.displayfirsttable.shape[0]+1,col)
					formula = '=sum({:s}:{:s})'.format (start_range, end_range)
					worksheet.write_formula(cell_location,formula,number_fmt)
					startrownumber = start_row+end_row
					endrownumber = start_row+self.displayfirsttable.shape[0]+1
					worksheet.conditional_format(startrownumber,col,endrownumber,col,{"type":"no_blanks","format":number_fmt_new})
					

				worksheet.write_formula(start_row+end_row+self.displayfirsttable.shape[0],self.displayfirsttable.shape[1],
				                        '=IFERROR(K{}/J{},0)'.format(start_row+end_row+self.displayfirsttable.shape[0]+1,
				                                                     start_row+end_row+self.displayfirsttable.shape[0]+1),percent_fmt)
				
				worksheet.conditional_format(start_row+end_row,self.displayfirsttable.shape[1],
				                             start_row+self.displayfirsttable.shape[0]+1,self.displayfirsttable.shape[1],
				                             {"type":"no_blanks","format":percent_fmt_new})
							
				for col in range(12,13):
					cell_location = xl_rowcol_to_cell (start_row+end_row+self.displayfirsttable.shape[0], col)
					start_range = xl_rowcol_to_cell (start_row+end_row, col)
					end_range = xl_rowcol_to_cell (start_row+self.displayfirsttable.shape[0]+1, col)
					formula = '=sum({:s}:{:s})'.format (start_range, end_range)
					worksheet.write_formula (cell_location, formula, money_fmt_total)
					startrowmoney = start_row+end_row
					endrowmoney = start_row+self.displayfirsttable.shape[0]+1
					worksheet.conditional_format(startrowmoney,col,endrowmoney,col,{"type":"no_blanks","format":money_fmt})
		except (AttributeError,KeyError,TypeError,IOError, ValueError) as e:
			self.logger.error(str(e))
			pass

		try:
			if self.read_sql__v_d_x.empty:
				pass
			else:
				display_row = 0
				if self.displayfirsttable is not None:
					display_row = self.displayfirsttable.shape[0]+4

				worksheet.write_string (start_row+display_row, start_col, "VDX (Display, Mobile and Instream)", format_write)
				
				worksheet.conditional_format(start_row+display_row, start_col, start_row+display_row,
				                             self.vdx_access_table.shape[1]+1,
				                             {"type":"no_blanks", "format":format_campaign_info})
				
				worksheet.conditional_format (start_row+display_row, start_col, start_row+display_row,
				                              self.vdx_access_table.shape[1]+1,
				                              {"type":"blanks", "format":format_campaign_info})
				
				worksheet.conditional_format (start_row+display_row+1, start_col, start_row+display_row+1,
				                              self.vdx_access_table.shape[1]+1,
				                              {"type":"blanks", "format":format_header})
				
				worksheet.conditional_format (start_row+display_row+1, start_col, start_row+display_row+1,
				                              self.vdx_access_table.shape[1]+1,
				                              {"type":"no_blanks", "format":format_header})
				
				worksheet.set_row (start_row+display_row+1, 29)
				worksheet.write_string (start_row+end_row+display_row+self.vdx_access_table.shape[0], start_col, "Subtotal", format_subtotal)
				
				for col in range(2,7):
					startrowformat = start_row+end_row+display_row+self.vdx_access_table.shape[0]
					worksheet.conditional_format(startrowformat,col,startrowformat,col,{"type":"no_blanks","format":format_subtotal_row})
					worksheet.conditional_format (startrowformat, col, startrowformat, col,
					                              {"type":"blanks", "format":format_subtotal_row})
				for col in range(7,8):
					startrowmoney = start_row+display_row+end_row
					endrowmoney = start_row+display_row+self.vdx_access_table.shape[0]+1
					worksheet.conditional_format (startrowmoney, col, endrowmoney, col,
					                      {"type":"no_blanks", "format":money_fmt})
					startrowformat = start_row+end_row+display_row+self.vdx_access_table.shape[0]
					worksheet.conditional_format(startrowformat,col,startrowformat,col,{"type":"no_blanks","format":format_subtotal_row})
					worksheet.conditional_format (startrowformat, col, startrowformat, col,
					                      {"type":"blanks", "format":format_subtotal_row})
					
				for col in range(8,9):
					cell_location = xl_rowcol_to_cell (start_row+end_row+display_row+self.vdx_access_table.shape[0], col)
					start_range = xl_rowcol_to_cell (start_row+display_row+end_row, col)
					end_range = xl_rowcol_to_cell (start_row+display_row+self.vdx_access_table.shape[0]+1, col)
					formula = '=sum({:s}:{:s})'.format (start_range, end_range)
					worksheet.write_formula (cell_location, formula, money_fmt_total)
					startrowmoney = start_row+display_row+end_row
					endrowmoney = start_row+display_row+self.vdx_access_table.shape[0]+1
					worksheet.conditional_format(startrowmoney,col,endrowmoney,col,{"type":"no_blanks","format":money_fmt})
					
				for col in range(9,12):
					cell_location = xl_rowcol_to_cell(start_row+end_row+display_row+self.vdx_access_table.shape[0],col)
					start_range = xl_rowcol_to_cell(start_row+display_row+end_row,col)
					end_range = xl_rowcol_to_cell(start_row+display_row+self.vdx_access_table.shape[0]+1,col)
					formula = '=sum({:s}:{:s})'.format (start_range, end_range)
					worksheet.write_formula(cell_location,formula,number_fmt)
					startrownumber = start_row+display_row+end_row
					endrownumber = start_row+display_row+self.vdx_access_table.shape[0]+1
					worksheet.conditional_format(startrownumber,col,endrownumber,col,{"type":"no_blanks","format":number_fmt_new})
					
				for col in range(12,13):
					startrowformat = start_row+end_row+display_row+self.vdx_access_table.shape[0]
					worksheet.conditional_format(startrowformat,col,startrowformat,col,{"type":"no_blanks","format":format_subtotal_row})
					worksheet.conditional_format (startrowformat, col, startrowformat, col,
					                              {"type":"blanks", "format":format_subtotal_row})
					startrownumber = start_row+display_row+end_row
					endrownumber = start_row+display_row+self.vdx_access_table.shape[0]+1
					worksheet.conditional_format (startrownumber, col, endrownumber, col,
					                              {"type":"no_blanks", "format":percent_fmt_new})
					
				for col in range(13,14):
					cell_location = xl_rowcol_to_cell (start_row+end_row+display_row+self.vdx_access_table.shape[0], col)
					start_range = xl_rowcol_to_cell (start_row+display_row+end_row, col)
					end_range = xl_rowcol_to_cell (start_row+display_row+self.vdx_access_table.shape[0]+1, col)
					formula = '=sum({:s}:{:s})'.format (start_range, end_range)
					worksheet.write_formula (cell_location, formula, money_fmt_total)
					startrowmoney = start_row+display_row+end_row
					endrowmoney = start_row+display_row+self.vdx_access_table.shape[0]+1
					worksheet.conditional_format(startrowmoney,col,endrowmoney,col,{"type":"no_blanks","format":money_fmt})
		
		except (AttributeError,KeyError,TypeError,IOError, ValueError) as e:
			self.logger.error(str(e))
			pass

		try:
			if self.read_sql_preroll.empty:
				pass
			else:
				display_row =0
				vdx_row =0
				if self.displayfirsttable is not None:
					display_row = self.displayfirsttable.shape[0]+4
				if self.vdx_access_table is not None:
					vdx_row = self.vdx_access_table.shape[0]+4

				worksheet.write_string (start_row+display_row+vdx_row, start_col, "Standard Pre Roll", format_write)
				
				worksheet.conditional_format (start_row+display_row+vdx_row, start_col, start_row+display_row+vdx_row,
				                              self.preroll_access_table.shape[1]+1,
				                              {"type":"no_blanks", "format":format_campaign_info})
				
				worksheet.conditional_format (start_row+display_row+vdx_row, start_col, start_row+display_row+vdx_row,
				                              self.preroll_access_table.shape[1]+1,
				                              {"type":"blanks", "format":format_campaign_info})
				
				worksheet.conditional_format (start_row+display_row+vdx_row+1, start_col, start_row+display_row+vdx_row+1,
				                              self.preroll_access_table.shape[1]+1,
				                              {"type":"blanks", "format":format_header})
				
				worksheet.conditional_format (start_row+display_row+vdx_row+1, start_col, start_row+display_row+vdx_row+1,
				                              self.preroll_access_table.shape[1]+1,
				                              {"type":"no_blanks", "format":format_header})
				
				worksheet.set_row (start_row+display_row+vdx_row+1, 29)
				worksheet.write_string (start_row+display_row+vdx_row+self.preroll_access_table.shape[0]+end_row, start_col, "Subtotal",
				                        format_subtotal)
				
				for col in range (2, 7):
					startrowformat = start_row+display_row+vdx_row+end_row+self.preroll_access_table.shape[0]
					worksheet.conditional_format (startrowformat, col, startrowformat, col,
					                              {"type":"no_blanks", "format":format_subtotal_row})
					worksheet.conditional_format (startrowformat, col, startrowformat, col,
					                              {"type":"blanks", "format":format_subtotal_row})
				
				for col in range (7, 8):
					startrowmoney = start_row+display_row+vdx_row+end_row
					endrowmoney = start_row+display_row+vdx_row+self.preroll_access_table.shape[0]+1
					worksheet.conditional_format (startrowmoney, col, endrowmoney, col,
					                              {"type":"no_blanks", "format":money_fmt})
					
					startrowformat = start_row+display_row+vdx_row+end_row+self.preroll_access_table.shape[0]
					worksheet.conditional_format (startrowformat, col, startrowformat, col,
					                              {"type":"no_blanks", "format":format_subtotal_row})
					worksheet.conditional_format (startrowformat, col, startrowformat, col,
					                              {"type":"blanks", "format":format_subtotal_row})
				
				for col in range (8, 9):
					cell_location = xl_rowcol_to_cell (start_row+display_row+vdx_row+end_row+self.preroll_access_table.shape[0], col)
					start_range = xl_rowcol_to_cell (start_row+display_row+vdx_row+end_row, col)
					end_range = xl_rowcol_to_cell (start_row+display_row+vdx_row+self.preroll_access_table.shape[0]+1, col)
					formula = '=sum({:s}:{:s})'.format (start_range, end_range)
					worksheet.write_formula (cell_location, formula, money_fmt_total)
					startrowmoney = start_row+display_row+vdx_row+end_row
					endrowmoney = start_row+display_row+vdx_row+self.preroll_access_table.shape[0]+1
					worksheet.conditional_format (startrowmoney, col, endrowmoney, col,
					                              {"type":"no_blanks", "format":money_fmt})
				
				for col in range (9, 11):
					cell_location = xl_rowcol_to_cell (start_row+display_row+vdx_row+end_row+self.preroll_access_table.shape[0], col)
					start_range = xl_rowcol_to_cell (start_row+display_row+vdx_row+end_row, col)
					end_range = xl_rowcol_to_cell (start_row+display_row+vdx_row+self.preroll_access_table.shape[0]+1, col)
					formula = '=sum({:s}:{:s})'.format (start_range, end_range)
					worksheet.write_formula (cell_location, formula, number_fmt)
					startrownumber = start_row+display_row+vdx_row+end_row
					endrownumber = start_row+display_row+vdx_row+self.preroll_access_table.shape[0]+1
					worksheet.conditional_format (startrownumber, col, endrownumber, col,
					                              {"type":"no_blanks", "format":number_fmt_new})
				
				worksheet.write_formula (start_row+display_row+vdx_row+end_row+self.preroll_access_table.shape[0],
				                         self.preroll_access_table.shape[1],
				                         '=IFERROR(K{}/J{},0)'.format (
					                         start_row+display_row+vdx_row+end_row+self.preroll_access_table.shape[0]+1,
					                         start_row+display_row+vdx_row+end_row+self.preroll_access_table.shape[0]+1), percent_fmt)
				
				worksheet.conditional_format (start_row+display_row+vdx_row+end_row, self.preroll_access_table.shape[1],
				                              start_row+display_row+vdx_row+self.preroll_access_table.shape[0]+1,
				                              self.preroll_access_table.shape[1],
				                              {"type":"no_blanks", "format":percent_fmt_new})
				
				for col in range (12, 13):
					cell_location = xl_rowcol_to_cell (start_row+display_row+vdx_row+end_row+self.preroll_access_table.shape[0], col)
					start_range = xl_rowcol_to_cell (start_row+display_row+vdx_row+end_row, col)
					end_range = xl_rowcol_to_cell (start_row+display_row+vdx_row+self.preroll_access_table.shape[0]+1, col)
					formula = '=sum({:s}:{:s})'.format (start_range, end_range)
					worksheet.write_formula (cell_location, formula, money_fmt_total)
					startrowmoney = start_row+display_row+vdx_row+end_row
					endrowmoney = start_row+display_row+vdx_row+self.preroll_access_table.shape[0]+1
					worksheet.conditional_format (startrowmoney, col, endrowmoney, col,
					                              {"type":"no_blanks", "format":money_fmt})
			
			"""worksheet_new = self.config.writernew.get_sheet_names("Delivery Summary")
			for col in range (0, 13):
				#worksheet_new = self.config.writernew.sheets["Delivery Summary"]
				cell = worksheet_new.cell(row=1, column=col)
				cell.style.alignment.horizontal = cell.style.alignment.HORIZONTAL_left"""
				
		except (AttributeError,KeyError,TypeError,IOError, ValueError) as e:
			self.logger.error(str(e))
			pass
	
		aligment_left = workbook.add_format ({"align":"left"})
		aligment_right = workbook.add_format({"align":"right"})
		aligment_center = workbook.add_format ({"align":"center"})
		worksheet.set_column ("B:B", 15, aligment_left)
		worksheet.set_column ("C:C", 14, aligment_center)
		worksheet.set_column ("D:D", 16, aligment_center)
		worksheet.set_column ("E:E", 21, aligment_center)
		worksheet.set_column ("F:F", 30, aligment_left)
		worksheet.set_column ("G:G", 9, aligment_center)
		worksheet.set_column ("H:H", 21, aligment_right)
		worksheet.set_column ("I:I", 17, aligment_right)
		worksheet.set_column ("J:R", 17, aligment_right)
		worksheet.conditional_format ("A1:R5", {"type":"blanks", "format":format_campaign_info})
		worksheet.conditional_format ("A1:R5", {"type":"no_blanks", "format":format_campaign_info})
		
		
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
		except (AttributeError,KeyError,TypeError,IOError, ValueError) as e:
			self.logger.error(str(e))
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
		except (AttributeError,KeyError,TypeError,IOError, ValueError) as e:
			self.logger.error(str(e))
			pass
		self.format_campaign_info ()
		self.logger.info("Summary Sheet Created for IO - {}".format(self.config.ioid))

if __name__=="__main__":
	pass
	# enable it when running for individual file
	#c=config.Config('2018-03-19', 582127,'2018-05-27')
	#o=Summary(c)
	#o.main()
	#c.saveAndCloseWriter()
