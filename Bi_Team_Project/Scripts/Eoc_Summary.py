# coding=utf-8
"""
Created by:Dharmendra
Date:2018-03-23
"""
from __future__ import print_function
import pandas as pd
import numpy as np
from xlsxwriter.utility import xl_rowcol_to_cell
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
		
		
		
		sqlvdxsummary = "select * from (select substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', "\
		                "1)-1) as "'Placement#'", SDATE as "'Start_Date'", EDATE as "'End_Date'", "\
		                "initcap(CREATIVE_DESC)  as "'Placement_Name'",COST_TYPE_DESC as "'Cost_type'",UNIT_COST as "\
		                ""'Unit_Cost'",BUDGET as "'Planned_Cost'",BOOKED_QTY as "'Booked_Imp#Booked_Eng'" from "\
		                "TFR_REP.SUMMARY_MV where (IO_ID = {}) AND (DATA_SOURCE = 'KM') AND CREATIVE_DESC IN(SELECT "\
		                "DISTINCT CREATIVE_DESC FROM TFR_REP.SUMMARY_MV) ORDER BY PLACEMENT_ID) WHERE Placement_Name "\
		                "Not IN ('Pre-Roll - Desktop','Pre-Roll - Desktop + Mobile','Pre-Roll – Desktop + Mobile',"\
		                "'Pre-Roll - In-Stream/Mobile Blend','Pre-Roll - Mobile','Pre-Roll -Desktop','Pre-Roll - "\
		                "In-Stream')".format (
			self.config.ioid)
	
		sqlvdxmv = "select substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as "'Placement#'", "\
	             "sum(IMPRESSIONS) as "'Impression'", sum(ENGAGEMENTS) as "'Eng'", sum(DPE_ENGAGEMENTS) as "\
	             ""'Deep'", sum(CPCV_COUNT) as "'Completions'" from TFR_REP.KEY_METRIC_MV WHERE IO_ID = {} GROUP "\
	             "BY PLACEMENT_ID, PLACEMENT_DESC ORDER BY PLACEMENT_ID".format (
		self.config.ioid)
	
		sqldisplaysummary = "select substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as "'Placement#'", "\
	                      "SDATE as "'Start_Date'", EDATE as "'End_Date'", CREATIVE_DESC  as "'Placement_Name'", "\
	                      "COST_TYPE_DESC as "'Cost_type'",UNIT_COST as "'Unit_Cost'",BUDGET as "'Planned_Cost'", "\
	                      "BOOKED_QTY as "'Booked_Imp#Booked_Eng'" FROM TFR_REP.SUMMARY_MV where IO_ID = {} AND "\
	                      "DATA_SOURCE = 'SalesFile' ORDER BY PLACEMENT_ID".format (
		self.config.ioid)
	
		sqldisplaymv = "select substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as "'Placement#'", "\
	                 "sum(VIEWS) as "'Delivered_Impresion'", sum(CLICKS) as "'Clicks'", sum(CONVERSIONS) as "\
	                 ""'Conversion'" from TFR_REP.DAILY_SALES_MV WHERE IO_ID = {} GROUP BY PLACEMENT_ID, "\
	                 "PLACEMENT_DESC ORDER BY PLACEMENT_ID".format (
			self.config.ioid)
	
		sqlprerollsummary = "select * from (select substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', "\
	                      "1)-1) as "'Placement#'", SDATE as "'Start_Date'", EDATE as "'End_Date'", "\
	                      "initcap(CREATIVE_DESC)  as "'Placement_Name'",COST_TYPE_DESC as "'Cost_type'","\
	                      "UNIT_COST as "'Unit_Cost'",BUDGET as "'Planned_Cost'",BOOKED_QTY as "\
	                      ""'Booked_Imp#Booked_Eng'" from TFR_REP.SUMMARY_MV where (IO_ID = {}) AND (DATA_SOURCE = "\
	                      "'KM') AND CREATIVE_DESC IN(SELECT DISTINCT CREATIVE_DESC FROM TFR_REP.SUMMARY_MV) ORDER "\
	                      "BY PLACEMENT_ID) WHERE Placement_Name IN ('Pre-Roll - Desktop','Pre-Roll - Desktop + "\
	                      "Mobile','Pre-Roll – Desktop + Mobile','Pre-Roll - In-Stream/Mobile Blend','Pre-Roll - "\
	                      "Mobile','Pre-Roll -Desktop','Pre-Roll - In-Stream')".format (
		self.config.ioid)
	
		sqlprerollmv = "select substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as "'Placement#'", "\
	                 "sum(IMPRESSIONS) as "'Impression'", sum(CPCV_COUNT) as "'Completions'" from "\
	                 "TFR_REP.KEY_METRIC_MV WHERE IO_ID = {} GROUP BY PLACEMENT_ID, PLACEMENT_DESC ORDER BY "\
	                 "PLACEMENT_ID".format (self.config.ioid)
	
		return sqlvdxsummary, sqldisplaysummary, sqlprerollsummary, sqldisplaymv, sqlvdxmv, sqlprerollmv


	def read_query_summary(self):
		"""
	Connecting with TFR and query
		:param self:
		:return:
		"""
		sqlvdxsummary, sqldisplaysummary, sqlprerollsummary, sqldisplaymv, sqlvdxmv, sqlprerollmv = self.connect_TFR_summary ()
		
		read_sql__v_d_x = None
		read_sql__display = None
		read_sql_preroll = None
		read_sql__display_mv = None
		read_sql__v_d_x_mv = None
		read_sql_preroll_mv = None
		try:
			self.logger.info (
				'Running Query on 10.29.20.76 in Summary MV for VDX placements for IO - {}'.format (self.config.ioid))
			read_sql__v_d_x = pd.read_sql (sqlvdxsummary, self.config.conn)
			
			self.logger.info (
				'Running Query on 10.29.20.76 in Summary MV for Display placements for IO - {}'.format (self.config.ioid))
			read_sql__display= pd.read_sql (sqldisplaysummary, self.config.conn)
			
			self.logger.info (
				'Running Query on 10.29.20.76 in Summary MV for Preroll placements for IO - {}'.format (self.config.ioid))
			read_sql_preroll = pd.read_sql (sqlprerollsummary, self.config.conn)
			
			self.logger.info (
				'Running Query on 10.29.20.76 in DailySales MV for Display placements for IO - {}'.format (self.config.ioid))
			read_sql__display_mv = pd.read_sql (sqldisplaymv, self.config.conn)
			
			self.logger.info(
				'Running Query on 10.29.20.76 in KeyMetric MV for VDX placements for IO - {}'.format(self.config.ioid))
			read_sql__v_d_x_mv = pd.read_sql (sqlvdxmv, self.config.conn)
			
			self.logger.info(
				'Running Query on 10.29.20.76 in KeyMetric MV for Preroll placements for IO - {}'.format (self.config.ioid))
			read_sql_preroll_mv = pd.read_sql (sqlprerollmv, self.config.conn)
		
		except AttributeError as e:
			self.logger.error(str(e)+' Connection Not established please rerun for IO - {}'.format(self.config.ioid))
		except Exception as e:
			self.logger.error(str(e)+' Table or view does not exist')
		return read_sql__v_d_x, read_sql__display, read_sql_preroll, read_sql__display_mv, read_sql__v_d_x_mv, read_sql_preroll_mv
	
	
	def access_data_summary(self):
		"""
	merging columns
		:param self:
		:return:
		"""
		
		self.logger.info('Query Stored for further processing for IO - {}'.format(self.config.ioid))
		
		read_sql__v_d_x, read_sql__display, read_sql_preroll, read_sql__display__m_v, read_sql__v_d_x_mv, read_sql_preroll_mv = \
			self.read_query_summary ()
		
		self.logger.info('Display placements for IO - {}'.format(self.config.ioid))
		displayfirsttable = None
		try:
			display_first__summary = read_sql__display.merge (read_sql__display__m_v, on="PLACEMENT#", how="inner",
			                                                suffixes=('_1', '_2'))
			
			displayfirsttable = display_first__summary[["PLACEMENT#", "START_DATE", "END_DATE", "PLACEMENT_NAME",
			                                             "COST_TYPE", "UNIT_COST", "PLANNED_COST",
			                                             "BOOKED_IMP#BOOKED_ENG", "DELIVERED_IMPRESION"]]
		except (KeyError,ValueError,AttributeError) as e:
			self.logger.error(str(e) +' No live Display placements for IO - {}'.format(self.config.ioid))
			
		self.logger.info ('VDX placements for IO - {}'.format(self.config.ioid))
		
		vdx_access_table = None
		try:
			vdx_second_summary = read_sql__v_d_x.merge (read_sql__v_d_x_mv, on="PLACEMENT#", how="inner", suffixes=('_1', '_2'))
			vdx_second__table = vdx_second_summary[["PLACEMENT#", "START_DATE", "END_DATE", "PLACEMENT_NAME",
			                                       "COST_TYPE", "UNIT_COST", "PLANNED_COST", "BOOKED_IMP#BOOKED_ENG",
			                                       "IMPRESSION", "ENG", "DEEP", "COMPLETIONS"]]
			
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
		except (KeyError,ValueError,AttributeError) as e:
			self.logger.error(str(e)+' No VDX Placements for IO - {}'.format(self.config.ioid))
			
		
		self.logger.info('Preroll Placements for IO - {}'.format(self.config.ioid))
		
		preroll_access_table = None
		try:
			preroll_third_summary = read_sql_preroll.merge (read_sql_preroll_mv, on="PLACEMENT#", how="inner",
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
		
		except (KeyError,ValueError,AttributeError) as e:
			self.logger.error(str(e)+' No preroll placements for IO - {}'.format(self.config.ioid))
		
		return displayfirsttable, vdx_access_table, preroll_access_table
	
	
	def summary_creation(self):
		"""
	Creating Summary Sheet
		:param self:
		:return:
		"""
		displayfirsttable, vdx_access_table, preroll_access_table = self.access_data_summary ()
		
		self.logger.info('Adding Delivery Metrices on Display Placements for IO - {}'.format(self.config.ioid))
		try:
			displayfirsttable['Delivery%'] = displayfirsttable['DELIVERED_IMPRESION']/displayfirsttable[
				'BOOKED_IMP#BOOKED_ENG']
			displayfirsttable['Spend'] = displayfirsttable['DELIVERED_IMPRESION']/1000*displayfirsttable[
				'UNIT_COST']
			displayfirsttable["PLACEMENT#"] = displayfirsttable["PLACEMENT#"].astype (int)
		except (KeyError,TypeError) as e:
			self.logger.error(str(e)+' No display placements for IO - {}'.format(self.config.ioid))
		
		self.logger.info ('Adding Delivery Metrices on VDX Placements for IO - {}'.format(self.config.ioid))
		try:
			vdx_access_table["Delivered_Engagements"] = vdx_access_table["Delivered_Engagements"].astype (int)
			vdx_access_table["Delivered_Impressions"] = vdx_access_table["Delivered_Impressions"].astype (int)
			vdx_access_table["PLACEMENT#"] = vdx_access_table["PLACEMENT#"].astype (int)
			choices_vdx_eng = vdx_access_table["Delivered_Engagements"]/vdx_access_table["BOOKED_IMP#BOOKED_ENG"]
			choices_vdx_cpcv = vdx_access_table["Delivered_Impressions"]/vdx_access_table["BOOKED_IMP#BOOKED_ENG"]
			choices_vdx_eng_spend = vdx_access_table["Delivered_Engagements"]*vdx_access_table["UNIT_COST"]
			choices_vdx_cpcv_spend = vdx_access_table["Delivered_Impressions"]/1000*vdx_access_table["UNIT_COST"]
			
			mask1 = vdx_access_table["COST_TYPE"].isin (['CPE', 'CPE+'])
			mast2 = vdx_access_table["COST_TYPE"].isin (['CPM', 'CPCV'])
			
			vdx_access_table['Delivery%'] = np.select ([mask1, mast2], [choices_vdx_eng, choices_vdx_cpcv],
			                                           default=0.00)
			vdx_access_table['Spend'] = np.select ([mask1, mast2], [choices_vdx_eng_spend, choices_vdx_cpcv_spend],
			                                       default=0.00)
			vdx_access_table['Delivery%'] = vdx_access_table['Delivery%'].replace (np.inf, 0.00)
			vdx_access_table['Spend'] = vdx_access_table['Spend'].replace (np.inf, 0.00)
		except (KeyError,TypeError) as e:
			self.logger.error (str (e)+' No vdx placements for IO - {}'.format(self.config.ioid))
			
		
		self.logger.info ('Adding Delivery Metrices on Preroll Placements for IO - {}'.format (self.config.ioid))
		try:
			preroll_access_table["PLACEMENT#"] = preroll_access_table["PLACEMENT#"].astype (int)
			preroll_access_table['Delivery%'] = preroll_access_table["Delivered_Impressions"]/preroll_access_table[
				"BOOKED_IMP#BOOKED_ENG"]
			preroll_access_table['Spend'] = preroll_access_table["Delivered_Impressions"]/1000*preroll_access_table[
				"UNIT_COST"]
			preroll_access_table['Delivery%'] = preroll_access_table['Delivery%'].replace (np.inf, 0.00)
			preroll_access_table['Spend'] = preroll_access_table['Spend'].replace (np.inf, 0.00)
		
		except (KeyError,TypeError) as e:
			self.logger.error(str(e)+' No preroll placements for IO - {}'.format(self.config.ioid))
		
		return displayfirsttable, vdx_access_table, preroll_access_table
	
	
	def rename_cols_sumary(self):
		"""
	Renaming Columns
		:param self:
		:return:
		"""
		displayfirsttable, vdx_access_table, preroll_access_table = self.summary_creation ()
		
		self.logger.info('Renaming Display Placements columns for IO - {}'.format(self.config.ioid))
		
		try:
			display_rename = displayfirsttable.rename (columns={
				"PLACEMENT#":"Placement#",
				"START_DATE":"Start Date", "END_DATE":"End Date",
				"PLACEMENT_NAME":"Placement Name",
				"COST_TYPE":"Cost Type", "UNIT_COST":"Unit Cost",
				"PLANNED_COST":"Planned Cost",
				"BOOKED_IMP#BOOKED_ENG":"Booked",
				"DELIVERED_IMPRESION":"Delivered_Impressions"
				},
				inplace=True)
		except (TypeError, KeyError, ValueError, AttributeError) as e:
			self.logger.error(str(e) + ' No columns to rename for display table for IO - {}'.format(self.config.ioid))
		
		self.logger.info ('Renaming VDX placements columns for IO - {}'.format(self.config.ioid))
		
		try:
			vdx_rename = vdx_access_table.rename (
				columns={
					"PLACEMENT#":"Placement#", "START_DATE":"Start Date", "END_DATE":"End Date",
					"PLACEMENT_NAME":"Placement Name",
					"COST_TYPE":"Cost Type", "UNIT_COST":"Unit Cost",
					"PLANNED_COST":"Planned Cost", "BOOKED_IMP#BOOKED_ENG":"Booked"
					},
				inplace=True)
			
		except (TypeError, KeyError, ValueError, AttributeError) as e:
			self.logger.error (str (e)+' No columns to rename for vdx table for IO - {}'.format (self.config.ioid))
			
		self.logger.info ('Renaming Preroll Placements columns for IO - {}'.format(self.config.ioid))
		
		try:
			preroll_rename = preroll_access_table.rename (
				columns={
					"PLACEMENT#":"Placement#", "START_DATE":"Start Date", "END_DATE":"End Date",
					"PLACEMENT_NAME":"Placement Name",
					"COST_TYPE":"Cost Type", "UNIT_COST":"Unit Cost",
					"PLANNED_COST":"Planned Cost", "BOOKED_IMP#BOOKED_ENG":"Booked"
					},
				inplace=True)
		except (TypeError, KeyError, ValueError, AttributeError) as e:
			self.logger.error (str (e)+' No columns to rename for preroll table for IO - {}'.format (self.config.ioid))
		return displayfirsttable, vdx_access_table, preroll_access_table
	
	
	def write_summary(self):
		"""
	Writing Data
		:param self:
		:return:
		"""
		
		self.logger.info('Columns renamed for IO: {}'.format(self.config.ioid))
		displayfirsttable, vdx_access_table, preroll_access_table = self.rename_cols_sumary ()
		data_common_columns = self.config.common_columns_summary ()
		
		self.logger.info('Writing Campaign Information for IO - {}'.format(self.config.ioid))
		summary = data_common_columns[1].to_excel (self.config.writer,
		                                           sheet_name="Summary({})".format (self.config.ioid),
		                                           startcol=1,
		                                           startrow=1, index=False, header=False)
		
		try:
			check_disp_empty = displayfirsttable.empty
			
			if check_disp_empty is True:
				pass
			else:
				self.logger.info('Writing Display Placements to summary for IO - {}'.format(self.config.ioid))
				display_write = displayfirsttable.to_excel (self.config.writer,
				                                              sheet_name="Summary({})".format (self.config.ioid),
				                                              startcol=3, startrow=8,
				                                              header=True, index=False)
		except (KeyError, AttributeError, ValueError) as e:
			self.logger.error (str (e)+' No display placements to write for IO - {}'.format (self.config.ioid))
			
		try:
			check_vdx_empty = vdx_access_table.empty
			
			if check_vdx_empty is True:
				pass
			else:
				self.logger.info ('Writing VDX Placements to summary for IO - {}'.format(self.config.ioid))
				vdx_write = vdx_access_table.to_excel (self.config.writer,
				                                       sheet_name="Summary({})".format (self.config.ioid),
				                                       startcol=3, startrow=len (displayfirsttable)+13,
				                                       header=True, index=False)
		except(KeyError, AttributeError, ValueError) as e:
			self.logger.error(str(e) + ' No vdx placements to write for IO - {}'.format(self.config.ioid))
		
		
		try:
			check_preroll_empty = preroll_access_table.empty
			
			if check_preroll_empty is True:
				pass
			else:
				self.logger.info ('Writing Preroll Placements to summary for IO - {}'.format(self.config.ioid))
				preroll_write = preroll_access_table.to_excel (self.config.writer,
				                                               sheet_name="Summary({})".format (self.config.ioid),
				                                               startcol=3,
				                                               startrow=len (displayfirsttable)+len (
					                                               vdx_access_table)+18,
					                                               header=True, index=False)
		except(KeyError, AttributeError, ValueError) as e:
			self.logger.error(str(e) + ' No preroll placements to write for IO - {}'.format(self.config.ioid))
			
		return displayfirsttable, vdx_access_table, preroll_access_table
	
	
	def format_summary(self):
		"""
	Applying formatting
		:param self:
		"""
		
		self.logger.info ('Connected for IO - {}'.format (self.config.ioid))
		self.logger.info ('Starting to build Summary for IO - {}'.format (self.config.ioid))
		self.logger.info('Apply formatting to all labels of summary sheet for IO - {}'.format(self.config.ioid))
		data_common_columns = self.config.common_columns_summary ()
		
		displayfirsttable, vdx_access_table, preroll_access_table = self.write_summary ()
		
		workbook = self.config.writer.book
		worksheet = self.config.writer.sheets["Summary({})".format (self.config.ioid)]
		worksheet.hide_gridlines (2)
		worksheet.set_row (0, 6)
		worksheet.set_column ("A:A", 2)
		try:
			check_disp_empty = displayfirsttable.empty
			check_vdx_empty = vdx_access_table.empty
			check_preroll_empty = preroll_access_table.empty
			
			worksheet.insert_image ("H2", "Exponential.png", {"url":"https://www.tribalfusion.com"})
			worksheet.insert_image ("I2", "Client_Logo.png")
			
			# 3 = data_common_columns[1].shape[0]
			# number_cols_common = data_common_columns[1].shape[1]
			number_rows_display = displayfirsttable.shape[0]
			number_cols_display = displayfirsttable.shape[1]
			number_rows_vdx = vdx_access_table.shape[0]
			number_cols_vdx = vdx_access_table.shape[1]
			number_rows_preroll = preroll_access_table.shape[0]
			number_cols_preroll = preroll_access_table.shape[1]
			
			forge_colour_info = workbook.add_format ()
			forge_colour_info.set_bg_color ('#F0F8FF')
			forge_colour_col = workbook.add_format ()
			forge_colour_col.set_bg_color ('#00B0F0')
			forge_colour_border = workbook.add_format ()
			forge_colour_border.set_bg_color ('#E7E6E6')
			
			alignment = workbook.add_format ({"align":"center"})
			
			format_border_bottom = workbook.add_format ()
			format_border_bottom.set_bottom (2)
			
			format_border_top = workbook.add_format ()
			format_border_top.set_top (2)
			
			format_border_right = workbook.add_format ()
			format_border_right.set_right (2)
			
			format_border_left = workbook.add_format ()
			format_border_left.set_left (2)
			
			format_sub = workbook.add_format ({"bold":True, "align":"center", "num_format":"#,##0"})
			format_subtotal = workbook.add_format ({"bold":True, "align":"center"})
			header_bold = workbook.add_format ({"bold":True})
			format_sub_num_money = workbook.add_format ({"bold":True, "num_format":"$#,###0.00", "align":"center"})
			format_sub_num_percent = workbook.add_format ({"bold":True, "num_format":"0.00%", "align":"center"})
			format_col = workbook.add_format ({"num_format":"#,##0"})
			
			percent_fmt = workbook.add_format ({"num_format":"0.00%"})
			money_fmt = workbook.add_format ({"num_format":"$#,###0.00"})
			
			# formatting Columns of IO from B2 to G5
			worksheet.conditional_format ("B2:O5", {"type":"blanks", "format":forge_colour_info})
			worksheet.conditional_format ("B2:O5", {"type":"no_blanks", "format":forge_colour_info})
			
			# formatting columns
			if check_disp_empty is True:
				pass
			else:
				worksheet.conditional_format (3+5, 3, 3+5, number_cols_display+2,
				                              {"type":"no_blanks", "format":forge_colour_col})
				worksheet.conditional_format (3+5, 3, 3+5, number_cols_display+2,
				                              {"type":"no_blanks", "format":header_bold})
			if check_vdx_empty is True:
				pass
			else:
				worksheet.conditional_format (3+number_rows_display+10, 3,
				                              3+number_rows_display+10, number_cols_vdx+2,
				                              {"type":"no_blanks", "format":forge_colour_col})
				worksheet.conditional_format (3+number_rows_display+10, 3,
				                              3+number_rows_display+10, number_cols_vdx+2,
				                              {"type":"no_blanks", "format":header_bold})
			
			if check_preroll_empty is True:
				pass
			else:
				worksheet.conditional_format (3+number_rows_display+number_rows_vdx+15, 3,
				                              3+number_rows_display+number_rows_vdx+15,
				                              number_cols_preroll+2,
				                              {"type":"no_blanks", "format":forge_colour_col})
				worksheet.conditional_format (3+number_rows_display+number_rows_vdx+15, 3,
				                              3+number_rows_display+number_rows_vdx+15,
				                              number_cols_preroll+2,
				                              {"type":"no_blanks", "format":header_bold})
			
			# formatting money and percent
			if check_disp_empty is True:
				pass
			else:
				worksheet.conditional_format (3+6, 8, 3+number_rows_display+5, 9,
				                              {"type":"no_blanks", "format":money_fmt})
				worksheet.conditional_format (3+6, 12, 3+number_rows_display+5, 12,
				                              {"type":"no_blanks", "format":percent_fmt})
				worksheet.conditional_format (3+6, 13, 3+number_rows_display+5, 13,
				                              {"type":"no_blanks", "format":money_fmt})
			
			if check_vdx_empty is True:
				pass
			else:
				worksheet.conditional_format (3+number_rows_display+11, 13,
				                              3+number_rows_display+number_rows_vdx+10, 13,
				                              {"type":"no_blanks", "format":percent_fmt})
				worksheet.conditional_format (3+number_rows_display+11, 14,
				                              3+number_rows_display+number_rows_vdx+10, 14,
				                              {"type":"no_blanks", "format":money_fmt})
				worksheet.conditional_format (3+number_rows_display+11, 8,
				                              3+number_rows_display+number_rows_vdx+10, 9,
				                              {"type":"no_blanks", "format":money_fmt})
			
			if check_preroll_empty is True:
				pass
			else:
				worksheet.conditional_format (3+number_rows_display+number_rows_vdx+16, 12,
				                              3+number_rows_display+number_rows_vdx+number_rows_preroll+15,
				                              12,
				                              {"type":"no_blanks", "format":percent_fmt})
				worksheet.conditional_format (3+number_rows_display+number_rows_vdx+16, 13,
				                              3+number_rows_display+number_rows_vdx+number_rows_preroll+15,
				                              13,
				                              {"type":"no_blanks", "format":money_fmt})
				worksheet.conditional_format (3+number_rows_display+number_rows_vdx+16, 8,
				                              3+number_rows_display+number_rows_vdx+number_rows_preroll+15,
				                              9,
				                              {"type":"no_blanks", "format":money_fmt})
			
			# formatting columns Booked, Delivered Impressions and Delivered Engagements
			if check_disp_empty is True:
				pass
			else:
				worksheet.conditional_format (3+6, 10, 3+number_rows_display+5, 11,
				                              {"type":"no_blanks", "format":format_col})
				"""worksheet.conditional_format(3+6,11,3+number_rows_display+5,11,
											 {"type":"no_blanks","format":format_col})
				worksheet.conditional_format(3+6,13,3+number_rows_display+5,13,
											 {"type":"no_blanks","format":format_col})"""
			
			if check_vdx_empty is True:
				pass
			else:
				worksheet.conditional_format (3+number_rows_display+11, 10,
				                              3+number_rows_display+number_rows_vdx+10, 13,
				                              {"type":"no_blanks", "format":format_col})
				"""worksheet.conditional_format(3+number_rows_display+11,14,
											 3+number_rows_display+number_rows_vdx+10,14,
											 {"type":"no_blanks","format":format_col})
				worksheet.conditional_format(3+number_rows_display+11,8,
											 3+number_rows_display+number_rows_vdx+10,9,
											 {"type":"no_blanks","format":format_col})"""
			
			if check_preroll_empty is True:
				pass
			else:
				worksheet.conditional_format (3+number_rows_display+number_rows_vdx+16, 10,
				                              3+number_rows_display+number_rows_vdx+number_rows_preroll+15,
				                              11,
				                              {"type":"no_blanks", "format":format_col})
				"""worksheet.conditional_format(3+number_rows_display+number_rows_vdx+16,13,
											 3+number_rows_display+number_rows_vdx+number_rows_preroll+15,
											 13,
											 {"type":"no_blanks","format":format_col})
				worksheet.conditional_format(3+number_rows_display+number_rows_vdx+16,8,
											 3+number_rows_display+number_rows_vdx+number_rows_preroll+15,
											 9,
											 {"type":"no_blanks","format":format_col})"""
			
			# addting subtotal and adding formatting for subtotal
			if check_disp_empty is True:
				pass
			else:
				worksheet.write (3+number_rows_display+6, 3, "Subtotal", format_subtotal)
			
			if check_vdx_empty is True:
				pass
			else:
				worksheet.write (3+number_rows_display+number_rows_vdx+11, 3, "Subtotal", format_subtotal)
			
			if check_preroll_empty is True:
				pass
			else:
				worksheet.write (3+number_rows_display+number_rows_vdx+number_rows_preroll+16, 3,
				                 "Subtotal",
				                 format_subtotal)
			
			# adding formulas to rows
			if check_disp_empty is True:
				pass
			else:
				worksheet.write_formula (3+number_rows_display+6, 9,
				                         '=sum(J{}:J{})'.format (3+7,
				                                                 3+number_rows_display+6),
				                         format_sub_num_money)
				worksheet.write_formula (3+number_rows_display+6, 10,
				                         '=sum(K{}:K{})'.format (3+7,
				                                                 3+number_rows_display+6), format_sub)
				worksheet.write_formula (3+number_rows_display+6, 11,
				                         '=sum(L{}:L{})'.format (3+7,
				                                                 3+number_rows_display+6), format_sub)
				worksheet.write_formula (3+number_rows_display+6, 12,
				                         '=IFERROR((L{}/K{}),0)'.format (3+number_rows_display+7,
				                                                         3+number_rows_display+7),
				                         format_sub_num_percent)
				worksheet.write_formula (3+number_rows_display+6, 13,
				                         '=sum(N{}:N{})'.format (3+7,
				                                                 3+number_rows_display+6),
				                         format_sub_num_money)
			
			if check_vdx_empty is True:
				pass
			else:
				worksheet.write_formula (3+number_rows_display+number_rows_vdx+11, 9,
				                         '=sum(J{}:J{})'.format (3+number_rows_display+12,
				                                                 3+number_rows_display+number_rows_vdx+11),
				                         format_sub_num_money)
				worksheet.write_formula (3+number_rows_display+number_rows_vdx+11, 10,
				                         '=sum(K{}:K{})'.format (3+number_rows_display+12,
				                                                 3+number_rows_display+number_rows_vdx+11),
				                         format_sub)
				worksheet.write_formula (3+number_rows_display+number_rows_vdx+11, 11,
				                         '=sum(L{}:L{})'.format (3+number_rows_display+12,
				                                                 3+number_rows_display+number_rows_vdx+11),
				                         format_sub)
				worksheet.write_formula (3+number_rows_display+number_rows_vdx+11, 12,
				                         '=sum(M{}:M{})'.format (3+number_rows_display+12,
				                                                 3+number_rows_display+number_rows_vdx+11),
				                         format_sub)
				worksheet.write_formula (3+number_rows_display+number_rows_vdx+11, 13,
				                         '=IFERROR(sum(L{}:M{})/sum(K{}),0)'.format (
					                         3+number_rows_display+number_rows_vdx+12,
					                         3+number_rows_display+number_rows_vdx+12,
					                         3+number_rows_display+number_rows_vdx+12),
				                         format_sub_num_percent)
				worksheet.write_formula (3+number_rows_display+number_rows_vdx+11, 14,
				                         '=sum(O{}:O{})'.format (3+number_rows_display+12,
				                                                 3+number_rows_display+number_rows_vdx+11),
				                         format_sub_num_money)
			
			if check_preroll_empty is True:
				pass
			else:
				worksheet.write_formula (3+number_rows_display+number_rows_vdx+number_rows_preroll+16, 9,
				                         '=sum(J{}:J{})'.format (3+number_rows_display+number_rows_vdx+17,
				                                                 3+number_rows_display+number_rows_vdx+number_rows_preroll+16),
				                         format_sub_num_money)
				worksheet.write_formula (3+number_rows_display+number_rows_vdx+number_rows_preroll+16, 10,
				                         '=sum(K{}:K{})'.format (3+number_rows_display+number_rows_vdx+17,
				                                                 3+number_rows_display+number_rows_vdx+number_rows_preroll+16),
				                         format_sub)
				worksheet.write_formula (3+number_rows_display+number_rows_vdx+number_rows_preroll+16, 11,
				                         '=sum(L{}:L{})'.format (3+number_rows_display+number_rows_vdx+17,
				                                                 3+number_rows_display+number_rows_vdx+number_rows_preroll+16),
				                         format_sub)
				worksheet.write_formula (3+number_rows_display+number_rows_vdx+number_rows_preroll+16, 12,
				                         '=IFERROR((L{}/K{}),0)'.format (
					                         3+number_rows_display+number_rows_vdx+number_rows_preroll+17,
					                         3+number_rows_display+number_rows_vdx+number_rows_preroll+17),
				                         format_sub_num_percent)
				worksheet.write_formula (3+number_rows_display+number_rows_vdx+number_rows_preroll+16, 13,
				                         '=sum(N{}:N{})'.format (3+number_rows_display+number_rows_vdx+17,
				                                                 3+number_rows_display+number_rows_vdx+number_rows_preroll+16),
				                         format_sub_num_money)
			
			# adding colour to last subtotal to each dataframe
			if check_disp_empty is True:
				pass
			else:
				worksheet.conditional_format (3+number_rows_display+6, 3,
				                              3+number_rows_display+6
				                              , number_cols_display+2, {"type":"blanks", "format":forge_colour_border})
				worksheet.conditional_format (3+number_rows_display+6, 3,
				                              3+number_rows_display+6
				                              , number_cols_display+2, {"type":"no_blanks", "format":forge_colour_border})
			if check_vdx_empty is True:
				pass
			else:
				worksheet.conditional_format (3+number_rows_display+number_rows_vdx+11, 3,
				                              3+number_rows_display+number_rows_vdx+11, number_cols_vdx+2,
				                              {"type":"blanks", "format":forge_colour_border})
				worksheet.conditional_format (3+number_rows_display+number_rows_vdx+11, 3,
				                              3+number_rows_display+number_rows_vdx+11, number_cols_vdx+2,
				                              {"type":"no_blanks", "format":forge_colour_border})
			if check_preroll_empty is True:
				pass
			else:
				worksheet.conditional_format (3+number_rows_display+number_rows_vdx+number_rows_preroll+16,
				                              3,
				                              3+number_rows_display+number_rows_vdx+number_rows_preroll+16,
				                              number_cols_preroll+2, {"type":"blanks", "format":forge_colour_border})
				worksheet.conditional_format (3+number_rows_display+number_rows_vdx+number_rows_preroll+16,
				                              3,
				                              3+number_rows_display+number_rows_vdx+number_rows_preroll+16,
				                              number_cols_preroll+2, {"type":"no_blanks", "format":forge_colour_border})
			
			# format bottom border
			if check_disp_empty is True:
				pass
			else:
				worksheet.conditional_format (3+number_rows_display+7, 3,
				                              3+number_rows_display+7
				                              , number_cols_display+2, {"type":"blanks", "format":format_border_top})
				
				"""worksheet.conditional_format(3+number_rows_display+7,3,
											 3+number_rows_display+6
											 ,number_cols_display+2,{"type":"no_blanks","format":format_border_bottom})"""
				
				"""worksheet.conditional_format(3+number_rows_display+6,3,
											 3+number_rows_display+6
											 ,number_cols_display+2,{"type":"no_blanks","format":format_border_left})"""
			if check_vdx_empty is True:
				pass
			else:
				worksheet.conditional_format (3+number_rows_display+number_rows_vdx+12, 3,
				                              3+number_rows_display+number_rows_vdx+12, number_cols_vdx+2,
				                              {"type":"blanks", "format":format_border_top})
				
				"""worksheet.conditional_format(3+number_rows_display+number_rows_vdx+11,3,
											 3+number_rows_display+number_rows_vdx+11,number_cols_vdx+2,
											 {"type":"no_blanks","format":format_border_bottom})"""
			
			if check_preroll_empty is True:
				pass
			else:
				worksheet.conditional_format (3+number_rows_display+number_rows_vdx+number_rows_preroll+17,
				                              3,
				                              3+number_rows_display+number_rows_vdx+number_rows_preroll+17,
				                              number_cols_preroll+2, {"type":"blanks", "format":format_border_top})
				
				"""worksheet.conditional_format(3+number_rows_display+number_rows_vdx+number_rows_preroll+16,
											 3,
											 3+number_rows_display+number_rows_vdx+number_rows_preroll+16,
											 number_cols_preroll+2,{"type":"no_blanks","format":format_border_bottom})"""
			
			# merge formatting
			format_merge_row = workbook.add_format ({
				"bold":True, "font_color":'#000000', "align":"centre",
				"fg_color":"#00B0F0", "border":2, "border_color":"#000000"
				})
			
			format_merge_row_wrap = workbook.add_format (
				{
					"bold":True, "font_color":'#000000', "align":"centre", 'valign':'vcenter',
					"border":2, 'text_wrap':True
					})
			
			if check_disp_empty is True:
				pass
			else:
				worksheet.merge_range (3+4, 3, 3+4, number_cols_display+2,
				                       "Campaign pacing", format_merge_row)
			
			if check_vdx_empty is True:
				pass
			else:
				worksheet.merge_range (3+number_rows_display+9, 3,
				                       3+number_rows_display+9,
				                       number_cols_vdx+2, "Campaign pacing", format_merge_row)
			
			if check_preroll_empty is True:
				pass
			else:
				worksheet.merge_range (3+number_rows_display+number_rows_vdx+14, 3,
				                       3+number_rows_display+number_rows_vdx+14,
				                       number_cols_preroll+2, "Campaign pacing",
				                       format_merge_row)
			
			# wrap and formatting for column B
			if check_disp_empty is True:
				pass
			else:
				worksheet.merge_range (3+5, 1, 3+number_rows_display+6, 2,
				                       "Standard Banners (Performance/Brand)",
				                       format_merge_row_wrap)
			
			if check_vdx_empty is True:
				pass
			else:
				worksheet.merge_range (3+number_rows_display+10, 1,
				                       3+number_rows_display+number_rows_vdx+11, 2,
				                       "VDX(Display and Instream)",
				                       format_merge_row_wrap)
			if check_preroll_empty is True:
				pass
			else:
				worksheet.merge_range (3+number_rows_display+number_rows_vdx+15, 1,
				                       3+number_rows_display+number_rows_vdx+number_rows_preroll+16, 2,
				                       "Standard Pre Roll",
				                       format_merge_row_wrap)
			
			# Right Border Formatting
			if check_disp_empty is True:
				pass
			else:
				worksheet.conditional_format (3+5, 14,
				                              3+number_rows_display+6, 14,
				                              {"type":"blanks", "format":format_border_left})
			
			if check_vdx_empty is True:
				pass
			else:
				worksheet.conditional_format (3+number_rows_display+10, 15,
				                              3+number_rows_display+number_rows_vdx+11, 15,
				                              {"type":"blanks", "format":format_border_left})
			
			if check_preroll_empty is True:
				pass
			else:
				worksheet.conditional_format (3+number_rows_display+number_rows_vdx+15, 14,
				                              3+number_rows_display+number_rows_vdx+number_rows_preroll+16,
				                              13, {"type":"blanks", "format":format_border_left})
			
			# Columns setting
			worksheet.set_column ("D:O", 21, alignment)
			worksheet.set_zoom (90)
			
			for row in range (5, 10):
				if check_disp_empty is True:
					worksheet.set_row (row, None, None, {'hidden':True})
			else:
				pass
			
			for row in range (5, 12):
				if check_disp_empty is True & check_vdx_empty is True:
					worksheet.set_row (row, None, None, {'hidden':True})
			else:
				pass
			
			for row in range (3+number_rows_display+8, 3+number_rows_display+13):
				if check_vdx_empty is True:
					worksheet.set_row (row, None, None, {'hidden':True})
			else:
				pass
		except Exception as e:
			self.logger.error(str(e) + ' No information in summary sheet saved for IO - {}'.format(self.config.ioid))
	
	def main(self):
		"""
	This is main function.
		:param self:
		"""
		self.config.common_columns_summary ()
		#self.connect_TFR_summary ()
		#self.read_query_summary ()
		#self.access_data_summary ()
		#self.summary_creation ()
		#self.rename_cols_sumary ()
		#self.write_summary ()
		
		self.format_summary ()
		self.logger.info('Summary Sheet Created for IO - {}'.format(self.config.ioid))

if __name__=="__main__":
	pass
	
	# enable it when running for individual file
	#c=config.Config("test", 605527)
	#o=Summary(c)
	#o.main()
	#c.saveAndCloseWriter()
