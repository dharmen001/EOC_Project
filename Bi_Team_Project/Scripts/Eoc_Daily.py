#coding=utf-8
#!/usr/bin/env python
"""
Created by:Dharmendra
Date:2018-03-23
"""
import datetime
import pandas as pd
import numpy as np
from xlsxwriter.utility import xl_rowcol_to_cell

import config
import pandas.io.formats.excel
import logging

pandas.io.formats.excel.header_style = None


class Daily(object):
	"""
To create display placements
	"""
	
	def __init__(self, config):
		
		"""
		:param config: Accesing Files
		"""
		self.config = config
		self.logger = self.config.logger
		
	def connect_TFR_daily(self):
		"""
		TFR Quries for making connection
		"""
		self.logger.info ('Starting to build Display Sheet for IO - {}'.format (self.config.ioid))
		
		read_display_summary = open("Display_Summary.sql")
		sql_sales_summary = read_display_summary.read().format(self.config.ioid,self.config.start_date,self.config.end_date)
		# self.logger.info ("Start executing: "+'Display_Summary.sql'+" at "+str (
		# 	datetime.datetime.now ().strftime ("%Y-%m-%d %H:%M"))+"\n"+sql_sales_summary)
		
		read_plc_info = open("Placement_info_display.sql")
		sql_sales_mv = read_plc_info.read().format(self.config.ioid,self.config.start_date,self.config.end_date)
		# self.logger.info ("Start executing: "+'Placement_info_display.sql'+" at "+str (
		# 	datetime.datetime.now ().strftime ("%Y-%m-%d %H:%M"))+"\n"+sql_sales_mv)
		
		read_adsize_display = open("Placement_info_display_adsize.sql")
		sql_sales_adsize_mv = read_adsize_display.read().format(self.config.ioid,self.config.start_date,self.config.end_date)
		# self.logger.info ("Start executing: "+'Placement_info_display_adsize.sql'+" at "+str (
		# 	datetime.datetime.now ().strftime ("%Y-%m-%d %H:%M"))+"\n"+sql_sales_adsize_mv)
		
		read_by_day_display = open("Placement_info_display_day.sql")
		sql_sales_daily_mv = read_by_day_display.read().format(self.config.ioid,self.config.start_date,self.config.end_date)
		# self.logger.info ("Start executing: "+'Placement_info_display_day.sql'+" at "+str (
		# 	datetime.datetime.now ().strftime ("%Y-%m-%d %H:%M"))+"\n"+sql_sales_daily_mv)
		
		
		self.sql_sales_summary = sql_sales_summary
		self.sql_sales_mv = sql_sales_mv
		self.sql_sales_adsize_mv = sql_sales_adsize_mv
		self.sql_sales_daily_mv = sql_sales_daily_mv
		
		#return sql_sales_summary, sql_sales_mv, sql_sales_adsize_mv, sql_sales_daily_mv
	
	def read_Query_daily(self):
		
		"""

		:return: Reading Quries Data
		"""
		self.logger.info ('Running Query for Display placements for IO {}'.format (self.config.ioid))

		#sql_sales_summary, sql_sales_mv, sql_sales_adsize_mv, sql_sales_daily_mv = self.connect_TFR_daily()

		read_sql_sales = pd.read_sql( self.sql_sales_summary, self.config.conn )
		read_sql_sales_mv = pd.read_sql( self.sql_sales_mv, self.config.conn )
		read_sql_adsize_mv = pd.read_sql( self.sql_sales_adsize_mv, self.config.conn )
		read_sql_daily_mv = pd.read_sql( self.sql_sales_daily_mv, self.config.conn )
		
		# self.read_sql_sales = read_sql_sales
		# self.read_sql_sales_mv = read_sql_sales_mv
		# self.read_sql_adsize_mv = read_sql_adsize_mv
		#self.read_sql_daily_mv = read_sql_daily_mv
		
		#return read_sql_sales, read_sql_sales_mv, read_sql_adsize_mv, read_sql_daily_mv
		
		self.read_sql_sales = read_sql_sales
		self.read_sql_sales_mv = read_sql_sales_mv
		self.read_sql_adsize_mv = read_sql_adsize_mv
		self.read_sql_daily_mv = read_sql_daily_mv
		
	def access_Data_KM_Sales_daily(self):
		
		"""

		:return: Accessing Columns by merging with summary
		"""
		self.logger.info('Query Stored for further processing of IO - {}'.format(self.config.ioid))

		#read_sql_sales, read_sql_sales_mv, read_sql_adsize_mv, read_sql_daily_mv = self.read_Query_daily()
		
		self.logger.info('Creating by placement information of IO {}'.format(self.config.ioid))
		
		display_sales_first_table = None
		try:
			standard_sales_first_table = self.read_sql_sales.merge( self.read_sql_sales_mv, on="PLACEMENT#", how="inner" )
			display_sales_first_table = standard_sales_first_table[
				["PLACEMENT#", "PLACEMENT_NAME", "COST_TYPE", "UNIT_COST", "BOOKED_IMP#BOOKED_ENG", "DELIVERED_IMPRESION",
				 "CLICKS", "CONVERSION"]]
		except (KeyError,ValueError,AttributeError) as e:
			self.logger.error(str(e) +' No Placement Information Display Placements IO - {}'.format(self.config.ioid))
			pass
			
		self.logger.info (
			'Creating by adsize information for display placements of IO {}'.format (self.config.ioid))
		
		adsize_sales_second_table= None
		try:
			standard_sales_second_table = self.read_sql_sales.merge( self.read_sql_adsize_mv, on="PLACEMENT#", how="inner" )
			adsize_sales_second_table = standard_sales_second_table[
				["PLACEMENT#", "PLACEMENT_NAME", "COST_TYPE", "UNIT_COST", "BOOKED_IMP#BOOKED_ENG",
				 "ADSIZE", "DELIVERED_IMPRESION", "CLICKS", "CONVERSION"]]
		except (KeyError,ValueError,AttributeError) as e:
			self.logger.error(str(e)+ ' No adSize information found for Display placement IO - {}'.format(self.config.ioid))
			pass
		daily_sales_third_table = None
		try:
			self.logger.info('Creating by day information for display placements of IO {}'.format(self.config.ioid))
			standard_sales_third_table = self.read_sql_sales.merge( self.read_sql_daily_mv, on="PLACEMENT#", how="inner" )
			daily_sales_third_table = standard_sales_third_table[["PLACEMENT#", "PLACEMENT_NAME", "COST_TYPE",
			                                                      "UNIT_COST", "BOOKED_IMP#BOOKED_ENG", "DAY",
			                                                      "DELIVERED_IMPRESION", "CLICKS", "CONVERSION"]]
			
		except (KeyError,ValueError,AttributeError) as e:
			self.logger.error(str(e) + ' No placement by day information found for Display placement IO - {}'.format(self.config.ioid))
			pass
		
		self.display_sales_first_table = display_sales_first_table
		self.adsize_sales_second_table = adsize_sales_second_table
		self.daily_sales_third_table = daily_sales_third_table
		
		#return display_sales_first_table, adsize_sales_second_table, daily_sales_third_table
	
	def KM_Sales_daily(self):
		
		"""

		:return: Joining Creative with placement Number
		"""
		#display_sales_first_table, adsize_sales_second_table, daily_sales_third_table =\
			#self.access_Data_KM_Sales_daily()
		
		self.logger.info(
			'Putting creative and placement together for placement information of IO {}'.format(self.config.ioid))
		self.display_sales_first_table["PLACEMENTNAME"] = self.display_sales_first_table[
			["PLACEMENT#", "PLACEMENT_NAME"]].apply(
			lambda x:".".join( x ), axis=1 )
		
		self.logger.info(
			'Putting creative and placement together for adsize information of IO {}'.format(self.config.ioid))
		self.adsize_sales_second_table["PLACEMENTNAME"] = self.adsize_sales_second_table[
			["PLACEMENT#", "PLACEMENT_NAME"]].apply(
			lambda x:".".join( x ), axis=1 )
		
		self.logger.info('Putting creative and placement for by day information of IO {}'.format(self.config.ioid))
		self.daily_sales_third_table["PLACEMENTNAME"] = self.daily_sales_third_table[
			["PLACEMENT#", "PLACEMENT_NAME"]].apply(
			lambda x:".".join( x ), axis=1 )
		
		self.logger.info('Adding Delivery Metrices for display placements information of IO {}'.format(self.config.ioid))
		choices_display_ctr = self.display_sales_first_table["CLICKS"]/self.display_sales_first_table[
			"DELIVERED_IMPRESION"]
		choices_display_conversion = self.display_sales_first_table["CONVERSION"]/self.display_sales_first_table[
			"DELIVERED_IMPRESION"]
		choices_display_spend = self.display_sales_first_table["DELIVERED_IMPRESION"]/1000*self.display_sales_first_table[
			"UNIT_COST"]
		
		choices_display_ecpa = (self.display_sales_first_table["DELIVERED_IMPRESION"]/1000*self.display_sales_first_table[
			"UNIT_COST"])/self.display_sales_first_table["CONVERSION"]
		
		mask1 = self.display_sales_first_table["COST_TYPE"].isin( ['CPM'] )
		
		self.display_sales_first_table["CTR%"] = np.select( [mask1], [choices_display_ctr], default=0.00 )
		self.display_sales_first_table["CONVERSIONRATE%"] = np.select( [mask1], [choices_display_conversion], default=0.00 )
		self.display_sales_first_table["SPEND"] = np.select( [mask1], [choices_display_spend], default=0.00 )
		self.display_sales_first_table["EPCA"] = np.select( [mask1], [choices_display_ecpa], default=0.00 )
		
		self.logger.info('Adding Deliver Metrices for display placements adsize information of IO {}'.format(self.config.ioid))
		choices_adsize_ctr = self.adsize_sales_second_table["CLICKS"]/self.adsize_sales_second_table[
			"DELIVERED_IMPRESION"]
		choices_adsize_conversion = self.adsize_sales_second_table["CONVERSION"]/self.adsize_sales_second_table[
			"DELIVERED_IMPRESION"]
		choices_adsize_spend = self.adsize_sales_second_table["DELIVERED_IMPRESION"]/1000*self.adsize_sales_second_table[
			"UNIT_COST"]
		choices_adsize_ecpa = choices_adsize_spend/(self.adsize_sales_second_table["CONVERSION"])
		
		mask2 = self.adsize_sales_second_table["COST_TYPE"].isin( ["CPM"] )
		
		self.adsize_sales_second_table["CTR%"] = np.select( [mask2], [choices_adsize_ctr], default=0.00 )
		self.adsize_sales_second_table["CONVERSIONRATE%"] = np.select( [mask2], [choices_adsize_conversion], default=0.00 )
		self.adsize_sales_second_table["SPEND"] = np.select( [mask2], [choices_adsize_spend], default=0.00 )
		self.adsize_sales_second_table["ECPA"] = np.select( [mask2], [choices_adsize_ecpa], default=0.00 )
		
		self.logger.info(
			'Adding Delivery Metrices for display placements daily information of IO {}'.format(self.config.ioid))
		choice_daily_ctr = self.daily_sales_third_table["CLICKS"]/self.daily_sales_third_table["DELIVERED_IMPRESION"]
		choice_daily_spend = self.daily_sales_third_table["DELIVERED_IMPRESION"]/1000*self.daily_sales_third_table["UNIT_COST"]
		choice_daily_cpa = (self.daily_sales_third_table["DELIVERED_IMPRESION"]/1000*self.daily_sales_third_table[
			"UNIT_COST"])/self.daily_sales_third_table["CONVERSION"]
		
		mask3 = self.daily_sales_third_table["COST_TYPE"].isin( ["CPM"] )
		
		self.daily_sales_third_table["CTR%"] = np.select( [mask3], [choice_daily_ctr], default=0.00 )
		self.daily_sales_third_table["SPEND"] = np.select( [mask3], [choice_daily_spend], default=0.00 )
		self.daily_sales_third_table["ECPA"] = np.select( [mask3], [choice_daily_cpa], default=0.00 )
		
		#self.display_sales_first_table = display_sales_first_table
		#self.adsize_sales_second_table = adsize_sales_second_table
		#self.daily_sales_third_table = daily_sales_third_table
		
		#return display_sales_first_table, adsize_sales_second_table, daily_sales_third_table
	
	def rename_KM_Sales_daily(self):
		"""Renaming The columns of Previous Functions"""
		#display_sales_first_table, adsize_sales_second_table, daily_sales_third_table = self.KM_Sales_daily()
		
		self.logger.info('Renaming of display placements information of IO {}'.format(self.config.ioid))
		rename_display_sales_first_table = self.display_sales_first_table.rename(
			columns={
				"PLACEMENT#":"Placement#", "PLACEMENT_NAME":"Placement Name",
				"COST_TYPE":"Cost Type", "UNIT_COST":"Unit Cost",
				"BOOKED_IMP#BOOKED_ENG":"Booked Impressions", "DELIVERED_IMPRESION":"Delivered Impressions"
				, "CLICKS":"Clicks",
				"CONVERSION":"Conversion"
				, "PLACEMENTNAME":"Placement# Name", "CTR%":"CTR"
				, "CONVERSIONRATE%":"Conversion Rate"
				, "SPEND":"Spend", "EPCA":"eCPA"
				}, inplace=True )
		
		self.logger.info('Renaming of display placements adsize information of IO {}'.format(self.config.ioid))
		rename_adsize_sales_second_table = self.adsize_sales_second_table.rename(
			columns={
				"PLACEMENT#":"Placement#", "PLACEMENT_NAME":"Placement Name",
				"COST_TYPE":"Cost Type", "UNIT_COST":"Unit Cost",
				"BOOKED_IMP#BOOKED_ENG":"Booked", "ADSIZE":"Adsize"
				, "DELIVERED_IMPRESION":"Delivered Impressions", "CLICKS":"Clicks", "CONVERSION":"Conversion",
				"PLACEMENTNAME":"Placement# Name"
				, "CTR%":"CTR", "CONVERSIONRATE%":"Conversion Rate", "SPEND":"Spend", "ECPA":"eCPA"
				}, inplace=True )
		
		self.logger.info( 'Renaming of display placements daily information of IO {}'.format(self.config.ioid))
		rename_daily_sales_third_table = self.daily_sales_third_table.rename(
			columns={
				"PLACEMENT#":"Placement#", "PLACEMENT_NAME":"Placement Name",
				"COST_TYPE":"Cost Type", "UNIT_COST":"Unit Cost", "BOOKED_IMP#BOOKED_ENG":"Booked",
				"DAY":"Date", "DELIVERED_IMPRESION":"Delivered Impressions", "CLICKS":"Clicks",
				"CONVERSION":"Conversion", "PLACEMENTNAME":"Placement# Name",
				"CTR%":"CTR", "SPEND":"Spend", "ECPA":"eCPA"
				}, inplace=True )
		
		# self.display_sales_first_table = display_sales_first_table
		# self.adsize_sales_second_table = adsize_sales_second_table
		# self.daily_sales_third_table = daily_sales_third_table
		
		#return display_sales_first_table, adsize_sales_second_table, daily_sales_third_table
	
	def accessing_nan_values(self):
		
		"""

		:return: Nan values handling
		"""
		#display_sales_first_table, adsize_sales_second_table, daily_sales_third_table = self.rename_KM_Sales_daily()
		
		self.display_sales_first_table["CTR"] = self.display_sales_first_table["CTR"].replace( np.nan, 0.00 )
		self.display_sales_first_table["Conversion Rate"] = self.display_sales_first_table["Conversion Rate"].replace(
			np.nan, 0.00 )
		self.display_sales_first_table["Spend"] = self.display_sales_first_table["Spend"].replace( np.nan, 0.00 )
		self.display_sales_first_table["eCPA"] = self.display_sales_first_table["eCPA"].replace( np.nan, 0.00 )
		
		self.adsize_sales_second_table["CTR"] = self.adsize_sales_second_table["CTR"].replace( np.nan, 0.00 )
		self.adsize_sales_second_table["Conversion Rate"] = self.adsize_sales_second_table["Conversion Rate"].replace(
			np.nan, 0.00 )
		self.adsize_sales_second_table["Spend"] = self.adsize_sales_second_table["Spend"].replace( np.nan, 0.00 )
		self.adsize_sales_second_table["eCPA"] = self.adsize_sales_second_table["eCPA"].replace( np.nan, 0.00 )
		
		self.daily_sales_third_table["CTR"] = self.daily_sales_third_table["CTR"].replace( np.nan, 0.00 )
		self.daily_sales_third_table["Spend"] = self.daily_sales_third_table["Spend"].replace( np.nan, 0.00 )
		self.daily_sales_third_table["eCPA"] = self.daily_sales_third_table["eCPA"].replace( np.nan, 0.00 )
		
		self.display_sales_first_table["CTR"] = self.display_sales_first_table["CTR"].replace( np.inf, 0.00 )
		self.display_sales_first_table["Conversion Rate"] = self.display_sales_first_table["Conversion Rate"].replace(
			np.inf, 0.00 )
		self.display_sales_first_table["Spend"] = self.display_sales_first_table["Spend"].replace( np.inf, 0.00 )
		self.display_sales_first_table["eCPA"] = self.display_sales_first_table["eCPA"].replace( np.inf, 0.00 )
		
		self.adsize_sales_second_table["CTR"] = self.adsize_sales_second_table["CTR"].replace( np.inf, 0.00 )
		self.adsize_sales_second_table["Conversion Rate"] = self.adsize_sales_second_table["Conversion Rate"].replace(
			np.inf, 0.00 )
		self.adsize_sales_second_table["Spend"] = self.adsize_sales_second_table["Spend"].replace( np.inf, 0.00 )
		self.adsize_sales_second_table["eCPA"] = self.adsize_sales_second_table["eCPA"].replace( np.inf, 0.00 )
		
		self.daily_sales_third_table["CTR"] = self.daily_sales_third_table["CTR"].replace( np.inf, 0.00 )
		self.daily_sales_third_table["Spend"] = self.daily_sales_third_table["Spend"].replace( np.inf, 0.00 )
		self.daily_sales_third_table["eCPA"] = self.daily_sales_third_table["eCPA"].replace( np.inf, 0.00 )
		
		# self.display_sales_first_table = display_sales_first_table
		# self.adsize_sales_second_table = adsize_sales_second_table
		# self.daily_sales_third_table = daily_sales_third_table
		
		#return display_sales_first_table, adsize_sales_second_table, daily_sales_third_table
	
	def accessing_main_column(self):
		
		"""

		:return: Accessing columns
		"""
		#display_sales_first_table, adsize_sales_second_table, daily_sales_third_table = self.accessing_nan_values()
		
		#debug = detailed info
		#info =confirmation that things accroding to the plan
		#warning = something unexpected
		#error = some function failed
		#critical = something failed application must close
		
		placement_sales_data = self.display_sales_first_table[["Placement# Name", "Unit Cost", "Booked Impressions",
		                                                  "Delivered Impressions", "Clicks", "CTR",
		                                                  "Conversion","Spend", "eCPA"]]
		
		
		adsize_sales_data_new = self.adsize_sales_second_table.loc[:,
		                        ["Placement# Name", "Adsize", "Delivered Impressions", "Clicks",
		                         "CTR", "Conversion", "Conversion Rate", "Spend", "eCPA"]]
		
		final_adsize = None
		try:
			final_adsize = adsize_sales_data_new[["Placement# Name","Adsize", "Delivered Impressions", "Clicks", "CTR",
			                                      "Conversion","Spend", "eCPA"]]
			
			
		# adsize_sales_data_pivot = pd.pivot_table( adsize_sales_data_new, index=["Adsize"],
		#                                           values=["Delivered Impressions",
		#                                                   "Clicks", "Conversion",
		#                                                   "Spend"], aggfunc=np.sum )
		#
		# adsize_sales_data_pivot_new = adsize_sales_data_pivot.reset_index()
		#
		# adsize_sales_data = None
		# final_adsize = None
		# try:
		# 	adsize_sales_data_pivot_new["CTR"] = adsize_sales_data_pivot_new["Clicks"]/adsize_sales_data_pivot_new[
		# 			"Delivered Impressions"]
		#
		# 	adsize_sales_data_pivot_new["Conversion Rate"] = adsize_sales_data_pivot_new["Conversion"]/\
		# 	                                                 adsize_sales_data_pivot_new["Delivered Impressions"]
		#
		# 	adsize_sales_data_pivot_new["eCPA"] = adsize_sales_data_pivot_new["Spend"]/adsize_sales_data_pivot_new[
		# 		"Conversion"]
		#
		# 	adsize_sales_data = adsize_sales_data_pivot_new[["Adsize", "Delivered Impressions", "Clicks", "CTR",
		#                                                      "Conversion", "Conversion Rate", "Spend", "eCPA"]]
		#
		# 	final_adsize = adsize_sales_data[["Adsize", "Delivered Impressions", "Clicks", "CTR",
		#                                           "Conversion", "Conversion Rate", "Spend", "eCPA"]]
		except KeyError as e:
			self.logger.error(str(e)+'Not found in adsize Display Placements Data')
			pass
		
		
		daily_sales_data = self.daily_sales_third_table.loc[:,
		                   ["Placement#", "Placement# Name", "Date", "Delivered Impressions",
		                    "Clicks", "CTR", "Conversion", "eCPA", "Spend",
		                    "Unit Cost"]]
		
		#daily_sales_data['Date'] = pd.to_datetime(daily_sales_data.Date)
		
		daily_sales_remove_zero = daily_sales_data[daily_sales_data['Delivered Impressions']==0]
		
		daily_sales_data = daily_sales_data.drop( daily_sales_remove_zero.index, axis=0 )
		
		daily_sales_data [ "Date" ] = pd.to_datetime ( daily_sales_data [ "Date" ] )
		
		daily_sales_data['Date'] = pd.to_datetime( daily_sales_data['Date'] ).dt.date
		
		excel_start_date = datetime.date( 1899, 12, 30 )
		daily_sales_data['Date'] = daily_sales_data['Date']-excel_start_date
		
		try:
			daily_sales_data.Date = daily_sales_data.Date.dt.days
		except (KeyError,AttributeError) as e:
			self.logger.error(str(e)+'Not found in day wise data')
			pass
		final_day_wise = daily_sales_data.loc[:, ["Placement# Name", "Date",
		                                          "Delivered Impressions", "Clicks", "CTR",
		                                          "Conversion", "Spend", "eCPA"]]
		
		
		
		
		#return placement_sales_data, final_adsize, final_day_wise
		self.placement_sales_data = placement_sales_data
		self.final_adsize = final_adsize
		self.final_day_wise = final_day_wise
		
	def write_KM_Sales_summary(self):
		
		"""

		:return: writing Data
		"""
		#data_common_columns = self.config.common_columns_summary()
		unqiue_final_day_wise = self.final_day_wise['Placement# Name'].nunique ()
		
		
		
		self.logger.info("Writing Summary on Display Sheet for IO - {}".format(self.config.ioid))
		
		"""writing_data_common_columns = data_common_columns[1].to_excel( self.config.writer,
		                                                               sheet_name="Performance Details", startcol=1,
		                                                               startrow=1,
		                                                               index=False, header=False )"""
		
		info_client = self.config.client_info.to_excel (self.config.writer, sheet_name="Performance Details",
		                                                startcol=1, startrow=1, index=True, header=False)
		info_campaign = self.config.campaign_info.to_excel (self.config.writer, sheet_name="Performance Details",
		                                                    startcol=1, startrow=2, index=True, header=False)
		info_ac_mgr = self.config.ac_mgr.to_excel (self.config.writer, sheet_name="Performance Details", startcol=4,
		                                           startrow=1, index=True, header=False)
		info_sales_rep = self.config.sales_rep.to_excel (self.config.writer, sheet_name="Performance Details",
		                                                 startcol=4, startrow=2, index=True, header=False)
		info_campaign_date = self.config.sdate_edate_final.to_excel (self.config.writer,
		                                                             sheet_name="Performance Details", startcol=7,
		                                                             startrow=1, index=True, header=False)
		
		self.logger.info ("Writing Placement level information on Display Sheet for IO - {}".format (self.config.ioid))
		
		try:
			check_placement_sales_data = self.placement_sales_data.empty
		
			if check_placement_sales_data is True:
				pass
			else:
				writing_placement_data = self.placement_sales_data.to_excel( self.config.writer,
				                                                        sheet_name="Performance Details",
				                                                        startcol=1, startrow=8, index=False,
				                                                        header=True )
			
			self.logger.info (
				"Writing ad size level information on Display Sheet for IO - {}".format (self.config.ioid))
			
			check_adsize_sales_data = self.final_adsize.empty
			start_row_adsize = len( self.placement_sales_data )+14
			
			if check_adsize_sales_data is True:
				pass
			else:
				for placement, placement_df in self.final_adsize.groupby('Placement# Name', as_index=False):
					writing_adsize_data = placement_df.to_excel(self.config.writer,
					                                             sheet_name="Performance Details",
					                                             startcol=1, startrow=start_row_adsize,
					                                             index=False,
					                                             header=False)
					
					workbook = self.config.writer.book
					worksheet = self.config.writer.sheets["Performance Details".format (self.config.ioid)]
					start_row_adsize += len(placement_df)+2
					worksheet.write_string (start_row_adsize-2, 1, 'Subtotal')
					start_row_new = start_row_adsize-len (placement_df)-1
					format_num = workbook.add_format ({"num_format":"#,##0"})
					percent_fmt = workbook.add_format ({"num_format":"0.00%", "align":"right"})
					money_fmt = workbook.add_format ({"num_format":"$#,###0.00", "align":"right"})
					worksheet.write_formula(start_row_adsize-2,3,'=sum(D{}:D{})'.format(start_row_new,start_row_adsize-2),format_num)
					
					worksheet.write_formula (start_row_adsize-2, 4,
					                         '=sum(E{}:E{})'.format (start_row_new, start_row_adsize-2),format_num)
					
					worksheet.write_formula (start_row_adsize-2, 5,
					                         '=IFERROR(E{}/D{},0)'.format (start_row_adsize-1, start_row_adsize-1),percent_fmt)
					
					worksheet.write_formula (start_row_adsize-2, 6,
					                         '=sum(G{}:G{})'.format (start_row_new, start_row_adsize-2),format_num)
					
					worksheet.write_formula (start_row_adsize-2, 7,
					                         '=sum(H{}:H{})'.format (start_row_new, start_row_adsize-2),money_fmt)
					
					worksheet.conditional_format(start_row_new-1,3,start_row_adsize-2,4,{"type":"no_blanks","format":format_num})
					
					worksheet.conditional_format (start_row_new-1, 5, start_row_adsize-2, 5,
					                              {"type":"no_blanks", "format":percent_fmt})
					
					worksheet.conditional_format (start_row_new-1, 6, start_row_adsize-2, 6,
					                              {"type":"no_blanks", "format":format_num})
					
					worksheet.conditional_format (start_row_new-1, 7, start_row_adsize-2, 8,
					                              {"type":"no_blanks", "format":money_fmt})
			
			check_daily_sales_data = self.final_day_wise.empty
			
			self.logger.info ("Writing Placement by day level information on Display Sheet for IO - {}".format (self.config.ioid))
			
			start_row_plc_day = len (self.placement_sales_data)+13+unqiue_final_day_wise*2+len (self.final_adsize)+5
			
			if check_daily_sales_data is True:
				pass
			else:
				for placement_by_day, placement_df_by_day in self.final_day_wise.groupby('Placement# Name', as_index=False):
					
					writing_daily_data = placement_df_by_day.to_excel(self.config.writer,sheet_name="Performance Details",
					                                                  startcol=1,startrow=start_row_plc_day,index=False,
					                                                  header=False,merge_cells=False)
					workbook = self.config.writer.book
					worksheet = self.config.writer.sheets["Performance Details".format (self.config.ioid)]
					start_row_plc_day += len(placement_df_by_day)+2
					worksheet.write_string(start_row_plc_day-2, 1, 'Subtotal')
					start_row_plc_day_new = start_row_plc_day-len (placement_df_by_day)-1
					
					format_num = workbook.add_format ({"num_format":"#,##0"})
					percent_fmt = workbook.add_format ({"num_format":"0.00%", "align":"right"})
					money_fmt = workbook.add_format ({"num_format":"$#,###0.00", "align":"right"})
					
					centre_date_format_wb = workbook.add_format ({'align':'center', 'num_format':'YYYY-MM-DD'})
					worksheet.conditional_format(start_row_plc_day_new-1,2,start_row_plc_day-2,2,{"type":"no_blanks","format":centre_date_format_wb})
					
					worksheet.write_formula(start_row_plc_day-2,3,'=sum(D{}:D{})'.format(start_row_plc_day_new,
					                                                                     start_row_plc_day-2),format_num)
					worksheet.write_formula(start_row_plc_day-2,4,'=sum(E{}:E{})'.format(start_row_plc_day_new,
					                                                                     start_row_plc_day-2),format_num)
					worksheet.write_formula (start_row_plc_day-2,5,'=IFERROR(E{}/D{},0)'.format (start_row_plc_day-1,
					                                                                             start_row_plc_day-1),percent_fmt)
					worksheet.write_formula (start_row_plc_day-2, 6,'=sum(G{}:G{})'.format (start_row_plc_day_new,
					                                                                        start_row_plc_day-2),format_num)
					worksheet.write_formula (start_row_plc_day-2, 7,'=sum(H{}:H{})'.format (start_row_plc_day_new,
					                                                                        start_row_plc_day-2),money_fmt)
					
					
					worksheet.conditional_format(start_row_plc_day_new-1,3,start_row_plc_day-2,4,
					                             {"type":"no_blanks","format":format_num})
					
					worksheet.conditional_format (start_row_plc_day_new-1, 5, start_row_plc_day-2, 5,
					                              {"type":"no_blanks", "format":percent_fmt})
					
					worksheet.conditional_format (start_row_plc_day_new-1, 6, start_row_plc_day-2, 6,
					                              {"type":"no_blanks", "format":format_num})
					
					worksheet.conditional_format (start_row_plc_day_new-1, 7, start_row_plc_day-2, 8,
					                              {"type":"no_blanks", "format":money_fmt})
		
		except (KeyError,AttributeError,TypeError,IOError) as e:
			self.logger.error(str(e))
			pass
			
			
	def formatting_daily(self):
		"""
		Applying formatting on Display Sheet
		"""
		
		self.logger.info('Applying Formatting on each label of Display sheet - {}'.format(self.config.ioid))
		try:
			
			workbook = self.config.writer.book
			worksheet = self.config.writer.sheets["Performance Details".format( self.config.ioid )]
			
			unqiue_final_day_wise = self.final_day_wise['Placement# Name'].nunique()
			format_grand = workbook.add_format ({"bold":True, "bg_color":"#A5A5A5"})
			format_header = workbook.add_format ({"bold":True, "bg_color":"#00B0F0"})
			format_header_center = workbook.add_format ({"bold":True, "bg_color":"#00B0F0","align":"center"})
			format_header_right = workbook.add_format ({"bold":True, "bg_color":"#00B0F0","align":"right"})
			
			format_colour = workbook.add_format ({"bg_color":'#00B0F0'})
			format_campaign_info = workbook.add_format ({"bold":True, "bg_color":'#00B0F0', "align":"left"})
			
			number_rows_placement = self.placement_sales_data.shape[0]
			number_cols_placement = self.placement_sales_data.shape[1]
			number_rows_adsize = self.final_adsize.shape[0]
			number_cols_adsize = self.final_adsize.shape[1]
			number_rows_daily = self.final_day_wise.shape[0]
			number_cols_daily = self.final_day_wise.shape[1]
			
			worksheet.hide_gridlines( 2 )
			worksheet.set_row( 0, 6 )
			worksheet.set_column( "A:A", 2)
			worksheet.set_zoom(75)
			alignment_center = workbook.add_format ({"align":"center"})
			alignment_left = workbook.add_format ({"align":"left"})
			alignment_right = workbook.add_format ({"align":"right"})
			
			worksheet.conditional_format ("A1:R5", {"type":"blanks", "format":format_campaign_info})
			worksheet.conditional_format ("A1:R5", {"type":"no_blanks", "format":format_campaign_info})
			
			worksheet.insert_image ("O7", "Exponential.png", {"url":"https://www.tribalfusion.com"})
			worksheet.insert_image ("O2", "Client_Logo.png")
			
			format_header_left = workbook.add_format ({"bold":True, "bg_color":'#00B0F0', "align":"left"})
			format_num = workbook.add_format ({"num_format":"#,##0"})
			percent_fmt = workbook.add_format ({"num_format":"0.00%", "align":"right"})
			money_fmt = workbook.add_format ({"num_format":"$#,###0.00", "align":"right"})
			
			# Placement data formatting
			worksheet.write_string(7,1,"Performance by Placement",format_header_left)
			worksheet.write_string(9+number_rows_placement,1,"Grand Total",format_grand)
			worksheet.conditional_format(7,2,7,number_cols_placement,{"type":"blanks","format":format_colour})
			worksheet.conditional_format(7, 2, 7, number_cols_placement,{"type":"no_blanks", "format":format_colour})
			worksheet.conditional_format(8,1,8,1,{"type":"no_blanks","format":format_header_left})
			worksheet.conditional_format(8,2,8, 2,{"type":"no_blanks", "format":format_header})
			worksheet.conditional_format (8, 3, 8, 9, {"type":"no_blanks", "format":format_header})
			
			
			for col in range(3,6):
				cell_location = xl_rowcol_to_cell(9+number_rows_placement,col)
				start_range = xl_rowcol_to_cell(9,col)
				end_range = xl_rowcol_to_cell(9+number_rows_placement-1,col)
				formula = '=sum({:s}:{:s})'.format (start_range, end_range)
				worksheet.write_formula(cell_location,formula,format_num)
				start_plc_row = 9
				end_plc_row = 9+number_rows_placement-1
				worksheet.conditional_format (start_plc_row, col, end_plc_row, col,
				                              {"type":"no_blanks", "format":format_num})
				start_range_format = 9+number_rows_placement
				worksheet.conditional_format(start_range_format,col,start_range_format,col,{"type":"no_blanks", "format":format_grand})
				worksheet.conditional_format (start_range_format, col, start_range_format, col,
				                              {"type":"blanks", "format":format_grand})
				
			for col in range(6,7):
				cell_location = xl_rowcol_to_cell(9+number_rows_placement,col)
				# start_range = xl_rowcol_to_cell(9,col)
				# end_range = xl_rowcol_to_cell(9+number_rows_placement-1,col)
				formula = '=IFERROR(F{}/E{},0)'.format(9+number_rows_placement+1,9+number_rows_placement+1)
				worksheet.write_formula(cell_location,formula,percent_fmt)
				start_plc_row = 9
				end_plc_row = 9+number_rows_placement-1
				worksheet.conditional_format(start_plc_row,col,end_plc_row,col,{"type":"no_blanks", "format":percent_fmt})
				start_range_format = 9+number_rows_placement
				worksheet.conditional_format (start_range_format, col, start_range_format, col,
				                              {"type":"no_blanks", "format":format_grand})
				worksheet.conditional_format (start_range_format, col, start_range_format, col,
				                              {"type":"blanks", "format":format_grand})
				
			for col in range(7,8):
				cell_location = xl_rowcol_to_cell(9+number_rows_placement,col)
				start_range = xl_rowcol_to_cell(9,col)
				end_range = xl_rowcol_to_cell(9+number_rows_placement-1,col)
				formula = '=sum({:s}:{:s})'.format (start_range, end_range)
				worksheet.write_formula(cell_location,formula,format_num)
				start_plc_row = 9
				end_plc_row = 9+number_rows_placement-1
				worksheet.conditional_format (start_plc_row, col, end_plc_row, col,
				                              {"type":"no_blanks", "format":format_num})
				start_range_format = 9+number_rows_placement
				worksheet.conditional_format (start_range_format, col, start_range_format, col,
				                              {"type":"no_blanks", "format":format_grand})
				worksheet.conditional_format (start_range_format, col, start_range_format, col,
				                              {"type":"blanks", "format":format_grand})
			
			for col in range(8,9):
				cell_location = xl_rowcol_to_cell(9+number_rows_placement,col)
				start_range = xl_rowcol_to_cell(9,col)
				end_range = xl_rowcol_to_cell(9+number_rows_placement-1,col)
				formula = '=sum({:s}:{:s})'.format (start_range, end_range)
				worksheet.write_formula(cell_location,formula,money_fmt)
				start_plc_row = 9
				end_plc_row = 9+number_rows_placement-1
				worksheet.conditional_format (start_plc_row, col, end_plc_row, col,{"type":"no_blanks", "format":money_fmt})
				start_range_format = 9+number_rows_placement
				worksheet.conditional_format (start_range_format, col, start_range_format, col,
				                              {"type":"no_blanks", "format":format_grand})
				worksheet.conditional_format (start_range_format, col, start_range_format, col,
				                              {"type":"blanks", "format":format_grand})
				
				
				
			for col in range(2,3):
				start_plc_row = 9
				end_plc_row = 9+number_rows_placement-1
				worksheet.conditional_format(start_plc_row,col,end_plc_row,col,{"type":"no_blanks","format":money_fmt})
				start_range = 9+number_rows_placement
				worksheet.conditional_format(start_range,col,start_range,col,{"type":"blanks","format":format_grand})
				worksheet.conditional_format (start_range, col, start_range, col,
				                              {"type":"no_blanks", "format":format_grand})
				
				#worksheet.conditional_format (8, col, 8, col, {"type":"no_blanks", "format":format_header_center})
				
			for col in range(9,10):
				start_plc_row = 9
				end_plc_row = 9+number_rows_placement-1
				worksheet.conditional_format(start_plc_row,col,end_plc_row,col,{"type":"no_blanks","format":money_fmt})
				start_range = 9+number_rows_placement
				worksheet.conditional_format (start_range, col, start_range, col,
				                              {"type":"blanks", "format":format_grand})
				worksheet.conditional_format (start_range, col, start_range, col,
				                              {"type":"no_blanks", "format":format_grand})
				
				
			#adsize data formatting
			
			worksheet.write_string(12+number_rows_placement,1,"Performance by Ad Size",format_header_left)
			
			for col in range(2,number_cols_adsize+1):
				worksheet.write_string(12+number_rows_placement,col,"",format_colour)
			
			worksheet.write_string(13+number_rows_placement, 1, "Placement # Name", format_header_left)
			worksheet.write_string(13+number_rows_placement, 2, "Ad Size", format_header_center)
			worksheet.write_string(13+number_rows_placement,3,"Delivered Impressions",format_header_right)
			worksheet.write_string(13+number_rows_placement,4,"Clicks",format_header_right)
			worksheet.write_string(13+number_rows_placement,5,"CTR %",format_header_right)
			worksheet.write_string(13+number_rows_placement,6,"Conversions",format_header_right)
			worksheet.write_string(13+number_rows_placement,7,"Spend",format_header_right)
			worksheet.write_string(13+number_rows_placement, 8, "eCPA", format_header_right)
			
			worksheet.write_string(13+number_rows_placement+number_rows_adsize+unqiue_final_day_wise*2,1,
			                       'Grand Total',format_grand)
			
			worksheet.write_formula(13+number_rows_placement+number_rows_adsize+unqiue_final_day_wise*2,3,
			                        '=SUMIF(B{}:B{},"Subtotal",D{}:D{})'.format(15+number_rows_placement,
			                                                                    13+number_rows_placement+number_rows_adsize+unqiue_final_day_wise*2,
			                                                                    15+number_rows_placement,
			                                                                    13+number_rows_placement+number_rows_adsize+unqiue_final_day_wise*2),
			                        format_num)
			
			worksheet.write_formula(13+number_rows_placement+number_rows_adsize+unqiue_final_day_wise*2,4,
			                        '=SUMIF(B{}:B{},"Subtotal",E{}:E{})'.format(15+number_rows_placement,
			                                                                    13+number_rows_placement
			                                                                    +number_rows_adsize+unqiue_final_day_wise*2,
			                                                                    15+number_rows_placement,
			                                                                    13+number_rows_placement
			                                                                    +number_rows_adsize+unqiue_final_day_wise*2))
			
			worksheet.write_formula(13+number_rows_placement+number_rows_adsize+unqiue_final_day_wise*2,5,
			                        '=IFERROR(E{}/D{},0)'.format(13+number_rows_placement+number_rows_adsize+unqiue_final_day_wise*2+1,
			                                                     13+number_rows_placement+number_rows_adsize
			                                                     +unqiue_final_day_wise*2+1),percent_fmt)
			
			worksheet.write_formula(13+number_rows_placement+number_rows_adsize+unqiue_final_day_wise*2,6,
			                        '=SUMIF(B{}:B{},"Subtotal",G{}:G{})'.format(15+number_rows_placement,
			                                                                    13+number_rows_placement+number_rows_adsize+unqiue_final_day_wise*2,
			                                                                    15+number_rows_placement,
			                                                                    13+number_rows_placement+number_rows_adsize+unqiue_final_day_wise*2),format_num)
			
			
			worksheet.write_formula(13+number_rows_placement+number_rows_adsize+unqiue_final_day_wise*2,7,
			                        '=SUMIF(B{}:B{},"Subtotal",H{}:H{})'.format(15+number_rows_placement,
			                                                                    13+number_rows_placement
			                                                                    +number_rows_adsize+unqiue_final_day_wise*2,
			                                                                    15+number_rows_placement,
			                                                                    13+number_rows_placement
			                                                                    +number_rows_adsize+unqiue_final_day_wise*2),money_fmt)
			
			for col in range(2,number_cols_adsize+1):
				worksheet.conditional_format(13+number_rows_placement+number_rows_adsize+unqiue_final_day_wise*2,col,
				                             13+number_rows_placement+number_rows_adsize+unqiue_final_day_wise*2,col,
				                             {"type":"blanks","format":format_grand})
				worksheet.conditional_format(13+number_rows_placement+number_rows_adsize+unqiue_final_day_wise*2,col,
				                             13+number_rows_placement+number_rows_adsize+unqiue_final_day_wise*2,col,
				                             {"type":"no_blanks","format":format_grand})
			
			
			#Day Wise Grand Total
			#worksheet.write_string (12+number_rows_placement, 1, "Performance by Ad Size", format_header_left)
			grand_total_row = 13+number_rows_placement+number_rows_adsize+unqiue_final_day_wise*2+number_rows_daily+unqiue_final_day_wise*2+4
			formula_range_grand = 14+number_rows_placement+number_rows_adsize+unqiue_final_day_wise*2+5
			writing_info_row = 13+number_rows_placement+number_rows_adsize+unqiue_final_day_wise*2+3
			writing_header_row = 13+number_rows_placement+number_rows_adsize+unqiue_final_day_wise*2+4
			worksheet.write_string (writing_info_row, 1, "Performance - by Placement and Date", format_header_left)
			worksheet.write_string(writing_header_row,1,"Placement # Name",format_header_left)
			worksheet.write_string(writing_header_row,2,"Date",format_header_center)
			worksheet.write_string(writing_header_row,3,"Delivered Impressions",format_header_right)
			worksheet.write_string(writing_header_row,4,"Clicks",format_header_right)
			worksheet.write_string (writing_header_row, 5, "CTR %", format_header_right)
			worksheet.write_string(writing_header_row, 6, "Conversions", format_header_right)
			worksheet.write_string(writing_header_row, 7, "Spend", format_header_right)
			worksheet.write_string(writing_header_row, 8, "eCPA", format_header_right)
			#centre_date_format_wb = workbook.add_format ({'align':'center', 'num_format':'YYYY-MM-DD'})
			worksheet.write_string(grand_total_row,1,'Grand Total',format_grand)
			
			#worksheet.conditional_format(formula_range_grand,2,grand_total_row,2,{"type":"no_blanks","format":centre_date_format_wb})
			
			for col in range(2,number_cols_daily+1):
				worksheet.write_string(writing_info_row,col,"",format_colour)
				worksheet.conditional_format(grand_total_row,col,grand_total_row,col,{"type":"blanks","format":format_grand})
				worksheet.conditional_format (grand_total_row, col, grand_total_row, col,
				                              {"type":"no_blanks", "format":format_grand})
				
			
			worksheet.write_formula(grand_total_row,3,'=SUMIF(B{}:B{},"Subtotal",D{}:D{})'.format(formula_range_grand,
			                                                                                      grand_total_row,
			                                                                                      formula_range_grand,
			                                                                                      grand_total_row),format_num)
			
			worksheet.write_formula(grand_total_row,4,'=SUMIF(B{}:B{},"Subtotal",E{}:E{})'.format(formula_range_grand,
			                                                                                      grand_total_row,formula_range_grand,
			                                                                                      grand_total_row),format_num)
			
			worksheet.write_formula (grand_total_row, 5,'=IFERROR(E{}/D{},0)'.format (grand_total_row+1,grand_total_row+1),percent_fmt)
			
			worksheet.write_formula(grand_total_row, 6,'=SUMIF(B{}:B{},"Sub-Total",G{}:G{})'.format(formula_range_grand,
			                                                                                        grand_total_row,
			                                                                                        formula_range_grand,
			                                                                                        grand_total_row),format_num)
			
			worksheet.write_formula(grand_total_row,7,'=SUMIF(B{}:B{},"Subtotal",H{}:H{})'.format(formula_range_grand,
			                                                                                      grand_total_row,
			                                                                                      formula_range_grand,
			                                                                                      grand_total_row),money_fmt)
			
			
			
			worksheet.set_column(1,1,45)
			worksheet.set_column(2,2,13,alignment_center)
			worksheet.set_column(3,4,20,alignment_right)
			worksheet.set_column(5,6,14,alignment_right)
			worksheet.set_column(7,7,21,alignment_right)
			worksheet.set_column(8,9,11,alignment_right)
			worksheet.set_column(10,17,15,alignment_right)
			
			
		except AttributeError as e:
			self.logger.error(str(e))
			pass
	
	def main(self):
		
		"""
Adding Main Function
		"""
		self.config.common_columns_summary()
		self.connect_TFR_daily()
		self.read_Query_daily()
		if self.read_sql_sales_mv.empty:
			self.logger.info("No Display placements for IO - {}".format(self.config.ioid))
			pass
		else:
			self.logger.info ("Display placements found for IO - {}".format (self.config.ioid))
			self.access_Data_KM_Sales_daily()
			self.KM_Sales_daily()
			self.rename_KM_Sales_daily()
			self.accessing_nan_values()
			self.accessing_main_column()
			self.write_KM_Sales_summary()
			#self.formatting_daily()
			self.logger.info("Display Sheet Created for IO {}".format(self.config.ioid))

if __name__=="__main__":
	pass
	
	#enable it when running for individual file
	#c = config.Config('test', 606087,'2018-01-02','2018-02-02')
	#o = Daily( c )
	#o.main()
	#c.saveAndCloseWriter()
