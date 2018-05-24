#coding=utf-8
"""
Created by:Dharmendra
Date:2018-03-23
"""
import datetime
import pandas as pd
import numpy as np
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
		sql_sales_summary = "select substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as "'Placement#'","\
		                    "CREATIVE_DESC  as "'Placement_Name'", COST_TYPE_DESC as "'Cost_type'",UNIT_COST as "\
		                    ""'Unit_Cost'", BOOKED_QTY as "'Booked_Imp#Booked_Eng'" FROM TFR_REP.SUMMARY_MV where "\
		                    "IO_ID = {} AND DATA_SOURCE = 'SalesFile' ORDER BY PLACEMENT_ID".format(
			self.config.ioid )
		
		sql_sales_mv = "select substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as "'Placement#'", "\
		               "sum(VIEWS) "\
		               "as "'Delivered_Impresion'", sum(CLICKS) as "'Clicks'", sum(CONVERSIONS) as "'Conversion'" "\
		               "from "\
		               ""\
		               ""\
		               ""\
		               ""\
		               ""\
		               ""\
		               ""\
		               ""\
		               "TFR_REP.DAILY_SALES_MV WHERE IO_ID = {} GROUP BY PLACEMENT_ID, PLACEMENT_DESC ORDER BY "\
		               "PLACEMENT_ID".format(
			self.config.ioid )
		
		sql_sales_adsize_mv = "select substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as "'Placement#'", "\
		                      "MEDIA_SIZE_DESC as "'Adsize'", sum(VIEWS) as "'Delivered_Impresion'", sum(CLICKS) as "\
		                      ""'Clicks'", sum(CONVERSIONS) as "'Conversion'" from TFR_REP.ADSIZE_SALES_MV WHERE "\
		                      "IO_ID "\
		                      ""\
		                      ""\
		                      ""\
		                      ""\
		                      ""\
		                      ""\
		                      ""\
		                      ""\
		                      "= {} GROUP BY PLACEMENT_ID,PLACEMENT_DESC, MEDIA_SIZE_DESC ORDER BY "\
		                      "PLACEMENT_ID".format( self.config.ioid )
		sql_sales_daily_mv = "select substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as "'Placement#'", "\
		                     "DAY_DESC as "'Day'", sum(VIEWS) as "'Delivered_Impresion'", sum(CLICKS) as "'Clicks'", "\
		                     ""\
		                     ""\
		                     ""\
		                     ""\
		                     ""\
		                     ""\
		                     ""\
		                     ""\
		                     "sum(CONVERSIONS) as "'Conversion'" from TFR_REP.DAILY_SALES_MV WHERE IO_ID = {} GROUP "\
		                     "BY "\
		                     ""\
		                     ""\
		                     ""\
		                     ""\
		                     ""\
		                     ""\
		                     ""\
		                     ""\
		                     "PLACEMENT_ID,PLACEMENT_DESC, DAY_DESC ORDER BY PLACEMENT_ID".format( self.config.ioid )
		
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
				"BOOKED_IMP#BOOKED_ENG":"Booked", "DELIVERED_IMPRESION":"Delivered Impressions"
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
		
		placement_sales_data = self.display_sales_first_table[["Placement# Name", "Cost Type", "Unit Cost", "Booked",
		                                                  "Delivered Impressions", "Clicks", "CTR",
		                                                  "Conversion",
		                                                  "Conversion Rate", "Spend", "eCPA"]]
		
		adsize_sales_data_new = self.adsize_sales_second_table.loc[:,
		                        ["Placement# Name", "Adsize", "Delivered Impressions", "Clicks",
		                         "CTR", "Conversion", "Conversion Rate", "Spend", "eCPA"]]
		
		adsize_sales_data_pivot = pd.pivot_table( adsize_sales_data_new, index=["Adsize"],
		                                          values=["Delivered Impressions",
		                                                  "Clicks", "Conversion",
		                                                  "Spend"], aggfunc=np.sum )
		
		adsize_sales_data_pivot_new = adsize_sales_data_pivot.reset_index()
		
		adsize_sales_data = None
		final_adsize = None
		try:
			adsize_sales_data_pivot_new["CTR"] = adsize_sales_data_pivot_new["Clicks"]/adsize_sales_data_pivot_new[
					"Delivered Impressions"]

			adsize_sales_data_pivot_new["Conversion Rate"] = adsize_sales_data_pivot_new["Conversion"]/\
			                                                 adsize_sales_data_pivot_new["Delivered Impressions"]

			adsize_sales_data_pivot_new["eCPA"] = adsize_sales_data_pivot_new["Spend"]/adsize_sales_data_pivot_new[
				"Conversion"]
		
			adsize_sales_data = adsize_sales_data_pivot_new[["Adsize", "Delivered Impressions", "Clicks", "CTR",
		                                                     "Conversion", "Conversion Rate", "Spend", "eCPA"]]
		
			final_adsize = adsize_sales_data[["Adsize", "Delivered Impressions", "Clicks", "CTR",
		                                          "Conversion", "Conversion Rate", "Spend", "eCPA"]]
		except KeyError as e:
			self.logger.error(str(e)+'Not found in adsize Display Placements Data')
			pass
		daily_sales_data = self.daily_sales_third_table.loc[:,
		                   ["Placement#", "Placement# Name", "Date", "Delivered Impressions",
		                    "Clicks", "CTR", "Conversion", "eCPA", "Spend",
		                    "Unit Cost"]]
		
		daily_sales_remove_zero = daily_sales_data[daily_sales_data['Delivered Impressions']==0]
		
		daily_sales_data = daily_sales_data.drop( daily_sales_remove_zero.index, axis=0 )
		
		# daily_sales_data [ "Date" ] = pd.to_datetime ( daily_sales_data [ "Date" ] )
		
		daily_sales_data['Date'] = pd.to_datetime( daily_sales_data['Date'] ).dt.date
		
		excel_start_date = datetime.date( 1899, 12, 30 )
		daily_sales_data['Date'] = daily_sales_data['Date']-excel_start_date
		
		try:
			daily_sales_data.Date = daily_sales_data.Date.dt.days
		except (KeyError,AttributeError) as e:
			self.logger.error(str(e)+'Not found in day wise data')
			pass
		final_day_wise = daily_sales_data.loc[:, ['Placement#', "Placement# Name", "Date",
		                                          "Delivered Impressions", "Clicks", "CTR",
		                                          "Conversion", "eCPA", "Spend"]]
		
		#return placement_sales_data, final_adsize, final_day_wise
		self.placement_sales_data = placement_sales_data
		self.final_adsize = final_adsize
		self.final_day_wise = final_day_wise
		
	def write_KM_Sales_summary(self):
		
		"""

		:return: writing Data
		"""
		data_common_columns = self.config.common_columns_summary()
		
		#placement_sales_data, final_adsize, final_day_wise = self.accessing_main_column()
		
		self.logger.info("Writing Summary on Display Sheet for IO - {}".format(self.config.ioid))
		writing_data_common_columns = data_common_columns[1].to_excel( self.config.writer,
		                                                               sheet_name="Performance Details".format
		                                                               ( self.config.ioid ), startcol=1,
		                                                               startrow=1,
		                                                               index=False, header=False )
		
		self.logger.info ("Writing Placement level information on Display Sheet for IO - {}".format (self.config.ioid))
		try:
			check_placement_sales_data = self.placement_sales_data.empty
		
			if check_placement_sales_data is True:
				pass
			else:
				writing_placement_data = self.placement_sales_data.to_excel( self.config.writer,
				                                                        sheet_name="Performance Details".format(
					                                                        self.config.ioid ),
				                                                        startcol=1, startrow=8, index=False,
				                                                        header=True )
			
			self.logger.info (
				"Writing ad size level information on Display Sheet for IO - {}".format (self.config.ioid))
			check_adsize_sales_data = self.final_adsize.empty
			if check_adsize_sales_data is True:
				pass
			else:
				writing_adsize_data = self.final_adsize.to_excel( self.config.writer,
				                                             sheet_name="Performance Details".format(
					                                             self.config.ioid ),
				                                             startcol=1, startrow=len( self.placement_sales_data )+13,
				                                             index=False,
				                                             header=True )
		
			check_daily_sales_data = self.final_day_wise.empty
			
			self.logger.info (
				"Writing Placement by day level information on Display Sheet for IO - {}".format (self.config.ioid))
		
			if check_daily_sales_data is True:
				pass
			else:
				
				startline = len( self.placement_sales_data )+len( self.final_adsize )+18
				startRow = startline
				endRow = 0
				
				for placement, placement_df in self.final_day_wise.groupby( 'Placement# Name', as_index=False ):
					writing_daily_data = placement_df.to_excel( self.config.writer,
					                                            sheet_name="Performance Details".format(
						                                            self.config.ioid ), encoding='UTF-8',
					                                            startcol=1,
					                                            startrow=startline, columns=["Placement# Name"],
					                                            index=False,
					                                            header=False, merge_cells=False )
					
					writing_daily_data_new = placement_df.to_excel( self.config.writer,
					                                                sheet_name="Performance Details".format(
						                                                self.config.ioid ), startcol=1,
					                                                startrow=startline+1,
					                                                columns=["Date", "Delivered Impressions",
					                                                         "Clicks", "CTR",
					                                                         "Conversion", "eCPA", "Spend"],
					                                                index=False, header=True, merge_cells=False )
					
					workbook = self.config.writer.book
					worksheet = self.config.writer.sheets["Performance Details".format( self.config.ioid )]
					startline += len( placement_df )+2
					# add Sub Total
					worksheet.write_string( startline, 1, 'Subtotal' )
					
					startRow = startline-len( placement_df )+1
					worksheet.write_formula( startline, 2, '=sum(C{}:C{})'.format( startRow, startline ) )
					worksheet.write_formula( startline, 3, '=sum(D{}:D{})'.format( startRow, startline ) )
					worksheet.write_formula( startline, 5, '=sum(F{}:F{})'.format( startRow, startline ) )
					worksheet.write_formula( startline, 4,
					                         '=IFERROR(sum(D{}:D{})/sum(C{}:C{}),0)'.format( startRow, startline, startRow,
					                                                                         startline ) )
					worksheet.write_formula( startline, 6,
					                         '=IFERROR(sum(H{}:H{})/sum(F{}:F{}),0)'.format( startRow, startline, startRow,
					                                                                         startline ) )
					
					worksheet.write_formula( startline, 7,
					                         '=sum(H{}:H{})'.format( startRow, startline ) )
					
					endRow = startline
					
					column_chart = workbook.add_chart( {'type':'column'} )
					column_x = "='Performance Details'!B{}:B{}".format(startRow, endRow )
					column_y = "='Performance Details'!C{}:C{}".format(startRow, endRow )
					column_chart.add_series( {
						'categories':column_x
						, 'values':column_y, 'name':'Impressions'
						} )
					
					column_chart.set_title( {'name':'Impressions vs CTR'} )
					
					line_chart = workbook.add_chart( {'type':'line'} )
					
					line_x = "='Performance Details'!B{}:B{}".format(startRow, endRow )
					line_y = "='Performance Details'!E{}:E{}".format(startRow, endRow )
					
					line_chart.add_series( {'categories':line_x, 'values':line_y, 'name':'CTR', 'y2_axis':True} )
					
					column_chart.combine( line_chart )
					
					column_chart.set_size( {'width':800} )
					worksheet.insert_chart( 'J{}'.format( startRow ), column_chart )
					column_chart.set_y_axis( {'num_format':'#,##0'} )
					line_chart.set_y2_axis( {'num_format':'0.00%'} )
					
					format_border_left = workbook.add_format( {"left":2} )
					format_border_right = workbook.add_format( {"right":2} )
					format_border_top = workbook.add_format( {"top":2} )
					format_border_bottom = workbook.add_format( {"bottom":2} )
					
					"""worksheet.conditional_format( startRow, 1, endRow, 1,
												  {"type":"no_blanks", "format":format_border_left} )"""
					
					format_placement_subtotal = workbook.add_format( {"bold":True, "bg_color":'#E7E6E6'} )
					worksheet.conditional_format( endRow, 1, endRow, 7,
					                              {"type":"no_blanks", "format":format_placement_subtotal} )
					
					worksheet.conditional_format( endRow+3, 1, endRow+3, 7,
					                              {"type":"no_blanks", "format":format_placement_subtotal} )
					
					worksheet.conditional_format( endRow+3, 1, endRow+3, 7,
					                              {"type":"blanks", "format":format_placement_subtotal} )
					
					worksheet.conditional_format( startRow-3, 0, endRow, 0,
					                              {"type":"blanks", "format":format_border_right} )
					
					worksheet.conditional_format( startRow-3, 8, endRow, 8,
					                              {"type":"blanks", "format":format_border_left} )
					
					worksheet.conditional_format( endRow+1, 1, endRow+1, 7,
					                              {"type":"blanks", "format":format_border_top} )
					
					worksheet.conditional_format( startRow-4, 1, startRow-4, 7,
					                              {"type":"blanks", "format":format_border_bottom} )
					"""worksheet.conditional_format(startRow - 3, 1, startRow - 3, 7,
												 {"type": "blanks", "format": format_border_top})"""
					
					startline += 3
					
		except AttributeError as e:
			pass
			
		#return placement_sales_data, final_adsize, final_day_wise
		#self.placement_sales_data = placement_sales_data
	def formatting_daily(self):
		
		"""
Applying formatting on Display Sheet
		"""
		#placement_sales_data, final_adsize, final_day_wise = self.write_KM_Sales_summary()
		
		self.logger.info('Applying Formatting on each label of Display sheet - {}'.format(self.config.ioid))
		try:
			workbook = self.config.writer.book
			worksheet = self.config.writer.sheets["Performance Details".format( self.config.ioid )]
			
			unqiue_final_day_wise = self.final_day_wise['Placement# Name'].nunique()
			
			data_common_columns = self.config.common_columns_summary()
			
			number_rows_placement = self.placement_sales_data.shape[0]
			number_cols_placement = self.placement_sales_data.shape[1]
			number_rows_adsize = self.final_adsize.shape[0]
			number_cols_adsize = self.final_adsize.shape[1]
			number_rows_daily = self.final_day_wise.shape[0]
			number_cols_daily = self.final_day_wise.shape[1]
			
			worksheet.hide_gridlines( 2 )
			worksheet.set_row( 0, 6 )
			worksheet.set_column( "A:A", 2 )
			
			alignment = workbook.add_format( {"align":"center"} )
			
			check_placement_sales_data = self.placement_sales_data.empty
			check_adsize_sales_data = self.final_adsize.empty
			check_daily_sales_data = self.final_day_wise.empty
			
			
			worksheet.insert_image( "O6", "Exponential.png", {"url":"https://www.tribalfusion.com"} )
			worksheet.insert_image( "O2", "Client_Logo.png" )
			
			# column b2 to O5 formatting
			#format_campaign_info = workbook.add_format( {"bg_color":'#F0F8FF', "align":"left"} )
			format_campaign_info = workbook.add_format ({"bold":True, "bg_color":'#00B0F0', "align":"left"})
			# column headers formatting
			format_col = workbook.add_format(
				{"bg_color":'#E7E6E6', "bold":True, "align":"center", "bottom":2, "top":2} )
			
			format_left_col = workbook.add_format( {"left":2} )
			format_right_col = workbook.add_format( {"right":2} )
			format_bottom_col = workbook.add_format( {"bottom":2} )
			format_top_col = workbook.add_format( {"top":2} )
			
			# values of subtotal and last column needs to be colour
			format_last_row = workbook.add_format( {"bg_color":'#E7E6E6', "bold":True, "align":"center"} )
			
			# format the placement by date table
			
			format_placement_by_date_header = workbook.add_format(
				{"bg_color":'#595959', 'font_color':'#FFFFFF', "bold":True, "align":"center"} )
			
			format_sub = workbook.add_format( {"bold":True, "bg_color":'#E7E6E6'} )
			format_subtotal = workbook.add_format( {"bold":True} )
			format_total = workbook.add_format( {"bg_color":'#E7E6E6', "bold":True} )
			
			percent_fmt = workbook.add_format( {"num_format":"0.00%", "align":"center"} )
			money_fmt = workbook.add_format( {"num_format":"$#,###0.00", "align":"center"} )
			
			format_number = workbook.add_format( {"num_format":"#,##0", "align":"center"} )
			date_format = workbook.add_format( {'num_format':'YYYY-MM-DD', "align":"left"} )
			
			# formatting campaign info
			#worksheet.conditional_format( "B2:L5", {"type":"blanks", "format":format_campaign_info} )
			#worksheet.conditional_format( "B2:L5", {"type":"no_blanks", "format":format_campaign_info} )
			worksheet.conditional_format ("A1:R5", {"type":"blanks", "format":format_campaign_info})
			worksheet.conditional_format ("A1:R5", {"type":"no_blanks", "format":format_campaign_info})
			
			# adding formula in bottom rows:
			if check_placement_sales_data is True:
				pass
			else:
				worksheet.write_formula( number_rows_placement+9, 4,
				                         '=sum(E{}:E{})'.format( 10, number_rows_placement+9 ), format_number )
				
				worksheet.write_formula( number_rows_placement+9, 5,
				                         '=sum(F{}:F{})'.format( 10, number_rows_placement+9 ), format_number )
				
				worksheet.write_formula( number_rows_placement+9, 6,
				                         '=sum(G{}:G{})'.format( 10, number_rows_placement+9 ), format_number )
				
				worksheet.write_formula( number_rows_placement+9, 7,
				                         '=IFERROR(sum(G{}:G{})/sum(F{}:F{}),0)'.format( 10,
				                                                                         number_rows_placement+9, 10,
				                                                                         number_rows_placement+9 ),
				                         percent_fmt )
				
				worksheet.write_formula( number_rows_placement+9, 8,
				                         '=sum(I{}:I{})'.format( 10, number_rows_placement+9 ), format_number )
				
				worksheet.write_formula( number_rows_placement+9, 9,
				                         '=IFERROR(sum(I{}:I{})/sum(F{}:F{}),0)'.format( 10, number_rows_placement+9,
				                                                                         10,
				                                                                         number_rows_placement+9 ),
				                         percent_fmt )
				
				worksheet.write_formula( number_rows_placement+9, 10,
				                         '=sum(K{}:K{})'.format( 10, number_rows_placement+9 ), money_fmt )
				
				worksheet.write_formula( number_rows_placement+9, 11,
				                         '=IFERROR(sum(K{}:K{})/sum(I{}:I{}),0)'.format( 10, number_rows_placement+9,
				                                                                         10, number_rows_placement+9 )
				                         , money_fmt )
			
			if check_adsize_sales_data is True:
				pass
			else:
				worksheet.write_formula( number_rows_placement+number_rows_adsize+14, 2,
				                         '=sum(C{}:C{})'.format( number_rows_placement+15,
				                                                 number_rows_placement+number_rows_adsize+14 ),
				                         format_number )
				
				worksheet.write_formula( number_rows_placement+number_rows_adsize+14, 3,
				                         '=sum(D{}:D{})'.format( number_rows_placement+15,
				                                                 number_rows_placement+number_rows_adsize+14 ),
				                         format_number )
				worksheet.write_formula( number_rows_placement+number_rows_adsize+14, 4,
				                         '=IFERROR(sum(D{}:D{})/sum(C{}:C{}),0)'.format( number_rows_placement+15,
				                                                                         number_rows_placement+
				                                                                         number_rows_adsize+14,
				                                                                         number_rows_placement+15,
				                                                                         number_rows_placement+
				                                                                         number_rows_adsize+14 ),
				                         percent_fmt )
				
				worksheet.write_formula( number_rows_placement+number_rows_adsize+14, 5,
				                         '=sum(F{}:F{})'.format( number_rows_placement+15,
				                                                 number_rows_placement+number_rows_adsize+14 ),
				                         format_number )
				worksheet.write_formula( number_rows_placement+number_rows_adsize+14, 6,
				                         '=IFERROR(sum(F{}:F{})/sum(C{}:C{}),0)'.format( number_rows_placement+15,
				                                                                         number_rows_placement+
				                                                                         number_rows_adsize+14,
				                                                                         number_rows_placement+15,
				                                                                         number_rows_placement+
				                                                                         number_rows_adsize+14 ),
				                         percent_fmt )
				worksheet.write_formula( number_rows_placement+number_rows_adsize+14, 7,
				                         '=sum(H{}:H{})'.format( number_rows_placement+15,
				                                                 number_rows_placement+number_rows_adsize+14 ),
				                         money_fmt )
				
				worksheet.write_formula( number_rows_placement+number_rows_adsize+14, 8,
				                         '=IFERROR(sum(H{}:H{})/sum(F{}:F{}),0)'.format( number_rows_placement+15,
				                                                                         number_rows_placement+
				                                                                         number_rows_adsize+14,
				                                                                         number_rows_placement+15,
				                                                                         number_rows_placement+
				                                                                         number_rows_adsize+14 ),
				                         money_fmt )
			
			# Colour formatting for Columns
			if check_placement_sales_data is True:
				pass
			else:
				worksheet.conditional_format( 8, 1, 8, number_cols_placement+1,
				                              {"type":"no_blanks", "format":format_col} )
			
			if check_adsize_sales_data is True:
				pass
			else:
				worksheet.conditional_format( number_rows_placement+13, 1, number_rows_placement+13,
				                              number_cols_adsize+1,
				                              {"type":"no_blanks", "format":format_col} )
			
			# Values_for_daily = 'Date','Delivered Impressions','Clicks','CTR','Conversion','eCPA','Spend'
			
			if check_daily_sales_data is True:
				pass
			else:
				worksheet.conditional_format( number_rows_placement+number_rows_adsize+19, 1,
				                              number_rows_placement+number_rows_adsize+19,
				                              number_cols_daily+1,
				                              {"type":"no_blanks", "format":format_placement_by_date_header} )
				
				worksheet.conditional_format( number_rows_placement+number_rows_adsize+20, 1,
				                              number_rows_placement+number_rows_adsize+number_rows_daily+
				                              unqiue_final_day_wise*5+15, number_cols_daily-2,
				                              {
					                              "type":"text", 'criteria':'containing', 'value':'Date',
					                              'format':format_placement_by_date_header
					                              } )
				
				worksheet.conditional_format( number_rows_placement+number_rows_adsize+20, 1,
				                              number_rows_placement+number_rows_adsize+number_rows_daily+
				                              unqiue_final_day_wise*5+15,
				                              number_cols_daily-2, {
					                              "type":"text", 'criteria':'containing',
					                              'value':'Delivered Impressions',
					                              'format':format_placement_by_date_header
					                              } )
				
				worksheet.conditional_format( number_rows_placement+number_rows_adsize+20, 1,
				                              number_rows_placement+number_rows_adsize+number_rows_daily+
				                              unqiue_final_day_wise*5+15,
				                              number_cols_daily-2, {
					                              "type":"text", 'criteria':'containing',
					                              'value':'Clicks', 'format':format_placement_by_date_header
					                              } )
				
				worksheet.conditional_format( number_rows_placement+number_rows_adsize+20, 1,
				                              number_rows_placement+number_rows_adsize+number_rows_daily+
				                              unqiue_final_day_wise*5+15,
				                              number_cols_daily-2, {
					                              "type":"text", 'criteria':'containing',
					                              'value':'CTR',
					                              'format':format_placement_by_date_header
					                              } )
				
				worksheet.conditional_format( number_rows_placement+number_rows_adsize+20, 1,
				                              number_rows_placement+number_rows_adsize+number_rows_daily+
				                              unqiue_final_day_wise*5+15,
				                              number_cols_daily-2, {
					                              "type":"text", 'criteria':'containing',
					                              'value':'Conversion',
					                              'format':format_placement_by_date_header
					                              } )
				
				worksheet.conditional_format( number_rows_placement+number_rows_adsize+20, 1,
				                              number_rows_placement+number_rows_adsize+number_rows_daily+
				                              unqiue_final_day_wise*5+15,
				                              number_cols_daily-2, {
					                              "type":"text", 'criteria':'containing',
					                              'value':'eCPA',
					                              'format':format_placement_by_date_header
					                              } )
				worksheet.conditional_format( number_rows_placement+number_rows_adsize+20, 1,
				                              number_rows_placement+number_rows_adsize+number_rows_daily+
				                              unqiue_final_day_wise*5+15,
				                              number_cols_daily-2, {
					                              "type":"text", 'criteria':'containing',
					                              'value':'Spend',
					                              'format':format_placement_by_date_header
					                              } )
			
			# Money and Percet Formatting
			if check_placement_sales_data is True:
				pass
			else:
				
				worksheet.conditional_format( 8, 3, number_rows_placement+8, 3,
				                              {"type":"no_blanks", "format":money_fmt} )
				
				worksheet.conditional_format( 8, 10, number_rows_placement+8, 11,
				                              {"type":"no_blanks", "format":money_fmt} )
				
				worksheet.conditional_format( 8, 7, number_rows_placement+8, 7,
				                              {"type":"no_blanks", "format":percent_fmt} )
				
				worksheet.conditional_format( 8, 9, number_rows_placement+8, 9,
				                              {"type":"no_blanks", "format":percent_fmt} )
				
				worksheet.conditional_format( 8, 4, number_rows_placement+8, 6,
				                              {"type":"no_blanks", "format":format_number} )
				worksheet.conditional_format( 8, 8, number_rows_placement+8, 8,
				                              {"type":"no_blanks", "format":format_number} )
			
			if check_adsize_sales_data is True:
				pass
			else:
				
				# Value = final_adsize.loc[final_adsize["Placement# Name"] == "Total"]
				
				worksheet.conditional_format( number_rows_placement+12, 2,
				                              number_rows_placement+number_rows_adsize+13, 3,
				                              {"type":"no_blanks", "format":format_number} )
				
				worksheet.conditional_format( number_rows_placement+12, 4,
				                              number_rows_placement+number_rows_adsize+13, 4,
				                              {"type":"no_blanks", "format":percent_fmt} )
				
				worksheet.conditional_format( number_rows_placement+12, 5,
				                              number_rows_placement+number_rows_adsize+13, 5,
				                              {"type":"no_blanks", "format":format_number} )
				
				worksheet.conditional_format( number_rows_placement+12, 6,
				                              number_rows_placement+number_rows_adsize+13, 6,
				                              {"type":"no_blanks", "format":percent_fmt} )
				
				worksheet.conditional_format( number_rows_placement+12, 7,
				                              number_rows_placement+number_rows_adsize+13, 8,
				                              {"type":"no_blanks", "format":money_fmt} )
			
			if check_daily_sales_data is True:
				pass
			else:
				worksheet.conditional_format( number_rows_placement+number_rows_adsize+20, 2,
				                              number_rows_placement+number_rows_adsize+number_rows_daily+
				                              unqiue_final_day_wise*5+15,
				                              2, {"type":"no_blanks", "format":format_number} )
				
				worksheet.conditional_format( number_rows_placement+number_rows_adsize+20, 3,
				                              number_rows_placement+number_rows_adsize+number_rows_daily+
				                              unqiue_final_day_wise*5+15,
				                              3, {"type":"no_blanks", "format":format_number} )
				
				worksheet.conditional_format( number_rows_placement+number_rows_adsize+20, 4,
				                              number_rows_placement+number_rows_adsize+number_rows_daily+
				                              unqiue_final_day_wise*5+15,
				                              4, {"type":"no_blanks", "format":percent_fmt} )
				
				worksheet.conditional_format( number_rows_placement+number_rows_adsize+20, 5,
				                              number_rows_placement+number_rows_adsize+number_rows_daily+
				                              unqiue_final_day_wise*5+15,
				                              5, {"type":"no_blanks", "format":format_number} )
				
				worksheet.conditional_format( number_rows_placement+number_rows_adsize+20, 6,
				                              number_rows_placement+number_rows_adsize+number_rows_daily+
				                              unqiue_final_day_wise*5+15,
				                              7, {"type":"no_blanks", "format":money_fmt} )
			
			# addting subtotal and adding formatting for subtotal
			if check_placement_sales_data is True:
				pass
			else:
				worksheet.write( number_rows_placement+9, 1, "Subtotal", format_subtotal )
			
			if check_adsize_sales_data is True:
				pass
			else:
				worksheet.write( number_rows_placement+number_rows_adsize+14, 1, "Subtotal",
				                 format_subtotal )
			
			if check_placement_sales_data is True:
				pass
			else:
				worksheet.conditional_format( number_rows_placement+10, 1, number_rows_placement+10,
				                              number_cols_placement,
				                              {"type":"blanks", "format":format_top_col} )
				
				"""worksheet.conditional_format(number_rows_placement + 9, 1, number_rows_placement + 9,
											 number_cols_placement,
											 {"type": "blanks", "format": format_bottom_col})"""
				
				worksheet.conditional_format( number_rows_placement+9, 1, number_rows_placement+9,
				                              number_cols_placement,
				                              {"type":"no_blanks", "format":format_sub} )
				
				worksheet.conditional_format( number_rows_placement+9, 1, number_rows_placement+9,
				                              number_cols_placement,
				                              {"type":"blanks", "format":format_sub} )
				
				worksheet.conditional_format( 8, 0, number_rows_placement+9, 0,
				                              {"type":"blanks", "format":format_right_col} )
				
				worksheet.conditional_format( 8, number_cols_placement+1, number_rows_placement+9,
				                              number_cols_placement+1,
				                              {"type":"blanks", "format":format_left_col} )
			
			if check_adsize_sales_data is True:
				pass
			else:
				worksheet.conditional_format( number_rows_placement+13, 0,
				                              number_rows_placement+number_rows_adsize+14, 0,
				                              {"type":"blanks", "format":format_right_col} )
				
				worksheet.conditional_format( number_rows_placement+13, number_cols_adsize+1,
				                              number_rows_placement+number_rows_adsize+14,
				                              number_cols_adsize+1, {"type":"blanks", "format":format_left_col} )
				
				worksheet.conditional_format( number_rows_placement+number_rows_adsize+14, 1,
				                              number_rows_placement+number_rows_adsize+14,
				                              number_cols_adsize, {"type":"no_blanks", "format":format_sub} )
				
				worksheet.conditional_format( number_rows_placement+number_rows_adsize+15, 1,
				                              number_rows_placement+number_rows_adsize+15, number_cols_adsize,
				                              {"type":"blanks", "format":format_top_col} )
			
			# Merge Row formatting
			format_merge_row = workbook.add_format( {
				"bold":True, "font_color":'#FFFFFF', "align":"centre",
				"fg_color":"#00B0F0", "border":2, "border_color":"#000000"
				} )
			
			format_merge_row_black = workbook.add_format( {
				"bold":True, "font_color":'#000000', "align":"centre",
				"fg_color":"#00B0F0", "border":2, "border_color":"#000000"
				} )
			
			if check_placement_sales_data is True:
				pass
			else:
				worksheet.merge_range( 7, 1, 7, number_cols_placement,
				                       "Standard Banner Performance - Summary", format_merge_row )
			
			if check_adsize_sales_data is True:
				pass
			else:
				worksheet.merge_range( number_rows_placement+12, 1, number_rows_placement+12, number_cols_adsize,
				                       "Standard Banner Performance - Ad Size Summary", format_merge_row )
			
			if check_daily_sales_data is True:
				pass
			else:
				worksheet.merge_range( number_rows_placement+number_rows_adsize+17, 1,
				                       number_rows_placement+number_rows_adsize+17, number_cols_daily-2,
				                       "Breakdown By Day + Placement", format_merge_row_black )
				worksheet.conditional_format( number_rows_placement+number_rows_adsize+18, 1,
				                              number_rows_placement+number_rows_adsize+19, number_cols_daily-2,
				                              {"type":"blanks", "format":format_sub} )
				worksheet.conditional_format( number_rows_placement+number_rows_adsize+18, 1,
				                              number_rows_placement+number_rows_adsize+19, number_cols_daily-2,
				                              {"type":"no_blanks", "format":format_sub} )
			
			# adding Grand Total
			if check_daily_sales_data is True:
				pass
			else:
				row_start = number_rows_placement+number_rows_adsize+21
				row_end = number_rows_placement+number_rows_adsize+number_rows_daily+unqiue_final_day_wise*5+18
				
				worksheet.write(
					row_end, 1,
					"Grand Total", format_subtotal )
				
				worksheet.write_formula(
					row_end,
					2, '=SUMIF(B{}:B{},"Subtotal",C{}:C{})'.format( row_start, row_end, row_start, row_end ),
					format_number )
				
				worksheet.write_formula( row_end,
				                         3, '=SUMIF(B{}:B{},"Subtotal",D{}:D{})'.format( row_start, row_end, row_start,
				                                                                         row_end ), format_number )
				
				worksheet.write_formula( row_end,
				                         4, '=D{}/C{}'.format( row_end+1, row_end+1 ), percent_fmt )
				
				worksheet.write_formula( row_end, 5, '=SUMIF(B{}:B{},"Subtotal",F{}:F{})'.format( row_start,
				                                                                                  row_end, row_start,
				                                                                                  row_end ),
				                         format_number )
				
				worksheet.write_formula( row_end, 7, '=SUMIF(B{}:B{},"Subtotal",H{}:H{})'.format( row_start, row_end,
				                                                                                  row_start, row_end ),
				                         money_fmt )
				
				worksheet.write_formula( row_end, 6, '=H{}/F{}'.format( row_end+1, row_end+1 ), money_fmt )
				
				worksheet.conditional_format( row_end, 1, row_end, 7, {"type":"no_blanks", "format":format_sub} )
				worksheet.conditional_format( row_end, 0, row_end, 0, {"type":"blanks", "format":format_right_col} )
				worksheet.conditional_format( row_end, 8, row_end, 8, {"type":"blanks", "format":format_left_col} )
				worksheet.conditional_format( row_end-1, 1, row_end-1, 7, {"type":"blanks", "format":format_bottom_col} )
				worksheet.conditional_format( row_end+1, 1, row_end+1, 7, {"type":"blanks", "format":format_top_col} )
			
			worksheet.set_column( "B:B", 25, date_format )
			worksheet.set_column( "C:L", 21, alignment )
			worksheet.set_zoom( 75 )
			
		except AttributeError as e:
			self.logger.info(str(e)+ ' Not found')
			pass
	
	def main(self):
		
		"""
Adding Main Function
		"""
		self.config.common_columns_summary()
		self.connect_TFR_daily()
		self.read_Query_daily()
		if self.read_sql_sales.empty:
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
			self.formatting_daily()
			self.logger.info("Display Sheet Created for IO {}".format(self.config.ioid))

if __name__=="__main__":
	pass
	
	# enable it when running for individual file
	#c = config.Config('test', 573967)
	#o = Daily( c )
	#o.main()
	#c.saveAndCloseWriter()
