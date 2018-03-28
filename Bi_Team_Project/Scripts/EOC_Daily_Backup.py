import pandas as pd
import numpy as np
import config


class Daily ( ) :
	def __init__ ( self , config ) :
		self.config = config
	
	def connect_TFR_daily ( self ) :
		sql_sales_summary = "select substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as "'Placement#'"," \
		                    "CREATIVE_DESC  as "'Placement_Name'", COST_TYPE_DESC as "'Cost_type'",UNIT_COST as " \
		                    ""'Unit_Cost'", BOOKED_QTY as "'Booked_Imp#Booked_Eng'" FROM TFR_REP.SUMMARY_MV where " \
		                    "IO_ID = {} AND DATA_SOURCE = 'SalesFile' ORDER BY PLACEMENT_ID".format (
			self.config.IO_ID )
		sql_sales_mv = "select substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as "'Placement#'", " \
		               "sum(VIEWS) " \
		               "as "'Delivered_Impresion'", sum(CLICKS) as "'Clicks'", sum(CONVERSIONS) as "'Conversion'" from " \
		               "" \
		               "TFR_REP.DAILY_SALES_MV WHERE IO_ID = {} GROUP BY PLACEMENT_ID, PLACEMENT_DESC ORDER BY " \
		               "PLACEMENT_ID".format (
			self.config.IO_ID )
		sql_sales_adsize_mv = "select substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as "'Placement#'", " \
		                      "MEDIA_SIZE_DESC as "'Adsize'", sum(VIEWS) as "'Delivered_Impresion'", sum(CLICKS) as " \
		                      ""'Clicks'", sum(CONVERSIONS) as "'Conversion'" from TFR_REP.ADSIZE_SALES_MV WHERE IO_ID " \
		                      "" \
		                      "= {} GROUP BY PLACEMENT_ID,PLACEMENT_DESC, MEDIA_SIZE_DESC ORDER BY " \
		                      "PLACEMENT_ID".format (
			self.config.IO_ID )
		sql_sales_daily_mv = "select substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as "'Placement#'", " \
		                     "DAY_DESC as "'Day'", sum(VIEWS) as "'Delivered_Impresion'", sum(CLICKS) as "'Clicks'", " \
		                     "" \
		                     "sum(CONVERSIONS) as "'Conversion'" from TFR_REP.DAILY_SALES_MV WHERE IO_ID = {} GROUP BY " \
		                     "" \
		                     "PLACEMENT_ID,PLACEMENT_DESC, DAY_DESC ORDER BY PLACEMENT_ID".format (
			self.config.IO_ID )
		
		return sql_sales_summary , sql_sales_mv , sql_sales_adsize_mv , sql_sales_daily_mv
	
	def read_Query_daily ( self ) :
		sql_sales_summary , sql_sales_mv , sql_sales_adsize_mv , sql_sales_daily_mv = self.connect_TFR_daily ( )
		
		read_sql_sales = pd.read_sql ( sql_sales_summary , self.config.conn )
		read_sql_sales_mv = pd.read_sql ( sql_sales_mv , self.config.conn )
		read_sql_adsize_mv = pd.read_sql ( sql_sales_adsize_mv , self.config.conn )
		read_sql_daily_mv = pd.read_sql ( sql_sales_daily_mv , self.config.conn )
		
		return read_sql_sales , read_sql_sales_mv , read_sql_adsize_mv , read_sql_daily_mv
	
	def access_Data_KM_Sales_daily ( self ) :
		
		read_sql_sales , read_sql_sales_mv , read_sql_adsize_mv , read_sql_daily_mv = self.read_Query_daily ( )
		
		standard_sales_first_table = read_sql_sales.merge ( read_sql_sales_mv , on = "PLACEMENT#" , how = "inner" )
		display_sales_first_table = standard_sales_first_table [
			[ "PLACEMENT#" , "PLACEMENT_NAME" , "COST_TYPE" , "UNIT_COST" ,
			  "BOOKED_IMP#BOOKED_ENG" , "DELIVERED_IMPRESION" , "CLICKS" ,
			  "CONVERSION" ] ]
		
		standard_sales_second_table = read_sql_sales.merge ( read_sql_adsize_mv , on = "PLACEMENT#" , how = "inner" )
		adsize_sales_second_table = standard_sales_second_table [
			[ "PLACEMENT#" , "PLACEMENT_NAME" , "COST_TYPE" , "UNIT_COST" ,
			  "BOOKED_IMP#BOOKED_ENG" , "ADSIZE" , "DELIVERED_IMPRESION" ,
			  "CLICKS" , "CONVERSION" ] ]
		
		standard_sales_third_table = read_sql_sales.merge ( read_sql_daily_mv , on = "PLACEMENT#" , how = "inner" )
		daily_sales_third_table = standard_sales_third_table [ [ "PLACEMENT#" , "PLACEMENT_NAME" , "COST_TYPE" ,
		                                                         "UNIT_COST" , "BOOKED_IMP#BOOKED_ENG" , "DAY" ,
		                                                         "DELIVERED_IMPRESION" , "CLICKS" , "CONVERSION" ] ]
		
		return display_sales_first_table , adsize_sales_second_table , daily_sales_third_table
	
	def KM_Sales_daily ( self ) :
		
		display_sales_first_table , adsize_sales_second_table , daily_sales_third_table = \
			self.access_Data_KM_Sales_daily ( )
		
		display_sales_first_table [ "PLACEMENTNAME" ] = display_sales_first_table [
			[ "PLACEMENT#" , "PLACEMENT_NAME" ] ].apply (
			lambda x : ".".join ( x ) , axis = 1 )
		
		adsize_sales_second_table [ "PLACEMENTNAME" ] = adsize_sales_second_table [
			[ "PLACEMENT#" , "PLACEMENT_NAME" ] ].apply (
			lambda x : ".".join ( x ) , axis = 1 )
		
		daily_sales_third_table [ "PLACEMENTNAME" ] = daily_sales_third_table [
			[ "PLACEMENT#" , "PLACEMENT_NAME" ] ].apply (
			lambda x : ".".join ( x ) , axis = 1 )
		
		choices_display_CTR = display_sales_first_table [ "CLICKS" ] / display_sales_first_table [
			"DELIVERED_IMPRESION" ]
		choices_display_Conversion = display_sales_first_table [ "CONVERSION" ] / display_sales_first_table [
			"DELIVERED_IMPRESION" ]
		choices_display_spend = display_sales_first_table [ "DELIVERED_IMPRESION" ] / 1000 * \
		                        display_sales_first_table [
			                        "UNIT_COST" ]
		choices_display_ecpa = (display_sales_first_table [ "DELIVERED_IMPRESION" ] / 1000 *
		                        display_sales_first_table [
			                        "UNIT_COST" ]) / display_sales_first_table [ "CONVERSION" ]
		
		mask1 = display_sales_first_table [ "COST_TYPE" ].isin ( [ 'CPM' ] )
		
		display_sales_first_table [ "CTR%" ] = np.select ( [ mask1 ] , [ choices_display_CTR ] , default = 0.00 )
		display_sales_first_table [ "CONVERSIONRATE%" ] = np.select ( [ mask1 ] , [ choices_display_Conversion ] ,
		                                                              default = 0.00 )
		display_sales_first_table [ "SPEND" ] = np.select ( [ mask1 ] , [ choices_display_spend ] , default = 0.00 )
		display_sales_first_table [ "EPCA" ] = np.select ( [ mask1 ] , [ choices_display_ecpa ] , default = 0.00 )
		
		choices_adsize_CTR = adsize_sales_second_table [ "CLICKS" ] / adsize_sales_second_table [
			"DELIVERED_IMPRESION" ]
		choices_adsize_conversion = adsize_sales_second_table [ "CONVERSION" ] / adsize_sales_second_table [
			"DELIVERED_IMPRESION" ]
		choices_adsize_spend = adsize_sales_second_table [ "DELIVERED_IMPRESION" ] / 1000 * adsize_sales_second_table [
			"UNIT_COST" ]
		choices_adsize_ecpa = (choices_adsize_spend) / (adsize_sales_second_table [ "CONVERSION" ])
		
		mask2 = adsize_sales_second_table [ "COST_TYPE" ].isin ( [ "CPM" ] )
		
		adsize_sales_second_table [ "CTR%" ] = np.select ( [ mask2 ] , [ choices_adsize_CTR ] , default = 0.00 )
		adsize_sales_second_table [ "CONVERSIONRATE%" ] = np.select ( [ mask2 ] , [ choices_adsize_conversion ] ,
		                                                              default = 0.00 )
		adsize_sales_second_table [ "SPEND" ] = np.select ( [ mask2 ] , [ choices_adsize_spend ] , default = 0.00 )
		adsize_sales_second_table [ "ECPA" ] = np.select ( [ mask2 ] , [ choices_adsize_ecpa ] , default = 0.00 )
		
		choice_daily_CTR = daily_sales_third_table [ "CLICKS" ] / daily_sales_third_table [ "DELIVERED_IMPRESION" ]
		choice_daily_spend = daily_sales_third_table [ "DELIVERED_IMPRESION" ] / 1000 * daily_sales_third_table [
			"UNIT_COST" ]
		choice_daily_CPA = (daily_sales_third_table [ "DELIVERED_IMPRESION" ] / 1000 * daily_sales_third_table [
			"UNIT_COST" ]) / \
		                   daily_sales_third_table [ "CONVERSION" ]
		
		mask3 = daily_sales_third_table [ "COST_TYPE" ].isin ( [ "CPM" ] )
		
		daily_sales_third_table [ "CTR%" ] = np.select ( [ mask3 ] , [ choice_daily_CTR ] , default = 0.00 )
		daily_sales_third_table [ "SPEND" ] = np.select ( [ mask3 ] , [ choice_daily_spend ] , default = 0.00 )
		daily_sales_third_table [ "ECPA" ] = np.select ( [ mask3 ] , [ choice_daily_CPA ] , default = 0.00 )
		
		return display_sales_first_table , adsize_sales_second_table , daily_sales_third_table
	
	def rename_KM_Sales_daily ( self ) :
		display_sales_first_table , adsize_sales_second_table , daily_sales_third_table = self.KM_Sales_daily ( )
		
		rename_display_sales_first_table = display_sales_first_table.rename (
			columns = {
				"PLACEMENT#" : "Placement#" , "PLACEMENT_NAME" : "Placement Name" ,
				"COST_TYPE" : "Cost Type" , "UNIT_COST" : "Unit Cost" ,
				"BOOKED_IMP#BOOKED_ENG" : "Booked" , "DELIVERED_IMPRESION" : "Delivered Impressions"
				, "CLICKS" : "Clicks" ,
				"CONVERSION" : "Conversion"
				, "PLACEMENTNAME" : "Placement# Name" , "CTR%" : "CTR"
				, "CONVERSIONRATE%" : "Conversion Rate"
				, "SPEND" : "Spend" , "EPCA" : "eCPA"
				} , inplace = True )
		
		rename_adsize_sales_second_table = adsize_sales_second_table.rename (
			columns = {
				"PLACEMENT#" : "Placement#" , "PLACEMENT_NAME" : "Placement Name" ,
				"COST_TYPE" : "Cost Type" , "UNIT_COST" : "Unit Cost" ,
				"BOOKED_IMP#BOOKED_ENG" : "Booked" , "ADSIZE" : "Adsize"
				, "DELIVERED_IMPRESION" : "Delivered Impressions" , "CLICKS" : "Clicks" , "CONVERSION" : "Conversion" ,
				"PLACEMENTNAME" : "Placement# Name"
				, "CTR%" : "CTR" , "CONVERSIONRATE%" : "Conversion Rate" , "SPEND" : "Spend" , "ECPA" : "eCPA"
				} , inplace = True )
		
		rename_daily_sales_third_table = daily_sales_third_table.rename (
			columns = {
				"PLACEMENT#" : "Placement#" , "PLACEMENT_NAME" : "Placement Name" ,
				"COST_TYPE" : "Cost Type" , "UNIT_COST" : "Unit Cost" , "BOOKED_IMP#BOOKED_ENG" : "Booked" ,
				"DAY" : "Date" , "DELIVERED_IMPRESION" : "Delivered Impressions" , "CLICKS" : "Clicks" ,
				"CONVERSION" : "Conversion" , "PLACEMENTNAME" : "Placement# Name" ,
				"CTR%" : "CTR" , "SPEND" : "Spend" , "ECPA" : "eCPA"
				} , inplace = True )
		
		return display_sales_first_table , adsize_sales_second_table , daily_sales_third_table
	
	def accessing_nan_values ( self ) :
		display_sales_first_table , adsize_sales_second_table , daily_sales_third_table = self.rename_KM_Sales_daily \
			( )
		
		display_sales_first_table [ "CTR" ] = display_sales_first_table [ "CTR" ].replace ( np.nan , 0.00 )
		display_sales_first_table [ "Conversion Rate" ] = display_sales_first_table [ "Conversion Rate" ].replace (
			np.nan , 0.00 )
		display_sales_first_table [ "Spend" ] = display_sales_first_table [ "Spend" ].replace ( np.nan , 0.00 )
		display_sales_first_table [ "eCPA" ] = display_sales_first_table [ "eCPA" ].replace ( np.nan , 0.00 )
		
		adsize_sales_second_table [ "CTR" ] = adsize_sales_second_table [ "CTR" ].replace ( np.nan , 0.00 )
		adsize_sales_second_table [ "Conversion Rate" ] = adsize_sales_second_table [ "Conversion Rate" ].replace (
			np.nan , 0.00 )
		adsize_sales_second_table [ "Spend" ] = adsize_sales_second_table [ "Spend" ].replace ( np.nan , 0.00 )
		adsize_sales_second_table [ "eCPA" ] = adsize_sales_second_table [ "eCPA" ].replace ( np.nan , 0.00 )
		
		daily_sales_third_table [ "CTR" ] = daily_sales_third_table [ "CTR" ].replace ( np.nan , 0.00 )
		daily_sales_third_table [ "Spend" ] = daily_sales_third_table [ "Spend" ].replace ( np.nan , 0.00 )
		daily_sales_third_table [ "eCPA" ] = daily_sales_third_table [ "eCPA" ].replace ( np.nan , 0.00 )
		
		display_sales_first_table [ "CTR" ] = display_sales_first_table [ "CTR" ].replace ( np.inf , 0.00 )
		display_sales_first_table [ "Conversion Rate" ] = display_sales_first_table [ "Conversion Rate" ].replace (
			np.inf , 0.00 )
		display_sales_first_table [ "Spend" ] = display_sales_first_table [ "Spend" ].replace ( np.inf , 0.00 )
		display_sales_first_table [ "eCPA" ] = display_sales_first_table [ "eCPA" ].replace ( np.inf , 0.00 )
		
		adsize_sales_second_table [ "CTR" ] = adsize_sales_second_table [ "CTR" ].replace ( np.inf , 0.00 )
		adsize_sales_second_table [ "Conversion Rate" ] = adsize_sales_second_table [ "Conversion Rate" ].replace (
			np.inf , 0.00 )
		adsize_sales_second_table [ "Spend" ] = adsize_sales_second_table [ "Spend" ].replace ( np.inf , 0.00 )
		adsize_sales_second_table [ "eCPA" ] = adsize_sales_second_table [ "eCPA" ].replace ( np.inf , 0.00 )
		
		daily_sales_third_table [ "CTR" ] = daily_sales_third_table [ "CTR" ].replace ( np.inf , 0.00 )
		daily_sales_third_table [ "Spend" ] = daily_sales_third_table [ "Spend" ].replace ( np.inf , 0.00 )
		daily_sales_third_table [ "eCPA" ] = daily_sales_third_table [ "eCPA" ].replace ( np.inf , 0.00 )
		
		return display_sales_first_table , adsize_sales_second_table , daily_sales_third_table
	
	def accessing_main_column ( self ) :
		
		display_sales_first_table , adsize_sales_second_table , daily_sales_third_table = self.accessing_nan_values ( )
		
		"""display_sales_first_table.loc["Grand Total"]=pd.Series(
			display_sales_first_table.loc[:,["Booked","Delivered Impressions","Clicks","Conversion","Spend"]].sum(),
			index=["Booked","Delivered Impressions","Clicks","Conversion","Spend"])

		display_sales_first_table["Placement# Name"]=display_sales_first_table["Placement# Name"].replace(np.nan,
																										  "Grand
																										  Total")
		display_sales_first_table[["Cost Type","Unit Cost","CTR",
								   "Conversion Rate","eCPA"]]=display_sales_first_table[
			["Cost Type","Unit Cost","CTR","Conversion Rate","eCPA"]].replace(np.nan,"")"""
		
		placement_sales_data = display_sales_first_table [ [ "Placement# Name" , "Cost Type" , "Unit Cost" , "Booked" ,
		                                                     "Delivered Impressions" , "Clicks" , "CTR" ,
		                                                     "Conversion" ,
		                                                     "Conversion Rate" , "Spend" , "eCPA" ] ]
		
		adsize_sales_data = adsize_sales_second_table.loc [ : ,
		                    [ "Placement# Name" , "Adsize" , "Delivered Impressions" , "Clicks" ,
		                      "CTR" , "Conversion" , "Conversion Rate" , "Spend" , "eCPA" ] ]
		
		"""cols = ["Delivered Impressions", "Clicks", "Conversion", "Spend"]
		adsize_sales_data['Placement# Name'] = adsize_sales_data['Placement# Name'].ffill()
		grand = adsize_sales_data[cols].sum()

		grand.loc['Placement# Name'] = 'Grand Total'

		adsize_sales_data_new = adsize_sales_data.groupby('Placement# Name')[cols].sum()
		adsize_sales_data_new.index = adsize_sales_data_new.index.astype(str)+'____'
		adsize_sales_data = (pd.concat([adsize_sales_data.set_index('Placement# Name'), adsize_sales_data_new],
		keys=('a', 'b')).sort_index(level=1).reset_index())
		adsize_sales_data['Placement# Name'] = np.where(adsize_sales_data['level_0'] == 'a', adsize_sales_data[
		'Placement# Name'], 'Total')
		adsize_sales_data = adsize_sales_data.drop('level_0', axis=1)
		adsize_sales_data.loc[len(adsize_sales_data.index)] = grand"""
		
		"""cols_ad=["Delivered Impressions","Clicks","Conversion","Spend"]
		adsize_sales_data['Placement# Name']=adsize_sales_data['Placement# Name'].ffill()
		grand=adsize_sales_data[cols_ad].sum()
		grand.loc['Placement# Name']='Grand Total'
		adsize_sales_data_new=adsize_sales_data.groupby('Placement# Name')[cols_ad].sum()
		try:
			adsize_sales_data_new.index=adsize_sales_data_new.index.astype(str)+'____'

			adsize_sales_data_new_new=pd.DataFrame(index=adsize_sales_data_new.index.astype(str)+'__')

			adsize_sales_data=pd.concat([adsize_sales_data.set_index('Placement# Name'),adsize_sales_data_new,
			adsize_sales_data_new_new],
					keys=('a','b','c')).sort_index(level=1).reset_index()

			m1=adsize_sales_data['level_0']=='a'
			m2=adsize_sales_data['level_0']=='c'

			adsize_sales_data['Placement# Name']=np.select([m1,m2],[adsize_sales_data['Placement# Name'],np.nan],
														   default='Total')
			adsize_sales_data=adsize_sales_data.drop('level_0',axis=1)
			adsize_sales_data.loc[len(adsize_sales_data.index)]=grand
		except TypeError as e:
			pass
		except KeyError as e:
			pass"""
		
		"""final_adsize=adsize_sales_data.loc[:,["Placement# Name","Adsize","Delivered Impressions","Clicks","CTR",
											  "Conversion","Conversion Rate","Spend","eCPA"]]"""
		
		final_adsize = adsize_sales_data.loc [ : , [ "Adsize" , "Delivered Impressions" , "Clicks" , "CTR" ,
		                                             "Conversion" , "Conversion Rate" , "Spend" , "eCPA" ] ]
		
		daily_sales_data = daily_sales_third_table.loc [ : , [ "Placement# Name" , "Date" , "Delivered Impressions" ,
		                                                       "Clicks" , "CTR" , "Conversion" , "eCPA" , "Spend" ,
		                                                       "Unit Cost" ] ]
		
		daily_sales_remove_zero = daily_sales_data [ daily_sales_data [ 'Delivered Impressions' ] == 0 ]
		
		daily_sales_data = daily_sales_data.drop ( daily_sales_remove_zero.index , axis = 0 )
		
		daily_sales_data [ "Date" ] = pd.to_datetime ( daily_sales_data [ "Date" ] )
		
		"""try:
			cols_day=["Delivered Impressions","Clicks","Conversion","Spend"]
			daily_sales_data['Placement# Name']=daily_sales_data['Placement# Name'].ffill()
			grand=daily_sales_data[cols_day].sum()
			grand.loc['Placement# Name']='Grand Total'
			daily_sales_data_new=daily_sales_data.groupby('Placement# Name')[cols_day].sum()
			daily_sales_data_new.index=daily_sales_data_new.index.astype(str)+'____'
			daily_sales_data_new_new=pd.DataFrame(index=daily_sales_data_new.index.astype(str)+'__')
			daily_sales_data=pd.concat([daily_sales_data.set_index('Placement# Name'),daily_sales_data_new,
											daily_sales_data_new_new],
										   keys=('a','b','c')).sort_index(level=1).reset_index()

			m1=daily_sales_data['level_0']=='a'
			m2=daily_sales_data['level_0']=='c'

			daily_sales_data['Placement# Name']=np.select([m1,m2],[daily_sales_data['Placement# Name'],np.nan],
														  default='Total')

			daily_sales_data=daily_sales_data.drop('level_0',axis=1)

			daily_sales_data.loc[len(daily_sales_data.index)]=grand
		except TypeError as e:
			pass
		except KeyError as e:
			pass"""
		
		final_day_wise = daily_sales_data.loc [ : ,
		                 [ "Placement# Name" , "Date" , "Delivered Impressions" , "Clicks" , "CTR" , "Conversion" ,
		                   "eCPA" , "Spend" ] ]
		
		final_day_wise = final_day_wise.set_index ( 'Placement# Name' )
		
		final_day_wise [ 'Date' ] = final_day_wise [ 'Date' ].dt.strftime ( '%Y-%m-%d' )
		
		final_day_wise_new = final_day_wise [ [ 'Delivered Impressions' , 'Clicks' , 'Conversion' , 'Spend' ] ].sum (
			level = 0 ).assign (
			Date = 'Subtotal' )
		
		final_day_wise_new [ 'CTR' ] = final_day_wise_new [ 'Clicks' ] / final_day_wise_new [ 'Delivered Impressions' ]
		final_day_wise_new [ 'eCPA' ] = final_day_wise_new [ 'Spend' ] / final_day_wise_new [ 'Conversion' ]
		
		final_day_wise_new_out = pd.concat ( [ final_day_wise , final_day_wise_new ] ).set_index ( 'Date' ,
		                                                                                           append = True
		                                                                                           ).sort_index (
			level = 0 )
		
		final_day_wise_new_out = final_day_wise_new_out [
			[ 'Delivered Impressions' , 'Clicks' , 'CTR' , 'Conversion' , 'eCPA' , 'Spend' ] ]
		
		print final_day_wise_new_out
		
		final_day_wise_new_out [ 'eCPA' ] = final_day_wise_new_out [ 'eCPA' ].replace ( np.inf , 0.00 )
		
		return placement_sales_data , final_adsize , final_day_wise_new_out
	
	def write_KM_Sales_summary ( self ) :
		
		data_common_columns = self.config.common_columns_summary ( )
		
		placement_sales_data , final_adsize , final_day_wise_new_out = self.accessing_main_column ( )
		
		print (final_day_wise_new_out.shape [ 0 ])
		
		writing_data_common_columns = data_common_columns [ 1 ].to_excel ( self.config.writer ,
		                                                                   sheet_name = "Standard banner({})".format
		                                                                   ( self.config.IO_ID ) , startcol = 1 ,
		                                                                   startrow = 1 ,
		                                                                   index = False , header = False )
		
		check_placement_sales_data = placement_sales_data.empty
		
		if check_placement_sales_data == True :
			pass
		else :
			writing_placement_data = placement_sales_data.to_excel ( self.config.writer ,
			                                                         sheet_name = "Standard banner({})".format (
				                                                         self.config.IO_ID ) ,
			                                                         startcol = 1 , startrow = 8 , index = False ,
			                                                         header = True )
		
		check_adsize_sales_data = final_adsize.empty
		
		if check_adsize_sales_data == True :
			pass
		else :
			writing_adsize_data = final_adsize.to_excel ( self.config.writer ,
			                                              sheet_name = "Standard banner({})".format (
				                                              self.config.IO_ID ) ,
			                                              startcol = 1 , startrow = len ( placement_sales_data ) + 12 ,
			                                              index = False ,
			                                              header = True )
		
		check_daily_sales_data = final_day_wise_new_out.empty
		
		if check_daily_sales_data == True :
			pass
		else :
			startline = len ( placement_sales_data ) + len ( final_adsize ) + 16
			for n , g in final_day_wise_new_out.groupby ( level = 0 ) :
				writing_daily_data = g.to_excel ( self.config.writer ,
				                                  sheet_name = "Standard banner({})".format ( self.config.IO_ID ) ,
				                                  startcol = 1 , startrow = startline , index = True , header = True )
				startline += len ( g ) + 4
			"""writing_daily_data=final_day_wise_new_out.to_excel(self.config.writer,sheet_name="Standard banner({
			})".format(self.config.IO_ID),
										  startcol=1,startrow=len(placement_sales_data)+len(final_adsize)+16,
										  index=True,header=True)"""
		
		return placement_sales_data , final_adsize , final_day_wise_new_out
	
	def formatting_daily ( self ) :
		placement_sales_data , final_adsize , final_day_wise_new_out = self.write_KM_Sales_summary ( )
		
		data_common_columns = self.config.common_columns_summary ( )
		
		number_rows_placement = placement_sales_data.shape [ 0 ]
		number_cols_placement = placement_sales_data.shape [ 1 ]
		number_rows_adsize = final_adsize.shape [ 0 ]
		number_cols_adsize = final_adsize.shape [ 1 ]
		number_rows_daily = final_day_wise_new_out.shape [ 0 ]
		number_cols_daily = final_day_wise_new_out.shape [ 1 ] + 2
		
		workbook = self.config.writer.book
		worksheet = self.config.writer.sheets [ "Standard banner({})".format ( self.config.IO_ID ) ]
		
		worksheet.hide_gridlines ( 2 )
		worksheet.set_row ( 0 , 6 )
		worksheet.set_column ( "A:A" , 2 )
		
		check_placement_sales_data = placement_sales_data.empty
		check_adsize_sales_data = final_adsize.empty
		check_daily_sales_data = final_day_wise_new_out.empty
		
		worksheet.insert_image ( "H2" , "Exponential.png" , { "url" : "https://www.tribalfusion.com" } )
		worksheet.insert_image ( "I2" , "Client_Logo.png" )
		
		forge_colour_info = workbook.add_format ( )
		forge_colour_info.set_bg_color ( '#F0F8FF' )
		forge_colour_col = workbook.add_format ( )
		forge_colour_col.set_bg_color ( '#00B0F0' )
		forge_colour_border = workbook.add_format ( )
		forge_colour_border.set_bg_color ( '#E7E6E6' )
		
		format_border_bottom = workbook.add_format ( )
		format_border_bottom.set_bottom ( 1 )
		
		format_border_right = workbook.add_format ( )
		format_border_right.set_right ( 1 )
		
		format_border_left = workbook.add_format ( )
		format_border_left.set_left ( 1 )
		
		format_sub = workbook.add_format ( { "bold" : True } )
		format_subtotal = workbook.add_format ( { "bold" : True , "align" : "centre" , "left" : 1 } )
		format_sub_num_money = workbook.add_format (
			{ "bold" : True , "num_format" : "$#,###0.00" , "align" : "right" } )
		format_sub_num_percent = workbook.add_format ( { "bold" : True , "num_format" : "0.00%" , "align" : "right" } )
		
		format_total = workbook.add_format ( { "bg_color" : '#E7E6E6' , "bold" : True } )
		
		percent_fmt = workbook.add_format ( { "num_format" : "0.00%" } )
		money_fmt = workbook.add_format ( { "num_format" : "$#,###0.00" } )
		
		worksheet.conditional_format ( "B2:O5" , { "type" : "blanks" , "format" : forge_colour_info } )
		worksheet.conditional_format ( "B2:O5" , { "type" : "no_blanks" , "format" : forge_colour_info } )
		
		# Colour formatting for Columns
		if check_placement_sales_data == True :
			pass
		else :
			worksheet.conditional_format ( 8 , 1 , 8 , number_cols_placement + 1 ,
			                               { "type" : "no_blanks" , "format" : forge_colour_col } )
		
		if check_adsize_sales_data == True :
			pass
		else :
			worksheet.conditional_format ( number_rows_placement + 12 , 1 , number_rows_placement + 12 ,
			                               number_cols_adsize + 1 ,
			                               { "type" : "no_blanks" , "format" : forge_colour_col } )
		
		if check_daily_sales_data == True :
			pass
		else :
			worksheet.conditional_format ( number_rows_placement + number_rows_adsize + 16 , 1 ,
			                               number_rows_placement + number_rows_adsize + 16 ,
			                               number_cols_daily + 1 ,
			                               { "type" : "no_blanks" , "format" : forge_colour_col } )
		
		# Money and Percet Formatting
		"""if check_placement_sales_data==True:
			pass
		else:
			worksheet.conditional_format(8, 3,number_rows_placement+8,3,{"type":"no_blanks","format":money_fmt})
			worksheet.conditional_format(8, 10,number_rows_placement+8,11,{"type":"no_blanks","format":money_fmt})
			worksheet.conditional_format(8, 7,number_rows_placement+8,7,{"type":"no_blanks","format":percent_fmt})
			worksheet.conditional_format(8, 9,number_rows_placement+8,9,{"type":"no_blanks","format":percent_fmt})

		if check_adsize_sales_data==True:
			pass
		else:

			#Value = final_adsize.loc[final_adsize["Placement# Name"] == "Total"]
			worksheet.conditional_format(number_rows_placement+12,4,number_rows_placement+number_rows_adsize+12,4,
										 {"type":"no_blanks","format":percent_fmt})
			worksheet.conditional_format(number_rows_placement+12,6,number_rows_placement+number_rows_adsize+12,6,
										 {"type":"no_blanks","format":percent_fmt})
			worksheet.conditional_format(number_rows_placement+12,7,number_rows_placement+number_rows_adsize+12,8,
										 {"type":"no_blanks","format":money_fmt})
			worksheet.conditional_format(number_rows_placement+12,1, number_rows_placement+number_rows_adsize+12,
			number_cols_adsize,
										 {"type":"cell","criteria":"equal to","value":Value,
										 "format":format_border_bottom})"""
		
		"""if check_daily_sales_data==True:
			pass
		else:
			worksheet.conditional_format(number_rows_placement+number_rows_adsize+16,5,
										 number_rows_placement+number_rows_adsize+number_rows_daily+16,5,
										 {"type":"no_blanks","format":percent_fmt})
			worksheet.conditional_format(number_rows_placement+number_rows_adsize+16,7,
										 number_rows_placement+number_rows_adsize+number_rows_daily+36,8,
										 {"type":"no_blanks","format":money_fmt})"""
		
		# bottom border format
		"""if check_placement_sales_data==True:
			pass
		else:
			worksheet.conditional_format(number_rows_placement+8,1,number_rows_placement+8,number_cols_placement,
										 {"type":"no_blanks","format":format_border_bottom})
			worksheet.conditional_format(number_rows_placement+8,1,number_rows_placement+8,number_cols_placement,
										 {"type":"blanks","format":format_border_bottom})
			worksheet.conditional_format(number_rows_placement+8,1,number_rows_placement+8,number_cols_placement,
										 {"type":"no_blanks","format":format_subtotal})
			worksheet.conditional_format(number_rows_placement+8,1,number_rows_placement+8,number_cols_placement,
										 {"type":"blanks","format":format_subtotal})
			worksheet.conditional_format(number_rows_placement+8,1,number_rows_placement+8,number_cols_placement,
										 {"type":"no_blanks","format":forge_colour_border})
			worksheet.conditional_format(number_rows_placement+8,1,number_rows_placement+8,number_cols_placement,
										 {"type":"blanks","format":forge_colour_border})

		if check_adsize_sales_data==True:
			pass
		else:
			worksheet.conditional_format(number_rows_placement+number_rows_adsize+12,1,
										 number_rows_placement+number_rows_adsize+12,number_cols_adsize,
										 {"type":"no_blanks","format":format_border_bottom})
			worksheet.conditional_format(number_rows_placement+number_rows_adsize+12,1,
										 number_rows_placement+number_rows_adsize+12,number_cols_adsize,
										 {"type":"blanks","format":format_border_bottom})
			worksheet.conditional_format(number_rows_placement+number_rows_adsize+12,1,
										 number_rows_placement+number_rows_adsize+12,number_cols_adsize,
										 {"type":"no_blanks","format":format_subtotal})
			worksheet.conditional_format(number_rows_placement+number_rows_adsize+12,1,
										 number_rows_placement+number_rows_adsize+12,number_cols_adsize,
										 {"type":"blanks","format":format_subtotal})
			worksheet.conditional_format(number_rows_placement+number_rows_adsize+12,1,
										 number_rows_placement+number_rows_adsize+12,number_cols_adsize,
										 {"type":"no_blanks","format":forge_colour_border})
			worksheet.conditional_format(number_rows_placement+number_rows_adsize+12,1,
										 number_rows_placement+number_rows_adsize+12,number_cols_adsize,
										 {"type":"blanks","format":forge_colour_border})

		if check_daily_sales_data==True:
			pass
		else:
			worksheet.conditional_format(number_rows_placement+number_rows_adsize+16+number_rows_daily,1,
										 number_rows_placement+number_rows_adsize+16+number_rows_daily,
										 number_cols_daily,
										 {"type":"no_blanks","format":format_border_bottom})
			worksheet.conditional_format(number_rows_placement+number_rows_adsize+16+number_rows_daily,1,
										 number_rows_placement+number_rows_adsize+16+number_rows_daily,
										 number_cols_daily,
										 {"type":"blanks","format":format_border_bottom})
			worksheet.conditional_format(number_rows_placement+number_rows_adsize+16+number_rows_daily,1,
										 number_rows_placement+number_rows_adsize+16+number_rows_daily,
										 number_cols_daily,
										 {"type":"no_blanks","format":format_subtotal})
			worksheet.conditional_format(number_rows_placement+number_rows_adsize+16+number_rows_daily,1,
										 number_rows_placement+number_rows_adsize+16+number_rows_daily,
										 number_cols_daily,
										 {"type":"blanks","format":format_subtotal})
			worksheet.conditional_format(number_rows_placement+number_rows_adsize+16+number_rows_daily,1,
										 number_rows_placement+number_rows_adsize+16+number_rows_daily,
										 number_cols_daily,
										 {"type":"no_blanks","format":forge_colour_border})
			worksheet.conditional_format(number_rows_placement+number_rows_adsize+16+number_rows_daily,1,
										 number_rows_placement+number_rows_adsize+16+number_rows_daily,
										 number_cols_daily,
										 {"type":"blanks","format":forge_colour_border})

		#Left Border Set
		if check_placement_sales_data==True:
			pass
		else:
			worksheet.conditional_format(9,1,number_rows_placement+9,1,{"type":"no_blanks",
			"format":format_border_left})

		if check_adsize_sales_data==True:
			pass
		else:
			worksheet.conditional_format(number_rows_placement+13,1,number_rows_placement+number_rows_adsize+13,1,
										 {"type":"no_blanks","format":format_border_left})
			worksheet.conditional_format(number_rows_placement+13,1,number_rows_placement+number_rows_adsize+12,1,
										 {"type":"blanks","format":format_border_left})

		if check_daily_sales_data==True:
			pass
		else:
			worksheet.conditional_format(number_rows_placement+number_rows_adsize+17,1,
										 number_rows_placement+number_rows_adsize+number_rows_daily+17,1,
										 {"type":"no_blanks","format":format_border_left})
			worksheet.conditional_format(number_rows_placement+number_rows_adsize+17,1,
										 number_rows_placement+number_rows_adsize+number_rows_daily+16,1,
										 {"type":"blanks","format":format_border_left})

		#Right Border Set
		if check_placement_sales_data==True:
			pass
		else:
			worksheet.conditional_format(9,number_cols_placement,number_rows_placement+9,number_cols_placement,
										 {"type":"no_blanks","format":format_border_right})

		if check_adsize_sales_data==True:
			pass
		else:
			worksheet.conditional_format(number_rows_placement+13,number_cols_adsize,
										 number_rows_placement+number_rows_adsize+13,number_cols_adsize,
										 {"type":"no_blanks","format":format_border_right})
			worksheet.conditional_format(number_rows_placement+13,number_cols_adsize,
										 number_rows_placement+number_rows_adsize+12,number_cols_adsize,
										 {"type":"blanks","format":format_border_right})

		if check_daily_sales_data==True:
			pass
		else:
			worksheet.conditional_format(number_rows_placement+number_rows_adsize+17,number_cols_daily,
										 number_rows_placement+number_rows_adsize+number_rows_daily+17,
										 number_cols_daily,
										 {"type":"no_blanks","format":format_border_right})
			worksheet.conditional_format(number_rows_placement+number_rows_adsize+17,number_cols_daily,
										 number_rows_placement+number_rows_adsize+number_rows_daily+16,
										 number_cols_daily,
										 {"type":"blanks","format":format_border_right})"""
		
		# Merge Row formatting
		"""format_merge_row=workbook.add_format({"bold":True,"font_color":'#000000',"align":"centre",
											  "fg_color":"#00B0F0","border":1,"border_color":"#000000"})"""
		
		"""if check_placement_sales_data==True:
			pass
		else:
			worksheet.merge_range(7,1,7,number_cols_placement,
								  "Standard Banner Performance - Summary",format_merge_row)"""
		
		"""if check_adsize_sales_data==True:
			pass
		else:
			worksheet.merge_range(number_rows_placement+11,1,number_rows_placement+11,number_cols_adsize,
								  "Standard Banner Performance - Ad Size Summary",format_merge_row)

		if check_daily_sales_data==True:
			pass
		else:
			worksheet.merge_range(number_rows_placement+number_rows_adsize+15,1,
								  number_rows_placement+number_rows_adsize+15,number_cols_daily,
								  "Breakdown By Day + Placement",format_merge_row)"""
		
		# format_total
		
		# Value_new  = final_adsize.loc[final_adsize["Placement# Name"] == "Total"]
		
		# Total = final_adsize[final_adsize['Placement# Name'] == 'Total']
		
		worksheet.set_column ( "B:N" , 21 )
		worksheet.set_zoom ( 90 )
		
		if check_placement_sales_data == True & check_adsize_sales_data == True & check_daily_sales_data == True :
			worksheet.hide ( )
		else :
			pass
		# return check_placement_sales_data, check_adsize_sales_data, check_daily_sales_data
	
	def main ( self ) :
		self.config.common_columns_summary ( )
		self.connect_TFR_daily ( )
		self.read_Query_daily ( )
		self.access_Data_KM_Sales_daily ( )
		self.KM_Sales_daily ( )
		self.rename_KM_Sales_daily ( )
		self.accessing_nan_values ( )
		self.accessing_main_column ( )
		self.write_KM_Sales_summary ( )
		# self.formatting_daily()


if __name__ == "__main__" :
	# pass
	
	# enable it when running for individual file
	c = config.Config ( 'test' , 565337 )
	o = Daily ( c )
	o.main ( )
	c.saveAndCloseWriter ( )
