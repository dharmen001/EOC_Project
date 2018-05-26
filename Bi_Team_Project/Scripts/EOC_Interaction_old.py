# coding=utf-8
"""
old EOC Prerorll

"""

# coding=utf-8
"""
Created by:Dharmendra
Date:2018-03-23
"""
import pandas as pd
import numpy as np
from xlsxwriter.utility import xl_rowcol_to_cell
import xlsxwriter
import config
import pandas.io.formats.excel
import logging

pandas.io.formats.excel.header_style = None


class Intraction (object):
	"""
Preroll placements Class
	"""
	
	def __init__(self, config):
		self.config = config
		self.logger = self.config.logger
	
	def connect_TFR_Intraction(self):
		"""
TFR Quries for Preroll Placements
		:return:
		"""
		self.logger.info ('Starting to build Preroll Sheet for IO - {}'.format (self.config.ioid))
		sql_preroll_summary = "select * from (select substr(PLACEMENT_DESC, 1, INSTR(PLACEMENT_DESC, '.', 1)-1) as Placement, SDATE as Start_Date, EDATE as End_Date, initcap(CREATIVE_DESC)  as Placement_Name, COST_TYPE_DESC as Cost_type, UNIT_COST as Unit_Cost, BUDGET as Planned_Cost, BOOKED_QTY as Booked_Imp_Booked_Eng from  TFR_REP.SUMMARY_MV where (IO_ID = {}) AND (DATA_SOURCE = 'KM') AND CREATIVE_DESC IN(SELECT DISTINCT CREATIVE_DESC FROM TFR_REP.SUMMARY_MV) ORDER BY PLACEMENT_ID) WHERE Placement_Name IN ('Pre-Roll - Desktop','Pre-Roll - Desktop + Mobile','Pre-Roll – Desktop + Mobile','Pre-Roll - In-Stream/Mobile Blend','Pre-Roll - Mobile','Pre-Roll -Desktop','Pre-Roll - In-Stream')".format (
			self.config.ioid)
		
		sql_preroll_mv = "select substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as Placement, sum(IMPRESSIONS) as Impression, sum(CPCV_COUNT) as Completions,sum(VWR_CLICK_THROUGHS) as Clickthroughs , sum(VWR_VIDEO_VIEW_100_PC_COUNT) as Video_Completions from TFR_REP.KEY_METRIC_MV WHERE IO_ID = {} GROUP BY PLACEMENT_ID, PLACEMENT_DESC ORDER BY PLACEMENT_ID".format (
			self.config.ioid)
		
		sql_preroll_video_views = "select substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as Placement,sum(IMPRESSIONS) as Impression, sum(VWR_VIDEO_VIEW_25_PC_COUNT) as Views25,sum(VWR_VIDEO_VIEW_50_PC_COUNT) as Views50, sum(VWR_VIDEO_VIEW_75_PC_COUNT) as Views75,sum(VWR_VIDEO_VIEW_100_PC_COUNT) as Video_Completions, sum(CPCV_COUNT) as Completions from TFR_REP.KEY_METRIC_MV WHERE IO_ID = {} GROUP BY PLACEMENT_ID, PLACEMENT_DESC ORDER BY PLACEMENT_ID".format (
			self.config.ioid)
		
		sql_preroll_day_mv = "select substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as Placement, TO_CHAR(TO_DATE(DAY_DESC, 'MM/DD/YYYY'),'YYYY-MM-DD') as Day, sum(IMPRESSIONS) as Impression, sum(CPCV_COUNT) as Completions, sum(VWR_CLICK_THROUGHS) as Clickthroughs , sum(VWR_VIDEO_VIEW_100_PC_COUNT) as Video_Completions from TFR_REP.KEY_METRIC_MV WHERE IO_ID = {} GROUP BY PLACEMENT_ID, PLACEMENT_DESC,DAY_DESC ORDER BY PLACEMENT_ID".format (
			self.config.ioid)
		
		sql_preroll_interaction = "select substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as Placement,BLAZE_TAG_NAME_DESC as Click_Tag, sum(VWR_INTERACTION) as VWR_Clickthrough from TFR_REP.INTERACTION_DETAIL_MV WHERE IO_ID = {} and BLAZE_ACTION_TYPE_DESC = 'Click-thru' GROUP BY PLACEMENT_ID, PLACEMENT_DESC, BLAZE_TAG_NAME_DESC ORDER BY PLACEMENT_ID".format (
			self.config.ioid)
		
		# return sql_preroll_summary, sql_preroll_mv, sql_preroll_video_views ,sql_preroll_day_mv ,
		# sql_preroll_interaction
		
		self.sql_preroll_summary = sql_preroll_summary
		self.sql_preroll_mv = sql_preroll_mv
		self.sql_preroll_video_views = sql_preroll_video_views
		self.sql_preroll_day_mv = sql_preroll_day_mv
		self.sql_preroll_interaction = sql_preroll_interaction
	
	def read_query_preroll(self):
		
		"""
Reading Queries Data directly from TFR
		:return:
		"""
		
		self.logger.info ('Running Query for Preroll placements for IO {}'.format (self.config.ioid))
		# sql_preroll_summary, sql_preroll_mv, sql_preroll_video_views, sql_preroll_day_mv, sql_preroll_interaction =
		# self.connect_TFR_Intraction()
		
		read_sql_preroll_summary = pd.read_sql (self.sql_preroll_summary, self.config.conn)
		
		read_sql_preroll_mv = pd.read_sql (self.sql_preroll_mv, self.config.conn)
		
		read_sql_preroll_video = pd.read_sql (self.sql_preroll_video_views, self.config.conn)
		
		read_sql_preroll_day = pd.read_sql (self.sql_preroll_day_mv, self.config.conn)
		
		read_sql_preroll_interaction = pd.read_sql (self.sql_preroll_interaction, self.config.conn)
		
		# self.read_sql_preroll_day = read_sql_preroll_day
		
		# return read_sql_preroll_summary, read_sql_preroll_mv, read_sql_preroll_video ,read_sql_preroll_day ,
		# read_sql_preroll_interaction
		
		self.read_sql_preroll_summary = read_sql_preroll_summary
		self.read_sql_preroll_mv = read_sql_preroll_mv
		self.read_sql_preroll_video = read_sql_preroll_video
		self.read_sql_preroll_day = read_sql_preroll_day
		self.read_sql_preroll_interaction = read_sql_preroll_interaction
	
	def accessing_preroll_columns(self):
		
		"""
Accessing Columns from Query
		:return:
		"""
		
		self.logger.info ('Query Stored for further processing of IO - {}'.format (self.config.ioid))
		# read_sql_preroll_summary, read_sql_preroll_mv, read_sql_preroll_video, read_sql_preroll_day,
		# read_sql_preroll_interaction  = self.read_query_preroll()
		
		self.logger.info ('Creating placement wise table of IO - {}'.format (self.config.ioid))
		
		placementprerollmv = self.read_sql_preroll_summary.merge (self.read_sql_preroll_mv, on="PLACEMENT", how="inner")
		
		prerollsummarymv = placementprerollmv.loc[:,
		                   ["PLACEMENT", "PLACEMENT_NAME", "COST_TYPE", "UNIT_COST", "IMPRESSION",
		                    "CLICKTHROUGHS", "COMPLETIONS", "VIDEO_COMPLETIONS"]]
		
		mask1 = prerollsummarymv["COST_TYPE"].isin (['CPM'])
		choiceprerollsummarymvcpm = prerollsummarymv["VIDEO_COMPLETIONS"]
		mask2 = prerollsummarymv["COST_TYPE"].isin (['CPCV'])
		choiceprerollsummarymvcpcv = prerollsummarymv["COMPLETIONS"]
		
		prerollsummarymv["Video Completions"] = np.select ([mask1, mask2], [choiceprerollsummarymvcpm,
		                                                                    choiceprerollsummarymvcpcv], default=0)
		
		prerollsummary = prerollsummarymv.loc[:, ["PLACEMENT", "PLACEMENT_NAME", "COST_TYPE", "UNIT_COST"
			, "IMPRESSION", "CLICKTHROUGHS", "Video Completions"]]
		
		prerollsummary["Placement# Name"] = prerollsummary[["PLACEMENT", "PLACEMENT_NAME"]].apply (
			lambda x:".".join (x), axis=1)
		
		prerollsummary["CTR"] = prerollsummary["CLICKTHROUGHS"]/prerollsummary["IMPRESSION"]
		prerollsummary["Video Completion Rate"] = prerollsummary["Video Completions"]/prerollsummary["IMPRESSION"]
		
		prerollsummaryfinal = prerollsummary.loc[:, ["Placement# Name", "COST_TYPE", "UNIT_COST", "IMPRESSION",
		                                             "CLICKTHROUGHS", "CTR", "Video Completions",
		                                             "Video Completion Rate"]]
		
		self.logger.info ('Creating Video wise table of IO - {}'.format (self.config.ioid))
		videoprerollmv = self.read_sql_preroll_summary.merge (self.read_sql_preroll_video, on="PLACEMENT", how="inner")
		
		videoprerollsummarymv = videoprerollmv.loc[:,
		                        ["PLACEMENT", "PLACEMENT_NAME", "IMPRESSION", "VIEWS25", "VIEWS50",
		                         "VIEWS75", "VIDEO_COMPLETIONS", "COMPLETIONS"]]
		
		mask3 = videoprerollmv["COST_TYPE"].isin (['CPM'])
		choicevideoprerollcpm = videoprerollmv["VIDEO_COMPLETIONS"]
		mask4 = videoprerollmv["COST_TYPE"].isin (['CPCV'])
		choicevideoprerollcpcv = videoprerollmv["COMPLETIONS"]
		
		videoprerollsummarymv["Video Completions"] = np.select ([mask3, mask4], [choicevideoprerollcpm,
		                                                                         choicevideoprerollcpcv], default=0)
		
		videoprerollsummarymv["Video Completion Rate"] = videoprerollsummarymv["Video Completions"]/\
		                                                 videoprerollsummarymv["IMPRESSION"]
		
		videoprerollsummarymv["Placement# Name"] = videoprerollsummarymv[["PLACEMENT", "PLACEMENT_NAME"]].apply (
			lambda x:".".join (x), axis=1)
		
		videoprerollsummarymvfinal = videoprerollsummarymv.loc[:, ["Placement# Name", "IMPRESSION", "VIEWS25", "VIEWS50"
			, "VIEWS75", "Video Completions", "Video Completion Rate"]]
		
		self.logger.info ('Creating placement by day wise table for IO - {}'.format (self.config.ioid))
		dayprerollmv = self.read_sql_preroll_summary.merge (self.read_sql_preroll_day, on="PLACEMENT", how="inner")
		
		dayprerollsummarymv = dayprerollmv.loc[:,
		                      ["PLACEMENT", "PLACEMENT_NAME", "DAY", "IMPRESSION", "CLICKTHROUGHS", "VIDEO_COMPLETIONS",
		                       "COMPLETIONS"]]
		
		mask5 = dayprerollmv["COST_TYPE"].isin (["CPM"])
		choicedayprerollcpm = dayprerollmv["VIDEO_COMPLETIONS"]
		mask6 = dayprerollmv["COST_TYPE"].isin (["CPCV"])
		choicedayprerollcpcv = dayprerollmv["COMPLETIONS"]
		
		dayprerollsummarymv["Video Completions"] = np.select ([mask5, mask6],
		                                                      [choicedayprerollcpm, choicedayprerollcpcv], default=0)
		
		dayprerollsummarymv["Video Completion Rate"] = dayprerollsummarymv["Video Completions"]/dayprerollsummarymv[
			"IMPRESSION"]
		dayprerollsummarymv["CTR"] = dayprerollsummarymv["CLICKTHROUGHS"]/dayprerollsummarymv["IMPRESSION"]
		
		dayprerollsummarymv["Placement# Name"] = dayprerollsummarymv[["PLACEMENT", "PLACEMENT_NAME"]].apply (
			lambda x:".".join (x), axis=1)
		
		dayprerollsummaryfinal = dayprerollsummarymv.loc[:,
		                         ["Placement# Name", "DAY", "IMPRESSION", "CLICKTHROUGHS", "CTR",
		                          "Video Completions", "Video Completion Rate"]]
		
		self.logger.info ('Creating Intraction wise table for IO - {}'.format (self.config.ioid))
		intractionsummarymv = self.read_sql_preroll_summary.merge (self.read_sql_preroll_interaction, on="PLACEMENT",
		                                                           how="inner")
		
		intractionclick = intractionsummarymv.loc[:, ["PLACEMENT", "PLACEMENT_NAME", "CLICK_TAG", "VWR_CLICKTHROUGH"]]
		intractionclick["Placement# Name"] = intractionclick[["PLACEMENT", "PLACEMENT_NAME"]].apply (
			lambda x:".".join (x), axis=1)
		
		intraction_final = None
		try:
			intraction_table_clicks = pd.pivot_table (intractionclick, index='Placement# Name',
			                                          values='VWR_CLICKTHROUGH',
			                                          columns='CLICK_TAG', aggfunc=np.sum, fill_value=0)
			intraction_table_clicks_new = intraction_table_clicks.reset_index ()
			intraction_table_clicks_r = intraction_table_clicks_new.loc[:, :]
			
			intraction_click_table_new = intraction_table_clicks_r.merge (prerollsummaryfinal, on="Placement# Name",
			                                                              how="inner")
			intraction_click_table_new["CTR"] = intraction_click_table_new["CLICKTHROUGHS"]/intraction_click_table_new[
				"IMPRESSION"]
			
			cols_drop = ["COST_TYPE", "UNIT_COST", "IMPRESSION", "CLICKTHROUGHS", "Video Completions",
			             "Video Completion Rate"]
			intraction_new_cols = intraction_click_table_new.drop (cols_drop, axis=1)
			
			intraction_new_cols["Total Clickthroughs"] = intraction_new_cols.iloc[:, 1:-1].sum (axis=1)
			
			intraction_final = intraction_new_cols.loc[:, :]
		
		except KeyError as e:
			self.logger.error (str (e)+' Not found in intraction table for IO - {}'.format (self.config.ioid))
			pass
		
		# return prerollsummaryfinal, videoprerollsummarymvfinal, intraction_final, dayprerollsummaryfinal
		self.prerollsummaryfinal = prerollsummaryfinal
		self.videoprerollsummarymvfinal = videoprerollsummarymvfinal
		self.intraction_final = intraction_final
		self.dayprerollsummaryfinal = dayprerollsummaryfinal
	
	def renameIntraction(self):
		"""
Renaming COlumns
		:return:
		"""
		# prerollsummaryfinal, videoprerollsummarymvfinal, intraction_final, dayprerollsummaryfinal = self.accessing_preroll_columns()
		
		self.logger.info ('Renaming columns for all tables')
		rename_preroll_summary_final = self.prerollsummaryfinal.rename (columns={
			"COST_TYPE":"Cost Type", "UNIT_COST":"Cost",
			"IMPRESSION":"Impressions",
			"CLICKTHROUGHS":"Clickthroughs"
		}, inplace=True)
		
		rename_video_preroll_summary_mv_final = self.videoprerollsummarymvfinal.rename (columns={
			"IMPRESSION":"Impressions",
			"VIEWS25":"25% View",
			"VIEWS50":"50% View",
			"VIEWS75":"75% View"
		}, inplace=True)
		
		renameday_preroll_summary_final = self.dayprerollsummaryfinal.rename (columns={
			"DAY":"Date", "IMPRESSION":"Impressions",
			"CLICKTHROUGHS":"Clickthroughs"
		}, inplace=True)
	
	# return prerollsummaryfinal, videoprerollsummarymvfinal, intraction_final, dayprerollsummaryfinal
	
	
	def writePreroll(self):
		"""
writing to excel all data
		:return:
		"""
		data_common_columns = self.config.common_columns_summary ()
		
		# prerollsummaryfinal, videoprerollsummarymvfinal, intraction_final, dayprerollsummaryfinal = self.renameIntraction()
		
		try:
			check_preroll_summary_final = self.prerollsummaryfinal.empty
			check_video_preroll_summary_mv_final = self.videoprerollsummarymvfinal.empty
			check_intraction_final = self.intraction_final.empty
			check_day_preroll_summary_final = self.dayprerollsummaryfinal.empty
			
			self.logger.info ('Writing Campaign information on preroll for IO - {}'.format (self.config.ioid))
			writing_data_common_columns = data_common_columns[1].to_excel (self.config.writer,
			                                                               sheet_name="Standard Pre Roll Details"
			                                                               .format (self.config.ioid), startcol=1,
			                                                               startrow=1,
			                                                               index=False, header=False)
			
			self.logger.info ('Writing placement information on preroll for IO - {}'.format (self.config.ioid))
			if check_day_preroll_summary_final is True:
				pass
			else:
				writing_preroll_summary_final = self.prerollsummaryfinal.to_excel (self.config.writer,
				                                                                   sheet_name="Standard Pre Roll Details".format (
					                                                                   self.config.ioid),
				                                                                   startcol=1, startrow=8, index=False,
				                                                                   header=True)
			
			self.logger.info ('Writing Video information on preroll for IO - {}'.format (self.config.ioid))
			if check_video_preroll_summary_mv_final is True:
				pass
			else:
				writing_video_preroll_summary_mv_final = self.videoprerollsummarymvfinal.to_excel (self.config.writer,
				                                                                                   sheet_name="Standard Pre Roll Details".format (
					                                                                                   self.config.ioid),
				                                                                                   startcol=1,
				                                                                                   startrow=len (
					                                                                                   self.prerollsummaryfinal)+13,
				                                                                                   index=False,
				                                                                                   header=True)
			
			self.logger.info ('Writing Intractions information on preroll for IO - {}'.format (self.config.ioid))
			if check_intraction_final is True:
				pass
			else:
				writing_intraction_final = self.intraction_final.to_excel (self.config.writer,
				                                                           sheet_name="Standard Pre Roll Details".format (
					                                                           self.config.ioid),
				                                                           startcol=1,
				                                                           startrow=len (self.prerollsummaryfinal)+len (
					                                                           self.videoprerollsummarymvfinal)+18
				                                                           , index=False, header=True)
			
			self.logger.info ('Writing placement by day informaton on preroll for IO - {}'.format (self.config.ioid))
			if check_day_preroll_summary_final is True:
				pass
			else:
				start_line = len (self.prerollsummaryfinal)+len (self.videoprerollsummarymvfinal)+len (
					self.intraction_final)+23
				start_row = start_line
				end_row = 0
				for placement, placement_df in self.dayprerollsummaryfinal.groupby ('Placement# Name'):
					
					writing_day_preroll_summary_final = placement_df.to_excel (self.config.writer,
					                                                           sheet_name="Standard Pre Roll Details".format (
						                                                           self.config.ioid),
					                                                           startcol=1, startrow=start_line,
					                                                           columns=["Placement# Name"],
					                                                           index=False, header=False,
					                                                           merge_cells=False)
					
					writing_day_preroll_summary_final_new = placement_df.to_excel (self.config.writer,
					                                                               sheet_name="Standard Pre Roll Details".format (
						                                                               self.config.ioid),
					                                                               startcol=1, startrow=start_line+1,
					                                                               columns=["Date",
					                                                                        "Impressions",
					                                                                        "Clickthroughs",
					                                                                        "CTR",
					                                                                        "Video Completions",
					                                                                        "Video Completion Rate"],
					                                                               index=False, header=True,
					                                                               merge_cells=False)
					start_line += len (placement_df)+2
					workbook = self.config.writer.book
					worksheet = self.config.writer.sheets["Standard Pre Roll Details".format (self.config.ioid)]
					worksheet.write_string (start_line, 1, 'Subtotal')
					start_row = start_line-len (placement_df)
					worksheet.write_formula (start_line, 2, '=sum(C{}:C{})'.format (start_row+1, start_line))
					worksheet.write_formula (start_line, 3, '=sum(D{}:D{})'.format (start_row+1, start_line))
					worksheet.write_formula (start_line, 4, '=IFERROR((D{}/C{}),0)'.format (start_line+1, start_line+1))
					worksheet.write_formula (start_line, 5, '=sum(F{}:F{})'.format (start_row+1, start_line))
					worksheet.write_formula (start_line, 6, '=IFERROR((F{}/C{}),0)'.format (start_line+1, start_line+1))
					
					end_row = start_line
					
					column_chart = workbook.add_chart ({'type':'column'})
					column_x = "='Standard Pre Roll Details'!B{}:B{}".format (start_row+1, end_row)
					column_y = "='Standard Pre Roll Details'!C{}:C{}".format (start_row+1, end_row)
					
					column_chart.add_series ({'categories':column_x, 'values':column_y, 'name':'Impression'})
					column_chart.set_title ({'name':'Impressions vs CTR'})
					
					line_chart = workbook.add_chart ({'type':'line'})
					
					line_x = "='Standard Pre Roll Details'!B{}:B{}".format (start_row+1, end_row)
					line_y = "='Standard Pre Roll Details'!E{}:E{}".format (start_row+1, end_row)
					
					line_chart.add_series ({'categories':line_x, 'values':line_y, 'name':'CTR', 'y2_axis':True})
					
					column_chart.combine (line_chart)
					
					column_chart.set_size ({'width':800})
					worksheet.insert_chart ('I{}'.format (start_row), column_chart)
					column_chart.set_y_axis ({'num_format':'#,##0'})
					line_chart.set_y2_axis ({'num_format':'0.00%'})
					
					format_subtotal = workbook.add_format ({"bold":True, "bg_color":'#E7E6E6'})
					percent_fmt = workbook.add_format ({"num_format":"0.00%", "align":"center"})
					# money_fmt = workbook.add_format({"num_format":"$#,###0.00", "align":"center"})
					format_number = workbook.add_format ({"num_format":"#,##0", "align":"center"})
					
					worksheet.conditional_format (end_row, 1, end_row, 6,
					                              {"type":"no_blanks", "format":format_subtotal})
					worksheet.conditional_format (end_row+3, 1, end_row+3, 6,
					                              {"type":"no_blanks", "format":format_subtotal})
					# worksheet.conditional_format(end_row+3, 1, end_row+3, 6, {"type":"blanks", "format":format_subtotal})
					worksheet.conditional_format (start_row, 2, end_row, 3,
					                              {"type":"no_blanks", "format":format_number})
					worksheet.conditional_format (start_row, 4, end_row, 4, {"type":"no_blanks", "format":percent_fmt})
					worksheet.conditional_format (start_row, 5, end_row, 5,
					                              {"type":"no_blanks", "format":format_number})
					worksheet.conditional_format (start_row, 6, end_row, 6, {"type":"no_blanks", "format":percent_fmt})
					
					format_border_left = workbook.add_format ({"left":2})
					format_border_right = workbook.add_format ({"right":2})
					format_border_top = workbook.add_format ({"top":2})
					format_border_bottom = workbook.add_format ({"bottom":2})
					
					worksheet.conditional_format (start_row-2, 0, end_row, 0,
					                              {"type":"blanks", "format":format_border_right})
					worksheet.conditional_format (start_row-2, 7, end_row, 7,
					                              {"type":"blanks", "format":format_border_left})
					worksheet.conditional_format (end_row+1, 1, end_row+1, 6,
					                              {"type":"blanks", "format":format_border_top})
					worksheet.conditional_format (start_row-3, 1, start_row-4, 6,
					                              {"type":"blanks", "format":format_border_bottom})
					
					format_placement_by_date_header = workbook.add_format (
						{"bg_color":'#595959', 'font_color':'#FFFFFF', "bold":True})
					
					worksheet.conditional_format (start_row-1, 1, start_row-1, 1, {
						"type":"text", 'criteria':'containing', 'value':'Date', 'format':format_placement_by_date_header
					})
					
					worksheet.conditional_format (start_row-1, 2, start_row-1, 2,
					                              {
						                              "type":"text", 'criteria':'containing', 'value':'Impressions',
						                              'format':format_placement_by_date_header
					                              })
					
					worksheet.conditional_format (start_row-1, 3, start_row-1, 3, {
						"type":"text", 'criteria':'containing', 'value':'Clickthroughs',
						'format':format_placement_by_date_header
					})
					
					worksheet.conditional_format (start_row-1, 4, start_row-1, 4, {
						"type":"text", 'criteria':'containing', 'value':'CTR',
						'format':format_placement_by_date_header
					})
					worksheet.conditional_format (start_row-1, 5, start_row-1, 5, {
						"type":"text", 'criteria':'containing', 'value':'Video Completions',
						'format':format_placement_by_date_header
					})
					worksheet.conditional_format (start_row-1, 6, start_row-1, 6, {
						"type":"text", 'criteria':'containing',
						'value':'Video Completion Rate', 'format':format_placement_by_date_header
					})
					
					worksheet.conditional_format (start_row-2, 1, start_row-2, 6,
					                              {'type':'blanks', 'format':format_subtotal})
					worksheet.conditional_format (start_row-2, 1, start_row-2, 6,
					                              {'type':'no_blanks', 'format':format_subtotal})
					
					start_line += 3
		
		except AttributeError as e:
			pass
		# self.logger.error(str(e)+' Not found in intraction')
		
		# return prerollsummaryfinal, videoprerollsummarymvfinal, intraction_final, dayprerollsummaryfinal
	
	def formatting(self):
		
		"""
Applying Formatting
		"""
		# prerollsummaryfinal, videoprerollsummarymvfinal, intraction_final, dayprerollsummaryfinal = self.writePreroll()
		
		self.logger.info ('Applying Formatting on preroll sheet for IO - {}'.format (self.config.ioid))
		try:
			workbook = self.config.writer.book
			worksheet = self.config.writer.sheets["Standard Pre Roll Details".format (self.config.ioid)]
			
			check_preroll_summary_final = self.prerollsummaryfinal.empty
			check_video_preroll_summary_mv_final = self.videoprerollsummarymvfinal.empty
			check_interaction_final = self.intraction_final.empty
			check_day_preroll_summary_final = self.dayprerollsummaryfinal.empty
			
			unique_day_preroll_summary_final = self.dayprerollsummaryfinal["Placement# Name"].nunique ()
			data_common_columns = self.config.common_columns_summary ()
			
			number_rows_preroll_summary = self.prerollsummaryfinal.shape[0]
			number_cols_preroll_summary = self.prerollsummaryfinal.shape[1]
			number_rows_video_preroll_summary = self.videoprerollsummarymvfinal.shape[0]
			number_cols_video_preroll_summary = self.videoprerollsummarymvfinal.shape[1]
			number_rows_interaction_final = self.intraction_final.shape[0]
			number_cols_interaction_final = self.intraction_final.shape[1]
			number_rows_day_preroll_summary = self.dayprerollsummaryfinal.shape[0]
			number_cols_day_preroll_summary = self.dayprerollsummaryfinal.shape[1]
			
			worksheet.hide_gridlines (2)
			worksheet.set_row (0, 6)
			worksheet.set_column ("A:A", 2)
			
			alignment = workbook.add_format ({"align":"center"})
			
			worksheet.insert_image ("O6", "Exponential.png", {"url":"https://www.tribalfusion.com"})
			worksheet.insert_image ("O2", "Client_Logo.png")
			
			# format_campaign_info = workbook.add_format( {"bg_color":'#F0F8FF', "align":"left"} )
			format_campaign_info = workbook.add_format ({"bold":True, "bg_color":'#00B0F0', "align":"left"})
			
			worksheet.conditional_format ("A1:R5", {"type":"blanks", "format":format_campaign_info})
			worksheet.conditional_format ("A1:R5", {"type":"no_blanks", "format":format_campaign_info})
			
			format_merge_row = workbook.add_format ({
				                                        "bold":True, "font_color":'#FFFFFF', "align":"centre",
				                                        "fg_color":"#00B0F0", "border":2
			                                        })
			
			format_headers = workbook.add_format ({"bold":True, "bg_color":'#E7E6E6'})
			
			money_fmt = workbook.add_format ({"num_format":"$#,###0.00", "align":"center"})
			format_number = workbook.add_format ({"num_format":"#,##0", "align":"center"})
			percent_fmt = workbook.add_format ({"num_format":"0.00%", "align":"center"})
			
			format_border_right = workbook.add_format ({"right":2})
			format_border_left = workbook.add_format ({"left":2})
			format_border_top = workbook.add_format ({"top":2})
			format_border_bottom = workbook.add_format ({"bottom":2})
			
			if check_day_preroll_summary_final is True:
				pass
			else:
				worksheet.merge_range (7, 1, 7, number_cols_preroll_summary, "Standard Pre Roll Performance - Summary",
				                       format_merge_row)
				worksheet.conditional_format (8, 1, 8, number_cols_preroll_summary,
				                              {"type":"no_blanks", "format":format_headers})
				worksheet.conditional_format (9, 3, number_rows_preroll_summary+8, 3,
				                              {"type":"no_blanks", "format":money_fmt})
				worksheet.conditional_format (9, 4, number_rows_preroll_summary+8, 5,
				                              {"type":"no_blanks", "format":format_number})
				worksheet.conditional_format (9, 6, number_rows_preroll_summary+8, 6,
				                              {"type":"no_blanks", "format":percent_fmt})
				worksheet.conditional_format (9, 7, number_rows_preroll_summary+8, 7,
				                              {"type":"no_blanks", "format":format_number})
				worksheet.conditional_format (9, 8, number_rows_preroll_summary+8, 8,
				                              {"type":"no_blanks", "format":percent_fmt})
				worksheet.write_string (number_rows_preroll_summary+9, 1, "Subtotal")
				worksheet.write_formula (number_rows_preroll_summary+9, 4,
				                         '=sum(E{}:E{})'.format (10, number_rows_preroll_summary+9), format_number)
				worksheet.write_formula (number_rows_preroll_summary+9, 5,
				                         '=sum(F{}:F{})'.format (10, number_rows_preroll_summary+9), format_number)
				worksheet.write_formula (number_rows_preroll_summary+9, 6,
				                         '=IFERROR((F{}/E{}),0)'.format (number_rows_preroll_summary+10,
				                                                         number_rows_preroll_summary+10), percent_fmt)
				worksheet.write_formula (number_rows_preroll_summary+9, 7,
				                         '=sum(H{}:H{})'.format (10, number_rows_preroll_summary+9), format_number)
				worksheet.write_formula (number_rows_preroll_summary+9, 8,
				                         '=IFERROR((H{}/E{}),0)'.format (number_rows_preroll_summary+10,
				                                                         number_rows_preroll_summary+10), percent_fmt)
				
				worksheet.conditional_format (8, 0, number_rows_preroll_summary+9, 0,
				                              {"type":"blanks", "format":format_border_right})
				worksheet.conditional_format (8, number_cols_preroll_summary+1, number_rows_preroll_summary+9,
				                              number_cols_preroll_summary+1,
				                              {"type":"blanks", "format":format_border_left})
				worksheet.conditional_format (number_rows_preroll_summary+10, 1, number_rows_preroll_summary+10,
				                              number_cols_preroll_summary,
				                              {"type":"blanks", "format":format_border_top})
				worksheet.conditional_format (number_rows_preroll_summary+9, 1, number_rows_preroll_summary+9,
				                              number_cols_preroll_summary,
				                              {"type":"blanks", "format":format_headers})
				worksheet.conditional_format (number_rows_preroll_summary+9, 1, number_rows_preroll_summary+9,
				                              number_cols_preroll_summary,
				                              {"type":"no_blanks", "format":format_headers})
			
			if check_video_preroll_summary_mv_final is True:
				pass
			else:
				worksheet.merge_range (number_rows_preroll_summary+12, 1, number_rows_preroll_summary+12,
				                       number_cols_video_preroll_summary, "Video Performance", format_merge_row)
				worksheet.conditional_format (number_rows_preroll_summary+13, 1, number_rows_preroll_summary+13,
				                              number_cols_video_preroll_summary,
				                              {"type":"no_blanks", "format":format_headers})
				worksheet.conditional_format (number_rows_preroll_summary+14, 2,
				                              number_rows_preroll_summary+number_rows_video_preroll_summary+14, 6, {
					                              "type":"no_blanks",
					                              "format":format_number
				                              })
				worksheet.conditional_format (number_rows_preroll_summary+14, 7,
				                              number_rows_preroll_summary+number_rows_video_preroll_summary+14, 7,
				                              {"type":"no_blanks", "format":percent_fmt})
				worksheet.write_string (number_rows_preroll_summary+number_rows_video_preroll_summary+14, 1, "Subtotal")
				
				worksheet.write_formula (number_rows_preroll_summary+number_rows_video_preroll_summary+14, 2,
				                         '=sum(C{}:C{})'.format (number_rows_preroll_summary+15,
				                                                 number_rows_preroll_summary+number_rows_video_preroll_summary+14))
				
				worksheet.write_formula (number_rows_preroll_summary+number_rows_video_preroll_summary+14, 3,
				                         '=sum(D{}:D{})'.format (number_rows_preroll_summary+15,
				                                                 number_rows_preroll_summary+number_rows_video_preroll_summary+14))
				
				worksheet.write_formula (number_rows_preroll_summary+number_rows_video_preroll_summary+14, 4,
				                         '=sum(E{}:E{})'.format (number_rows_preroll_summary+15,
				                                                 number_rows_preroll_summary+number_rows_video_preroll_summary+14))
				
				worksheet.write_formula (number_rows_preroll_summary+number_rows_video_preroll_summary+14, 5,
				                         '=sum(F{}:F{})'.format (number_rows_preroll_summary+15,
				                                                 number_rows_preroll_summary+number_rows_video_preroll_summary+14))
				
				worksheet.write_formula (number_rows_preroll_summary+number_rows_video_preroll_summary+14, 6,
				                         '=sum(G{}:G{})'.format (number_rows_preroll_summary+15,
				                                                 number_rows_preroll_summary+number_rows_video_preroll_summary+14))
				
				worksheet.write_formula (number_rows_preroll_summary+number_rows_video_preroll_summary+14, 7,
				                         '=IFERROR((G{}/C{}),0)'.format (
					                         number_rows_preroll_summary+number_rows_video_preroll_summary+15,
					                         number_rows_preroll_summary
					                         +number_rows_video_preroll_summary+15))
				
				worksheet.conditional_format (number_rows_preroll_summary+13, 0,
				                              number_rows_preroll_summary+number_rows_video_preroll_summary+14, 0,
				                              {"type":"blanks", "format":format_border_right})
				
				worksheet.conditional_format (number_rows_preroll_summary+13, number_cols_video_preroll_summary+1,
				                              number_rows_preroll_summary+number_rows_video_preroll_summary+14,
				                              number_cols_video_preroll_summary+1,
				                              {"type":"blanks", "format":format_border_left})
				
				worksheet.conditional_format (number_rows_preroll_summary+number_rows_video_preroll_summary+15, 1,
				                              number_rows_preroll_summary+number_rows_video_preroll_summary+15,
				                              number_cols_video_preroll_summary,
				                              {"type":"blanks", "format":format_border_top})
				
				worksheet.conditional_format (number_rows_preroll_summary+number_rows_video_preroll_summary+14, 1,
				                              number_rows_preroll_summary+number_rows_video_preroll_summary+14,
				                              number_cols_video_preroll_summary,
				                              {"type":"no_blanks", "format":format_headers})
			# worksheet.conditional_format()
			
			if check_interaction_final is True:
				pass
			else:
				worksheet.merge_range (number_rows_preroll_summary+number_rows_video_preroll_summary+17, 1,
				                       number_rows_preroll_summary+number_rows_video_preroll_summary+17,
				                       number_cols_interaction_final, "Click Throughs", format_merge_row)
				
				worksheet.conditional_format (number_rows_preroll_summary+number_rows_video_preroll_summary+18, 1,
				                              number_rows_preroll_summary+number_rows_video_preroll_summary+18,
				                              number_cols_interaction_final,
				                              {"type":"no_blanks", "format":format_headers})
				
				worksheet.conditional_format (number_rows_preroll_summary+number_rows_video_preroll_summary+19,
				                              number_cols_interaction_final-2,
				                              number_rows_preroll_summary+number_rows_video_preroll_summary+number_rows_interaction_final+19,
				                              number_cols_interaction_final-2,
				                              {"type":"no_blanks", "format":format_number})
				
				worksheet.conditional_format (number_rows_preroll_summary+number_rows_video_preroll_summary+19,
				                              number_cols_interaction_final-1,
				                              number_rows_preroll_summary+number_rows_video_preroll_summary+number_rows_interaction_final+19,
				                              number_cols_interaction_final-1,
				                              {"type":"no_blanks", "format":percent_fmt})
				
				worksheet.conditional_format (number_rows_preroll_summary+number_rows_video_preroll_summary+19,
				                              number_cols_interaction_final,
				                              number_rows_preroll_summary+number_rows_video_preroll_summary
				                              +number_rows_interaction_final+19, number_cols_interaction_final,
				                              {"type":"no_blanks", "format":format_number})
				worksheet.write_string (
					number_rows_preroll_summary+number_rows_video_preroll_summary+number_rows_interaction_final+19, 1,
					"Subtotal")
				
				for col in range (2, number_cols_interaction_final-1):
					cell_location = xl_rowcol_to_cell (
						number_rows_preroll_summary+number_rows_video_preroll_summary+number_rows_interaction_final+19,
						col)
					start_range = xl_rowcol_to_cell (number_rows_preroll_summary+number_rows_video_preroll_summary+19,
					                                 col)
					end_range = xl_rowcol_to_cell (
						number_rows_preroll_summary+number_rows_video_preroll_summary+number_rows_interaction_final+18,
						col)
					formula = '=sum({:s}:{:s})'.format (start_range, end_range)
					worksheet.write_formula (cell_location, formula, format_number)
				
				# print number_cols_interaction_final, number_cols_interaction_final+1
				for col in range (number_cols_interaction_final, number_cols_interaction_final+1):
					cell_location = xl_rowcol_to_cell (
						number_rows_preroll_summary+number_rows_video_preroll_summary+number_rows_interaction_final+19,
						col)
					start_range = xl_rowcol_to_cell (number_rows_preroll_summary+number_rows_video_preroll_summary+19,
					                                 col)
					end_range = xl_rowcol_to_cell (
						number_rows_preroll_summary+number_rows_video_preroll_summary+number_rows_interaction_final+18,
						col)
					formula = '=sum({:s}:{:s})'.format (start_range, end_range)
					worksheet.write_formula (cell_location, formula, format_number)
				
				for col in range (number_cols_interaction_final-1, number_cols_interaction_final):
					cell_location = xl_rowcol_to_cell (
						number_rows_preroll_summary+number_rows_video_preroll_summary+number_rows_interaction_final+19,
						col)
					start_range = xl_rowcol_to_cell (
						number_rows_preroll_summary+number_rows_video_preroll_summary+number_rows_interaction_final+19,
						col+1)
					end_range = xl_rowcol_to_cell (number_rows_preroll_summary+9, 4)
					formula = '=IFERROR(({:s}/{:s}),0)'.format (start_range, end_range)
					worksheet.write_formula (cell_location, formula, percent_fmt)
				
				worksheet.conditional_format (number_rows_preroll_summary+number_rows_video_preroll_summary+18, 0,
				                              number_rows_preroll_summary+number_rows_video_preroll_summary
				                              +number_rows_interaction_final+19, 0,
				                              {"type":"blanks", "format":format_border_right})
				worksheet.conditional_format (number_rows_preroll_summary+number_rows_video_preroll_summary+18,
				                              number_cols_interaction_final+1,
				                              number_rows_preroll_summary+number_rows_video_preroll_summary+number_rows_interaction_final+19,
				                              0,
				                              {"type":"blanks", "format":format_border_left})
				
				worksheet.conditional_format (
					number_rows_preroll_summary+number_rows_video_preroll_summary+number_rows_interaction_final+20, 1,
					number_rows_preroll_summary+number_rows_video_preroll_summary
					+number_rows_interaction_final+20, number_cols_interaction_final,
					{"type":"blanks", "format":format_border_top})
				
				worksheet.conditional_format (
					number_rows_preroll_summary+number_rows_video_preroll_summary+number_rows_interaction_final+19, 1,
					number_rows_preroll_summary+number_rows_video_preroll_summary
					+number_rows_interaction_final+19, number_cols_interaction_final,
					{"type":"no_blanks", "format":format_headers})
			
			row_start = number_rows_preroll_summary+number_rows_video_preroll_summary+number_rows_interaction_final+26
			row_end = number_rows_preroll_summary+number_rows_video_preroll_summary+number_rows_interaction_final+\
			          number_rows_day_preroll_summary+unique_day_preroll_summary_final*5+21
			
			if check_day_preroll_summary_final is True:
				pass
			else:
				worksheet.write_string (row_end+3, 1, "Grand Total", format_headers)
				worksheet.write_formula (row_end+3, 2,
				                         '=SUMIF(B{}:B{},"Subtotal",C{}:C{})'.format (row_start, row_end, row_start,
				                                                                      row_end), format_number)
				worksheet.write_formula (row_end+3, 3,
				                         '=SUMIF(B{}:B{},"Subtotal",D{}:D{})'.format (row_start, row_end, row_start,
				                                                                      row_end), format_number)
				worksheet.write_formula (row_end+3, 4, '=IFERROR(D{}/C{},0)'.format (row_end+4, row_end+4), percent_fmt)
				worksheet.write_formula (row_end+3, 5,
				                         '=SUMIF(B{}:B{},"Subtotal",F{}:F{})'.format (row_start, row_end, row_start,
				                                                                      row_end), format_number)
				worksheet.write_formula (row_end+3, 6, '=IFERROR((F{}/C{}),0)'.format (row_end+4, row_end+4),
				                         percent_fmt)
				
				worksheet.conditional_format (row_end+3, 2, row_end+3, 6, {"type":"no_blanks", "format":format_headers})
				worksheet.conditional_format (row_end+3, 0, row_end+3, 0,
				                              {"type":"blanks", "format":format_border_right})
				worksheet.conditional_format (row_end+2, 1, row_end+2, 6,
				                              {"type":"blanks", "format":format_border_bottom})
				worksheet.conditional_format (row_end+3, 7, row_end+3, 7,
				                              {"type":"blanks", "format":format_border_left})
				worksheet.conditional_format (row_end+4, 1, row_end+4, 6, {"type":"blanks", "format":format_border_top})
			# formatPlacementByDateHeader = workbook.add_format({"bg_color":'#595959', 'font_color':'#FFFFFF',
			# "bold":True, "align":"center"})
			
			
			# row_start = number_rows_preroll_summary+number_rows_video_preroll_summary
			# +number_rows_interaction_final+24
			# row_end = number_rows_preroll_summary+number_rows_video_preroll_summary+number_rows_interaction_final\
			# +number_rows_day_preroll_summary+unique_day_preroll_summary_final*5+22
			
			"""worksheet.conditional_format(row_start,1,row_end,number_cols_day_preroll_summary-1,{"type":"text", 'criteria':'containing',
			                                                                    'value':'Date','format':formatPlacementByDateHeader})"""
			
			format_merge_row_black = workbook.add_format ({
				                                              "bold":True, "font_color":'#000000', "align":"centre",
				                                              "fg_color":"#00B0F0", "border":2, "border_color":"#000000"
			                                              })
			
			if check_day_preroll_summary_final is True:
				pass
			else:
				worksheet.merge_range (
					number_rows_preroll_summary+number_rows_video_preroll_summary+number_rows_interaction_final+22
					, 1, number_rows_preroll_summary+number_rows_video_preroll_summary+number_rows_interaction_final+22,
					number_cols_day_preroll_summary-1, "Breakdown By Day + Placement", format_merge_row_black)
			
			# worksheet.conditional_format("")
			# worksheet.conditional_format()
			
			worksheet.set_column ("C:L", 21, alignment)
			worksheet.set_column ("B:B", 21)
			worksheet.set_zoom (75)
		
		except AttributeError as e:
			pass
	
	def main(self):
		"""
main function
		"""
		self.config.common_columns_summary ()
		self.connect_TFR_Intraction ()
		self.read_query_preroll ()
		if self.read_sql_preroll_summary.empty:
			self.logger.info ("No instream placements for IO - {}".format (self.config.ioid))
			pass
		else:
			self.accessing_preroll_columns ()
			self.renameIntraction ()
			self.writePreroll ()
			self.logger.info ("Instream placements found for IO - {}".format (self.config.ioid))
			self.formatting ()
			self.logger.info ('Instream Sheet created for IO {}'.format (self.config.ioid))


if __name__=="__main__":
	pass

# enable it when running for individual file
# c = config.Config('Test', 606817)
# o = Intraction( c )
# o.main()
# c.saveAndCloseWriter()
