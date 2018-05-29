#coding=utf-8
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
from functools import reduce
pandas.io.formats.excel.header_style = None

class Intraction(object):
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
		sql_preroll_summary = "select * from (select substr(PLACEMENT_DESC, 1, INSTR(PLACEMENT_DESC, '.', 1)-1) as Placement, SDATE as Start_Date, EDATE as End_Date, initcap(CREATIVE_DESC)  as Placement_Name, COST_TYPE_DESC as Cost_type, UNIT_COST as Unit_Cost, BUDGET as Planned_Cost, BOOKED_QTY as Booked_Imp_Booked_Eng from  TFR_REP.SUMMARY_MV where (IO_ID = {}) AND (DATA_SOURCE = 'KM') AND CREATIVE_DESC IN(SELECT DISTINCT CREATIVE_DESC FROM TFR_REP.SUMMARY_MV) ORDER BY PLACEMENT_ID) WHERE Placement_Name IN ('Pre-Roll - Desktop','Pre-Roll - Desktop + Mobile','Pre-Roll – Desktop + Mobile','Pre-Roll - In-Stream/Mobile Blend','Pre-Roll - Mobile','Pre-Roll -Desktop','Pre-Roll - In-Stream')".format(self.config.ioid)
		
		sql_preroll_mv = "select substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as Placement, sum(IMPRESSIONS) as Impression, sum(CPCV_COUNT) as Completions,sum(VWR_CLICK_THROUGHS) as Clickthroughs , sum(VWR_VIDEO_VIEW_100_PC_COUNT) as Video_Completions from TFR_REP.KEY_METRIC_MV WHERE IO_ID = {} GROUP BY PLACEMENT_ID, PLACEMENT_DESC ORDER BY PLACEMENT_ID".format(self.config.ioid)
		
		sql_preroll_video_views = "select substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as Placement,sum(IMPRESSIONS) as Impression, sum(VWR_VIDEO_VIEW_25_PC_COUNT) as Views25,sum(VWR_VIDEO_VIEW_50_PC_COUNT) as Views50, sum(VWR_VIDEO_VIEW_75_PC_COUNT) as Views75,sum(VWR_VIDEO_VIEW_100_PC_COUNT) as Video_Completions, sum(CPCV_COUNT) as Completions from TFR_REP.KEY_METRIC_MV WHERE IO_ID = {} GROUP BY PLACEMENT_ID, PLACEMENT_DESC ORDER BY PLACEMENT_ID".format(self.config.ioid)
		
		sql_video_details = "select substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as Placement,FEV_INT_VIDEO_DESC,sum(VWR_VIDEO_VIEW_0_PC_COUNT) as Views0,sum(VWR_VIDEO_VIEW_25_PC_COUNT) as Views25,sum(VWR_VIDEO_VIEW_50_PC_COUNT) as Views50,sum(VWR_VIDEO_VIEW_75_PC_COUNT) as Views75,sum(VWR_VIDEO_VIEW_100_PC_COUNT) as Video_Completions FROM TFR_REP.VIDEO_DETAIL_MV WHERE IO_ID = {} GROUP BY PLACEMENT_ID, PLACEMENT_DESC,FEV_INT_VIDEO_DESC ORDER BY PLACEMENT_ID".format(self.config.ioid)
		
		sql_preroll_day_mv = "select substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as Placement, TO_CHAR(TO_DATE(DAY_DESC, 'MM/DD/YYYY'),'YYYY-MM-DD') as DAY_DESC, sum(IMPRESSIONS) as Impression, sum(CPCV_COUNT) as Completions, sum(VWR_CLICK_THROUGHS) as Clickthroughs , sum(VWR_VIDEO_VIEW_100_PC_COUNT) as Video_Completions from TFR_REP.KEY_METRIC_MV WHERE IO_ID = {} GROUP BY PLACEMENT_ID, PLACEMENT_DESC,DAY_DESC ORDER BY PLACEMENT_ID".format(self.config.ioid)
		
		sql_preroll_interaction = "select substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as Placement,BLAZE_TAG_NAME_DESC as Click_Tag, sum(VWR_INTERACTION) as VWR_Clickthrough from TFR_REP.INTERACTION_DETAIL_MV WHERE IO_ID = {} and BLAZE_ACTION_TYPE_DESC = 'Click-thru' GROUP BY PLACEMENT_ID, PLACEMENT_DESC, BLAZE_TAG_NAME_DESC ORDER BY PLACEMENT_ID".format(self.config.ioid)
		
		sql_preroll_video_player = "SELECT substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as Placement,sum(VWR_MUTE) as Vwrmute,sum(VWR_UNMUTE) as Vwrunmute,sum(VWR_PAUSE) as Vwrpause,sum(VWR_REWIND) as Vwrrewind, sum(VWR_RESUME) as Vwrresume,sum(VWR_REPLAY) as Vwrreplay, sum(VWR_FULL_SCREEN) as Vwrfullscreen FROM TFR_REP.VIDEO_DETAIL_MV WHERE IO_ID = {} GROUP BY PLACEMENT_ID, PLACEMENT_DESC ORDER BY PLACEMENT_ID".format(self.config.ioid)
		
		self.sql_preroll_summary = sql_preroll_summary
		self.sql_preroll_mv = sql_preroll_mv
		self.sql_preroll_video_views = sql_preroll_video_views
		self.sql_video_details = sql_video_details
		self.sql_preroll_day_mv = sql_preroll_day_mv
		self.sql_preroll_interaction = sql_preroll_interaction
		self.sql_preroll_video_player = sql_preroll_video_player
		
	def read_query_preroll(self):
		
		"""
Reading Queries Data directly from TFR
		:return:
		"""
		
		self.logger.info ('Running Query for Preroll placements for IO {}'.format (self.config.ioid))
		#sql_preroll_summary, sql_preroll_mv, sql_preroll_video_views, sql_preroll_day_mv, sql_preroll_interaction = self.connect_TFR_Intraction()
		
		read_sql_preroll_summary = pd.read_sql(self.sql_preroll_summary,self.config.conn)
		
		read_sql_preroll_mv = pd.read_sql(self.sql_preroll_mv,self.config.conn)
		
		read_sql_preroll_video = pd.read_sql(self.sql_preroll_video_views, self.config.conn)
		
		read_sql_video_details = pd.read_sql(self.sql_video_details,self.config.conn)
		
		read_sql_preroll_day = pd.read_sql(self.sql_preroll_day_mv, self.config.conn)
		
		read_sql_preroll_interaction = pd.read_sql(self.sql_preroll_interaction, self.config.conn)
	
		read_sql_preroll_video_player = pd.read_sql(self.sql_preroll_video_player,self.config.conn)
		
		#self.read_sql_preroll_day = read_sql_preroll_day
	
		#return read_sql_preroll_summary, read_sql_preroll_mv, read_sql_preroll_video ,read_sql_preroll_day ,read_sql_preroll_interaction
		
		self.read_sql_preroll_summary = read_sql_preroll_summary
		self.read_sql_preroll_mv = read_sql_preroll_mv
		self.read_sql_preroll_video = read_sql_preroll_video
		self.read_sql_video_details = read_sql_video_details
		self.read_sql_preroll_day = read_sql_preroll_day
		self.read_sql_preroll_interaction = read_sql_preroll_interaction
		self.read_sql_preroll_video_player = read_sql_preroll_video_player
		
	def accessing_preroll_columns(self):
		
		"""
Accessing Columns from Query
		:return:
		"""
		
		self.logger.info ('Query Stored for further processing of IO - {}'.format (self.config.ioid))
		#read_sql_preroll_summary, read_sql_preroll_mv, read_sql_preroll_video, read_sql_preroll_day, read_sql_preroll_interaction  = self.read_query_preroll()
		
		self.logger.info('Creating placement wise table of IO - {}'.format(self.config.ioid))
		prerollsummaryfinal = None
		videoprerollsummarymvfinal = None
		intraction_final = None
		dayprerollsummaryfinal = None
		video_player_summary_final = None
		try:
			placementprerollmv = self.read_sql_preroll_summary.merge(self.read_sql_preroll_mv, on = "PLACEMENT", how= "inner")
			
			prerollsummarymv = placementprerollmv.loc[:,["PLACEMENT","PLACEMENT_NAME","COST_TYPE","UNIT_COST","IMPRESSION",
			                                       "CLICKTHROUGHS","COMPLETIONS","VIDEO_COMPLETIONS"]]
			
			mask1 = prerollsummarymv["COST_TYPE"].isin(['CPM'])
			choiceprerollsummarymvcpm = prerollsummarymv["VIDEO_COMPLETIONS"]
			mask2 = prerollsummarymv["COST_TYPE"].isin(['CPCV'])
			choiceprerollsummarymvcpcv = prerollsummarymv["COMPLETIONS"]
			choicespendcpm = prerollsummarymv["IMPRESSION"]/1000*prerollsummarymv["UNIT_COST"]
			choicespendcpcv = prerollsummarymv["VIDEO_COMPLETIONS"]*prerollsummarymv["UNIT_COST"]
			
			prerollsummarymv["Video Completions"] = np.select([mask1,mask2],[choiceprerollsummarymvcpm,
			                                                                 choiceprerollsummarymvcpcv])
			
			prerollsummarymv["Spend"] = np.select ([mask1, mask2], [choicespendcpm, choicespendcpcv])
			
			prerollsummary = prerollsummarymv.loc[:,["PLACEMENT","PLACEMENT_NAME", "COST_TYPE", "UNIT_COST"
			                                        ,"IMPRESSION","CLICKTHROUGHS", "Video Completions","Spend"]]
			
			prerollsummary["Placement# Name"] = prerollsummary[["PLACEMENT","PLACEMENT_NAME"]].apply(lambda x:".".join(x), axis=1)
			
			prerollsummary["CTR"] = prerollsummary["CLICKTHROUGHS"]/prerollsummary["IMPRESSION"]
			prerollsummary["Video Completion Rate"] = prerollsummary["Video Completions"]/prerollsummary["IMPRESSION"]
			
			prerollsummaryfinal = prerollsummary.loc[:,["Placement# Name","COST_TYPE","UNIT_COST","IMPRESSION",
			                                            "CLICKTHROUGHS","CTR","Video Completions","Video Completion Rate",
			                                            "Spend"]]
			
			
			self.logger.info ('Creating Video wise table of IO - {}'.format (self.config.ioid))
			videoprerollmv = self.read_sql_preroll_summary.merge(self.read_sql_preroll_video, on="PLACEMENT")
			
			videoprerollsummarymv = videoprerollmv.loc[:,["PLACEMENT","PLACEMENT_NAME","COST_TYPE","IMPRESSION","COMPLETIONS"]]
			
			placement_by_preroll_video = [videoprerollsummarymv, self.read_sql_video_details]
			preroll_video_summary = reduce(lambda left, right:pd.merge (left, right, on=['PLACEMENT']),placement_by_preroll_video)
			
			
			mask3 = preroll_video_summary["COST_TYPE"].isin(['CPM'])
			choicevideoprerollcpm = preroll_video_summary["VIDEO_COMPLETIONS"]
			mask4 = preroll_video_summary["COST_TYPE"].isin(['CPCV'])
			choicevideoprerollcpcv = preroll_video_summary["COMPLETIONS"]
			
			preroll_video_summary["Video Completions"] = np.select([mask3,mask4],[choicevideoprerollcpm,
			                                                                      choicevideoprerollcpcv])
			mask8 = preroll_video_summary["COST_TYPE"].isin (['CPM', 'CPCV'])
			
			
			preroll_video_summary_new = preroll_video_summary.loc[preroll_video_summary.reset_index().groupby(['PLACEMENT'])['VIEWS0'].idxmax()]
			
			
			preroll_video_summary_new.loc[mask8,'VIEWS0'] = preroll_video_summary_new['IMPRESSION']
			
			
			preroll_video_summary = preroll_video_summary.drop(preroll_video_summary_new.index).append(preroll_video_summary_new).sort_index()
			
			preroll_video_summary["Views"] = preroll_video_summary["VIEWS0"]
			
			preroll_video_summary["Video Completion Rate"] = preroll_video_summary["VIDEO_COMPLETIONS"]/preroll_video_summary["Views"]
			
			preroll_video_summary["Placement# Name"] = preroll_video_summary[["PLACEMENT","PLACEMENT_NAME"]].apply(lambda x:".".join(x), axis=1)
			
			videoprerollsummarymvfinal = preroll_video_summary.loc[:,["Placement# Name","FEV_INT_VIDEO_DESC","Views","VIEWS25","VIEWS50"
			                                                          ,"VIEWS75","Video Completions","Video Completion Rate"]]
			
			videoprerollsummarymvfinal = videoprerollsummarymvfinal.loc[:, ["Placement# Name","FEV_INT_VIDEO_DESC","Views","VIEWS25","VIEWS50",
			                                                                "VIEWS75", "Video Completions",
			                                                                "Video Completion Rate"]]
			
			
			self.logger.info('Creating placement by day wise table for IO - {}'.format(self.config.ioid))
			dayprerollmv = self.read_sql_preroll_summary.merge(self.read_sql_preroll_day, on="PLACEMENT")
			
			
			dayprerollsummarymv = dayprerollmv.loc[:,["PLACEMENT","PLACEMENT_NAME","COST_TYPE","UNIT_COST","DAY_DESC","IMPRESSION","CLICKTHROUGHS","VIDEO_COMPLETIONS",
			                                          "COMPLETIONS"]]
			
			mask5 = dayprerollsummarymv["COST_TYPE"].isin(["CPM"])
			choicedayprerollcpm = dayprerollsummarymv["VIDEO_COMPLETIONS"]
			mask6 = dayprerollsummarymv["COST_TYPE"].isin(["CPCV"])
			choicedayprerollcpcv = dayprerollsummarymv["COMPLETIONS"]
			
			dayprerollsummarymv["Video Completions"] = np.select([mask5,mask6],[choicedayprerollcpm,choicedayprerollcpcv])
			choicedayspendcpm = dayprerollsummarymv["IMPRESSION"]/1000*dayprerollsummarymv["UNIT_COST"]
			choicedayspendcpcv = dayprerollsummarymv["Video Completions"]*dayprerollsummarymv["UNIT_COST"]
			dayprerollsummarymv["Spend"] = np.select([mask5,mask6],[choicedayspendcpm,choicedayspendcpcv])
			dayprerollsummarymv["VCR%"] = dayprerollsummarymv["Video Completions"]/dayprerollsummarymv["IMPRESSION"]
			dayprerollsummarymv["CTR"] = dayprerollsummarymv["CLICKTHROUGHS"]/dayprerollsummarymv["IMPRESSION"]
			
			dayprerollsummarymv["Placement# Name"] = dayprerollsummarymv[["PLACEMENT","PLACEMENT_NAME"]].apply(lambda x:".".join(x), axis=1)
			
			dayprerollsummaryfinal = dayprerollsummarymv.loc[:,["Placement# Name","DAY_DESC","IMPRESSION","CLICKTHROUGHS","CTR",
			                                                    "Video Completions","VCR%","Spend"]]
			
			self.logger.info('Creating Video player table for IO - {}'.format(self.config.ioid))
			video_player_mv = [self.read_sql_preroll_summary,self.read_sql_preroll_video_player]
			video_player_summary = reduce (lambda left, right:pd.merge (left, right, on=['PLACEMENT']),video_player_mv)
			
			video_player_summary["Placement# Name"] = video_player_summary[["PLACEMENT","PLACEMENT_NAME"]].apply(lambda x:".".join(x), axis=1)
			video_player_summary_final = video_player_summary.loc[:,["Placement# Name","VWRMUTE","VWRUNMUTE","VWRPAUSE",
			                                                         "VWRREWIND","VWRRESUME","VWRREPLAY","VWRFULLSCREEN"]]
			
			
			self.logger.info('Creating Intraction wise table for IO - {}'.format(self.config.ioid))
			intractionsummarymv = self.read_sql_preroll_summary.merge(self.read_sql_preroll_interaction,on="PLACEMENT")
			
			intractionclick = intractionsummarymv.loc[:,["PLACEMENT","PLACEMENT_NAME","CLICK_TAG","VWR_CLICKTHROUGH"]]
			intractionclick["Placement# Name"] = intractionclick[["PLACEMENT","PLACEMENT_NAME"]].apply(lambda x:".".join(x), axis=1)
			
			intraction_final = None
			intraction_table_clicks = pd.pivot_table(intractionclick,index = 'Placement# Name',values='VWR_CLICKTHROUGH',
			                                       columns='CLICK_TAG',aggfunc=np.sum,fill_value=0)
			intraction_table_clicks_new = intraction_table_clicks.reset_index()
			intraction_table_clicks_r = intraction_table_clicks_new.loc[:, :]
			
			
		
			intraction_click_table_new = intraction_table_clicks_r.merge(prerollsummaryfinal, on="Placement# Name", how="inner")
			
			#intraction_click_table_new["CTR"] = intraction_click_table_new["CLICKTHROUGHS"]/intraction_click_table_new["IMPRESSION"]
		
			
			cols_drop = ["COST_TYPE","UNIT_COST","IMPRESSION","CLICKTHROUGHS","Video Completions","Video Completion Rate",
			             "CTR","Spend","Placement# Name"]
			intraction_new_cols = intraction_click_table_new.drop(cols_drop,axis=1)
		
			#intraction_new_cols["Total Clickthroughs"] = intraction_new_cols.iloc[:,1:-1].sum(axis=1)
		
			intraction_final = intraction_new_cols.loc[:, :]
		
		except (KeyError,AttributeError,TypeError,IOError) as e:
			self.logger.error(str(e)+ ' Not found in intraction table for IO - {}'.format(self.config.ioid))
			pass
		
	
		self.prerollsummaryfinal = prerollsummaryfinal
		self.videoprerollsummarymvfinal = videoprerollsummarymvfinal
		self.video_player_summary_final = video_player_summary_final
		self.intraction_final = intraction_final
		self.dayprerollsummaryfinal = dayprerollsummaryfinal
		
		
	def renameIntraction(self):
		"""
Renaming COlumns
		:return:
		"""
		#prerollsummaryfinal, videoprerollsummarymvfinal, intraction_final, dayprerollsummaryfinal = self.accessing_preroll_columns()
		
		self.logger.info('Renaming columns for all tables')
		rename_preroll_summary_final = self.prerollsummaryfinal.rename(columns={"COST_TYPE":"Cost Type","UNIT_COST":"Unit Cost",
		                                                                "IMPRESSION":"Impressions",
		                                                                "CLICKTHROUGHS":"Clickthroughs","CTR":"CTR %",
		                                                                        "Video Completion Rate":"VCR %"},inplace=True)
		
		rename_video_preroll_summary_mv_final = self.videoprerollsummarymvfinal.rename(columns={"FEV_INT_VIDEO_DESC":"Video Name",
		                                                                                        "VIEWS25":"25% View",
		                                                                                        "VIEWS50":"50% View",
		                                                                                        "VIEWS75":"75% View"},inplace=True)
		
		renameday_preroll_summary_final = self.dayprerollsummaryfinal.rename(columns={"DAY_DESC":"Date","IMPRESSION":"Impressions",
		                                                                      "CLICKTHROUGHS":"Clickthroughs"},inplace=True)
		
		rename_video_player_summary_final = self.video_player_summary_final.rename(columns = {"VWRMUTE":"Mute",
		                                                                                      "VWRUNMUTE":"Unmute",
		                                                                                      "VWRPAUSE":"Pause",
		                                                                                      "VWRREWIND":"Rewind",
		                                                                                      "VWRRESUME":"Resume",
		                                                                                      "VWRREPLAY":"Replay",
		                                                                                      "VWRFULLSCREEN":"Fullscreen"},inplace=True)
		
	def writePreroll(self):
		"""
writing to excel all data
		:return:
		"""
		data_common_columns = self.config.common_columns_summary()
		
		try:
			check_preroll_summary_final = self.prerollsummaryfinal.empty
			check_video_preroll_summary_mv_final = self.videoprerollsummarymvfinal.empty
			check_intraction_final = self.intraction_final.empty
			check_day_preroll_summary_final = self.dayprerollsummaryfinal.empty
			check_video_player_summary_final = self.video_player_summary_final.empty
			
			self.logger.info('Writing Campaign information on preroll for IO - {}'.format(self.config.ioid))
			writing_data_common_columns = data_common_columns[1].to_excel(self.config.writer,sheet_name="Standard Pre Roll Details"
			                                                           .format(self.config.ioid),startcol=1,startrow=1,
			                                                                   index=False,header=False)
			
			self.logger.info ('Writing placement information on preroll for IO - {}'.format (self.config.ioid))
			if check_day_preroll_summary_final is True:
				pass
			else:
				writing_preroll_summary_final = self.prerollsummaryfinal.to_excel(self.config.writer,
				                                                          sheet_name="Standard Pre Roll Details".format(self.config.ioid),
				                                                          startcol=1,startrow=8,index=False,header=True)
			
			self.logger.info('Writing Video information on preroll for IO - {}'.format (self.config.ioid))
			if check_video_preroll_summary_mv_final is True:
				pass
			else:
				writing_video_preroll_summary_mv_final = self.videoprerollsummarymvfinal.to_excel(self.config.writer,
				                                                                        sheet_name="Standard Pre Roll Details".format(self.config.ioid),
				                                                                        startcol=1,
				                                                                        startrow=len(self.prerollsummaryfinal)+13,
				                                                                        index=False,header=True)
			
			self.logger.info ('Writing Intractions information on preroll for IO - {}'.format (self.config.ioid))
			if check_intraction_final is True:
				pass
			else:
				writing_video_player_final = self.video_player_summary_final.to_excel(self.config.writer,
				                                                  sheet_name="Standard Pre Roll Details".format(self.config.ioid),
				                                                  startcol=1,startrow=len(self.prerollsummaryfinal)+len(self.videoprerollsummarymvfinal)+18
				                                                  ,index=False,header=True)
				
			
			self.logger.info('Writing Video player information on preroll for IO - {}'.format (self.config.ioid))
			if check_video_player_summary_final is True:
				pass
			else:
				writing_intraction_player_final = self.intraction_final.to_excel(self.config.writer,
				                                                            sheet_name="Standard Pre Roll Details".format(self.config.ioid),
				                                                            startcol=self.video_player_summary_final.shape[1]+1,
				                                                            startrow = len(self.prerollsummaryfinal)+len(self.videoprerollsummarymvfinal)+18,
				                                                            index=False,header=True)
			
			self.logger.info('Writing placement by day informaton on preroll for IO - {}'.format(self.config.ioid))
			if check_day_preroll_summary_final is True:
				pass
			else:
				start_line = len( self.prerollsummaryfinal )+len( self.videoprerollsummarymvfinal )+len( self.intraction_final )+23
				for placement,placement_df in self.dayprerollsummaryfinal.groupby('Placement# Name'):
					
					writing_day_preroll_summary_final = placement_df.to_excel(self.config.writer,sheet_name="Standard Pre Roll Details".format(self.config.ioid),
					                                                      startcol=1,startrow=start_line,
					                                                      index=False, header=False,merge_cells=False)
					
					workbook = self.config.writer.book
					worksheet = self.config.writer.sheets["Standard Pre Roll Details".format (self.config.ioid)]
					start_line += len(placement_df)+2
					worksheet.write_string(start_line-2,1,'Subtotal')
					start_row = start_line-len(placement_df)
					format_num = workbook.add_format ({"num_format":"#,##0"})
					percent_fmt = workbook.add_format ({"num_format":"0.00%", "align":"right"})
					money_fmt = workbook.add_format ({"num_format":"$#,###0.00", "align":"right"})
					
					worksheet.write_formula(start_line-2,3,'=sum(D{}:D{})'.format( start_row-1, start_line-2),format_num)
					worksheet.write_formula (start_line-2,4, '=sum(E{}:E{})'.format (start_row-1, start_line-2),format_num)
					worksheet.write_formula( start_line-2,5, '=IFERROR((E{}/D{}),0)'.format(start_line-1,start_line-1),percent_fmt)
					worksheet.write_formula( start_line-2,6, '=sum(G{}:G{})'.format(start_row-1, start_line-2),format_num)
					worksheet.write_formula( start_line-2,7, '=IFERROR((G{}/D{}),0)'.format(start_line-1, start_line-1),percent_fmt)
					worksheet.write_formula(start_line-2,8,'=sum(I{}:I{})'.format(start_row-1, start_line-2),money_fmt)
					
					worksheet.conditional_format(start_row-2,3,start_line,4,{"type":"no_blanks","format":format_num})
					worksheet.conditional_format(start_row-2,5,start_line,5,{"type":"no_blanks","format":percent_fmt})
					worksheet.conditional_format(start_row-2,6,start_line,6,{"type":"no_blanks","format":format_num})
					worksheet.conditional_format(start_row-2,7,start_line,7,{"type":"no_blanks","format":percent_fmt})
					worksheet.conditional_format(start_row-2,8,start_line,8,{"type":"no_blanks","format":money_fmt})
					
		except (AttributeError,TypeError,IOError,KeyError) as e:
			self.logger.error(str(e)+"Not Found")
			pass
			#self.logger.error(str(e)+' Not found in intraction')
				
		#return prerollsummaryfinal, videoprerollsummarymvfinal, intraction_final, dayprerollsummaryfinal
	
	def formatting(self):
		
		"""
Applying Formatting
		"""
		#prerollsummaryfinal, videoprerollsummarymvfinal, intraction_final, dayprerollsummaryfinal = self.writePreroll()
		
		self.logger.info('Applying Formatting on preroll sheet for IO - {}'.format(self.config.ioid))
		try:
			workbook = self.config.writer.book
			worksheet = self.config.writer.sheets["Standard Pre Roll Details".format(self.config.ioid)]
			worksheet.set_zoom (75)
			
			check_preroll_summary_final = self.prerollsummaryfinal.empty
			check_video_preroll_summary_mv_final = self.videoprerollsummarymvfinal.empty
			check_interaction_final = self.intraction_final.empty
			check_day_preroll_summary_final = self.dayprerollsummaryfinal.empty
			
			
			unique_day_preroll_summary_final = self.dayprerollsummaryfinal["Placement# Name"].nunique()
			data_common_columns = self.config.common_columns_summary()
			
			number_rows_preroll_summary = self.prerollsummaryfinal.shape[0]
			number_cols_preroll_summary = self.prerollsummaryfinal.shape[1]
			number_rows_video_preroll_summary = self.videoprerollsummarymvfinal.shape[0]
			number_cols_video_preroll_summary = self.videoprerollsummarymvfinal.shape[1]
			number_rows_interaction_final = self.intraction_final.shape[0]
			number_cols_interaction_final = self.intraction_final.shape[1]
			number_rows_day_preroll_summary = self.dayprerollsummaryfinal.shape[0]
			number_cols_day_preroll_summary = self.dayprerollsummaryfinal.shape[1]
			number_rows_video_player_summary_final = self.video_player_summary_final.shape[0]
			number_cols_video_player_summary_final = self.video_player_summary_final.shape[1]
			
			worksheet.hide_gridlines(2)
			worksheet.set_row(0, 6)
			worksheet.set_column("A:A", 2)
			
			alignment_center = workbook.add_format( {"align":"center"} )
			alignment_left = workbook.add_format({"align":"left"})
			alignment_right = workbook.add_format({"align":"right"})
			
			worksheet.insert_image("O7", "Exponential.png", {"url":"https://www.tribalfusion.com"})
			worksheet.insert_image("O2", "Client_Logo.png")
			
			#format_campaign_info = workbook.add_format( {"bg_color":'#F0F8FF', "align":"left"} )
			format_campaign_info = workbook.add_format ({"bold":True, "bg_color":'#00B0F0', "align":"left"})
			
			worksheet.conditional_format("A1:R5", {"type":"blanks", "format":format_campaign_info} )
			worksheet.conditional_format("A1:R5", {"type":"no_blanks", "format":format_campaign_info} )
			
			format_headers = workbook.add_format({"bold":True,"bg_color": '#E7E6E6'})
			
			money_fmt = workbook.add_format( {"num_format":"$#,###0.00","align":"center"})
			format_number = workbook.add_format({"num_format":"#,##0","align":"center"} )
			percent_fmt = workbook.add_format( {"num_format":"0.00%", "align":"center"} )
			
			worksheet.set_column (1, 1, 45, alignment_left)
			worksheet.set_column (2, 2, 15, alignment_center)
			worksheet.set_column (3, number_cols_interaction_final+18, 15, alignment_right)
			
		except AttributeError as e:
			pass
	def main(self):
		"""
main function
		"""
		self.config.common_columns_summary()
		self.connect_TFR_Intraction()
		self.read_query_preroll()
		if self.read_sql_preroll_summary.empty:
			self.logger.info ("No instream placements for IO - {}".format (self.config.ioid))
			pass
		else:
			self.logger.info ("Instream placements found for IO - {}".format (self.config.ioid))
			self.accessing_preroll_columns()
			self.renameIntraction()
			self.writePreroll()
			self.formatting()
			self.logger.info('Instream Sheet created for IO {}'.format(self.config.ioid))

if __name__=="__main__":
	#pass
	
	# enable it when running for individual file
	c = config.Config('Test', 598397,'2018-01-01','2018-02-01')
	o = Intraction( c )
	o.main()
	c.saveAndCloseWriter()
