# coding=utf-8
import pandas as pd
import numpy as np
from xlsxwriter.utility import xl_rowcol_to_cell
import xlsxwriter
import config
import pandas.io.formats.excel
pandas.io.formats.excel.header_style = None

class Intraction():
	def __init__(self, config):
		self.config = config
	
	def connect_TFR_Intraction(self):
		
		sql_preroll_summary = "select * from (select substr(PLACEMENT_DESC, 1, INSTR(PLACEMENT_DESC, '.', 1)-1) as Placement, SDATE as Start_Date, EDATE as End_Date, initcap(CREATIVE_DESC)  as Placement_Name, COST_TYPE_DESC as Cost_type, UNIT_COST as Unit_Cost, BUDGET as Planned_Cost, BOOKED_QTY as Booked_Imp_Booked_Eng from  TFR_REP.SUMMARY_MV where (IO_ID = {}) AND (DATA_SOURCE = 'KM') AND CREATIVE_DESC IN(SELECT DISTINCT CREATIVE_DESC FROM TFR_REP.SUMMARY_MV) ORDER BY PLACEMENT_ID) WHERE Placement_Name IN ('Pre-Roll - Desktop','Pre-Roll - Desktop + Mobile','Pre-Roll – Desktop + Mobile','Pre-Roll - In-Stream/Mobile Blend','Pre-Roll - Mobile','Pre-Roll -Desktop','Pre-Roll - In-Stream')".format(self.config.IO_ID)
		
		sql_preroll_mv = "select substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as Placement, sum(IMPRESSIONS) as Impression, sum(CPCV_COUNT) as Completions,sum(VWR_CLICK_THROUGHS) as Clickthroughs , sum(VWR_VIDEO_VIEW_100_PC_COUNT) as Video_Completions from TFR_REP.KEY_METRIC_MV WHERE IO_ID = {} GROUP BY PLACEMENT_ID, PLACEMENT_DESC ORDER BY PLACEMENT_ID".format(self.config.IO_ID)
		
		sql_preroll_video_views = "select substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as Placement,sum(IMPRESSIONS) as Impression, sum(VWR_VIDEO_VIEW_25_PC_COUNT) as Views25,sum(VWR_VIDEO_VIEW_50_PC_COUNT) as Views50, sum(VWR_VIDEO_VIEW_75_PC_COUNT) as Views75,sum(VWR_VIDEO_VIEW_100_PC_COUNT) as Video_Completions, sum(CPCV_COUNT) as Completions from TFR_REP.KEY_METRIC_MV WHERE IO_ID = {} GROUP BY PLACEMENT_ID, PLACEMENT_DESC ORDER BY PLACEMENT_ID".format(self.config.IO_ID)
		
		sql_preroll_day_mv = "select substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as Placement, TO_CHAR(TO_DATE(DAY_DESC, 'MM/DD/YYYY'),'YYYY-MM-DD') as Day, sum(IMPRESSIONS) as Impression, sum(CPCV_COUNT) as Completions, sum(VWR_CLICK_THROUGHS) as Clickthroughs , sum(VWR_VIDEO_VIEW_100_PC_COUNT) as Video_Completions from TFR_REP.KEY_METRIC_MV WHERE IO_ID = {} GROUP BY PLACEMENT_ID, PLACEMENT_DESC,DAY_DESC ORDER BY PLACEMENT_ID".format(self.config.IO_ID)
		
		sql_preroll_interaction = "select substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as Placement,BLAZE_TAG_NAME_DESC as Click_Tag, sum(VWR_INTERACTION) as VWR_Clickthrough from TFR_REP.INTERACTION_DETAIL_MV WHERE IO_ID = {} and BLAZE_ACTION_TYPE_DESC = 'Click-thru' GROUP BY PLACEMENT_ID, PLACEMENT_DESC, BLAZE_TAG_NAME_DESC ORDER BY PLACEMENT_ID".format(self.config.IO_ID)
		
		return sql_preroll_summary, sql_preroll_mv, sql_preroll_video_views ,sql_preroll_day_mv ,sql_preroll_interaction
	
	def read_query_preroll(self):
		
		sql_preroll_summary, sql_preroll_mv, sql_preroll_video_views, sql_preroll_day_mv, sql_preroll_interaction = self.connect_TFR_Intraction()
		
		read_sql_preroll_summary = pd.read_sql(sql_preroll_summary,self.config.conn)
		
		read_sql_preroll_mv = pd.read_sql(sql_preroll_mv,self.config.conn)
		
		read_sql_preroll_video = pd.read_sql(sql_preroll_video_views, self.config.conn)
		
		read_sql_preroll_day = pd.read_sql(sql_preroll_day_mv, self.config.conn)
		
		read_sql_preroll_interaction = pd.read_sql(sql_preroll_interaction, self.config.conn)
	
		return read_sql_preroll_summary, read_sql_preroll_mv, read_sql_preroll_video ,read_sql_preroll_day ,read_sql_preroll_interaction
	
	def accessing_preroll_columns(self):
		
		read_sql_preroll_summary, read_sql_preroll_mv, read_sql_preroll_video, read_sql_preroll_day, read_sql_preroll_interaction  = self.read_query_preroll()
		
		placementPrerollMV = read_sql_preroll_summary.merge(read_sql_preroll_mv, on = "PLACEMENT", how= "inner")
		
		prerollSummaryMv = placementPrerollMV.loc[:,["PLACEMENT","PLACEMENT_NAME","COST_TYPE","UNIT_COST","IMPRESSION",
		                                       "CLICKTHROUGHS","COMPLETIONS","VIDEO_COMPLETIONS"]]
		
		mask1 = prerollSummaryMv["COST_TYPE"].isin(['CPM'])
		choicePrerollSummaryMvCPM = prerollSummaryMv["VIDEO_COMPLETIONS"]
		mask2 = prerollSummaryMv["COST_TYPE"].isin(['CPCV'])
		choicePrerollSummaryMvCPCV = prerollSummaryMv["COMPLETIONS"]
		
		prerollSummaryMv["Video Completions"] = np.select([mask1,mask2],[choicePrerollSummaryMvCPM,
		                                                                 choicePrerollSummaryMvCPCV],default=0)
		
		prerollSummary = prerollSummaryMv.loc[:,["PLACEMENT","PLACEMENT_NAME", "COST_TYPE", "UNIT_COST"
		                                        ,"IMPRESSION","CLICKTHROUGHS", "Video Completions"]]
		
		prerollSummary["Placement# Name"] = prerollSummary[["PLACEMENT","PLACEMENT_NAME"]].apply(lambda x:".".join(x), axis=1)
		
		prerollSummary["CTR"] = prerollSummary["CLICKTHROUGHS"]/prerollSummary["IMPRESSION"]
		prerollSummary["Video Completion Rate"] = prerollSummary["Video Completions"]/prerollSummary["IMPRESSION"]
		
		prerollSummaryFinal = prerollSummary.loc[:,["Placement# Name","COST_TYPE","UNIT_COST","IMPRESSION",
		                                            "CLICKTHROUGHS","CTR","Video Completions","Video Completion Rate"]]
		
		#print prerollSummaryFinal
		videoPrerollMv = read_sql_preroll_summary.merge(read_sql_preroll_video, on="PLACEMENT", how="inner")
		
		videoPrerollSummaryMv = videoPrerollMv.loc[:,["PLACEMENT","PLACEMENT_NAME","IMPRESSION", "VIEWS25","VIEWS50",
		                                              "VIEWS75","VIDEO_COMPLETIONS","COMPLETIONS"]]
		
		mask3 = videoPrerollMv["COST_TYPE"].isin(['CPM'])
		choiceVideoPrerollCPM = videoPrerollMv["VIDEO_COMPLETIONS"]
		mask4 = videoPrerollMv["COST_TYPE"].isin(['CPCV'])
		choiceVideoPrerollCPCV = videoPrerollMv["COMPLETIONS"]
		
		videoPrerollSummaryMv["Video Completions"] = np.select([mask3,mask4],[choiceVideoPrerollCPM,
		                                                                      choiceVideoPrerollCPCV], default=0)
		
		videoPrerollSummaryMv["Video Completion Rate"] = videoPrerollSummaryMv["Video Completions"]/\
		                                                 videoPrerollSummaryMv["IMPRESSION"]
		
		videoPrerollSummaryMv["Placement# Name"] = videoPrerollSummaryMv[["PLACEMENT","PLACEMENT_NAME"]].apply(lambda x:".".join(x), axis=1)
		
		videoPrerollSummaryMvFinal = videoPrerollSummaryMv.loc[:,["Placement# Name","IMPRESSION","VIEWS25","VIEWS50"
		                                                          ,"VIEWS75","Video Completions","Video Completion Rate"]]
		

		dayPrerollMv = read_sql_preroll_summary.merge(read_sql_preroll_day, on="PLACEMENT", how="inner")
		
		dayPrerollSummaryMv = dayPrerollMv.loc[:,["PLACEMENT","PLACEMENT_NAME","DAY","IMPRESSION","CLICKTHROUGHS","VIDEO_COMPLETIONS",
		                                          "COMPLETIONS"]]
		
		mask5 = dayPrerollMv["COST_TYPE"].isin(["CPM"])
		choiceDayPrerollCPM = dayPrerollMv["VIDEO_COMPLETIONS"]
		mask6 = dayPrerollMv["COST_TYPE"].isin(["CPCV"])
		choiceDayPrerollCPCV = dayPrerollMv["COMPLETIONS"]
		
		dayPrerollSummaryMv["Video Completions"] = np.select([mask5,mask6],[choiceDayPrerollCPM,choiceDayPrerollCPCV],default=0)
		
		dayPrerollSummaryMv["Video Completion Rate"] = dayPrerollSummaryMv["Video Completions"]/dayPrerollSummaryMv["IMPRESSION"]
		dayPrerollSummaryMv["CTR"] = dayPrerollSummaryMv["CLICKTHROUGHS"]/dayPrerollSummaryMv["IMPRESSION"]
		
		dayPrerollSummaryMv["Placement# Name"] = dayPrerollSummaryMv[["PLACEMENT","PLACEMENT_NAME"]].apply(lambda x:".".join(x), axis=1)
		
		dayPrerollSummaryFinal = dayPrerollSummaryMv.loc[:,["Placement# Name","DAY","IMPRESSION","CLICKTHROUGHS","CTR",
		                                                    "Video Completions","Video Completion Rate"]]
		
		
				
		intractionSummaryMv = read_sql_preroll_summary.merge(read_sql_preroll_interaction,on="PLACEMENT", how="inner")
		#print intractionSummaryMv
		#print ("yy", read_sql_preroll_summary)
		#print ("dh", read_sql_preroll_interaction)
		
		intractionClick = intractionSummaryMv.loc[:,["PLACEMENT","PLACEMENT_NAME","CLICK_TAG","VWR_CLICKTHROUGH"]]
		intractionClick["Placement# Name"] = intractionClick[["PLACEMENT","PLACEMENT_NAME"]].apply(lambda x:".".join(x), axis=1)
		#print read_sql_preroll_interaction
		
		#print intractionClick
		
		#print intractionSummaryMv

		intractionTableClicks = pd.pivot_table(intractionClick,index = 'Placement# Name',values='VWR_CLICKTHROUGH',
		                                       columns='CLICK_TAG',aggfunc=np.sum,fill_value=0)
		
		intractionTableClicksNew = intractionTableClicks.reset_index()
		intractionTableClicksR = intractionTableClicksNew.loc[:, :]
		#intractionTableClicksR["PLACEMENT"] = intractionTableClicksR["PLACEMENT"].astype(int)
		intractionClickTableNew = intractionTableClicksR.merge(prerollSummaryFinal, on="Placement# Name", how="inner")
		intractionClickTableNew["CTR"] = intractionClickTableNew["CLICKTHROUGHS"]/intractionClickTableNew["IMPRESSION"]
		#print intractionClickTableNew
		colsDrop = ["COST_TYPE","UNIT_COST","IMPRESSION","CLICKTHROUGHS","Video Completions","Video Completion Rate"]
		intractionNewCols = intractionClickTableNew.drop(colsDrop,axis=1)
		intractionFinal = intractionNewCols.loc[:, :]
		
		return prerollSummaryFinal, videoPrerollSummaryMvFinal, intractionFinal, dayPrerollSummaryFinal
	
	def renameIntraction(self):
		prerollSummaryFinal, videoPrerollSummaryMvFinal, intractionFinal, dayPrerollSummaryFinal = self.accessing_preroll_columns()
		
		renamePrerollSummaryFinal = prerollSummaryFinal.rename(columns={"COST_TYPE":"Cost Type","UNIT_COST":"Cost",
		                                                                "IMPRESSION":"Impressions",
		                                                                "CLICKTHROUGHS":"Clickthroughs"},inplace=True)
		
		renameVideoPrerollSummaryMvFinal = videoPrerollSummaryMvFinal.rename(columns={"IMPRESSION":"Impressions",
		                                                                              "VIEWS25":"25% View",
		                                                                              "VIEWS50":"50% View",
		                                                                              "VIEWS75":"75% View"},inplace=True)
		
		renamedayPrerollSummaryFinal = dayPrerollSummaryFinal.rename(columns={"DAY":"Date","IMPRESSION":"Impressions",
		                                                                      "CLICKTHROUGHS":"Clickthroughs"},inplace=True)
		
		return prerollSummaryFinal, videoPrerollSummaryMvFinal, intractionFinal, dayPrerollSummaryFinal
	
	def writePreroll(self):
		data_common_columns = self.config.common_columns_summary()
		
		prerollSummaryFinal, videoPrerollSummaryMvFinal, intractionFinal, dayPrerollSummaryFinal = self.renameIntraction()
		
		checkPrerollSummaryFinal = prerollSummaryFinal.empty
		checkVideoPrerollSummaryMvFinal = videoPrerollSummaryMvFinal.empty
		checkIntractionFinal = intractionFinal.empty
		checkDayPrerollSummaryFinal = dayPrerollSummaryFinal.empty
		
		writingDataCommonColumns = data_common_columns[1].to_excel(self.config.writer,sheet_name="Standard Preroll({})"
		                                                           .format(self.config.IO_ID),startcol=1,startrow=1,
		                                                                   index=False,header=False)
		
		if checkDayPrerollSummaryFinal == True:
			pass
		else:
			writingPrerollSummaryFinal = prerollSummaryFinal.to_excel(self.config.writer,
			                                                          sheet_name="Standard Preroll({})".format(self.config.IO_ID),
			                                                          startcol=1,startrow=8,index=False,header=True)
		if checkVideoPrerollSummaryMvFinal == True:
			pass
		else:
			writingVideoPrerollSummaryMvFinal = videoPrerollSummaryMvFinal.to_excel(self.config.writer,
			                                                                        sheet_name="Standard Preroll({})".format(self.config.IO_ID),
			                                                                        startcol=1,
			                                                                        startrow=len(prerollSummaryFinal)+13,
			                                                                        index=False,header=True)
		if checkIntractionFinal == True:
			pass
		else:
			writingIntractionFinal = intractionFinal.to_excel(self.config.writer,
			                                                  sheet_name="Standard Preroll({})".format(self.config.IO_ID),
			                                                  startcol=1,startrow=len(prerollSummaryFinal)+len(videoPrerollSummaryMvFinal)+18
			                                                  ,index=False,header=True)
			
		
		
		
		
		
		if checkDayPrerollSummaryFinal == True:
			pass
		else:
			startline = len( prerollSummaryFinal )+len( videoPrerollSummaryMvFinal )+len( intractionFinal )+23
			startRow = startline
			endrow = 0
			for placement,placement_df in dayPrerollSummaryFinal.groupby('Placement# Name'):
				writingDayPrerollSummaryFinal = placement_df.to_excel(self.config.writer,sheet_name="Standard Preroll({})".format(self.config.IO_ID),
				                                                      startcol=1,startrow=startline, columns=["Placement# Name"],
				                                                      index=False, header=False,merge_cells=False)
				
				writingDayPrerollSummaryFinalNew = placement_df.to_excel(self.config.writer,
				                                                         sheet_name="Standard Preroll({})".format(self.config.IO_ID),
				                                                         startcol=1,startrow=startline+1,columns=["Date",
				                                                                                                  "Impressions",
				                                                                                                  "Clickthroughs",
				                                                                                                  "CTR",
				                                                                                                  "Video Completions",
				                                                                                                  "Video Completion Rate"],
				                                                         index=False,header=True,merge_cells=False)
				startline += len(placement_df)+2
				workbook = self.config.writer.book
				worksheet = self.config.writer.sheets["Standard Preroll({})".format(self.config.IO_ID)]
				worksheet.write_string(startline,1,'Subtotal')
				startline +=3
	def main(self):
		self.config.common_columns_summary()
		self.connect_TFR_Intraction()
		self.read_query_preroll()
		self.accessing_preroll_columns()
		self.renameIntraction()
		self.writePreroll()

if __name__=="__main__":
	# pass
	
	# enable it when running for individual file
	c = config.Config('Test', 601397)
	o = Intraction( c )
	o.main()
	c.saveAndCloseWriter()
