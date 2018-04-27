#coding=utf-8
"""
Created by:Dharmendra
Date:2018-03-23
"""
import pandas as pd
import numpy as np
import config
import logging
from xlsxwriter.utility import xl_rowcol_to_cell
import logging
from functools import reduce

class Video(object):
	"""
Class for VDX Placements
	"""
	
	def __init__(self, config):
		self.config = config
		self.logger = self.config.logger

	def connect_tfr_video(self):
		"""
		TFR Queries for vdx placements
		:return:
		"""
		self.logger.info('Starting to build vdx placements for IO - {}'.format(self.config.ioid))
		
		sql_vdx_summary = "select * from (select substr(PLACEMENT_DESC, 1, INSTR(PLACEMENT_DESC, '.', 1)-1) as Placement, " \
		                  "SDATE as Start_Date, EDATE as End_Date, initcap(CREATIVE_DESC)  as Placement_Name, " \
		                  "COST_TYPE_DESC as Cost_type, UNIT_COST as Unit_Cost, BUDGET as Planned_Cost, " \
		                  "BOOKED_QTY as Booked_Imp_Booked_Eng from  TFR_REP.SUMMARY_MV where (IO_ID = {}) " \
		                  "AND (DATA_SOURCE = 'KM') AND CREATIVE_DESC " \
		                  "IN(SELECT DISTINCT CREATIVE_DESC FROM TFR_REP.SUMMARY_MV) ORDER BY PLACEMENT_ID) " \
		                  "WHERE Placement_Name Not IN ('Pre-Roll - Desktop','Pre-Roll - Desktop + Mobile'," \
		                  "'Pre-Roll – Desktop + Mobile','Pre-Roll - In-Stream/Mobile Blend','Pre-Roll - Mobile','Pre-Roll -Desktop'," \
		                  "'Pre-Roll - In-Stream')".format(self.config.ioid)
		
		sql_vdx_km = "select substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as Placement, PRODUCT ," \
		             "sum(IMPRESSIONS) as Impressions,sum(ENGAGEMENTS) as Engagements, " \
		             "sum(DPE_ENGAGEMENTS) as DpeEngaments,sum(ENG_CLICK_THROUGHS) as EngClickthrough," \
		             "sum(DPE_CLICK_THROUGHS) as DpeClickthrough,sum(VWR_CLICK_THROUGHS) as VwrClickthrough, " \
		             "sum(ENG_TOTAL_TIME_SPENT) as Engtotaltimespent,sum(DPE_TOTAL_TIME_SPENT) as Dpetotaltimespent, " \
		             "sum(VWR_VIDEO_VIEW_0_PC_COUNT) as View0,sum(VWR_VIDEO_VIEW_25_PC_COUNT) as View25, " \
		             "sum(VWR_VIDEO_VIEW_50_PC_COUNT) as View50,sum(VWR_VIDEO_VIEW_75_PC_COUNT) as View75, " \
		             "sum(VWR_VIDEO_VIEW_100_PC_COUNT) as View100,sum(ENG_VIDEO_VIEW_0_PC_COUNT) as Eng0," \
		             "sum(ENG_VIDEO_VIEW_25_PC_COUNT) as Eng25,sum(ENG_VIDEO_VIEW_50_PC_COUNT) as Eng50," \
		             "sum(ENG_VIDEO_VIEW_75_PC_COUNT) as Eng75,sum(ENG_VIDEO_VIEW_100_PC_COUNT) as Eng100," \
		             "sum(DPE_VIDEO_VIEW_0_PC_COUNT) as Dpe0,sum(DPE_VIDEO_VIEW_25_PC_COUNT) as Dpe25, " \
		             "sum(DPE_VIDEO_VIEW_50_PC_COUNT) as Dpe50,sum(DPE_VIDEO_VIEW_75_PC_COUNT) as Dpe75, " \
		             "sum(DPE_VIDEO_VIEW_100_PC_COUNT) as Dpe100,sum(ENG_INTERACTIVE_ENGAGEMENTS) " \
		             "as EngIntractiveEngagements,sum(CPCV_COUNT) as completions from TFR_REP.KEY_METRIC_MV WHERE IO_ID = {} GROUP BY PLACEMENT_ID, " \
		             "PLACEMENT_DESC,PRODUCT ORDER BY PLACEMENT_ID".format(self.config.ioid)
		
		sql_adsize_km = "SELECT substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as Placement,PRODUCT, " \
		                "MEDIA_SIZE_DESC as AdSize,sum(IMPRESSIONS) as Impressions, " \
		                "sum(ENGAGEMENTS) as Engagements, sum(DPE_ENGAGEMENTS) as DpeEngagements," \
		                "sum(ENG_CLICK_THROUGHS) as EngClickthroughs, sum(DPE_CLICK_THROUGHS) as DpeClickthroughs," \
		                "sum(VWR_CLICK_THROUGHS) as VwrClickthroughs,sum(VWR_VIDEO_VIEW_100_PC_COUNT) as View100," \
		                "sum(ENG_VIDEO_VIEW_100_PC_COUNT) as Eng100,sum(DPE_VIDEO_VIEW_100_PC_COUNT) as Dpe100,sum(CPCV_COUNT) as completions " \
		                "FROM TFR_REP.ADSIZE_KM_MV WHERE IO_ID = {} GROUP BY PLACEMENT_ID, PLACEMENT_DESC, " \
		                "MEDIA_SIZE_DESC,PRODUCT ORDER BY PLACEMENT_ID".format(self.config.ioid)
		
		sql_video_km = "SELECT substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as Placement, PRODUCT, " \
		               "sum(VWR_VIDEO_VIEW_0_PC_COUNT) as View0,sum(VWR_VIDEO_VIEW_25_PC_COUNT) as View25," \
		               "sum(VWR_VIDEO_VIEW_50_PC_COUNT) as View50,sum(VWR_VIDEO_VIEW_75_PC_COUNT) as View75," \
		               "sum(VWR_VIDEO_VIEW_100_PC_COUNT) as View100,sum(ENG_VIDEO_VIEW_0_PC_COUNT) as Eng0," \
		               "sum(ENG_VIDEO_VIEW_25_PC_COUNT) as Eng25,sum(ENG_VIDEO_VIEW_50_PC_COUNT) as Eng50," \
		               "sum(ENG_VIDEO_VIEW_75_PC_COUNT) as Eng75,sum(ENG_VIDEO_VIEW_100_PC_COUNT) as Eng100," \
		               "sum(DPE_VIDEO_VIEW_0_PC_COUNT) as Dpe0,sum(DPE_VIDEO_VIEW_25_PC_COUNT) as Dpe25," \
		               "sum(DPE_VIDEO_VIEW_50_PC_COUNT) as Dpe50,sum(DPE_VIDEO_VIEW_100_PC_COUNT) as Dpe100 " \
		               "FROM  TFR_REP.VIDEO_DETAIL_MV WHERE IO_ID = {} GROUP BY PLACEMENT_ID, PLACEMENT_DESC,PRODUCT ORDER BY " \
		               "PLACEMENT_ID".format(self.config.ioid)
		
		sql_video_player_intraction = "SELECT substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as Placement, PRODUCT, " \
		                              "sum(VWR_MUTE) as Vwrmute,sum(VWR_UNMUTE) as Vwrunmute, " \
		                              "sum(VWR_PAUSE) as Vwrpause, sum(VWR_REWIND) as Vwrrewind," \
		                              "sum(VWR_RESUME) as Vwrresume,sum(VWR_REPLAY) as Vwrreplay, " \
		                              "sum(VWR_FULL_SCREEN) as Vwrfullscreen,sum(ENG_MUTE) as Engmute," \
		                              "sum(ENG_UNMUTE) as Engunmute, sum(ENG_PAUSE) as Engpause," \
		                              "sum(ENG_REWIND) as Engrewind,sum(ENG_RESUME) as Engresume, " \
		                              "sum(ENG_REPLAY) as Engreplay,sum(ENG_FULL_SCREEN) as Engfullscreen," \
		                              "sum(DPE_MUTE) as Dpemute,sum(DPE_UNMUTE) as Dpeunmute," \
		                              "sum(DPE_PAUSE) as Dpepause, sum(DPE_REWIND) as Dperewind, " \
		                              "sum(DPE_RESUME) as Dperesume,sum(DPE_REPLAY) as Dpereplay, " \
		                              "sum(DPE_FULL_SCREEN) as Dpefullscreen,FROM TFR_REP.VIDEO_DETAIL_MV " \
		                              "WHERE IO_ID = {} GROUP BY PLACEMENT_ID, PLACEMENT_DESC,PRODUCT " \
		                              "ORDER BY PLACEMENT_ID".format(self.config.ioid)
		
		sql_ad_intraction = "SELECT substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as Placement,PRODUCT, " \
		                    "sum(VWR_INTERACTION) as Vwradintraction,sum(ENG_INTERACTION) as Engadintraction," \
		                    "sum(DPE_INTERACTION) as Dpeadintraction " \
		                    "FROM TFR_REP.INTERACTION_DETAIL_MV WHERE IO_ID = {} and BLAZE_ACTION_TYPE_DESC = 'Interaction' " \
		                    "GROUP BY PLACEMENT_ID, PLACEMENT_DESC,PRODUCT ORDER BY PLACEMENT_ID".format(self.config.ioid)
		
		sql_click_throughs = "SELECT substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as Placement, PRODUCT, " \
		                     "sum(VWR_INTERACTION) as Vwrclickintraction," \
		                     "sum(ENG_INTERACTION) as Engclickintraction," \
		                     "sum(DPE_INTERACTION) as Dpeclickintraction FROM TFR_REP.INTERACTION_DETAIL_MV " \
		                     "WHERE IO_ID = {} and BLAZE_ACTION_TYPE_DESC = 'Click-thru' " \
		                     "GROUP BY PLACEMENT_ID, PLACEMENT_DESC,PRODUCT ORDER BY PLACEMENT_ID".format(self.config.ioid)
		
		sql_vdx_day_km = "SELECT substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as Placement, PRODUCT, " \
		                 "TO_CHAR(TO_DATE(DAY_DESC, 'MM/DD/YYYY'),'YYYY-MM-DD') as Day,sum(IMPRESSIONS) as Impressions, " \
		                 "sum(ENGAGEMENTS) as Engagements, sum(DPE_ENGAGEMENTS) as Dpeengagements," \
		                 "sum(VWR_VIDEO_VIEW_100_PC_COUNT) as View100, sum(CPCV_COUNT) as completions," \
		                 "sum(ENG_VIDEO_VIEW_100_PC_COUNT) as Eng100," \
		                 "sum(DPE_VIDEO_VIEW_100_PC_COUNT) as Dpe100 FROM TFR_REP.KEY_METRIC_MV WHERE IO_ID = {} " \
		                 "GROUP BY PLACEMENT_ID, PLACEMENT_DESC, DAY_DESC,PRODUCT ORDER BY PLACEMENT_ID".format(self.config.ioid)
		
		self.sql_vdx_summary = sql_vdx_summary
		self.sql_vdx_km = sql_vdx_km
		self.sql_adsize_km = sql_adsize_km
		self.sql_video_km = sql_video_km
		self.sql_video_player_intraction = sql_video_player_intraction
		self.sql_ad_intraction = sql_ad_intraction
		self.sql_click_throughs = sql_click_throughs
		self.sql_vdx_day_km = sql_vdx_day_km
	
	def read_query_video(self):
		"""
		Reading Query from TFR
		:return:
		"""
		self.logger.info ('Running Query for VDX placements for IO {}'.format (self.config.ioid))
		
		read_sql_vdx_summary = pd.read_sql(self.sql_vdx_summary, self.config.conn)
		read_sql_vdx_km = pd.read_sql(self.sql_vdx_km, self.config.conn)
		read_sql_adsize_km = pd.read_sql(self.sql_adsize_km, self.config.conn)
		read_sql_video_km = pd.read_sql(self.sql_video_km, self.config.conn)
		read_sql_video_player_interaction = pd.read_sql(self.sql_video_km, self.config.conn)
		read_sql_ad_intraction = pd.read_sql(self.sql_ad_intraction, self.config.conn)
		read_sql_click_throughs = pd.read_sql(self.sql_click_throughs, self.config.conn)
		read_sql_vdx_day_km = pd.read_sql(self.sql_vdx_day_km, self.config.conn)
	
		self.read_sql_vdx_summary = read_sql_vdx_summary
		self.read_sql_vdx_km = read_sql_vdx_km
		self.read_sql_adsize_km = read_sql_adsize_km
		self.read_sql_video_km = read_sql_video_km
		self.read_sql_video_player_interaction = read_sql_video_player_interaction
		self.read_sql_ad_intraction = read_sql_ad_intraction
		self.read_sql_click_throughs = read_sql_click_throughs
		self.read_sql_vdx_day_km = read_sql_vdx_day_km
	
	def access_vdx_columns(self):
		"""
		Accessing VDX Columns
		:return:
		"""
		self.logger.info ('Query Stored for further processing of IO - {}'.format (self.config.ioid))
		self.logger.info ('Creating placement wise table of IO - {}'.format (self.config.ioid))
		#print ("Dharm",self.read_sql_vdx_summary)
		#print ("harsh", self.read_sql_vdx_km)
		
		#placementvdx = None
		placementvdxs = [self.read_sql_vdx_summary,self.read_sql_vdx_km]
		placementvdxsummary = reduce(lambda left,right: pd.merge(left,right, on='PLACEMENT'),placementvdxs)
		
		
		
		placementvdxsummaryfirst = placementvdxsummary.loc[:,["PLACEMENT","PLACEMENT_NAME","COST_TYPE","PRODUCT",
		                                                      "UNIT_COST","IMPRESSIONS","ENGAGEMENTS","DPEENGAMENTS",
		                                                      "ENGCLICKTHROUGH","DPECLICKTHROUGH","VWRCLICKTHROUGH",
		                                                      "ENGTOTALTIMESPENT","DPETOTALTIMESPENT","COMPLETIONS",
		                                                      "ENGINTRACTIVEENGAGEMENTS","VIEW100","ENG100","DPE100"]]
		
		
		placementvdxsummaryfirst["Placement# Name"] = placementvdxsummaryfirst[["PLACEMENT",
		                                                                        "PLACEMENT_NAME"]].apply(lambda x:".".join(x),
		                                                                                                 axis=1)
		
		
		
		mask1 = placementvdxsummaryfirst["COST_TYPE"].isin(['CPE+'])
		choicedeepengagement = placementvdxsummaryfirst['DPEENGAMENTS']/placementvdxsummaryfirst['IMPRESSIONS']
		mask2 = placementvdxsummaryfirst["COST_TYPE"].isin(['CPE','CPM','CPCV'])
		choiceengagements = placementvdxsummaryfirst["ENGAGEMENTS"]/placementvdxsummaryfirst['IMPRESSIONS']
		
		placementvdxsummaryfirst["Engagements Rate"] = np.select([mask1,mask2],
		                                                              [choicedeepengagement,choiceengagements],default=0.00)
		
		
		mask3 = placementvdxsummaryfirst["COST_TYPE"].isin(['CPE+','CPE','CPM','CPCV'])
		choicevwrctr = placementvdxsummaryfirst['VWRCLICKTHROUGH']/placementvdxsummaryfirst['IMPRESSIONS']
		
		placementvdxsummaryfirst["Viewer CTR"] = np.select([mask3],[choicevwrctr],default=0.00)
		
		
		choiceengctr = placementvdxsummaryfirst["ENGCLICKTHROUGH"]/placementvdxsummaryfirst["ENGAGEMENTS"]
		choicedeepctr = placementvdxsummaryfirst["DPECLICKTHROUGH"]/placementvdxsummaryfirst["DPEENGAMENTS"]
		placementvdxsummaryfirst["Engager CTR"] = np.select([mask1,mask2],[choicedeepctr,choiceengctr],default=0.00)
		
		mask4 = placementvdxsummaryfirst["PRODUCT"].isin(['InStream'])
		choicevwrvcr = placementvdxsummaryfirst['VIEW100']/placementvdxsummaryfirst['IMPRESSIONS']
		placementvdxsummaryfirst['Viewer VCR'] = np.select([mask4 & mask3],[choicevwrvcr],default='N/A')
		
		
		mask5 = placementvdxsummaryfirst["PRODUCT"].isin(['Display','Mobile'])
		mask6 = placementvdxsummaryfirst['COST_TYPE'].isin(['CPE','CPM'])
		choiceengvcrcpecpm = placementvdxsummaryfirst['ENG100']/placementvdxsummaryfirst['ENGAGEMENTS']
		mask7 = placementvdxsummaryfirst["COST_TYPE"].isin(['CPE+'])
		mask8 = placementvdxsummaryfirst["COST_TYPE"].isin(['CPCV'])
		choiceengvcrcpe_plus = placementvdxsummaryfirst['DPE100']/placementvdxsummaryfirst['ENGAGEMENTS']
		choiceengvcrcpcv = placementvdxsummaryfirst['COMPLETIONS']/placementvdxsummaryfirst['ENGAGEMENTS']
		
		placementvdxsummaryfirst['Engager VCR'] = np.select([mask5 & mask6,mask7,mask8],[choiceengvcrcpecpm,
		                                                                                 choiceengvcrcpe_plus,
		                                                                                 choiceengvcrcpcv],default='N/A')
		
		choiceintratecpe_plus = placementvdxsummaryfirst['']/placementvdxsummaryfirst['']
		print (placementvdxsummaryfirst)
		
		
	def main(self):
		"""
Main Function
		"""
		self.config.common_columns_summary()
		self.connect_tfr_video()
		self.read_query_video()
		if self.read_sql_vdx_day_km.empty:
			pass
		else:
			self.access_vdx_columns()
			#self.access_columns_KM_Video()
			#self.rename_KM_Data_Video()
			#self.write_video_data()
			#self.formatting_Video()


if __name__=="__main__":
	#pass
	#enable it when running for individual file
	c = config.Config ('Origin', 603857)
	o = Video (c)
	o.main ()
	c.saveAndCloseWriter ()




















