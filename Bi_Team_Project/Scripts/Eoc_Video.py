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
import pandas.io.formats.excel
pandas.io.formats.excel.header_style = None

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
		             "sum(DPE_VIDEO_VIEW_100_PC_COUNT) as Dpe100,sum(DPE_INTERACTIVE_ENGAGEMENTS) as DpeIntractiveEngagements," \
		             "sum(ENG_INTERACTIVE_ENGAGEMENTS) " \
		             "as EngIntractiveEngagements,sum(CPCV_COUNT) as completions from TFR_REP.KEY_METRIC_MV WHERE IO_ID = {} GROUP BY PLACEMENT_ID, " \
		             "PLACEMENT_DESC,PRODUCT ORDER BY PLACEMENT_ID".format(self.config.ioid)
		
		sql_adsize_km = "SELECT substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as Placement,PRODUCT, " \
		                "MEDIA_SIZE_DESC as AdSize,sum(IMPRESSIONS) as Impressions, " \
		                "sum(ENGAGEMENTS) as Engagements, sum(DPE_ENGAGEMENTS) as DpeEngagements," \
		                "sum(ENG_CLICK_THROUGHS) as EngClickthroughs, sum(DPE_CLICK_THROUGHS) as DpeClickthroughs," \
		                "sum(VWR_CLICK_THROUGHS) as VwrClickthroughs,sum(VWR_VIDEO_VIEW_100_PC_COUNT) as View100," \
		                "sum(ENG_VIDEO_VIEW_100_PC_COUNT) as Eng100,sum(DPE_VIDEO_VIEW_100_PC_COUNT) as Dpe100," \
		                "sum(ENG_TOTAL_TIME_SPENT) as Engtotaltimespent,sum(DPE_TOTAL_TIME_SPENT) as Dpetotaltimespent," \
		                "sum(ENG_INTERACTIVE_ENGAGEMENTS) as EngIntractiveEngagements," \
		                "sum(DPE_INTERACTIVE_ENGAGEMENTS) as DpeIntractiveEngagements,sum(CPCV_COUNT) as completions " \
		                "FROM TFR_REP.ADSIZE_KM_MV WHERE IO_ID = {} GROUP BY PLACEMENT_ID, PLACEMENT_DESC, " \
		                "MEDIA_SIZE_DESC,PRODUCT ORDER BY PLACEMENT_ID".format(self.config.ioid)
		
		sql_video_km = "SELECT substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as Placement," \
		               "PRODUCT,FEV_INT_VIDEO_DESC as Videoname," \
		               "sum(VWR_VIDEO_VIEW_0_PC_COUNT) as View0," \
		               "sum(VWR_VIDEO_VIEW_25_PC_COUNT) as View25," \
		               "sum(VWR_VIDEO_VIEW_50_PC_COUNT) as View50," \
		               "sum(VWR_VIDEO_VIEW_75_PC_COUNT) as View75," \
		               "sum(VWR_VIDEO_VIEW_100_PC_COUNT) as View100," \
		               "sum(ENG_VIDEO_VIEW_0_PC_COUNT) as Eng0," \
		               "sum(ENG_VIDEO_VIEW_25_PC_COUNT) as Eng25," \
		               "sum(ENG_VIDEO_VIEW_50_PC_COUNT) as Eng50," \
		               "sum(ENG_VIDEO_VIEW_75_PC_COUNT) as Eng75," \
		               "sum(ENG_VIDEO_VIEW_100_PC_COUNT) as Eng100," \
		               "sum(DPE_VIDEO_VIEW_0_PC_COUNT) as Dpe0," \
		               "sum(DPE_VIDEO_VIEW_25_PC_COUNT) as Dpe25," \
		               "sum(DPE_VIDEO_VIEW_50_PC_COUNT) as Dpe50, " \
		               "sum(DPE_VIDEO_VIEW_75_PC_COUNT) as Dpe75," \
		               "sum(DPE_VIDEO_VIEW_100_PC_COUNT) as Dpe100 " \
		               "FROM  TFR_REP.VIDEO_DETAIL_MV WHERE IO_ID = {} GROUP BY PLACEMENT_ID,PLACEMENT_DESC,PRODUCT,FEV_INT_VIDEO_DESC ORDER BY PLACEMENT_ID".format(self.config.ioid)
		
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
		                 "sum(ENG_VIDEO_VIEW_100_PC_COUNT) as Eng100,sum(DPE_INTERACTIVE_ENGAGEMENTS) as DpeIntractiveEngagements, " \
		                 "sum(DPE_VIDEO_VIEW_100_PC_COUNT) as Dpe100 FROM TFR_REP.KEY_METRIC_MV WHERE IO_ID = {} " \
		                 "GROUP BY PLACEMENT_ID, PLACEMENT_DESC, DAY_DESC,PRODUCT ORDER BY PLACEMENT_ID".format(self.config.ioid)
		
		sql_adsize_km_rate = "SELECT substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as Placement," \
		                     "sum(DPE_INTERACTIVE_ENGAGEMENTS) as DpeIntractiveEngagements," \
		                     "sum(ENG_INTERACTIVE_ENGAGEMENTS) as EngIntractiveEngagements," \
		                     "sum(ENG_TOTAL_TIME_SPENT) as Engtotaltimespent,sum(DPE_TOTAL_TIME_SPENT) as Dpetotaltimespent " \
		                     "from TFR_REP.KEY_METRIC_MV WHERE IO_ID = {} GROUP BY PLACEMENT_ID, " \
		                     "PLACEMENT_DESC ORDER BY PLACEMENT_ID".format(self.config.ioid)
		
		sql_km_for_video = "select substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as Placement," \
		                   "sum(IMPRESSIONS) as Impressions,sum(ENGAGEMENTS) as Engagements, sum(CPCV_COUNT) as completions," \
		                   "sum(DPE_ENGAGEMENTS) as Dpeengaments From TFR_REP.KEY_METRIC_MV WHERE IO_ID = {} GROUP BY PLACEMENT_ID, PLACEMENT_DESC ORDER BY  PLACEMENT_ID".format(self.config.ioid)
		
		
		self.sql_vdx_summary = sql_vdx_summary
		self.sql_vdx_km = sql_vdx_km
		self.sql_adsize_km = sql_adsize_km
		self.sql_video_km = sql_video_km
		self.sql_video_player_intraction = sql_video_player_intraction
		self.sql_ad_intraction = sql_ad_intraction
		self.sql_click_throughs = sql_click_throughs
		self.sql_vdx_day_km = sql_vdx_day_km
		self.sql_adsize_km_rate = sql_adsize_km_rate
		self.sql_km_for_video = sql_km_for_video
	
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
		read_sql_km_for_video = pd.read_sql(self.sql_km_for_video, self.config.conn)
		read_sql_adsize_km_rate = pd.read_sql(self.sql_adsize_km_rate, self.config.conn)
		
		#print (list(read_sql_video_km))

		self.read_sql_vdx_summary = read_sql_vdx_summary
		self.read_sql_vdx_km = read_sql_vdx_km
		self.read_sql_adsize_km = read_sql_adsize_km
		self.read_sql_video_km = read_sql_video_km
		self.read_sql_video_player_interaction = read_sql_video_player_interaction
		self.read_sql_ad_intraction = read_sql_ad_intraction
		self.read_sql_click_throughs = read_sql_click_throughs
		self.read_sql_vdx_day_km = read_sql_vdx_day_km
		self.read_sql_km_for_video = read_sql_km_for_video
		self.read_sql_adsize_km_rate = read_sql_adsize_km_rate
	
	def access_vdx_columns(self):
		"""
		Accessing VDX Columns
		:return:
		"""
		self.logger.info ('Query Stored for further processing of IO - {}'.format (self.config.ioid))
		self.logger.info ('Creating placement wise table of IO - {}'.format (self.config.ioid))
		
		placementvdxs = [self.read_sql_vdx_summary,self.read_sql_vdx_km]
		
		placementvdxsummary = reduce(lambda left,right: pd.merge(left,right, on='PLACEMENT'),placementvdxs)
		
		
		
		placementvdxsummarynew = placementvdxsummary.loc[:,["PLACEMENT","PLACEMENT_NAME","COST_TYPE","PRODUCT",
		                                                      "IMPRESSIONS","ENGAGEMENTS","DPEENGAMENTS",
		                                                      "ENGCLICKTHROUGH","DPECLICKTHROUGH","VWRCLICKTHROUGH",
		                                                      "ENGTOTALTIMESPENT","DPETOTALTIMESPENT","COMPLETIONS",
		                                                      "ENGINTRACTIVEENGAGEMENTS","DPEINTRACTIVEENGAGEMENTS",
		                                                      "VIEW100","ENG100","DPE100"]]
		
		
		placementvdxsummarynew["Placement# Name"] = placementvdxsummarynew[["PLACEMENT",
		                                                                        "PLACEMENT_NAME"]].apply(lambda x:".".join(x),
		                                                                                                 axis=1)
		
		placementvdxsummary = placementvdxsummarynew.loc[:,["Placement# Name","COST_TYPE","PRODUCT",
		                                                         "IMPRESSIONS", "ENGAGEMENTS", "DPEENGAMENTS",
		                                                         "ENGCLICKTHROUGH", "DPECLICKTHROUGH", "VWRCLICKTHROUGH",
		                                                         "ENGTOTALTIMESPENT", "DPETOTALTIMESPENT", "COMPLETIONS",
		                                                         "ENGINTRACTIVEENGAGEMENTS", "DPEINTRACTIVEENGAGEMENTS",
		                                                         "VIEW100", "ENG100", "DPE100"]]
		
		placementvdxsummaryfirst = placementvdxsummary.append(placementvdxsummary.sum(numeric_only=True),ignore_index=True)
		
		placementvdxsummaryfirst["COST_TYPE"] = placementvdxsummaryfirst["COST_TYPE"].fillna('CPE')
		
		placementvdxsummaryfirst["PRODUCT"] = placementvdxsummaryfirst["PRODUCT"].fillna ('Grand Total')
		
		placementvdxsummaryfirst["Placement# Name"] = placementvdxsummaryfirst["Placement# Name"].fillna ('Grand Total')
		
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
		placementvdxsummaryfirst['Viewer VCR'] = pd.to_numeric(placementvdxsummaryfirst['Viewer VCR'],errors='coerce')
		
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
		
		#placementvdxsummaryfirst["Engager VCR"] = placementvdxsummaryfirst["Viewer VCR"].astype (object)
		placementvdxsummaryfirst['Engager VCR'] = pd.to_numeric (placementvdxsummaryfirst['Engager VCR'], errors='coerce')
		
		choiceintratecpe_plus = placementvdxsummaryfirst['DPEINTRACTIVEENGAGEMENTS']/placementvdxsummaryfirst['DPEENGAMENTS']
		choiceintrateotherthancpe_plus = placementvdxsummaryfirst['ENGINTRACTIVEENGAGEMENTS']/placementvdxsummaryfirst['ENGAGEMENTS']
		
		
		placementvdxsummaryfirst['Interaction Rate'] = np.select([mask2,mask1],[choiceintrateotherthancpe_plus,
		                                                                        choiceintratecpe_plus],default=0.00)
		
		choiceatscpe_plus = ((placementvdxsummaryfirst['DPETOTALTIMESPENT']/placementvdxsummaryfirst['DPEENGAMENTS'])/1000).apply('{0:.2f}'.format)
		choiceatsotherthancpe_plus = ((placementvdxsummaryfirst['ENGTOTALTIMESPENT']/placementvdxsummaryfirst['ENGAGEMENTS'])/1000).apply('{0:.2f}'.format)
		
		
		placementvdxsummaryfirst['Active Time Spent'] = np.select([mask2,mask1],[choiceatsotherthancpe_plus,
		                                                                         choiceatscpe_plus],default=0.00)
		
		placementvdxsummaryfirst['Active Time Spent'] = placementvdxsummaryfirst['Active Time Spent'].astype(float)
		
		placementvdxsummaryfirstnew = placementvdxsummaryfirst.replace(np.nan,'N/A',regex=True)
		
		#d = {"N/A":" "}
		
		
		#mask17 = placementvdxsummaryfirstnew["Placement# Name"].isin (["Grand Total"])
		#cols = ["Viewer VCR","Engager VCR"]
		#placementvdxsummaryfirstnew.update(placementvdxsummaryfirstnew.loc[mask17,cols].replace(d))
		
		placementvdxsummaryfirstnew.loc[placementvdxsummaryfirstnew.index[-1],["Viewer VCR","Engager VCR"]] = np.nan
		
		placementsummaryfinalnew = placementvdxsummaryfirstnew.loc[:,["Placement# Name","PRODUCT","Engagements Rate","Viewer CTR",
		                                                        "Engager CTR","Viewer VCR","Engager VCR",
		                                                        "Interaction Rate","Active Time Spent"]]
		
		placementsummaryfinal = placementsummaryfinalnew.loc[:,["Placement# Name","PRODUCT","Engagements Rate",
		                                                        "Viewer CTR","Engager CTR","Viewer VCR",
		                                                        "Engager VCR","Interaction Rate","Active Time Spent"]]
		
		
		
		unique_plc = placementsummaryfinal['Placement# Name'].nunique()
		
		
		#Adsize Roll Up
		
		placementadsize = [self.read_sql_vdx_summary,self.read_sql_adsize_km]
		placementadziesummary = reduce (lambda left, right:pd.merge (left, right, on='PLACEMENT'), placementadsize)
		
		placementadsizefirst = placementadziesummary.loc[:,["PLACEMENT","PLACEMENT_NAME","COST_TYPE","PRODUCT","ADSIZE",
		                                                    "IMPRESSIONS","ENGAGEMENTS","DPEENGAGEMENTS",
		                                                    "ENGCLICKTHROUGHS","DPECLICKTHROUGHS","VWRCLICKTHROUGHS",
		                                                    "VIEW100","ENG100","DPE100","ENGTOTALTIMESPENT",
		                                                    "DPETOTALTIMESPENT","ENGINTRACTIVEENGAGEMENTS",
		                                                    "COMPLETIONS","DPEINTRACTIVEENGAGEMENTS"]]
		
		placementadsizefirst["Placement# Name"] = placementadsizefirst[["PLACEMENT",
		                                                                        "PLACEMENT_NAME"]].apply(lambda x:".".join(x),
		                                                                                                 axis=1)
		
		placementadsizetable = placementadsizefirst.loc[:,["Placement# Name","COST_TYPE","PRODUCT","ADSIZE",
		                                                   "IMPRESSIONS", "ENGAGEMENTS", "DPEENGAGEMENTS",
		                                                   "ENGCLICKTHROUGHS", "DPECLICKTHROUGHS", "VWRCLICKTHROUGHS",
		                                                   "VIEW100", "ENG100", "DPE100", "ENGTOTALTIMESPENT",
		                                                   "DPETOTALTIMESPENT","ENGINTRACTIVEENGAGEMENTS",
		                                                   "COMPLETIONS", "DPEINTRACTIVEENGAGEMENTS"]]
		
		
		placementadsizegrouping = pd.pivot_table(placementadsizetable,index=['Placement# Name','ADSIZE','COST_TYPE'],
		                                         values=["IMPRESSIONS","ENGAGEMENTS","DPEENGAGEMENTS",
		                                                 "ENGCLICKTHROUGHS","DPECLICKTHROUGHS","VWRCLICKTHROUGHS",
		                                                 "VIEW100","ENG100","DPE100","ENGTOTALTIMESPENT","DPETOTALTIMESPENT",
		                                                 "ENGINTRACTIVEENGAGEMENTS",
		                                                 "COMPLETIONS",
		                                                 "DPEINTRACTIVEENGAGEMENTS"],aggfunc=np.sum)
		
		placementadsizegroupingnew = placementadsizegrouping.reset_index()
		
		placementadsizegroup = placementadsizegroupingnew.loc[:,:]
		
		placementadsizegroup = placementadsizegroup.append (placementadsizegroup.sum (numeric_only=True),
		                                                       ignore_index=True)
		
		
		
		placementadsizegroup["COST_TYPE"] = placementadsizegroup["COST_TYPE"].fillna ('CPE')
		placementadsizegroup["ADSIZE"] = placementadsizegroup["ADSIZE"].fillna ('Grand Total')
		placementadsizegroup["Placement# Name"] = placementadsizegroup["Placement# Name"].fillna ('Grand Total')
		
		mask9 = placementadsizegroup["COST_TYPE"].isin(["CPE+"])
		choiceadsizeengagementcpe_plus = placementadsizegroup["DPEENGAGEMENTS"]/placementadsizegroup["IMPRESSIONS"]
		mask10 = placementadsizegroup["COST_TYPE"].isin(["CPE","CPM","CPCV"])
		choiceplacementadsizegroupcpe = placementadsizegroup["ENGAGEMENTS"]/placementadsizegroup["IMPRESSIONS"]
		placementadsizegroup["Engagements Rate"] = np.select ([mask9,mask10], [choiceadsizeengagementcpe_plus,
		                                                                      choiceplacementadsizegroupcpe], default=0.00)
		
		mask11 = placementadsizegroup["COST_TYPE"].isin(["CPE","CPM","CPCV","CPE+"])
		choiceadsizeengagementvwrctr = placementadsizegroup["VWRCLICKTHROUGHS"]/placementadsizegroup["IMPRESSIONS"]
		
		placementadsizegroup["Viewer CTR"] = np.select([mask11],[choiceadsizeengagementvwrctr],default=0.00)
		
		choiceadsizeengagementengctr = placementadsizegroup["ENGCLICKTHROUGHS"]/placementadsizegroup["ENGAGEMENTS"]
		choiceadsizeengagementdpegctr = placementadsizegroup["DPECLICKTHROUGHS"]/placementadsizegroup["DPEENGAGEMENTS"]
		
		placementadsizegroup["Engager CTR"] = np.select([mask10,mask9],[choiceadsizeengagementengctr,
		                                                                 choiceadsizeengagementdpegctr],default=0.00)
		
		mask12 = placementadsizegroup["ADSIZE"].isin(["1x10"])
		mask13 = placementadsizegroup["COST_TYPE"].isin(["CPE","CPE+","CPM"])
		mask14 = placementadsizegroup["COST_TYPE"].isin(["CPCV"])
		choiceadsizevwrvcrcpe = placementadsizegroup["VIEW100"]/placementadsizegroup["IMPRESSIONS"]
		choiceadsizevwrvcrcpcv = placementadsizegroup["COMPLETIONS"]/placementadsizegroup["IMPRESSIONS"]
		placementadsizegroup["Viewer VCR"] = np.select([mask12 & mask13,mask12 & mask14],[choiceadsizevwrvcrcpe,
		                                                                  choiceadsizevwrvcrcpcv],default='N/A')
		
		placementadsizegroup['Viewer VCR'] = pd.to_numeric (placementadsizegroup['Viewer VCR'], errors='coerce')
		
		mask15 = ~placementadsizegroup["ADSIZE"].isin(["1x10"])
		mask16 = placementadsizegroup["COST_TYPE"].isin(["CPE","CPM"])
		choiceadsizeengvcrcpe = placementadsizegroup["ENG100"]/placementadsizegroup["ENGAGEMENTS"]
		choiceadsizeengvcrcpe_plus = placementadsizegroup["DPE100"]/placementadsizegroup["DPEENGAGEMENTS"]
		choiceadsizeengvcrcpcv = placementadsizegroup["COMPLETIONS"]/placementadsizegroup["ENGAGEMENTS"]
		
		placementadsizegroup["Engager VCR"] = np.select([mask15 & mask16,mask15 & mask9,mask15 & mask14],
		                                                [choiceadsizeengvcrcpe,choiceadsizeengvcrcpe_plus,
		                                                 choiceadsizeengvcrcpcv],default='N/A')
		
		placementadsizegroup['Engager VCR'] = pd.to_numeric (placementadsizegroup['Engager VCR'],
		                                                         errors='coerce')
		
		
		choiceadsizeinteracratecpe = placementadsizegroup["ENGINTRACTIVEENGAGEMENTS"]/placementadsizegroup["ENGAGEMENTS"]
		choiceadsizeinteracratecpe_plus = placementadsizegroup["DPEINTRACTIVEENGAGEMENTS"]/placementadsizegroup["DPEENGAGEMENTS"]
		
		placementadsizegroup["Interaction Rate"] = np.select([mask10,mask9],[choiceadsizeinteracratecpe,
		                                                                     choiceadsizeinteracratecpe_plus],default=0.00)
		
		
		choiceadsizeatscpe = ((placementadsizegroup["ENGTOTALTIMESPENT"]/placementadsizegroup["ENGAGEMENTS"])/1000).apply('{0:.2f}'.format)
		choiceadsizeatscpe_plus = ((placementadsizegroup["DPETOTALTIMESPENT"]/placementadsizegroup["DPEENGAGEMENTS"])/1000).apply('{0:.2f}'.format)
		
		placementadsizegroup["Active Time Spent"] = np.select([mask10,mask14],[choiceadsizeatscpe,choiceadsizeatscpe_plus],
		                                                      default=0.00)
		
		placementadsizegroup['Active Time Spent'] = placementadsizegroup['Active Time Spent'].astype (float)
		
		placementadsizegroupfirstnew = placementadsizegroup.replace (np.nan, 'N/A', regex=True)
		
		placementadsizegroupfirstnew.loc[placementadsizegroupfirstnew.index[-1],["Viewer VCR","Engager VCR"]] = np.nan
		
		#e = {"N/A":" "}
		
		#mask18 = placementadsizegroupfirstnew["Placement# Name"].isin (["Grand Total"])
		#cols_adsize = ["Viewer VCR", "Engager VCR"]
		#placementadsizegroupfirstnew.update(placementadsizegroupfirstnew.loc[mask18, cols_adsize].replace(e))
		
		placementadsizefinal = placementadsizegroupfirstnew.loc[:,["Placement# Name","ADSIZE","Engagements Rate",
		                                                          "Viewer CTR","Engager CTR","Viewer VCR","Engager VCR",
		                                                          "Interaction Rate","Active Time Spent"]]
		
		
		#video wise roll up
		placement_video = [self.read_sql_vdx_summary,self.read_sql_video_km,self.read_sql_km_for_video]
		placement_video_summary = reduce (lambda left, right:pd.merge (left, right, on='PLACEMENT'), placement_video)
		
		placement_by_video = placement_video_summary.loc[:,["PLACEMENT","PLACEMENT_NAME","COST_TYPE","PRODUCT",
		                                                    "VIDEONAME","IMPRESSIONS","ENGAGEMENTS","COMPLETIONS",
		                                                    "DPEENGAMENTS","VIEW0","VIEW25","VIEW50","VIEW75","VIEW100",
		                                                    "ENG0","ENG25","ENG50","ENG75","ENG100","DPE0","DPE25",
		                                                    "DPE50","DPE75","DPE100"]]
		
		placement_by_video["Placement# Name"] = placement_by_video[["PLACEMENT",
		                                                            "PLACEMENT_NAME"]].apply(lambda x:".".join(x),axis=1)
		
		placement_by_video_new = placement_by_video.loc[:,["Placement# Name","COST_TYPE","PRODUCT","VIDEONAME",
		                                                   "VIEW0", "VIEW25", "VIEW50", "VIEW75", "VIEW100",
		                                                   "ENG0", "ENG25", "ENG50", "ENG75", "ENG100","DPE0","DPE25",
		                                                   "DPE50", "DPE75", "DPE100","IMPRESSIONS","ENGAGEMENTS",
		                                                   "DPEENGAMENTS","COMPLETIONS"]]
		
		#print(placement_by_video_new)
		"""Conditions for 25%view"""
		mask17 =  placement_by_video_new["PRODUCT"].isin(['Display','Mobile'])
		mask18 = placement_by_video_new["COST_TYPE"].isin(["CPE","CPM","CPCV"])
		mask19 = placement_by_video_new["PRODUCT"].isin (["InStream"])
		mask20 = placement_by_video_new["COST_TYPE"].isin (["CPE", "CPM", "CPE+", "CPCV"])
		mask21 = placement_by_video_new["COST_TYPE"].isin(["CPE+"])
		
		choice25video_eng = placement_by_video_new["ENG25"]
		choice25video_vwr = placement_by_video_new["VIEW25"]
		choice25video_deep = placement_by_video_new["DPE25"]
		
		placement_by_video_new["25_pc_video"] = np.select([mask17 & mask18,mask19 & mask20,mask17 & mask21],
		                                                  [choice25video_eng,choice25video_vwr,choice25video_deep])
		
		
		
		"""Conditions for 50%view"""
		choice50video_eng = placement_by_video_new["ENG50"]
		choice50video_vwr = placement_by_video_new["VIEW50"]
		choice50video_deep = placement_by_video_new["DPE50"]
		
		placement_by_video_new["50_pc_video"] = np.select([mask17 & mask18,mask19 & mask20,mask17 & mask21],[choice50video_eng,
		                                                                                      choice50video_vwr,choice50video_deep])
		
		
		"""Conditions for 75%view"""
		
		choice75video_eng = placement_by_video_new["ENG75"]
		choice75video_vwr = placement_by_video_new["VIEW50"]
		choice75video_deep = placement_by_video_new["DPE75"]
		
		placement_by_video_new["75_pc_video"] = np.select([mask17 & mask18,mask19 & mask20,mask17 & mask21],[choice75video_eng,
		                                                                                                     choice75video_vwr,
		                                                                                                     choice75video_deep])
		
		"""Conditions for 100%view"""
		
		choice100video_eng = placement_by_video_new["ENG100"]
		choice100video_vwr = placement_by_video_new["VIEW100"]
		choice100video_deep = placement_by_video_new["DPE100"]
		
		placement_by_video_new["100_pc_video"] = np.select([mask17 & mask18,mask19 & mask20,mask17 & mask21],[choice100video_eng,
		                                                                                                      choice100video_vwr,choice100video_deep])
		
		"""conditions for 0%view"""
		
		choice0video_eng = placement_by_video_new["ENG0"]
		choice0video_vwr = placement_by_video_new["VIEW0"]
		choice0video_deep = placement_by_video_new["DPE0"]
		
		placement_by_video_new["Views"] = np.select([mask17 & mask18,mask19 & mask20,mask17 & mask21],[choice0video_eng,
		                                                                                               choice0video_vwr,
		                                                                                               choice0video_deep])
		
		
		
		placement_by_video_summary = placement_by_video_new.loc[:,["Placement# Name","PRODUCT","VIDEONAME","COST_TYPE",
		                                                           "Views","25_pc_video","50_pc_video","75_pc_video",
		                                                           "100_pc_video","IMPRESSIONS","ENGAGEMENTS",
		                                                           "DPEENGAMENTS"]]
		
		print (placement_by_video_summary)
		
		self.placementsummaryfinal = placementsummaryfinal
		self.unique_plc = unique_plc
		self.placementadsizefinal = placementadsizefinal
		
	def write_video_data(self):
		"""
		
		Writing Video Data
		:return:
		"""
		startline_placement = 9
		
		#Writing placement Data
		for placement, placement_df in self.placementsummaryfinal.groupby('Placement# Name'):
			
			write_pl = placement_df.to_excel(self.config.writer, sheet_name="vdx({})".format(self.config.ioid),
			                                          startcol=1,startrow=startline_placement,columns=["Placement# Name"],header=False,index=False)
			
			if placement_df.iloc[0,0] != "Grand Total":
				startline_placement +=1
			
			write_pls = placement_df.to_excel(self.config.writer,sheet_name="vdx({})".format(self.config.ioid),
				                                           startcol=1,startrow=startline_placement,columns=["PRODUCT",
				                                                                                    "Engagements Rate","Viewer CTR",
				                                                                                    "Engager CTR",
				                                                                                    "Viewer VCR","Engager VCR",
				                                                                                    "Interaction Rate","Active Time Spent"],header=False,index=False)
			
			startline_placement += len (placement_df)+1
	
		startline_adsize = 9 + len(self.placementsummaryfinal) + self.unique_plc*2+3
		
		
		#Writing adsize Data
		for adzise, adsize_df in self.placementadsizefinal.groupby('Placement# Name'):
			
			write_adsize_plc = adsize_df.to_excel(self.config.writer,sheet_name="vdx({})".format(self.config.ioid),
			                                       startcol =1, startrow =startline_adsize,columns = ["Placement# Name"],
			                                       header=False, index=False)
			
			if adsize_df.iloc[0, 0]!="Grand Total":
				startline_adsize += 1
			
			write_adsize = adsize_df.to_excel(self.config.writer,sheet_name ="vdx({})".format(self.config.ioid),
			                                   startcol=1, startrow = startline_adsize,columns = ["ADSIZE","Engagements Rate",
			                                                                                      "Viewer CTR","Engager CTR",
			                                                                                      "Viewer VCR","Engager VCR",
			                                                                                      "Interaction Rate","Active Time Spent"],
			                                  header=False, index=False)
			
			startline_adsize += len(adsize_df)+1
		
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




















