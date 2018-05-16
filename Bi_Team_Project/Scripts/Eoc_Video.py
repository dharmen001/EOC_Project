#coding=utf-8
"""
Created by:Dharmendra
Date:2018-03-23
"""

import pandas as pd
import numpy as np
import config
import logging
from xlsxwriter.utility import xl_rowcol_to_cell, xl_range
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
		
		sql_video_player_intraction = "SELECT PRODUCT, " \
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
		                              "sum(DPE_FULL_SCREEN) as Dpefullscreen FROM TFR_REP.VIDEO_DETAIL_MV " \
		                              "WHERE IO_ID = {} GROUP BY PRODUCT " \
		                              "ORDER BY PRODUCT".format(self.config.ioid)
		
		"""sql_ad_intraction = "SELECT PRODUCT,BLAZE_ACTION_TYPE_DESC,BLAZE_TAG_NAME_DESC, " \
		                    "sum(VWR_INTERACTION) as Vwradintraction,sum(ENG_INTERACTION) as Engadintraction," \
		                    "sum(DPE_INTERACTION) as Dpeadintraction " \
		                    "FROM TFR_REP.INTERACTION_DETAIL_MV WHERE IO_ID = {}" \
		                    "GROUP BY PRODUCT,BLAZE_ACTION_TYPE_DESC,BLAZE_TAG_NAME_DESC ORDER BY PRODUCT".format(self.config.ioid)"""
		
		sql_ad_intraction = "select * from(select product, blaze_action_type_desc, blaze_tag_name_desc, " \
		                     "sum(decode(product, 'InStream', vwr_interaction, eng_interaction)) as interaction " \
		                     "from Interaction_detail_mv where io_id = {} group by product, blaze_action_type_desc, " \
		                     "blaze_tag_name_desc order by product, blaze_action_type_desc, " \
		                     "blaze_tag_name_desc) pivot (sum(interaction) for blaze_action_type_desc in " \
		                     "('Click-thru' Clickthru, 'Interaction' Interaction)) order by product, blaze_tag_name_desc".format(self.config.ioid)
		
		sql_click_throughs = "SELECT PRODUCT,BLAZE_ACTION_TYPE_DESC,BLAZE_TAG_NAME_DESC, " \
		                     "sum(VWR_INTERACTION) as Vwrclickintraction," \
		                     "sum(ENG_INTERACTION) as Engclickintraction," \
		                     "sum(DPE_INTERACTION) as Dpeclickintraction FROM TFR_REP.INTERACTION_DETAIL_MV " \
		                     "WHERE IO_ID = {} and BLAZE_ACTION_TYPE_DESC = 'Click-thru' " \
		                     "GROUP BY PRODUCT,BLAZE_ACTION_TYPE_DESC,BLAZE_TAG_NAME_DESC ORDER BY PRODUCT".format(self.config.ioid)
		
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
		
		sql_km_for_video = "select substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as Placement,PRODUCT," \
		                   "sum(IMPRESSIONS) as Impressions,sum(ENGAGEMENTS) as Engagements, sum(CPCV_COUNT) as completions," \
		                   "sum(DPE_ENGAGEMENTS) as Dpeengaments From TFR_REP.KEY_METRIC_MV WHERE IO_ID = {} GROUP BY PLACEMENT_ID, PLACEMENT_DESC,PRODUCT ORDER BY  PLACEMENT_ID".format(self.config.ioid)
		
		
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
		read_sql_video_player_interaction = pd.read_sql(self.sql_video_player_intraction, self.config.conn)
		read_sql_ad_intraction = pd.read_sql(self.sql_ad_intraction, self.config.conn)
		read_sql_click_throughs = pd.read_sql(self.sql_click_throughs, self.config.conn)
		read_sql_vdx_day_km = pd.read_sql(self.sql_vdx_day_km, self.config.conn)
		read_sql_km_for_video = pd.read_sql(self.sql_km_for_video, self.config.conn)
		read_sql_adsize_km_rate = pd.read_sql(self.sql_adsize_km_rate, self.config.conn)
		

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
		
		
		
		unique_plc_summary = placementsummaryfinal['Placement# Name'].nunique()
		
		
		#Adsize Roll Up
		
		self.logger.info ('Creating adsize wise table of IO - {}'.format (self.config.ioid))
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
		self.logger.info ('Creating Video wise table of IO - {}'.format (self.config.ioid))
		#placement_video = [self.read_sql_vdx_summary,self.read_sql_video_km,self.read_sql_km_for_video]
		placement_video = [self.read_sql_vdx_summary, self.read_sql_video_km]
		placement_video_summary = reduce (lambda left, right:pd.merge (left, right, on='PLACEMENT'), placement_video)
		
		#print (placement_video_summary)
		
		placement_by_video = placement_video_summary.loc[:,["PLACEMENT","PLACEMENT_NAME","COST_TYPE","PRODUCT",
		                                                    "VIDEONAME","VIEW0","VIEW25","VIEW50","VIEW75","VIEW100",
		                                                    "ENG0","ENG25","ENG50","ENG75","ENG100","DPE0","DPE25",
		                                                    "DPE50","DPE75","DPE100"]]
		
		#print (placement_by_video)
		
		placement_by_video["Placement# Name"] = placement_by_video[["PLACEMENT",
		                                                            "PLACEMENT_NAME"]].apply(lambda x:".".join(x),axis=1)
		
		placement_by_video_new = placement_by_video.loc[:,["PLACEMENT","Placement# Name","COST_TYPE","PRODUCT","VIDEONAME",
		                                                   "VIEW0", "VIEW25", "VIEW50", "VIEW75", "VIEW100",
		                                                   "ENG0", "ENG25", "ENG50", "ENG75", "ENG100","DPE0","DPE25",
		                                                   "DPE50", "DPE75", "DPE100"]]
		
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
		choice75video_vwr = placement_by_video_new["VIEW75"]
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
		
		
		
		placement_by_video_summary = placement_by_video_new.loc[:,["PLACEMENT","Placement# Name","PRODUCT","VIDEONAME","COST_TYPE",
		                                                           "Views","25_pc_video","50_pc_video","75_pc_video",
		                                                           "100_pc_video"]]
		
		placement_by_km_video = [placement_by_video_summary,self.read_sql_km_for_video]
		placement_by_km_video_summary = reduce(lambda left, right:pd.merge (left, right, on=['PLACEMENT','PRODUCT']), placement_by_km_video)
		
		#dup_col =["IMPRESSIONS","ENGAGEMENTS","DPEENGAMENTS"]
		
		#placement_by_video_summary.loc[placement_by_video_summary.duplicated(dup_col),dup_col] = np.nan
		
		#print ("Dhar",placement_by_video_summary)
		
		'''adding views based on conditions'''
		
		placement_by_video_summary_new = placement_by_km_video_summary.loc[placement_by_km_video_summary.reset_index().groupby(['PLACEMENT','PRODUCT'])['Views'].idxmax()]
		#print (placement_by_video_summary_new)
		
		#mask22 = (placement_by_video_summary_new.PRODUCT.str.upper ()=='DISPLAY') & (placement_by_video_summary_new.COST_TYPE=='CPE')
		placement_by_video_summary_new.loc[mask17 & mask18, 'Views'] = placement_by_video_summary_new['ENGAGEMENTS']
		placement_by_video_summary_new.loc[mask19 & mask20, 'Views'] = placement_by_video_summary_new['IMPRESSIONS']
		placement_by_video_summary_new.loc[mask17 & mask21, 'Views'] = placement_by_video_summary_new['DPEENGAMENTS']
		
		
		placement_by_video_summary = placement_by_video_summary.drop(placement_by_video_summary_new.index).append(placement_by_video_summary_new).sort_index()
		
		placement_by_video_summary["Video Completion Rate"] = placement_by_video_summary["100_pc_video"]/placement_by_video_summary["Views"]
		
		placement_by_video_final = placement_by_video_summary.loc[:,["Placement# Name","PRODUCT","VIDEONAME","Views",
		                                                             "25_pc_video","50_pc_video","75_pc_video","100_pc_video",
		                                                             "Video Completion Rate"]]
		
		
		
		#Intraction wise data
		
		'''intractionclickgrouping = pd.pivot_table(self.read_sql_click_throughs,index = ['PRODUCT'],
		                                         values=["VWRCLICKINTRACTION","ENGCLICKINTRACTION","DPECLICKINTRACTION"],
		                                         columns=["BLAZE_TAG_NAME_DESC"],aggfunc=np.sum,fill_value=0)
		
		intractionclickgroupingnew = intractionclickgrouping.reset_index()
		
		intractionclickgroupingnew_r = intractionclickgroupingnew.loc[:,["PRODUCT","VWRCLICKINTRACTION","ENGCLICKINTRACTION","DPECLICKINTRACTION"]]
		
		intractionadinteractiongrouping  = pd.pivot_table(self.read_sql_ad_intraction,index = ['PRODUCT'],
		                                                  values=["VWRADINTRACTION","ENGADINTRACTION","DPEADINTRACTION"]
		                                                  ,columns=["BLAZE_TAG_NAME_DESC"],aggfunc=np.sum,fill_value=0)
		
		intractionadinteractiongroupingnew = intractionadinteractiongrouping.reset_index()
		
		intractionadinteractiongrouping_r = intractionadinteractiongroupingnew.loc[:,["PRODUCT","VWRADINTRACTION","ENGADINTRACTION",
		                                                                              "DPEADINTRACTION"]]'''
		self.logger.info ('Creating Intraction wise table of IO - {}'.format (self.config.ioid))
		video_player = self.read_sql_video_player_interaction.loc[:,["PRODUCT","VWRMUTE","VWRUNMUTE","VWRPAUSE","VWRREWIND",
		                                                             "VWRRESUME","VWRREPLAY","VWRFULLSCREEN","ENGMUTE",
		                                                             "ENGUNMUTE","ENGPAUSE","ENGREWIND","ENGRESUME","ENGREPLAY",
		                                                             "ENGFULLSCREEN"]]
		
		mask22 = video_player["PRODUCT"].isin(['Display','Mobile'])
		mask23 = video_player["PRODUCT"].isin(['InStream'])
		choice_intraction_mute = video_player["ENGMUTE"]
		choice_intraction_unmute = video_player["ENGUNMUTE"]
		choice_intraction_pause = video_player["ENGPAUSE"]
		choice_intraction_rewind = video_player["ENGREWIND"]
		choice_intraction_resume = video_player["ENGRESUME"]
		choice_intraction_replay = video_player["ENGREPLAY"]
		choice_intraction_fullscreen = video_player["ENGFULLSCREEN"]
		choice_interaction_ins_mute = video_player["VWRMUTE"]
		choice_interaction_ins_unmute = video_player["VWRUNMUTE"]
		choice_interaction_ins_pause = video_player["VWRPAUSE"]
		choice_interaction_ins_rewind = video_player["VWRREWIND"]
		choice_interaction_ins_resume = video_player["VWRRESUME"]
		choice_interaction_ins_replay = video_player["VWRREPLAY"]
		choice_interaction_ins_fullscreen = video_player["VWRFULLSCREEN"]
		
		video_player["Mute"] = np.select([mask22,mask23],[choice_intraction_mute,choice_interaction_ins_mute])
		video_player["Unmute"] = np.select ([mask22,mask23], [choice_intraction_unmute,choice_interaction_ins_unmute])
		video_player["Pause"] = np.select ([mask22,mask23], [choice_intraction_pause,choice_interaction_ins_pause])
		video_player["Rewind"] = np.select ([mask22,mask23], [choice_intraction_rewind,choice_interaction_ins_rewind])
		video_player["Resume"] = np.select ([mask22,mask23], [choice_intraction_resume,choice_interaction_ins_resume])
		video_player["Replay"] = np.select ([mask22,mask23], [choice_intraction_replay,choice_interaction_ins_replay])
		video_player["Fullscreen"] = np.select ([mask22,mask23], [choice_intraction_fullscreen,choice_interaction_ins_fullscreen])
		
		video_player.rename (columns={"PRODUCT":"Product"},inplace = True)
		
		video_player_final = video_player.loc[:,["Product","Mute","Unmute","Pause","Rewind","Resume","Replay","Fullscreen"]]
		
		#print (self.read_sql_ad_intraction)
		
		intractions_click_ad = pd.pivot_table(self.read_sql_ad_intraction,index="PRODUCT",values="CLICKTHRU",
		                                      columns="BLAZE_TAG_NAME_DESC",
		                                      aggfunc=np.sum,fill_value=0)
		
		
		intractions_click_ad_new = intractions_click_ad.reset_index()
		intractions_clicks = intractions_click_ad_new.loc[:,:]
		cols_drop = ["PRODUCT"]
		intractions_clicks_new = intractions_clicks.drop(cols_drop,axis=1)
		
		intractions_intrac_ad = pd.pivot_table (self.read_sql_ad_intraction, index="PRODUCT", values="INTERACTION",
		                                        columns="BLAZE_TAG_NAME_DESC",aggfunc=np.sum,fill_value=0)
		
		intractions_intrac_ad_new = intractions_intrac_ad.reset_index()
		intractions_intrac = intractions_intrac_ad_new.loc[:,:]
		intractions_intrac_new = intractions_intrac.drop(cols_drop,axis=1)
		
		self.placementsummaryfinal = placementsummaryfinal
		self.unique_plc_summary = unique_plc_summary
		self.placementadsizefinal = placementadsizefinal
		self.placement_by_video_final = placement_by_video_final
		self.video_player_final = video_player_final
		self.intractions_clicks_new = intractions_clicks_new
		self.intractions_intrac_new = intractions_intrac_new
		
		
	def write_video_data(self):
		"""
		
		Writing Video Data
		:return:
		"""
		
		self.logger.info ('writing data to excel of IO - {}'.format (self.config.ioid))
		data_common_columns = self.config.common_columns_summary ()
		
		writing_data_common_columns = data_common_columns[1].to_excel (self.config.writer,
		                                                               sheet_name="VDX Details({})".format(self.config.ioid),
		                                                               startcol=1, startrow=1,
		                                                               index=False, header=False)
		
		startline_placement = 9
		#Writing placement Data
		for placement, placement_df in self.placementsummaryfinal.groupby('Placement# Name'):
			
			write_pl = placement_df.to_excel(self.config.writer, sheet_name="VDX Details({})".format(self.config.ioid),
			                                          startcol=1,startrow=startline_placement,columns=["Placement# Name"],header=False,index=False)
			
			if placement_df.iloc[0,0] != "Grand Total":
				startline_placement +=1
			
			write_pls = placement_df.to_excel(self.config.writer,sheet_name="VDX Details({})".format(self.config.ioid),
				                                           startcol=1,startrow=startline_placement,columns=["PRODUCT",
				                                                                                    "Engagements Rate","Viewer CTR",
				                                                                                    "Engager CTR",
				                                                                                    "Viewer VCR","Engager VCR",
				                                                                                    "Interaction Rate","Active Time Spent"],header=False,index=False)
			
			
			
			startline_placement += len (placement_df)+1
	
		startline_adsize = 9 + len(self.placementsummaryfinal) + self.unique_plc_summary*2+3
		
		
		#Writing adsize Data
		for adzise, adsize_df in self.placementadsizefinal.groupby('Placement# Name'):
			
			write_adsize_plc = adsize_df.to_excel(self.config.writer,sheet_name="VDX Details({})".format(self.config.ioid),
			                                       startcol =1, startrow =startline_adsize,columns = ["Placement# Name"],
			                                       header=False, index=False)
			
			if adsize_df.iloc[0, 0]!="Grand Total":
				startline_adsize += 1
			
			write_adsize = adsize_df.to_excel(self.config.writer,sheet_name ="VDX Details({})".format(self.config.ioid),
			                                   startcol=1, startrow = startline_adsize,columns = ["ADSIZE","Engagements Rate",
			                                                                                      "Viewer CTR","Engager CTR",
			                                                                                      "Viewer VCR","Engager VCR",
			                                                                                      "Interaction Rate","Active Time Spent"],
			                                  header=False, index=False)
			
			startline_adsize += len(adsize_df)+1
		
		
		startline_video = 9 + len(self.placementsummaryfinal) + self.unique_plc_summary*2+3+len(self.placementadsizefinal)+self.unique_plc_summary*2+3
		#writing video wise Data
		
		for video, video_df  in self.placement_by_video_final.groupby('Placement# Name'):
			
			write_video_plc = video_df.to_excel(self.config.writer,sheet_name="VDX Details({})".format(self.config.ioid),
			                                    startcol=1, startrow=startline_video,columns = ["Placement# Name"],
			                                    header =False,index=False)
			
			write_video = video_df.to_excel(self.config.writer,sheet_name="VDX Details({})".format(self.config.ioid),
			                                startcol=1, startrow=startline_video+1, columns=["PRODUCT","VIDEONAME",
			                                                                                 "Views","25_pc_video",
			                                                                                 "50_pc_video","75_pc_video",
			                                                                                 "100_pc_video",
			                                                                                 "Video Completion Rate"],
			                                header=False, index=False)
			startline_video += len (video_df)+2
		
		startline_player = 9 + len(self.placementsummaryfinal) + self.unique_plc_summary*2+3+len(self.placementadsizefinal)+self.unique_plc_summary*2+3+len(self.placement_by_video_final)+self.unique_plc_summary*2+3
		
		write_player_interaction = self.video_player_final.to_excel(self.config.writer,sheet_name="VDX Details({})".format(self.config.ioid),
		                                                            startcol=1,startrow=startline_player,index=False)
		
		write_intraction_clicks = self.intractions_clicks_new.to_excel(self.config.writer,
		                                                      sheet_name="VDX Details({})".format(self.config.ioid),
		                                                      startcol=9,startrow=startline_player,index=False,merge_cells=False)
		                                                      
		write_intraction = self.intractions_intrac_new.to_excel(self.config.writer,sheet_name="VDX Details({})".format(self.config.ioid),
		                                                        startcol=9+self.intractions_clicks_new.shape[1],
		                                                        startrow=startline_player, index=False,
		                                                        merge_cells=False)
	
	def formatting_Video(self):
		"""
		Formmating
		:return:
		"""
		self.logger.info ('Applying formatting in VDX sheet - {}'.format (self.config.ioid))
		workbook = self.config.writer.book
		worksheet = self.config.writer.sheets["VDX Details({})".format(self.config.ioid)]
		worksheet.hide_gridlines (2)
		worksheet.set_row (0, 6)
		worksheet.set_column ("A:A", 2)
		worksheet.set_zoom(75)
		worksheet.insert_image ("M7", "Exponential.png", {"url":"https://www.tribalfusion.com"})
		worksheet.insert_image ("M2", "Client_Logo.png")
		number_cols_plc_summary = self.placementsummaryfinal.shape[1]
		number_cols_adsize = self.placementadsizefinal.shape[1]
		number_cols_video = self.placement_by_video_final.shape[1]
		
		format_hearder = workbook.add_format ({"bold":True, "bg_color":'#00B0F0', "align":"left"})
		format_colour = workbook.add_format({"bg_color":'#00B0F0'})
		format_campaign_info = workbook.add_format ({"bold":True,"bg_color":'#00B0F0', "align":"left"})
		format_grand_total = workbook.add_format({"bold":True,"bg_color":"#A5A5A5","num_format":"#,##0"})
		format_grand = workbook.add_format({"bold":True,"bg_color":"#A5A5A5"})
		
		
		worksheet.conditional_format ("A1:R5", {"type":"blanks", "format":format_campaign_info})
		
		worksheet.conditional_format ("A1:R5", {"type":"no_blanks", "format":format_campaign_info})
		
		worksheet.write_string (7, 1, "VDX Performance KPIs - by Placement and Platform",
		                        format_hearder)
		
		worksheet.write_string (8, 1, "Unit", format_hearder)
		worksheet.write_string (8, 2, "Engagement Rate", format_hearder)
		worksheet.write_string (8, 3, "Viewer CTR", format_hearder)
		worksheet.write_string (8, 4, "Engager CTR", format_hearder)
		worksheet.write_string (8, 5, "Viewer VCR (Primary Video)", format_hearder)
		worksheet.write_string (8, 6, "Engager VCR (Primary Video)", format_hearder)
		worksheet.write_string (8, 7, "Interaction Rate", format_hearder)
		worksheet.write_string (8, 8, "Active Time Spent", format_hearder)
		worksheet.conditional_format(7,1,7,number_cols_plc_summary-1,{"type":"blanks","format":format_colour})
		
		percent_fmt = workbook.add_format ({"num_format":"0.00%", "align":"right"})
		
		grand_fmt = workbook.add_format ({"num_format":"0.00%", "bg_color":'#A5A5A5',"bold":True})
		
		ats_format = workbook.add_format({"bg_color":'#A5A5A5',"bold":True})
		
		format_num = workbook.add_format ({"num_format":"#,##0"})
		worksheet.write_string (9+len (self.placementsummaryfinal)+self.unique_plc_summary*2+1, 1, "Ad Size Breakdown",
		                        format_hearder)
		worksheet.write_string (9+len (self.placementsummaryfinal)+self.unique_plc_summary*2+2, 1, "Ad Size",
		                        format_hearder)
		worksheet.write_string (9+len (self.placementsummaryfinal)+self.unique_plc_summary*2+2, 2, "Engagement Rate",
		                        format_hearder)
		worksheet.write_string (9+len (self.placementsummaryfinal)+self.unique_plc_summary*2+2, 3, "Viewer CTR",
		                        format_hearder)
		worksheet.write_string (9+len (self.placementsummaryfinal)+self.unique_plc_summary*2+2, 4, "Engager CTR",
		                        format_hearder)
		worksheet.write_string (9+len (self.placementsummaryfinal)+self.unique_plc_summary*2+2, 5, "Viewer VCR (Primary Video)",
		                        format_hearder)
		
		worksheet.write_string (9+len (self.placementsummaryfinal)+self.unique_plc_summary*2+2, 6,"Engager VCR (Primary Video)",format_hearder)
		
		worksheet.write_string (9+len (self.placementsummaryfinal)+self.unique_plc_summary*2+2, 7,"Interaction Rate",
		                        format_hearder)
		
		worksheet.write_string(9+len (self.placementsummaryfinal)+self.unique_plc_summary*2+2,8,"Active Time Spent",format_hearder)
		
		worksheet.conditional_format(9+len (self.placementsummaryfinal)+self.unique_plc_summary*2+1,1,
		                             9+len (self.placementsummaryfinal)+self.unique_plc_summary*2+1,number_cols_adsize-1,{"type":"blanks",
		                                                                                                                  "format":format_colour})
		
		worksheet.write_string (9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
		                        self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3,
		                        1, "Video Details",format_hearder)
		
		worksheet.write_string(9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
		                       self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+4,1, "Unit",format_hearder)
		
		worksheet.write_string(9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
		                       self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+4, 2, "Video Name",
		                       format_hearder)
		
		worksheet.write_string (9+self.placementsummaryfinal.shape[0]+
		                        self.unique_plc_summary*2+1+self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+4,
		                        3, "Views",format_hearder)
		
		worksheet.write_string (9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
		                        self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+4,4,"25% View",format_hearder)
		
		worksheet.write_string(9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
		                       self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+4, 5, "50% View",
		                       format_hearder)
		
		worksheet.write_string (9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
		                        self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+4, 6, "75% View",format_hearder)
		
		worksheet.write_string (9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
		                        self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+4, 7, "Video Completion",
		                        format_hearder)
		
		worksheet.write_string (9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
		                        self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+4, 8, "Video Completion Rate",
		                        format_hearder)
		
		worksheet.conditional_format(9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
		                             self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3,1,
		                             9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
		                             self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3,number_cols_video-1,
		                             {"type":"blanks","format":format_colour})
		
		worksheet.write_string (9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
		                        self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3+
		                        self.placement_by_video_final.shape[0]+self.unique_plc_summary*2+3,
		                        1,"Interaction Details",
		                        format_hearder)
		
		worksheet.conditional_format(9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
		                        self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3+
		                        self.placement_by_video_final.shape[0]+self.unique_plc_summary*2+3,1,
		                             9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
		                        self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3+
		                        self.placement_by_video_final.shape[0]+self.unique_plc_summary*2+3,
		                             9+self.intractions_clicks_new.shape[1]+self.intractions_intrac_new.shape[1],
		                             {"type":"blanks","format":format_colour})
		
		worksheet.write_string(9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
		                       self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3+
		                       self.placement_by_video_final.shape[0]+self.unique_plc_summary*2+4,2,"Video Player Interactions",
		                       format_hearder)
		
		worksheet.conditional_format (9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
		                        self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3+
		                        self.placement_by_video_final.shape[0]+self.unique_plc_summary*2+4,1,
		                        9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
		                        self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3+
		                        self.placement_by_video_final.shape[0]+self.unique_plc_summary*2+4,
		                        9+self.intractions_clicks_new.shape[1]+self.intractions_intrac_new.shape[1],{"type":"blanks",
		                                                                                                     "format":format_colour})
		
		worksheet.write_string(9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
		                       self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3+
		                       self.placement_by_video_final.shape[0]+self.unique_plc_summary*2+4, 9,
		                       "Clickthroughs",format_hearder)
		
		worksheet.write_string(9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
		                       self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3+
		                       self.placement_by_video_final.shape[0]+self.unique_plc_summary*2+4,
		                       9+self.intractions_clicks_new.shape[1],"Ad Interactions",format_hearder)
		
		worksheet.write_string(9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
		                       self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3+
		                       self.placement_by_video_final.shape[0]+self.unique_plc_summary*2+5,
		                       9+self.intractions_clicks_new.shape[1]+self.intractions_intrac_new.shape[1],"Total Interactions",format_hearder)
		
		worksheet.write_string(9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
		                       self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3+
		                       self.placement_by_video_final.shape[0]+self.unique_plc_summary*2+5+self.video_player_final.shape[0]+1,
		                       1,"Grand Total",format_grand)
		
		for col in range (2, 9+self.intractions_clicks_new.shape[1]+self.intractions_intrac_new.shape[1]+1):
			
			cell_location = xl_rowcol_to_cell(9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
			                                  self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3+
			                                  self.placement_by_video_final.shape[0]+self.unique_plc_summary*2+5+self.video_player_final.shape[0]+1
			                                  , col)
			start_range = xl_rowcol_to_cell(9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
			                                self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3+
			                                self.placement_by_video_final.shape[0]+self.unique_plc_summary*2+6,col)
			
			end_range = xl_rowcol_to_cell(9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
			                              self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3+
			                              self.placement_by_video_final.shape[0]+self.unique_plc_summary*2+5+
			                              self.video_player_final.shape[0],col)
			
			formula = '=sum({:s}:{:s})'.format (start_range, end_range)
			
			worksheet.write_formula(cell_location,formula,format_grand_total)
		
		start_range_x = 9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3+self.placement_by_video_final.shape[0]+self.unique_plc_summary*2+6
		
		for row in range(self.video_player_final.shape[0]):
			#print (row)
			#print (len(self.video_player_final))
			cell_range = xl_range(start_range_x,2,start_range_x,9+self.intractions_clicks_new.shape[1]+self.intractions_intrac_new.shape[1]-1)
			formula = 'sum({:s})'.format(cell_range)
			worksheet.write_formula(start_range_x,
			                        9+self.intractions_clicks_new.shape[1]+self.intractions_intrac_new.shape[1],
			                        formula,format_num)
			start_range_x += 1
		worksheet.conditional_format(9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
		                             self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3+
		                             self.placement_by_video_final.shape[0]+self.unique_plc_summary*2+5,1,
		                             9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
		                             self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3+
		                             self.placement_by_video_final.shape[0]+self.unique_plc_summary*2+5,
		                             9+self.intractions_clicks_new.shape[1]+self.intractions_intrac_new.shape[1],{"type":"no_blanks","format":format_hearder})
		
		#applying grand Total Formatting
		
		worksheet.conditional_format(9+len (self.placementsummaryfinal)+self.unique_plc_summary*2+1-4,1,
		                             9+len (self.placementsummaryfinal)+self.unique_plc_summary*2+1-4,number_cols_plc_summary-2,
		                             {"type":"blanks","format":grand_fmt})
		
		worksheet.conditional_format (9+len (self.placementsummaryfinal)+self.unique_plc_summary*2+1-4, 1,
		                              9+len (self.placementsummaryfinal)+self.unique_plc_summary*2+1-4,
		                              number_cols_plc_summary-2,{"type":"no_blanks","format":grand_fmt})
		
		worksheet.conditional_format (9+len (self.placementsummaryfinal)+self.unique_plc_summary*2+1-4, number_cols_plc_summary-1,
		                              9+len (self.placementsummaryfinal)+self.unique_plc_summary*2+1-4,number_cols_plc_summary-1,
		                              {"type":"no_blanks","format":ats_format})
		
		worksheet.conditional_format(9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
		                             self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3-4,1,
		                             9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
		                             self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3-4,number_cols_adsize-2,
		                             {"type":"blanks","format":grand_fmt})
		
		worksheet.conditional_format(9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
		                             self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3-4, 1,
		                             9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
		                             self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3-4,
		                             number_cols_adsize-2,{"type":"no_blanks","format":grand_fmt})
		
		worksheet.conditional_format(9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
		                             self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3-4,number_cols_adsize-1,
		                             9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
		                             self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3-4,
		                             number_cols_adsize-1,{"type":"no_blanks","format":ats_format})
		
		#applying percent formatting
		
		for col in range(2,number_cols_plc_summary-1):
			start_plc_row = 10
			end_plc_row = 9+len (self.placementsummaryfinal)+self.unique_plc_summary*2+1-6
			worksheet.conditional_format(start_plc_row,col,end_plc_row,col,{"type":"no_blanks","format":percent_fmt})
		
		
		for col in range(2,number_cols_adsize-1):
			start_adsize_row = 9 + len(self.placementsummaryfinal) + self.unique_plc_summary*2+4
			end_adsize_row = 9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+\
			                 self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3-6
			worksheet.conditional_format(start_adsize_row,col,end_adsize_row,col,{"type":"no_blanks","format":percent_fmt})
		
		for col in range(3,number_cols_video-1):
			start_video_row = 9 + len(self.placementsummaryfinal) + self.unique_plc_summary*2+3+len(self.placementadsizefinal)+self.unique_plc_summary*2+4
			end_video_row = 9+self.placementsummaryfinal.shape[0]+\
			                self.unique_plc_summary*2+1+self.placementadsizefinal.shape[0]+\
			                self.unique_plc_summary*2+3+self.placement_by_video_final.shape[0]+\
			                self.unique_plc_summary*2+3-4
			worksheet.conditional_format(start_video_row,col,end_video_row,col,{"type":"no_blanks","format":format_num})
		
		for col in range(number_cols_video-1,number_cols_video):
			start_video_row_vcr = 9 + len(self.placementsummaryfinal) + self.unique_plc_summary*2+3+len(self.placementadsizefinal)+self.unique_plc_summary*2+4
			end_video_row_vcr = 9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+\
			                    self.placementadsizefinal.shape[0]+\
			                    self.unique_plc_summary*2+3+self.placement_by_video_final.shape[0]+\
			                    self.unique_plc_summary*2+3-4
			worksheet.conditional_format(start_video_row_vcr,col,end_video_row_vcr,col,{"type":"no_blanks","format":percent_fmt})
		
		for col in range (2, 9+self.intractions_clicks_new.shape[1]+self.intractions_intrac_new.shape[1]):
			start_intraction_row = 9+self.placementsummaryfinal.shape[0]+\
			                       self.unique_plc_summary*2+1+self.placementadsizefinal.shape[0]+\
			                       self.unique_plc_summary*2+3+self.placement_by_video_final.shape[0]+self.unique_plc_summary*2+6
			
			end_intraction_row  = 9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+\
			                      self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3+\
			                      self.placement_by_video_final.shape[0]+\
			                      self.unique_plc_summary*2+5+self.video_player_final.shape[0]
			
			worksheet.conditional_format(start_intraction_row,col,end_intraction_row,col,{"type":"no_blanks","format":format_num})
			
		alignment = workbook.add_format ({"align":"right"})
		worksheet.set_column ("C:Z", 25, alignment)
		worksheet.set_column ("B:B", 47)
	def main(self):
		"""
Main Function
		"""
		self.config.common_columns_summary()
		self.connect_tfr_video()
		self.read_query_video()
		if self.read_sql_vdx_day_km.empty:
			self.logger.info ("No VDX placements for IO - {}".format (self.config.ioid))
			pass
		else:
			self.access_vdx_columns()
			self.write_video_data()
			self.formatting_Video()
			self.logger.info ('VDX Sheet Created for IO - {}'.format (self.config.ioid))

if __name__=="__main__":
	pass
	#enable it when running for individual file
	#c = config.Config ('Origin', 605937)
	#o = Video (c)
	#o.main ()
	#c.saveAndCloseWriter ()




















