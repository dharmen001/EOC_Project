# coding=utf-8
# !/usr/bin/env python
"""
Created by:Dharmendra
Date:2018-03-23
"""
import datetime
import pandas as pd
import numpy as np
from xlsxwriter.utility import xl_rowcol_to_cell, xl_range
from functools import reduce
import pandas.io.formats.excel

pandas.io.formats.excel.header_style = None


class Video (object):
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
		self.logger.info ('Starting to cretae vdx placements for IO - {}'.format (self.config.ioid))
		
		self.logger.info("Start executing: "+'VDX_Summary.sql'+" at "+str (datetime.datetime.now ().strftime ("%Y-%m-%d %H:%M")))
		read_vdx_summary = open('VDX_Summary.sql')
		sql_vdx_summary = read_vdx_summary.read().format(self.config.ioid,self.config.start_date,self.config.end_date)
		
		
		self.logger.info("Start executing: "+'Placement_info_vdx.sql'+" at "+str (datetime.datetime.now ().strftime ("%Y-%m-%d %H:%M")))
		read_plc_info_vdx = open('Placement_info_vdx.sql')
		sql_vdx_km = read_plc_info_vdx.read().format(self.config.ioid,self.config.start_date,self.config.end_date)
		
		self.logger.info("Start executing: "+'Placement_info_vdx_adsize.sql'+" at "+str (datetime.datetime.now ().strftime ("%Y-%m-%d %H:%M")))
		read_adsize_vdx = open('Placement_info_vdx_adsize.sql')
		sql_adsize_km = read_adsize_vdx.read().format(self.config.ioid,self.config.start_date,self.config.end_date)
		
		self.logger.info("Start executing: "+'Placement_info_vdx_video.sql'+" at "+str (datetime.datetime.now ().strftime ("%Y-%m-%d %H:%M")))
		read_video_vdx = open('Placement_info_vdx_video.sql')
		sql_video_km = read_video_vdx.read().format(self.config.ioid,self.config.start_date,self.config.end_date)
		
		self.logger.info("Start executing: "+'Placement_info_vdx_intraction.sql'+" at "+str (datetime.datetime.now ().strftime ("%Y-%m-%d %H:%M")))
		read_video_intraction = open('Placement_info_vdx_intraction.sql')
		sql_video_player_intraction = read_video_intraction.read().format(self.config.ioid,self.config.start_date,self.config.end_date)
		
		self.logger.info("Start executing: "+'Placement_info_vdx_ad_intraction.sql'+" at "+str (datetime.datetime.now ().strftime ("%Y-%m-%d %H:%M")))
		read_video_ad_intrac = open("Placement_info_vdx_ad_intraction.sql")
		sql_ad_intraction = read_video_ad_intrac.read().format(self.config.ioid,self.config.start_date,self.config.end_date)
		
		self.logger.info("Start executing: "+'Placement_info_vdx_clickthroughs.sql'+" at "+str (datetime.datetime.now ().strftime ("%Y-%m-%d %H:%M")))
		read_video_clickthrough = open("Placement_info_vdx_clickthroughs.sql")
		sql_click_throughs = read_video_clickthrough.read().format(self.config.ioid,self.config.start_date,self.config.end_date)
		
		self.logger.info("Start executing: "+'Placement_info_vdx_day.sql'+" at "+str (datetime.datetime.now ().strftime ("%Y-%m-%d %H:%M")))
		read_vdx_day = open("Placement_info_vdx_day.sql")
		sql_vdx_day_km = read_vdx_day.read().format(self.config.ioid,self.config.start_date,self.config.end_date)
		
		self.logger.info("Start executing: "+'Placement_info_vdx_adsize_intraction_rate.sql'+" at "+str (datetime.datetime.now ().strftime ("%Y-%m-%d %H:%M")))
		read_adsize_km_rate = open("Placement_info_vdx_adsize_intraction_rate.sql")
		sql_adsize_km_rate = read_adsize_km_rate.read().format(self.config.ioid,self.config.start_date,self.config.end_date)
		
		self.logger.info("Start executing: "+'Placement_info_vdx_km.sql'+" at "+str (datetime.datetime.now ().strftime ("%Y-%m-%d %H:%M")))
		read_vdx_km_video = open("Placement_info_vdx_km.sql")
		sql_km_for_video = read_vdx_km_video.read().format(self.config.ioid,self.config.start_date,self.config.end_date)
		
		
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
		
		read_sql_vdx_summary = pd.read_sql (self.sql_vdx_summary, self.config.conn)
		
		read_sql_vdx_km = pd.read_sql (self.sql_vdx_km, self.config.conn)
		
		read_sql_adsize_km = pd.read_sql (self.sql_adsize_km, self.config.conn)
		
		read_sql_video_km = pd.read_sql (self.sql_video_km, self.config.conn)
		
		read_sql_video_player_interaction = pd.read_sql (self.sql_video_player_intraction, self.config.conn)
		
		read_sql_ad_intraction = pd.read_sql (self.sql_ad_intraction, self.config.conn)
		
		read_sql_km_for_video = pd.read_sql (self.sql_km_for_video, self.config.conn)
		
		self.read_sql_vdx_summary = read_sql_vdx_summary
		self.read_sql_vdx_km = read_sql_vdx_km
		self.read_sql_adsize_km = read_sql_adsize_km
		self.read_sql_video_km = read_sql_video_km
		self.read_sql_video_player_interaction = read_sql_video_player_interaction
		self.read_sql_ad_intraction = read_sql_ad_intraction
		self.read_sql_km_for_video = read_sql_km_for_video
		
	def access_vdx_columns(self):
		"""
		Accessing VDX Columns
		:return:
		"""
		self.logger.info ('Query Stored for further processing of IO - {}'.format (self.config.ioid))
		self.logger.info ('Creating placement wise table of IO - {}'.format (self.config.ioid))
		
		placementsummaryfinal = None
		placementadsizefinal = None
		placement_by_video_final = None
		video_player_final = None
		intractions_clicks_new = None
		intractions_intrac_new = None
		
		try:
			if self.read_sql_video_km.empty:
				pass
			else:
				placementvdxs = [self.read_sql_vdx_summary, self.read_sql_vdx_km]
				
				placementvdxsummary = reduce (lambda left, right:pd.merge (left, right, on='PLACEMENT#'), placementvdxs)
				placementvdxsummarynew = placementvdxsummary.loc[:, ["PLACEMENT#", "PLACEMENT_NAME", "COST_TYPE", "PRODUCT",
				                                                     "IMPRESSIONS", "ENGAGEMENTS", "DPEENGAMENTS",
				                                                     "ENGCLICKTHROUGH", "DPECLICKTHROUGH", "VWRCLICKTHROUGH",
				                                                     "ENGTOTALTIMESPENT", "DPETOTALTIMESPENT", "COMPLETIONS",
				                                                     "ENGINTRACTIVEENGAGEMENTS", "DPEINTRACTIVEENGAGEMENTS",
				                                                     "VIEW100", "ENG100", "DPE100"]]
				
				placementvdxsummarynew["Placement# Name"] = placementvdxsummarynew[["PLACEMENT#",
				                                                                    "PLACEMENT_NAME"]].apply (
					lambda x:".".join (x),
					axis=1)
				
				placementvdxsummary = placementvdxsummarynew.loc[:, ["Placement# Name", "COST_TYPE", "PRODUCT",
				                                                     "IMPRESSIONS", "ENGAGEMENTS", "DPEENGAMENTS",
				                                                     "ENGCLICKTHROUGH", "DPECLICKTHROUGH", "VWRCLICKTHROUGH",
				                                                     "ENGTOTALTIMESPENT", "DPETOTALTIMESPENT", "COMPLETIONS",
				                                                     "ENGINTRACTIVEENGAGEMENTS", "DPEINTRACTIVEENGAGEMENTS",
				                                                     "VIEW100", "ENG100", "DPE100"]]
				
				placementvdxsummaryfirst = placementvdxsummary.append (placementvdxsummary.sum (numeric_only=True),
				                                                       ignore_index=True)
				
				placementvdxsummaryfirst["COST_TYPE"] = placementvdxsummaryfirst["COST_TYPE"].fillna ('CPE')
				
				placementvdxsummaryfirst["PRODUCT"] = placementvdxsummaryfirst["PRODUCT"].fillna ('Grand Total')
				
				placementvdxsummaryfirst["Placement# Name"] = placementvdxsummaryfirst["Placement# Name"].fillna ('Grand '
				                                                                                                  'Total')
				
				mask1 = placementvdxsummaryfirst["COST_TYPE"].isin (['CPE+'])
				choicedeepengagement = placementvdxsummaryfirst['DPEENGAMENTS']/placementvdxsummaryfirst['IMPRESSIONS']
				mask2 = placementvdxsummaryfirst["COST_TYPE"].isin (['CPE', 'CPM', 'CPCV'])
				choiceengagements = placementvdxsummaryfirst["ENGAGEMENTS"]/placementvdxsummaryfirst['IMPRESSIONS']
				
				placementvdxsummaryfirst["Engagements Rate"] = np.select ([mask1, mask2],
				                                                          [choicedeepengagement, choiceengagements],
				                                                          default=0.00)
				
				mask3 = placementvdxsummaryfirst["COST_TYPE"].isin (['CPE+', 'CPE', 'CPM', 'CPCV'])
				mask_vdx_first = placementvdxsummaryfirst["COST_TYPE"].isin (['CPCV'])
				choicevwrctr = placementvdxsummaryfirst['VWRCLICKTHROUGH']/placementvdxsummaryfirst['IMPRESSIONS']
				
				placementvdxsummaryfirst["Viewer CTR"] = np.select ([mask3], [choicevwrctr], default=0.00)
				
				choiceengctr = placementvdxsummaryfirst["ENGCLICKTHROUGH"]/placementvdxsummaryfirst["ENGAGEMENTS"]
				choicedeepctr = placementvdxsummaryfirst["DPECLICKTHROUGH"]/placementvdxsummaryfirst["DPEENGAMENTS"]
				placementvdxsummaryfirst["Engager CTR"] = np.select ([mask1, mask2], [choicedeepctr, choiceengctr],
				                                                     default=0.00)
				
				mask4 = placementvdxsummaryfirst["PRODUCT"].isin (['InStream'])
				mask_vdx_first_vwr_vcr = placementvdxsummaryfirst["COST_TYPE"].isin (['CPE+', 'CPE', 'CPM'])
				choicevwrvcr = placementvdxsummaryfirst['VIEW100']/placementvdxsummaryfirst['IMPRESSIONS']
				choicevwrvcr_vdx_first = placementvdxsummaryfirst['COMPLETIONS']/placementvdxsummaryfirst['IMPRESSIONS']
				
				placementvdxsummaryfirst['Viewer VCR'] = np.select ([mask4 & mask_vdx_first_vwr_vcr, mask4 & mask_vdx_first],
				                                                    [choicevwrvcr, choicevwrvcr_vdx_first], default='N/A')
				placementvdxsummaryfirst['Viewer VCR'] = pd.to_numeric (placementvdxsummaryfirst['Viewer VCR'],
				                                                        errors='coerce')
				
				mask5 = placementvdxsummaryfirst["PRODUCT"].isin (['Display', 'Mobile'])
				mask6 = placementvdxsummaryfirst['COST_TYPE'].isin (['CPM', 'CPE'])
				choiceengvcrcpecpm = placementvdxsummaryfirst['ENG100']/placementvdxsummaryfirst['ENGAGEMENTS']
				mask7 = placementvdxsummaryfirst["COST_TYPE"].isin (['CPE+'])
				mask8 = placementvdxsummaryfirst["COST_TYPE"].isin (['CPCV'])
				choiceengvcrcpe_plus = placementvdxsummaryfirst['DPE100']/placementvdxsummaryfirst['DPEENGAMENTS']
				choiceengvcrcpcv = placementvdxsummaryfirst['COMPLETIONS']/placementvdxsummaryfirst['ENGAGEMENTS']
				
				placementvdxsummaryfirst['Engager VCR'] = np.select ([mask5 & mask6, mask5 & mask7, mask5 & mask8],
				                                                     [choiceengvcrcpecpm,
				                                                      choiceengvcrcpe_plus,
				                                                      choiceengvcrcpcv],
				                                                     default='N/A')
				
				
				placementvdxsummaryfirst['Engager VCR'] = pd.to_numeric (placementvdxsummaryfirst['Engager VCR'],
				                                                         errors='coerce')
				
				choiceintratecpe_plus = placementvdxsummaryfirst['DPEINTRACTIVEENGAGEMENTS']/placementvdxsummaryfirst[
					'DPEENGAMENTS']
				choiceintrateotherthancpe_plus = placementvdxsummaryfirst['ENGINTRACTIVEENGAGEMENTS']/\
				                                 placementvdxsummaryfirst['ENGAGEMENTS']
				
				placementvdxsummaryfirst['Interaction Rate'] = np.select ([mask2, mask1], [choiceintrateotherthancpe_plus,
				                                                                           choiceintratecpe_plus],
				                                                          default=0.00)
				
				choiceatscpe_plus = (
					(placementvdxsummaryfirst['DPETOTALTIMESPENT']/placementvdxsummaryfirst['DPEENGAMENTS'])/1000).apply (
					'{0:.2f}'.format)
				choiceatsotherthancpe_plus = (
					(placementvdxsummaryfirst['ENGTOTALTIMESPENT']/placementvdxsummaryfirst['ENGAGEMENTS'])/1000).apply (
					'{0:.2f}'.format)
				
				placementvdxsummaryfirst['Active Time Spent'] = np.select ([mask2, mask1], [choiceatsotherthancpe_plus,
				                                                                            choiceatscpe_plus], default=0.00)
				
				placementvdxsummaryfirst['Active Time Spent'] = placementvdxsummaryfirst['Active Time Spent'].astype (float)
				
				placementvdxsummaryfirstnew = placementvdxsummaryfirst.replace (np.nan, 'N/A', regex=True)
				
			
				placementvdxsummaryfirstnew.loc[placementvdxsummaryfirstnew.index[-1], ["Viewer VCR", "Engager VCR"]] = np.nan
				
				placementsummaryfinalnew = placementvdxsummaryfirstnew.loc[:,
				                           ["Placement# Name", "PRODUCT", "Engagements Rate", "Viewer CTR",
				                            "Engager CTR", "Viewer VCR", "Engager VCR",
				                            "Interaction Rate", "Active Time Spent"]]
				
				placementsummaryfinal = placementsummaryfinalnew.loc[:, ["Placement# Name", "PRODUCT", "Engagements Rate",
				                                                         "Viewer CTR", "Engager CTR", "Viewer VCR",
				                                                         "Engager VCR", "Interaction Rate",
				                                                         "Active Time Spent"]]
		except (AttributeError, KeyError, TypeError, IOError, ValueError) as e:
			self.logger.error(str(e))
			pass
	
		self.logger.info ('Creating adsize wise table of IO - {}'.format (self.config.ioid))
		
		try:
			if self.read_sql_adsize_km.empty:
				pass
			else:
				placementadsize = [self.read_sql_vdx_summary, self.read_sql_adsize_km]
				placementadziesummary = reduce (lambda left, right:pd.merge (left, right, on='PLACEMENT#'), placementadsize)
				placementadsizefirst = placementadziesummary.loc[:,
				                       ["PLACEMENT#", "PLACEMENT_NAME", "COST_TYPE", "PRODUCT", "ADSIZE",
				                        "IMPRESSIONS", "ENGAGEMENTS", "DPEENGAGEMENTS",
				                        "ENGCLICKTHROUGHS", "DPECLICKTHROUGHS", "VWRCLICKTHROUGHS",
				                        "VIEW100", "ENG100", "DPE100", "ENGTOTALTIMESPENT",
				                        "DPETOTALTIMESPENT", "ENGINTRACTIVEENGAGEMENTS",
				                        "COMPLETIONS", "DPEINTRACTIVEENGAGEMENTS"]]
				
				placementadsizefirst["Placement# Name"] = placementadsizefirst[["PLACEMENT#",
				                                                                "PLACEMENT_NAME"]].apply (lambda x:".".join
				(x),
				                                                                                          axis=1)
				
				placementadsizetable = placementadsizefirst.loc[:, ["Placement# Name", "COST_TYPE", "PRODUCT", "ADSIZE",
				                                                    "IMPRESSIONS", "ENGAGEMENTS", "DPEENGAGEMENTS",
				                                                    "ENGCLICKTHROUGHS", "DPECLICKTHROUGHS", "VWRCLICKTHROUGHS",
				                                                    "VIEW100", "ENG100", "DPE100", "ENGTOTALTIMESPENT",
				                                                    "DPETOTALTIMESPENT", "ENGINTRACTIVEENGAGEMENTS",
				                                                    "COMPLETIONS", "DPEINTRACTIVEENGAGEMENTS"]]
				
				placementadsizegrouping = pd.pivot_table (placementadsizetable,
				                                          index=['Placement# Name', 'ADSIZE', 'COST_TYPE'],
				                                          values=["IMPRESSIONS", "ENGAGEMENTS", "DPEENGAGEMENTS",
				                                                  "ENGCLICKTHROUGHS", "DPECLICKTHROUGHS", "VWRCLICKTHROUGHS",
				                                                  "VIEW100", "ENG100", "DPE100", "ENGTOTALTIMESPENT",
				                                                  "DPETOTALTIMESPENT",
				                                                  "ENGINTRACTIVEENGAGEMENTS",
				                                                  "COMPLETIONS",
				                                                  "DPEINTRACTIVEENGAGEMENTS"], aggfunc=np.sum)
				
				placementadsizegroupingnew = placementadsizegrouping.reset_index ()
				
				placementadsizegroup = placementadsizegroupingnew.loc[:, :]
				
				placementadsizegroup = placementadsizegroup.append (placementadsizegroup.sum (numeric_only=True),
				                                                    ignore_index=True)
				
				placementadsizegroup["COST_TYPE"] = placementadsizegroup["COST_TYPE"].fillna ('CPE')
				placementadsizegroup["ADSIZE"] = placementadsizegroup["ADSIZE"].fillna ('Grand Total')
				placementadsizegroup["Placement# Name"] = placementadsizegroup["Placement# Name"].fillna ('Grand Total')
				
				mask9 = placementadsizegroup["COST_TYPE"].isin (["CPE+"])
				choiceadsizeengagementcpe_plus = placementadsizegroup["DPEENGAGEMENTS"]/placementadsizegroup["IMPRESSIONS"]
				mask10 = placementadsizegroup["COST_TYPE"].isin (["CPE", "CPM", "CPCV"])
				choiceplacementadsizegroupcpe = placementadsizegroup["ENGAGEMENTS"]/placementadsizegroup["IMPRESSIONS"]
				placementadsizegroup["Engagements Rate"] = np.select ([mask9, mask10], [choiceadsizeengagementcpe_plus,
				                                                                        choiceplacementadsizegroupcpe],
				                                                      default=0.00)
				
				mask11 = placementadsizegroup["COST_TYPE"].isin (["CPE", "CPM", "CPCV", "CPE+"])
				choiceadsizeengagementvwrctr = placementadsizegroup["VWRCLICKTHROUGHS"]/placementadsizegroup["IMPRESSIONS"]
				
				placementadsizegroup["Viewer CTR"] = np.select ([mask11], [choiceadsizeengagementvwrctr], default=0.00)
				
				choiceadsizeengagementengctr = placementadsizegroup["ENGCLICKTHROUGHS"]/placementadsizegroup["ENGAGEMENTS"]
				choiceadsizeengagementdpegctr = placementadsizegroup["DPECLICKTHROUGHS"]/placementadsizegroup[
					"DPEENGAGEMENTS"]
				
				placementadsizegroup["Engager CTR"] = np.select ([mask10, mask9], [choiceadsizeengagementengctr,
				                                                                   choiceadsizeengagementdpegctr],
				                                                 default=0.00)
				
				mask12 = placementadsizegroup["ADSIZE"].isin (["1x10"])
				mask13 = placementadsizegroup["COST_TYPE"].isin (["CPE", "CPE+", "CPM"])
				mask14 = placementadsizegroup["COST_TYPE"].isin (["CPCV"])
				choiceadsizevwrvcrcpe = placementadsizegroup["VIEW100"]/placementadsizegroup["IMPRESSIONS"]
				choiceadsizevwrvcrcpcv = placementadsizegroup["COMPLETIONS"]/placementadsizegroup["IMPRESSIONS"]
				placementadsizegroup["Viewer VCR"] = np.select ([mask12 & mask13, mask12 & mask14], [choiceadsizevwrvcrcpe,
				                                                                                     choiceadsizevwrvcrcpcv],
				                                                default='N/A')
				
				placementadsizegroup['Viewer VCR'] = pd.to_numeric (placementadsizegroup['Viewer VCR'], errors='coerce')
				
				mask15 = ~placementadsizegroup["ADSIZE"].isin (["1x10"])
				mask16 = placementadsizegroup["COST_TYPE"].isin (["CPE", "CPM"])
				choiceadsizeengvcrcpe = placementadsizegroup["ENG100"]/placementadsizegroup["ENGAGEMENTS"]
				choiceadsizeengvcrcpe_plus = placementadsizegroup["DPE100"]/placementadsizegroup["DPEENGAGEMENTS"]
				choiceadsizeengvcrcpcv = placementadsizegroup["COMPLETIONS"]/placementadsizegroup["ENGAGEMENTS"]
				
				placementadsizegroup["Engager VCR"] = np.select ([mask15 & mask16, mask15 & mask9, mask15 & mask14],
				                                                 [choiceadsizeengvcrcpe, choiceadsizeengvcrcpe_plus,
				                                                  choiceadsizeengvcrcpcv], default='N/A')
				
				placementadsizegroup['Engager VCR'] = pd.to_numeric (placementadsizegroup['Engager VCR'],
				                                                     errors='coerce')
				
				choiceadsizeinteracratecpe = placementadsizegroup["ENGINTRACTIVEENGAGEMENTS"]/placementadsizegroup[
					"ENGAGEMENTS"]
				choiceadsizeinteracratecpe_plus = placementadsizegroup["DPEINTRACTIVEENGAGEMENTS"]/placementadsizegroup[
					"DPEENGAGEMENTS"]
				
				placementadsizegroup["Interaction Rate"] = np.select ([mask10, mask9], [choiceadsizeinteracratecpe,
				                                                                        choiceadsizeinteracratecpe_plus],
				                                                      default=0.00)
				
				choiceadsizeatscpe = (
					(placementadsizegroup["ENGTOTALTIMESPENT"]/placementadsizegroup["ENGAGEMENTS"])/1000).apply (
					'{0:.2f}'.format)
				choiceadsizeatscpe_plus = (
					(placementadsizegroup["DPETOTALTIMESPENT"]/placementadsizegroup["DPEENGAGEMENTS"])/1000).apply (
					'{0:.2f}'.format)
				
				placementadsizegroup["Active Time Spent"] = np.select ([mask10, mask14],
				                                                       [choiceadsizeatscpe, choiceadsizeatscpe_plus],
				                                                       default=0.00)
				
				placementadsizegroup['Active Time Spent'] = placementadsizegroup['Active Time Spent'].astype (float)
				
				placementadsizegroupfirstnew = placementadsizegroup.replace (np.nan, 'N/A', regex=True)
				
				placementadsizegroupfirstnew.loc[placementadsizegroupfirstnew.index[-1], ["Viewer VCR", "Engager VCR"]] = \
					np.nan
				
				placementadsizefinal = placementadsizegroupfirstnew.loc[:, ["Placement# Name", "ADSIZE", "Engagements Rate",
				                                                            "Viewer CTR", "Engager CTR", "Viewer VCR",
				                                                            "Engager VCR",
				                                                            "Interaction Rate", "Active Time Spent"]]
		except (AttributeError, KeyError, TypeError, IOError, ValueError) as e:
			self.logger.error(str(e))
			pass
		
		self.logger.info ('Creating Video wise table of IO - {}'.format (self.config.ioid))
		
		try:
			if self.read_sql_video_km.empty:
				pass
			else:
				placement_video = [self.read_sql_vdx_summary, self.read_sql_video_km]
				placement_video_summary = reduce (lambda left, right:pd.merge (left, right, on='PLACEMENT#'), placement_video)
				
				placement_by_video = placement_video_summary.loc[:, ["PLACEMENT#", "PLACEMENT_NAME", "COST_TYPE", "PRODUCT",
				                                                     "VIDEONAME", "VIEW0", "VIEW25", "VIEW50", "VIEW75",
				                                                     "VIEW100",
				                                                     "ENG0", "ENG25", "ENG50", "ENG75", "ENG100", "DPE0",
				                                                     "DPE25",
				                                                     "DPE50", "DPE75", "DPE100"]]
				
				placement_by_video["Placement# Name"] = placement_by_video[["PLACEMENT#",
				                                                            "PLACEMENT_NAME"]].apply (lambda x:".".join (x),
				                                                                                      axis=1)
				
				placement_by_video_new = placement_by_video.loc[:,
				                         ["PLACEMENT#", "Placement# Name", "COST_TYPE", "PRODUCT", "VIDEONAME",
				                          "VIEW0", "VIEW25", "VIEW50", "VIEW75", "VIEW100",
				                          "ENG0", "ENG25", "ENG50", "ENG75", "ENG100", "DPE0", "DPE25",
				                          "DPE50", "DPE75", "DPE100"]]
				
				placement_by_km_video = [placement_by_video_new, self.read_sql_km_for_video]
				placement_by_km_video_summary = reduce (lambda left, right:pd.merge (left, right, on=['PLACEMENT#', 'PRODUCT']),
				                                        placement_by_km_video)
				
				"""Conditions for 25%view"""
				mask17 = placement_by_km_video_summary["PRODUCT"].isin (['Display', 'Mobile'])
				mask18 = placement_by_km_video_summary["COST_TYPE"].isin (["CPE", "CPM", "CPCV"])
				mask19 = placement_by_km_video_summary["PRODUCT"].isin (["InStream"])
				mask20 = placement_by_km_video_summary["COST_TYPE"].isin (["CPE", "CPM", "CPE+", "CPCV"])
				mask_video_video_completions = placement_by_km_video_summary["COST_TYPE"].isin (["CPCV"])
				mask21 = placement_by_km_video_summary["COST_TYPE"].isin (["CPE+"])
				mask22 = placement_by_km_video_summary["COST_TYPE"].isin (["CPE", "CPM"])
				mask23 = placement_by_km_video_summary["PRODUCT"].isin (['Display', 'Mobile', 'InStream'])
				mask24 = placement_by_km_video_summary["COST_TYPE"].isin (["CPE", "CPM", "CPE+"])
				
				choice25video_eng = placement_by_km_video_summary["ENG25"]
				choice25video_vwr = placement_by_km_video_summary["VIEW25"]
				choice25video_deep = placement_by_km_video_summary["DPE25"]
				
				placement_by_km_video_summary["25_pc_video"] = np.select ([mask17 & mask18, mask19 & mask20, mask17 & mask21],
				                                                          [choice25video_eng, choice25video_vwr,
				                                                           choice25video_deep])
				
				"""Conditions for 50%view"""
				choice50video_eng = placement_by_km_video_summary["ENG50"]
				choice50video_vwr = placement_by_km_video_summary["VIEW50"]
				choice50video_deep = placement_by_km_video_summary["DPE50"]
				
				placement_by_km_video_summary["50_pc_video"] = np.select ([mask17 & mask18, mask19 & mask20, mask17 & mask21],
				                                                          [choice50video_eng,
				                                                           choice50video_vwr, choice50video_deep])
				
				"""Conditions for 75%view"""
				
				choice75video_eng = placement_by_km_video_summary["ENG75"]
				choice75video_vwr = placement_by_km_video_summary["VIEW75"]
				choice75video_deep = placement_by_km_video_summary["DPE75"]
				
				placement_by_km_video_summary["75_pc_video"] = np.select ([mask17 & mask18, mask19 & mask20, mask17 & mask21],
				                                                          [choice75video_eng,
				                                                           choice75video_vwr,
				                                                           choice75video_deep])
				
				"""Conditions for 100%view"""
				
				choice100video_eng = placement_by_km_video_summary["ENG100"]
				choice100video_vwr = placement_by_km_video_summary["VIEW100"]
				choice100video_deep = placement_by_km_video_summary["DPE100"]
				choicecompletions = placement_by_km_video_summary['COMPLETIONS']
				
				placement_by_km_video_summary["100_pc_video"] = np.select (
					[mask17 & mask22, mask19 & mask24, mask17 & mask21, mask23 & mask_video_video_completions],
					[choice100video_eng, choice100video_vwr, choice100video_deep, choicecompletions])
				
				"""conditions for 0%view"""
				
				choice0video_eng = placement_by_km_video_summary["ENG0"]
				choice0video_vwr = placement_by_km_video_summary["VIEW0"]
				choice0video_deep = placement_by_km_video_summary["DPE0"]
				
				placement_by_km_video_summary["Views"] = np.select ([mask17 & mask18, mask19 & mask20, mask17 & mask21],
				                                                    [choice0video_eng,
				                                                     choice0video_vwr,
				                                                     choice0video_deep])
				
				placement_by_video_summary = placement_by_km_video_summary.loc[:,
				                             ["PLACEMENT#", "Placement# Name", "PRODUCT", "VIDEONAME", "COST_TYPE",
				                              "Views", "25_pc_video", "50_pc_video", "75_pc_video", "100_pc_video",
				                              "ENGAGEMENTS", "IMPRESSIONS", "DPEENGAMENTS"]]
				
				"""adding views based on conditions"""
				
				placement_by_video_summary_new = placement_by_km_video_summary.loc[
					placement_by_km_video_summary.reset_index ().groupby (['PLACEMENT#', 'PRODUCT'])['Views'].idxmax ()]
				
				placement_by_video_summary_new.loc[mask17 & mask18, 'Views'] = placement_by_video_summary_new['ENGAGEMENTS']
				placement_by_video_summary_new.loc[mask19 & mask20, 'Views'] = placement_by_video_summary_new['IMPRESSIONS']
				placement_by_video_summary_new.loc[mask17 & mask21, 'Views'] = placement_by_video_summary_new['DPEENGAMENTS']
				
				placement_by_video_summary = placement_by_video_summary.drop (placement_by_video_summary_new.index).append (
					placement_by_video_summary_new).sort_index ()
				
				placement_by_video_summary["Video Completion Rate"] = placement_by_video_summary["100_pc_video"]/\
				                                                      placement_by_video_summary["Views"]
				
				placement_by_video_final = placement_by_video_summary.loc[:,
				                           ["Placement# Name", "PRODUCT", "VIDEONAME", "Views",
				                            "25_pc_video", "50_pc_video", "75_pc_video", "100_pc_video",
				                            "Video Completion Rate"]]
		except (AttributeError, KeyError, TypeError, IOError, ValueError) as e:
			self.logger.error(str(e))
			pass
		
		self.logger.info ('Creating Intraction wise table of IO - {}'.format (self.config.ioid))
		try:
			if self.read_sql_video_player_interaction.empty:
				pass
			else:
				video_player = self.read_sql_video_player_interaction.loc[:,
				               ["PRODUCT", "VWRMUTE", "VWRUNMUTE", "VWRPAUSE", "VWRREWIND",
				                "VWRRESUME", "VWRREPLAY", "VWRFULLSCREEN", "ENGMUTE",
				                "ENGUNMUTE", "ENGPAUSE", "ENGREWIND", "ENGRESUME", "ENGREPLAY",
				                "ENGFULLSCREEN"]]
				
				mask22 = video_player["PRODUCT"].isin (['Display', 'Mobile'])
				mask23 = video_player["PRODUCT"].isin (['InStream'])
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
				
				video_player["Mute"] = np.select ([mask22, mask23], [choice_intraction_mute, choice_interaction_ins_mute])
				video_player["Unmute"] = np.select ([mask22, mask23], [choice_intraction_unmute,
				                                                       choice_interaction_ins_unmute])
				video_player["Pause"] = np.select ([mask22, mask23], [choice_intraction_pause, choice_interaction_ins_pause])
				video_player["Rewind"] = np.select ([mask22, mask23], [choice_intraction_rewind,
				                                                       choice_interaction_ins_rewind])
				video_player["Resume"] = np.select ([mask22, mask23], [choice_intraction_resume,
				                                                       choice_interaction_ins_resume])
				video_player["Replay"] = np.select ([mask22, mask23], [choice_intraction_replay,
				                                                       choice_interaction_ins_replay])
				video_player["Fullscreen"] = np.select ([mask22, mask23],
				                                        [choice_intraction_fullscreen, choice_interaction_ins_fullscreen])
				
				video_player.rename (columns={"PRODUCT":"Product"}, inplace=True)
				
				video_player_final = video_player.loc[:,
				                     ["Product", "Mute", "Unmute", "Pause", "Rewind", "Resume", "Replay", "Fullscreen"]]
		except (AttributeError, KeyError, TypeError, IOError, ValueError) as e:
			self.logger.error(str(e))
			pass
		
		try:
			if self.read_sql_ad_intraction.empty:
				pass
			else:
				intractions_click_ad = pd.pivot_table (self.read_sql_ad_intraction, index="PRODUCT", values="CLICKTHRU",
				                                       columns="BLAZE_TAG_NAME_DESC",
				                                       aggfunc=np.sum, fill_value=0)
				
				intractions_click_ad_new = intractions_click_ad.reset_index ()
				intractions_clicks = intractions_click_ad_new.loc[:, :]
				
				cols_drop = ["PRODUCT"]
				intractions_clicks_new = intractions_clicks.drop (cols_drop, axis=1)
				
				intractions_intrac_ad = pd.pivot_table (self.read_sql_ad_intraction, index="PRODUCT", values="INTERACTION",
				                                        columns="BLAZE_TAG_NAME_DESC", aggfunc=np.sum, fill_value=0)
				
				intractions_intrac_ad_new = intractions_intrac_ad.reset_index ()
				intractions_intrac = intractions_intrac_ad_new.loc[:, :]
				intractions_intrac_new = intractions_intrac.drop (cols_drop, axis=1)
		
		except (AttributeError, KeyError, TypeError, IOError, ValueError) as e:
			self.logger.error(str(e))
			pass
		
		self.placementsummaryfinal = placementsummaryfinal
		#self.unique_plc_summary = unique_plc_summary
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
		unique_plc_summary = self.placementsummaryfinal['Placement# Name'].nunique ()
		self.unique_plc_summary = unique_plc_summary
		self.logger.info ('writing data to excel of IO - {}'.format (self.config.ioid))
		#print("Dha",self.intractions_clicks_new)
		#print("hhh",self.intractions_intrac_new)
		
		try:
			info_client = self.config.client_info.to_excel (self.config.writer, sheet_name="VDX Details",
			                                                startcol=1, startrow=1, index=True, header=False)
			info_campaign = self.config.campaign_info.to_excel (self.config.writer, sheet_name="VDX Details",
			                                                    startcol=1, startrow=2, index=True, header=False)
			info_ac_mgr = self.config.ac_mgr.to_excel (self.config.writer, sheet_name="VDX Details", startcol=4,
			                                           startrow=1, index=True, header=False)
			info_sales_rep = self.config.sales_rep.to_excel (self.config.writer, sheet_name="VDX Details",
			                                                 startcol=4, startrow=2, index=True, header=False)
			info_campaign_date = self.config.sdate_edate_final.to_excel (self.config.writer,
			                                                             sheet_name="VDX Details", startcol=7,
			                                                             startrow=1, index=True, header=False)
			info_agency = self.config.agency_info.to_excel (self.config.writer, sheet_name="VDX Details",
			                                                startcol=1, startrow=3, index=True, header=False)
			info_currency = self.config.currency_info.to_excel (self.config.writer, sheet_name="VDX Details",
			                                                    startcol=7, startrow=3, index=True, header=False)
			
		except (AttributeError, KeyError, TypeError, IOError, ValueError) as e:
			self.logger.error(str(e))
			pass
		
		startline_placement = 9
		try:
			if self.read_sql_vdx_km.empty:
				pass
			else:
				for placement, placement_df in self.placementsummaryfinal.groupby ('Placement# Name'):
					
					write_pl = placement_df.to_excel (self.config.writer, sheet_name="VDX Details".format (self.config.ioid),
					                                  startcol=1, startrow=startline_placement, columns=["Placement# Name"],
					                                  header=False, index=False)
					
					if placement_df.iloc[0, 0]!="Grand Total":
						startline_placement += 1
					
					write_pls = placement_df.to_excel (self.config.writer, sheet_name="VDX Details".format (self.config.ioid),
					                                   startcol=1, startrow=startline_placement, columns=["PRODUCT",
					                                                                                      "Engagements Rate",
					                                                                                      "Viewer CTR",
					                                                                                      "Engager CTR",
					                                                                                      "Viewer VCR",
					                                                                                      "Engager VCR",
					                                                                                      "Interaction Rate",
					                                                                                      "Active Time Spent"],
					                                   header=False, index=False)
					
					startline_placement += len (placement_df)+1
				
		except (AttributeError, KeyError, TypeError, IOError, ValueError) as e:
			self.logger.error(str(e))
			pass
		
		startline_adsize = 9+len (self.placementsummaryfinal)+self.unique_plc_summary*2+3
		try:
			
			if self.read_sql_adsize_km.empty:
				pass
			else:
				for adzise, adsize_df in self.placementadsizefinal.groupby ('Placement# Name'):
					
					write_adsize_plc = adsize_df.to_excel (self.config.writer,
					                                       sheet_name="VDX Details".format (self.config.ioid),
					                                       startcol=1, startrow=startline_adsize, columns=["Placement# Name"],
					                                       header=False, index=False)
					
					if adsize_df.iloc[0, 0]!="Grand Total":
						startline_adsize += 1
					
					write_adsize = adsize_df.to_excel (self.config.writer, sheet_name="VDX Details".format (self.config.ioid),
					                                   startcol=1, startrow=startline_adsize,
					                                   columns=["ADSIZE", "Engagements Rate",
					                                            "Viewer CTR", "Engager CTR",
					                                            "Viewer VCR", "Engager VCR",
					                                            "Interaction Rate", "Active Time Spent"],
					                                   header=False, index=False)
					
					startline_adsize += len (adsize_df)+1
		except (AttributeError, KeyError, TypeError, IOError, ValueError) as e:
			self.logger.error(str(e))
			pass
			
		startline_video = 9+len (self.placementsummaryfinal)+self.unique_plc_summary*2+3+len (
			self.placementadsizefinal)+self.unique_plc_summary*2+3
		try:
			
			
			if self.read_sql_video_km.empty:
				pass
			else:
				for video, video_df in self.placement_by_video_final.groupby ('Placement# Name'):
					write_video_plc = video_df.to_excel (self.config.writer, sheet_name="VDX Details".format (
						self.config.ioid),
					                                     startcol=1, startrow=startline_video, columns=["Placement# Name"],
					                                     header=False, index=False)
					
					write_video = video_df.to_excel (self.config.writer, sheet_name="VDX Details".format (self.config.ioid),
					                                 startcol=1, startrow=startline_video+1, columns=["PRODUCT", "VIDEONAME",
					                                                                                  "Views", "25_pc_video",
					                                                                                  "50_pc_video",
					                                                                                  "75_pc_video",
					                                                                                  "100_pc_video",
					                                                                                  "Video Completion Rate"],
					                                 header=False, index=False)
					startline_video += len (video_df)+2
		
		except (AttributeError, KeyError, TypeError, IOError, ValueError) as e:
			self.logger.error(str(e))
			pass
		
		startline_player = 9+len (self.placementsummaryfinal)+self.unique_plc_summary*2+3+len (
			self.placementadsizefinal)+self.unique_plc_summary*2+3+len (
			self.placement_by_video_final)+self.unique_plc_summary*2+3
		try:
			if self.read_sql_video_player_interaction.empty:
				pass
			else:
				write_player_interaction = self.video_player_final.to_excel (self.config.writer,
				                                                             sheet_name="VDX Details".format (
					                                                             self.config.ioid),
				                                                             startcol=1, startrow=startline_player,
				                                                             index=False)
		except (AttributeError, KeyError, TypeError, IOError, ValueError) as e:
			self.logger.error(str(e))
			pass
		
		
		try:
			if self.read_sql_ad_intraction.empty:
				pass
			else:
				write_intraction_clicks = self.intractions_clicks_new.to_excel (self.config.writer,
				                                                                sheet_name="VDX Details".format(self.config.ioid),
				                                                                startcol=9, startrow=startline_player,
				                                                                index=False, merge_cells=False)
				
				write_intraction = self.intractions_intrac_new.to_excel (self.config.writer,
				                                                         sheet_name="VDX Details".format(self.config.ioid),
				                                                         startcol=9+self.intractions_clicks_new.shape[1],
				                                                         startrow=startline_player, index=False,
				                                                         merge_cells=False)
		except (AttributeError, KeyError, TypeError, IOError, ValueError) as e:
			self.logger.error(str(e))
			pass
			
	def formatting_Video(self):
		"""
		Formmating
		:return:
		"""
		self.logger.info ('Applying formatting in VDX sheet - {}'.format (self.config.ioid))
		
		try:
			workbook = self.config.writer.book
			worksheet = self.config.writer.sheets["VDX Details".format (self.config.ioid)]
			worksheet.hide_gridlines (2)
			worksheet.set_row (0, 6)
			worksheet.set_column ("A:A", 2)
			worksheet.set_zoom (75)
			worksheet.insert_image ("O6", "Exponential.png", {"url":"https://www.tribalfusion.com"})
			worksheet.insert_image ("O2", "Client_Logo.png")
			worksheet.write_string (2, 8, self.config.status)
			worksheet.write_string (2, 7, "Campaign Status")
			#worksheet.write_string (3, 1, "Agency Name")
			#worksheet.write_string (3, 7, "Currency")
			number_cols_plc_summary = self.placementsummaryfinal.shape[1]
			number_cols_adsize = self.placementadsizefinal.shape[1]
			number_cols_video = self.placement_by_video_final.shape[1]
		
			format_hearder_right = workbook.add_format ({"bold":True, "bg_color":'#00B0F0', "align":"right"})
			format_hearder_left = workbook.add_format ({"bold":True, "bg_color":'#00B0F0', "align":"left"})
			format_colour = workbook.add_format ({"bg_color":'#00B0F0'})
			format_campaign_info = workbook.add_format ({"bold":True, "bg_color":'#00B0F0', "align":"left"})
			format_grand_total = workbook.add_format ({"bold":True, "bg_color":"#A5A5A5", "num_format":"#,##0"})
			format_grand = workbook.add_format ({"bold":True, "bg_color":"#A5A5A5"})
			
			worksheet.conditional_format ("A1:R5", {"type":"blanks", "format":format_campaign_info})
			
			worksheet.conditional_format ("A1:R5", {"type":"no_blanks", "format":format_campaign_info})
			
			worksheet.write_string (7, 1, "VDX Performance KPIs - by Placement and Platform",
			                        format_hearder_left)
			
			worksheet.write_string (8, 1, "Unit", format_hearder_left)
			worksheet.write_string (8, 2, "Engagement Rate", format_hearder_right)
			worksheet.write_string (8, 3, "Viewer CTR", format_hearder_right)
			worksheet.write_string (8, 4, "Engager CTR", format_hearder_right)
			worksheet.write_string (8, 5, "Viewer VCR (Primary Video)", format_hearder_right)
			worksheet.write_string (8, 6, "Engager VCR (Primary Video)", format_hearder_right)
			worksheet.write_string (8, 7, "Interaction Rate", format_hearder_right)
			worksheet.write_string (8, 8, "Active Time Spent", format_hearder_right)
			worksheet.conditional_format (7, 1, 7, number_cols_plc_summary-1, {"type":"blanks", "format":format_colour})
			
			percent_fmt = workbook.add_format ({"num_format":"0.00%", "align":"right"})
			
			grand_fmt = workbook.add_format ({"num_format":"0.00%", "bg_color":'#A5A5A5', "bold":True})
			
			ats_format = workbook.add_format ({"bg_color":'#A5A5A5', "bold":True})
			
			format_num = workbook.add_format ({"num_format":"#,##0"})
			
			worksheet.write_string (9+len (self.placementsummaryfinal)+self.unique_plc_summary*2+1, 1,
			                        "Ad Size Breakdown",
			                        format_hearder_left)
			worksheet.write_string (9+len (self.placementsummaryfinal)+self.unique_plc_summary*2+2, 1, "Ad Size",
			                        format_hearder_left)
			worksheet.write_string (9+len (self.placementsummaryfinal)+self.unique_plc_summary*2+2, 2,
			                        "Engagement Rate",
			                        format_hearder_right)
			worksheet.write_string (9+len (self.placementsummaryfinal)+self.unique_plc_summary*2+2, 3, "Viewer CTR",
			                        format_hearder_right)
			worksheet.write_string (9+len (self.placementsummaryfinal)+self.unique_plc_summary*2+2, 4, "Engager CTR",
			                        format_hearder_right)
			worksheet.write_string (9+len (self.placementsummaryfinal)+self.unique_plc_summary*2+2, 5,
			                        "Viewer VCR (Primary Video)",
			                        format_hearder_right)
			
			worksheet.write_string (9+len (self.placementsummaryfinal)+self.unique_plc_summary*2+2, 6,
			                        "Engager VCR (Primary Video)", format_hearder_right)
			
			worksheet.write_string (9+len (self.placementsummaryfinal)+self.unique_plc_summary*2+2, 7,
			                        "Interaction Rate",
			                        format_hearder_right)
			
			worksheet.write_string (9+len (self.placementsummaryfinal)+self.unique_plc_summary*2+2, 8,
			                        "Active Time Spent", format_hearder_right)
			
			worksheet.conditional_format (9+len (self.placementsummaryfinal)+self.unique_plc_summary*2+1, 1,
			                              9+len (self.placementsummaryfinal)+self.unique_plc_summary*2+1,
			                              number_cols_adsize-1, {
				                              "type":"blanks",
				                              "format":format_colour
				                              })
			
			worksheet.write_string (9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
			                        self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3,
			                        1, "Video Details", format_hearder_left)
			
			worksheet.write_string (9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
			                        self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+4, 1, "Unit",
			                        format_hearder_left)
			
			worksheet.write_string (9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
			                        self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+4, 2, "Video Name",
			                        format_hearder_right)
			
			worksheet.write_string (9+self.placementsummaryfinal.shape[0]+
			                        self.unique_plc_summary*2+1+self.placementadsizefinal.shape[
				                        0]+self.unique_plc_summary*2+4,
			                        3, "Views", format_hearder_right)
			
			worksheet.write_string (9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
			                        self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+4, 4, "25% View",
			                        format_hearder_right)
			
			worksheet.write_string (9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
			                        self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+4, 5, "50% View",
			                        format_hearder_right)
			
			worksheet.write_string (9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
			                        self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+4, 6, "75% View",
			                        format_hearder_right)
			
			worksheet.write_string (9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
			                        self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+4, 7,
			                        "Video Completion",
			                        format_hearder_right)
			
			worksheet.write_string (9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
			                        self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+4, 8,
			                        "Video Completion Rate",
			                        format_hearder_right)
			
			worksheet.conditional_format (9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
			                              self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3, 1,
			                              9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
			                              self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3,
			                              number_cols_video-1,
			                              {"type":"blanks", "format":format_colour})
			
			worksheet.write_string (9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
			                        self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3+
			                        self.placement_by_video_final.shape[0]+self.unique_plc_summary*2+3,
			                        1, "Interaction Details",
			                        format_hearder_left)
			
			worksheet.conditional_format (9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
			                              self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3+
			                              self.placement_by_video_final.shape[0]+self.unique_plc_summary*2+3, 1,
			                              9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
			                              self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3+
			                              self.placement_by_video_final.shape[0]+self.unique_plc_summary*2+3,
			                              9+self.intractions_clicks_new.shape[1]+self.intractions_intrac_new.shape[1],
			                              {"type":"blanks", "format":format_colour})
			
			worksheet.write_string (9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
			                        self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3+
			                        self.placement_by_video_final.shape[0]+self.unique_plc_summary*2+4, 2,
			                        "Video Player Interactions",
			                        format_hearder_right)
			
			worksheet.conditional_format (9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
			                              self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3+
			                              self.placement_by_video_final.shape[0]+self.unique_plc_summary*2+4, 1,
			                              9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
			                              self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3+
			                              self.placement_by_video_final.shape[0]+self.unique_plc_summary*2+4,
			                              9+self.intractions_clicks_new.shape[1]+self.intractions_intrac_new.shape[1],
			                              {
				                              "type":"blanks",
				                              "format":format_colour
				                              })
			
			worksheet.write_string (9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
			                        self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3+
			                        self.placement_by_video_final.shape[0]+self.unique_plc_summary*2+4, 9,
			                        "Clickthroughs", format_hearder_right)
			
			worksheet.write_string (9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
			                        self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3+
			                        self.placement_by_video_final.shape[0]+self.unique_plc_summary*2+4,
			                        9+self.intractions_clicks_new.shape[1], "Ad Interactions", format_hearder_right)
			
			worksheet.write_string (9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
			                        self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3+
			                        self.placement_by_video_final.shape[0]+self.unique_plc_summary*2+5,
			                        9+self.intractions_clicks_new.shape[1]+self.intractions_intrac_new.shape[1],
			                        "Total Interactions", format_hearder_right)
			
			worksheet.write_string (9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
			                        self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3+
			                        self.placement_by_video_final.shape[0]+self.unique_plc_summary*2+5+
			                        self.video_player_final.shape[0]+1,
			                        1, "Grand Total", format_grand)
			
			
			
			for col in range (2, 9+self.intractions_clicks_new.shape[1]+self.intractions_intrac_new.shape[1]+1):
				cell_location = xl_rowcol_to_cell (
					9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
					self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3+
					self.placement_by_video_final.shape[0]+self.unique_plc_summary*2+5+
					self.video_player_final.shape[0]+1
					, col)
				start_range = xl_rowcol_to_cell (9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
				                                 self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3+
				                                 self.placement_by_video_final.shape[0]+self.unique_plc_summary*2+6,
				                                 col)
				
				end_range = xl_rowcol_to_cell (9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
				                               self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3+
				                               self.placement_by_video_final.shape[0]+self.unique_plc_summary*2+5+
				                               self.video_player_final.shape[0], col)
				
				formula = '=sum({:s}:{:s})'.format (start_range, end_range)
				
				worksheet.write_formula (cell_location, formula, format_grand_total)
			
			start_range_x = 9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+\
			                self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3+\
			                self.placement_by_video_final.shape[0]+self.unique_plc_summary*2+6
			
			for row in range (self.video_player_final.shape[0]):

				cell_range = xl_range (start_range_x, 2, start_range_x,
				                       9+self.intractions_clicks_new.shape[1]+self.intractions_intrac_new.shape[1]-1)
				formula = 'sum({:s})'.format (cell_range)
				worksheet.write_formula (start_range_x,
				                         9+self.intractions_clicks_new.shape[1]+self.intractions_intrac_new.shape[1],
				                         formula, format_num)
				start_range_x += 1
			
			worksheet.conditional_format (9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
			                              self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3+
			                              self.placement_by_video_final.shape[0]+self.unique_plc_summary*2+5, 1,
			                              9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
			                              self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3+
			                              self.placement_by_video_final.shape[0]+self.unique_plc_summary*2+5,
			                              9+self.intractions_clicks_new.shape[1]+self.intractions_intrac_new.shape[1],
			                              {"type":"no_blanks", "format":format_hearder_left})
			
			worksheet.conditional_format (9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
			                              self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3+
			                              self.placement_by_video_final.shape[0]+self.unique_plc_summary*2+5, 1,
			                              9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
			                              self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3+
			                              self.placement_by_video_final.shape[0]+self.unique_plc_summary*2+5,
			                              9+self.intractions_clicks_new.shape[1]+self.intractions_intrac_new.shape[1],
			                              {"type":"blanks", "format":format_hearder_left})
			
			
			worksheet.conditional_format (9+len (self.placementsummaryfinal)+self.unique_plc_summary*2+1-4, 1,
			                              9+len (self.placementsummaryfinal)+self.unique_plc_summary*2+1-4,
			                              number_cols_plc_summary-2,
			                              {"type":"blanks", "format":grand_fmt})
			
			worksheet.conditional_format (9+len (self.placementsummaryfinal)+self.unique_plc_summary*2+1-4, 1,
			                              9+len (self.placementsummaryfinal)+self.unique_plc_summary*2+1-4,
			                              number_cols_plc_summary-2, {"type":"no_blanks", "format":grand_fmt})
			
			worksheet.conditional_format (9+len (self.placementsummaryfinal)+self.unique_plc_summary*2+1-4,
			                              number_cols_plc_summary-1,
			                              9+len (self.placementsummaryfinal)+self.unique_plc_summary*2+1-4,
			                              number_cols_plc_summary-1,
			                              {"type":"no_blanks", "format":ats_format})
			
			worksheet.conditional_format (9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
			                              self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3-4, 1,
			                              9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
			                              self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3-4,
			                              number_cols_adsize-2,
			                              {"type":"blanks", "format":grand_fmt})
			
			worksheet.conditional_format (9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
			                              self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3-4, 1,
			                              9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
			                              self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3-4,
			                              number_cols_adsize-2, {"type":"no_blanks", "format":grand_fmt})
			
			worksheet.conditional_format (9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
			                              self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3-4,
			                              number_cols_adsize-1,
			                              9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+
			                              self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3-4,
			                              number_cols_adsize-1, {"type":"no_blanks", "format":ats_format})
			
			
			for col in range (2, number_cols_plc_summary-1):
				start_plc_row = 10
				end_plc_row = 9+len (self.placementsummaryfinal)+self.unique_plc_summary*2+1-6
				worksheet.conditional_format (start_plc_row, col, end_plc_row, col,
				                              {"type":"no_blanks", "format":percent_fmt})
			
			for col in range (2, number_cols_adsize-1):
				start_adsize_row = 9+len (self.placementsummaryfinal)+self.unique_plc_summary*2+4
				end_adsize_row = 9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+\
				                 self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3-6
				worksheet.conditional_format (start_adsize_row, col, end_adsize_row, col,
				                              {"type":"no_blanks", "format":percent_fmt})
			
			for col in range (3, number_cols_video-1):
				start_video_row = 9+len (self.placementsummaryfinal)+self.unique_plc_summary*2+3+len (
					self.placementadsizefinal)+self.unique_plc_summary*2+4
				end_video_row = 9+self.placementsummaryfinal.shape[0]+\
				                self.unique_plc_summary*2+1+self.placementadsizefinal.shape[0]+\
				                self.unique_plc_summary*2+3+self.placement_by_video_final.shape[0]+\
				                self.unique_plc_summary*2+3-4
				worksheet.conditional_format (start_video_row, col, end_video_row, col,
				                              {"type":"no_blanks", "format":format_num})
			
			for col in range (number_cols_video-1, number_cols_video):
				start_video_row_vcr = 9+len (self.placementsummaryfinal)+self.unique_plc_summary*2+3+len (
					self.placementadsizefinal)+self.unique_plc_summary*2+4
				end_video_row_vcr = 9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+\
				                    self.placementadsizefinal.shape[0]+\
				                    self.unique_plc_summary*2+3+self.placement_by_video_final.shape[0]+\
				                    self.unique_plc_summary*2+3-4
				worksheet.conditional_format (start_video_row_vcr, col, end_video_row_vcr, col,
				                              {"type":"no_blanks", "format":percent_fmt})
			
			for col in range (2, 9+self.intractions_clicks_new.shape[1]+self.intractions_intrac_new.shape[1]):
				start_intraction_row = 9+self.placementsummaryfinal.shape[0]+\
				                       self.unique_plc_summary*2+1+self.placementadsizefinal.shape[0]+\
				                       self.unique_plc_summary*2+3+self.placement_by_video_final.shape[
					                       0]+self.unique_plc_summary*2+6
				
				end_intraction_row = 9+self.placementsummaryfinal.shape[0]+self.unique_plc_summary*2+1+\
				                     self.placementadsizefinal.shape[0]+self.unique_plc_summary*2+3+\
				                     self.placement_by_video_final.shape[0]+\
				                     self.unique_plc_summary*2+5+self.video_player_final.shape[0]
				
				worksheet.conditional_format (start_intraction_row, col, end_intraction_row, col,
				                              {"type":"no_blanks", "format":format_num})
				
			
			alignment = workbook.add_format ({"align":"right"})
			
			worksheet.set_column (2, 9+self.intractions_clicks_new.shape[1]+self.intractions_intrac_new.shape[1], 25,
			                      alignment)
			worksheet.set_column ("B:B", 47)
		
		except (AttributeError, KeyError, TypeError, IOError, ValueError) as e:
			self.logger.error(str(e))
			pass
	
	def main(self):
		"""
Main Function
		"""
		self.config.common_columns_summary ()
		self.connect_tfr_video ()
		self.read_query_video ()
		if self.read_sql_vdx_km.empty or self.read_sql_vdx_summary.empty \
				or self.read_sql_adsize_km.empty or self.read_sql_video_km.empty\
				or self.read_sql_video_player_interaction.empty or self.read_sql_ad_intraction.empty\
				or self.read_sql_km_for_video.empty:
			self.logger.info ("No VDX placements for IO - {}".format (self.config.ioid))
			pass
		else:
			self.logger.info ("VDX placements found for IO - {}".format (self.config.ioid))
			self.access_vdx_columns ()
			self.write_video_data ()
			self.formatting_Video ()
			self.logger.info ('VDX Sheet Created for IO - {}'.format (self.config.ioid))


if __name__=="__main__":
	pass
# enable it when running for individual file
	#c = config.Config('Origin', 608607,'2018-04-16','2018-04-23')
	#o = Video (c)
	#o.main ()
	#c.saveAndCloseWriter ()
