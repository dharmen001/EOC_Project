#coding=utf-8
# !/usr/bin/env python
"""
Created by:Dharmendra
Date:2018-03-23
"""
import datetime
import pandas as pd
import cx_Oracle
import logging

class Config(object):
	"""
	This class is configuration for all classes
	"""
	def __init__(self, start_date, ioid, end_date):#start_date,end_date):
		
		self.start_date = start_date
		self.ioid = ioid
		self.end_date = end_date
		self.LoggFile ()
		self.logger.info('Trying to connect with TFR for io: {}'.format(self.ioid))
		try:
			self.conn = cx_Oracle.connect("TFR_REP/welcome@10.29.20.76/tfrdb")
		except Exception as e:
			self.logger.error (str(e)+'TNS:Connect timeout occurred: Please Retry for IO: {}'.format (self.ioid))
		self.path = ("C://EOC_Project//Bi_Team_Project//Reports//{}.xlsx".format(self.ioid))
		self.writer = pd.ExcelWriter(self.path, engine="xlsxwriter", datetime_format="YYYY-MM-DD")
		

	def saveAndCloseWriter(self):
		"""
		To finally Save and close file
		:return: Nothing
		"""
		self.writer.save()
		self.writer.close()
		self.conn.close()
	def common_columns_summary(self):
		"""
		reading data from csv file for ioid
		:return: read_common_columns, data_common_columns
		"""
		
		sql_client_info = "SELECT DISTINCT CLIENT_DESC from TFR_REP.SUMMARY_MV WHERE IO_ID = {}".format(self.ioid)
		sql_campaign_info = "SELECT DISTINCT IO_DESC FROM TFR_REP.SUMMARY_MV WHERE IO_ID = {}".format(self.ioid)
		sql_acct_mgr = "SELECT DISTINCT ACCOUNT_MGR FROM TFR_REP.SUMMARY_MV WHERE IO_ID = {}".format(self.ioid)
		sql_sales_rep = "SELECT DISTINCT SALES_REP FROM TFR_REP.SUMMARY_MV WHERE IO_ID = {}".format(self.ioid)
		sql_sdate = "SELECT TO_CHAR(MIN(SDATE),'YYYY-MM-DD') as SDATE from TFR_REP.SUMMARY_MV WHERE IO_ID = {}".format(self.ioid)
		sql_edate = "select (CASE WHEN (TO_CHAR(max(EDATE),'YYYY-MM-DD')) <= TO_CHAR(sysdate-1,'YYYY-MM-DD') THEN (TO_CHAR(max(EDATE),'YYYY-MM-DD')) ELSE TO_CHAR(sysdate-1,'YYYY-MM-DD') END) AS EDATE FROM TFR_REP.SUMMARY_MV where IO_ID = {}".format(self.ioid)
		sql_end_date = "SELECT TO_CHAR(MAX(EDATE),'YYYY-MM-DD') as EDATENEW from TFR_REP.SUMMARY_MV WHERE IO_ID = {}".format(self.ioid)
		
		read_sql_client_info = pd.read_sql(sql_client_info,self.conn)
		read_last_row_client_info = read_sql_client_info.iloc[-1:]
		read_last_row_client_info.rename(columns={"CLIENT_DESC":"Client Name"},inplace=True)
		client_info = read_last_row_client_info.set_index('Client Name').reset_index().transpose()
		
		read_sql_io_info = pd.read_sql(sql_campaign_info,self.conn)
		read_last_row_io_info = read_sql_io_info.iloc[-1:]
		read_last_row_io_info.rename(columns = {"IO_DESC":"Campaign Name"},inplace=True)
		campaign_info = read_last_row_io_info.set_index('Campaign Name').reset_index().transpose()
		
		read_sql_acct_mgr = pd.read_sql(sql_acct_mgr,self.conn)
		read_last_row_sql_acct_mgr = read_sql_acct_mgr.iloc[-1:]
		read_last_row_sql_acct_mgr.rename(columns = {"ACCOUNT_MGR":"Expo Account Manager"},inplace = True)
		ac_mgr = read_last_row_sql_acct_mgr.set_index('Expo Account Manager').reset_index().transpose()
		
		read_sql_sales_rep = pd.read_sql(sql_sales_rep,self.conn)
		read_last_row_sales_rep = read_sql_sales_rep.iloc[-1:]
		read_last_row_sales_rep.rename(columns = {"SALES_REP":"Expo Sales Contact"},inplace = True)
		sales_rep =  read_last_row_sales_rep.set_index('Expo Sales Contact').reset_index().transpose()
		
		
		read_sql_sdate = pd.read_sql(sql_sdate,self.conn)
		read_last_row_sdate = read_sql_sdate.iloc[-1:]
		read_last_row_sdate.rename(columns = {"SDATE":"Start_Date"},inplace = True)
		
		read_sql_edate = pd.read_sql(sql_edate,self.conn)
		read_last_row_edate = read_sql_edate.iloc[-1:]
		read_last_row_edate.rename (columns={"EDATE":"End_Date"}, inplace=True)
		
		
		read_sql_end_date = pd.read_sql(sql_end_date,self.conn)
		read_last_row_end_date = read_sql_end_date.iloc[-1:]
		read_last_row_end_date.rename (columns={"EDATENEW":"End_Date_New"}, inplace=True)
		final_end_date = read_last_row_end_date.iloc[0,0]
		
		
		sdate_edate = pd.concat([read_last_row_sdate,read_last_row_edate],axis =1)
		try:
			sdate_edate["Campaign Report date"] = sdate_edate[["Start_Date", "End_Date"]].apply (lambda x:" to ".join (x), axis=1)
		except TypeError as e:
			self.logger.error(str(e))
			pass
		sdate_edate_new = sdate_edate.iloc[-1:,-1]
		sdate_edate_new = sdate_edate_new.to_frame()
		sdate_edate_final = sdate_edate_new.set_index('Campaign Report date').reset_index().transpose()
		
		u_date = datetime.date.today()-datetime.timedelta(1)
		#e_date = datetime.date(1)
		new_date = u_date.strftime('%Y-%m-%d')
		
		if final_end_date > new_date:
			#word = "Campaign Status"
			status = "Live"
			
		else:
			#word = "Campaign Status"
			status = "Ended"
			
		
		
		#self.data_common_columns = data_common_columns
		self.client_info = client_info
		self.campaign_info = campaign_info
		self.ac_mgr = ac_mgr
		self.sales_rep = sales_rep
		self.sdate_edate_final = sdate_edate_final
		self.status = status
		#self.word = word
		#return read_common_columns, data_common_columns,final_new
	
	def LoggFile(self):
		# logging.basicConfig(level=logging.INFO, filename="C:\\EOC_Project\\Bi_Team_Project\\logs\\logfile")
		# create logger with 'spam_application'
		"""
		logger for console and output
		"""
		logger = logging.getLogger('EOCApp')
		logger.setLevel(logging.DEBUG)
		
		# create formatter and add it to the handlers
		formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
		
		# create file handler which logs even debug messages
		fh = logging.FileHandler('C:\\EOC_Project\\Bi_Team_Project\\logs\\logfile({}).log'.format(self.ioid))
		fh.setLevel(logging.ERROR)
		fh.setFormatter(formatter)
		logger.addHandler(fh)
		
		# create console handler with a higher log level
		ch = logging.StreamHandler()
		ch.setLevel(logging.DEBUG)
		ch.setFormatter(formatter)
		logger.addHandler(ch)
		
		self.logger = logger