#coding=utf-8
"""
Created by:Dharmendra
Date:2018-03-23
"""
import pandas as pd
import cx_Oracle
import logging

class Config(object):
	"""
	This class is configuration for all classes
	"""
	def __init__(self, ioname, ioid):
		
		self.ioname = ioname
		self.ioid = ioid
		self.LoggFile ()
		self.logger.info('Trying to connect with TFR for io: {}'.format(self.ioid))
		try:
			self.conn = cx_Oracle.connect("TFR_REP/welcome@10.29.20.76/tfrdb")
		except Exception as e:
			self.logger.error (str(e)+'TNS:Connect timeout occurred: Please Retry for IO: {}'.format (self.ioid))
		self.path = ("C://EOC_Project//Bi_Team_Project//Reports//{}({}).xlsx".format(self.ioname, self.ioid))
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
		read_common_columns = pd.read_csv("C://EOC_Project//Bi_Team_Project//EOC_Data//Eociocommoncolumn.csv")
		data_common_columns_new = read_common_columns.loc[read_common_columns.IOID==self.ioid, :]
		data_common_columns = data_common_columns_new.loc[:, ["Columns-IO", "Values-IO", "Columns-AM-Sales",
		                                                      "Values-AM-Sales",
		                                                      "Columns-Campaign-Info", "Values-Campaign-Info"]]
		# self.data_common_columns = data_common_columns
		return read_common_columns, data_common_columns
	
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