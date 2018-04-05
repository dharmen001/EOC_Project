# coding=utf-8
import xlrd
import pandas as pd
import xlsxwriter
import config

class read_daily():
	def __init__(self, config):
		self.config
	def daily_data(self):
		readData = pd.read_excel(self.config)
		
	