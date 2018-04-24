#coding=utf-8
"""
Created by:Dharmendra
Date:2018-03-23
"""
import pandas as pd
import numpy as np
import config
from xlsxwriter.utility import xl_rowcol_to_cell
import logging

class Video(object):
	"""
Class for VDX Placements
	"""
	
	def __init__(self, config):
		self.config = config
		self.logger = self.config.logger

	

	def main(self):
		"""
Main Function
		"""
		self.config.common_columns_summary()
		#self.connect_TFR_Video()
		#self.read_Query_Video()
		#self.access_Data_KM_Video()
		#self.access_columns_KM_Video()
		#self.rename_KM_Data_Video()
		#self.write_video_data()
		#self.formatting_Video()


if __name__=="__main__":
	#pass
	#enable it when running for individual file
	c = config.Config ('Origin', 600857)
	o = Video (c)
	o.main ()
	c.saveAndCloseWriter ()




















