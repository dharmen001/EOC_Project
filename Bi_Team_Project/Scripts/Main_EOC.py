#coding=utf-8
# !/usr/bin/env python
"""
This is main class running for all scripts
"""
import sys
import Eoc_Summary
import Eoc_Daily
import Eoc_Video
import Eoc_Intraction
import EOC_definition
from config import Config


if __name__ == '__main__':
	
	START_DATE = sys.argv[1]
	IO_ID = int (sys.argv[2])
	END_DATE = sys.argv[3]
	#FILE_PATH = sys.argv[4]
	#IO_Name = input("Enter IO Name:")
	#START_DATE = input("Enter the Start Date:")
	#IO_Name = input ("Enter IO Name:")
	#IO_ID = int (input ("Enter the IO:"))
	#END_DATE = input("Enter the End Date:")
	c = Config(START_DATE,IO_ID,END_DATE)
	obj_summary = Eoc_Summary.Summary(c)
	obj_summary.main()
	#obj_daily = Eoc_Daily.Daily(c)
	#obj_daily.main()
	#obj_Video = Eoc_Video.Video(c)
	#obj_Video.main()
	#obj_Intraction = Eoc_Intraction.Intraction(c)
	#obj_Intraction.main()
	#obj_definition = EOC_definition.definition(c)
	#obj_definition.main()
	c.saveAndCloseWriter()
	o = Eoc_Summary.Summary
