#coding=utf-8
"""
This is main class running for all scripts
"""
import Eoc_Summary
import Eoc_Daily
import Eoc_Video
import Eoc_Intraction
import EOC_definition
from config import Config


if __name__ == '__main__':
    IO_Name = input("Enter IO Name:")
    IO_ID = int(input("Enter the IO:"))
    START_DATE = input("Enter the Start Date:")
    END_DATE = input("Enter the End Date:")
    c = Config(IO_Name, IO_ID,START_DATE,END_DATE)#,START_DATE,END_DATE)
    obj_summary = Eoc_Summary.Summary(c)
    obj_summary.main()
    obj_daily = Eoc_Daily.Daily(c)
    obj_daily.main()
    obj_Video = Eoc_Video.Video(c)
    obj_Video.main()
    obj_Intraction = Eoc_Intraction.Intraction(c)
    obj_Intraction.main()
    obj_definition = EOC_definition.definition(c)
    obj_definition.main()
    c.saveAndCloseWriter()
    o = Eoc_Summary.Summary
