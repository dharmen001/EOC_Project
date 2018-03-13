import pandas as pd
import cx_Oracle
from cx_Oracle import DatabaseError

class Config():
    def __init__(self, IO_Name, IO_ID):
        self.IO_Name=IO_Name
        self.IO_ID= IO_ID
        try:
            self.conn=cx_Oracle.connect("TFR_REP/welcome@10.29.20.76/tfrdb")
        except DatabaseError as D:
            print ("TNS:Connect timeout occurred")
            print ("Enter the Name and ID again")
        self.path=("C://EOC_Project//Bi_Team_Project//Reports//{}({}).xlsx".format(self.IO_Name,self.IO_ID))


