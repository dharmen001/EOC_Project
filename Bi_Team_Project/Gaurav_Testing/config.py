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
        self.writer=pd.ExcelWriter(self.path, engine="xlsxwriter", datetime_format="MM-DD-YYYY")

    def saveAndCloseWriter(self):
        self.writer.save()
        self.writer.close()

    def common_columns_summary(self):
        read_common_columns = pd.read_csv("C://EOC_Project//Bi_Team_Project//EOC_Data//Eociocommoncolumn.csv")
        data_common_columns_new = read_common_columns.loc[read_common_columns.IOID == self.IO_ID, :]
        data_common_columns = data_common_columns_new.loc[:, ["Columns-IO", "Values-IO", "Columns-AM-Sales",
                                                              "Values-AM-Sales", "Columns-Campaign-Info",
                                                              "Values-Campaign-Info"]]



        return read_common_columns, data_common_columns

