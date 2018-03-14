import datetime
import xlsxwriter
import pandas as pd
import EOC_Summary_Header
import EOC_Summary_Detail
import numpy as np
from xlsxwriter.utility import xl_rowcol_to_cell
import config
from config import Config

class EocWorkbook():
    def __init__(self, r, c):
        self.rowN = r
        self.colN = c
        self.path = 'C:\zPersonal_Gaurav\EOC Template.xlsx'
        self.writer = pd.ExcelWriter(self.path, engine="xlsxwriter")

    def CreateWorkbook(self):
        workbook = xlsxwriter.Workbook(self.path)
        return workbook

    def CreateWorksheet(self, workbook, sheetName):
        worksheet = workbook.add_worksheet(sheetName)
        return worksheet

    def Print_Header(self, ws, df):
        ws.write(self.rowN, self.colN, "Client Name")
        client = df.iloc[0, 3]
        ws.write(self.rowN, self.colN + 1, client)

        ws.write(self.rowN + 1, self.colN, "Campaign Name")
        io = df.iloc[0,2]
        ws.write(self.rowN + 1, self.colN + 1, io)

        ws.write(self.rowN, self.colN + 3, "Expo Account Manager")
        am = df.iloc[0,4]
        ws.write(self.rowN, self.colN + 4, am)

        ws.write(self.rowN + 1, self.colN + 3, "Expo Sales Contact")
        s_rep = df.iloc[0,5]
        ws.write(self.rowN + 1, self.colN + 4, s_rep)

        #date check condition
        ws.write(self.rowN, self.colN + 6, "Campaign Report date")
        curr_dt = datetime.datetime.now() - 1
        dt = df.iloc[0, 1]
        if dt >= curr_dt:
            dt = str(df.iloc[0,0]) + str(df.iloc[0,1])
            status = "Live"
        else:
            dt = str(df.iloc[0, 0]) + str(curr_dt)
            status = "Ended"

        ws.write(self.rowN, self.colN + 7, dt)

        ws.write(self.rowN + 1, self.colN + 6, "Campaign Status")
        ws.write(self.rowN + 1, self.colN + 7, status)

        #return self.rowN, self.colN
        return 0

    def CloseWorkbook(self, workbook):
        print('Report Created')
        workbook.close()


if __name__=="__main__":
    #IO_Name = input("Enter IO Name:")
    #IO_ID = int(input("Enter the IO:"))
    #c = Config(IO_Name, IO_ID)
    c = Config("Test",565337)

    #Read Summary Data
    obj = EOC_Summary_Header.Summary_Header(c)
    df_header = EOC_Summary_Header.Summary_Header.read_query_summary(obj)
    print(df_header)

    obj = EOC_Summary_Detail.Summary_Detail(c)
    df_km = EOC_Summary_Detail.Summary_Detail.read_summary_KM(obj)
    #print(df_km)

    df_sales = EOC_Summary_Detail.Summary_Detail.read_summary_Sales(obj)
    #print(df_sales)

    myObj = EocWorkbook(1,1)
    wb = myObj.CreateWorkbook()
    ws = myObj.CreateWorksheet(wb, "Summary")
    s = myObj.Print_Header(ws,df_header)
    myObj.CloseWorkbook(wb)
