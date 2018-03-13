import xlsxwriter
import pandas as pd
import EOC_Summary_Header
import EOC_Summary_Detail
from config import Config

class EocWorkbook():
    def __init__(self, r, c):
        self.rowN = r
        self.colN = c

    def CreateWorkbook(self):
        workbook = xlsxwriter.Workbook('C:\zPersonal_Gaurav\EOC Template.xlsx')
        return workbook

    def CreateWorksheet(self, workbook, sheetName):
        worksheet = workbook.add_worksheet(sheetName)
        return worksheet

    def PrintData(self, ws, pd):
        ws.write(self.rowN, self.colN, pd)


    def CloseWorkbook(self, workbook):
        print('Report Created')
        workbook.close()

if __name__=="__main__":
    IO_Name = input("Enter IO Name:")
    IO_ID = int(input("Enter the IO:"))

    c = Config(IO_Name, IO_ID)
    obj = EOC_Summary_Header.Summary_Header(c)
    df_header = EOC_Summary_Header.Summary_Header.read_query_summary(obj)
    #print(df_header)

    obj = EOC_Summary_Detail.Summary_Detail(c)
    df_km = EOC_Summary_Detail.Summary_Detail.read_summary_KM(obj)
    #print(df_km)

    df_sales = EOC_Summary_Detail.Summary_Detail.read_summary_Sales(obj)
    print(df_sales)

