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
        curr_dt = datetime.date.today()
        yest_dt = curr_dt - datetime.timedelta(days=1)
        s_dt = df.iloc[0, 0]
        s_dt = s_dt.date()
        dt = df.iloc[0, 1]
        dt=dt.date()

        if dt > datetime.date.today():
            dt = str(s_dt) + str(dt)
            status = "Live"
        else:
            dt = str(s_dt) + " - " + str(yest_dt)
            status = "Ended"

        ws.write(self.rowN, self.colN + 7, dt)
        ws.write(self.rowN + 1, self.colN + 6, "Campaign Status")
        ws.write(self.rowN + 1, self.colN + 7, status)
        return self.rowN + 6

    def Print_Sales_Summary(self, ws, df, rowN):
        self.colN = 3
        self.rowN =  rowN
        ws.write(self.rowN, self.colN, "Campaign pacing")
        cell_1 = xl_rowcol_to_cell(self.rowN, self.colN)
        cell_2 = xl_rowcol_to_cell(self.rowN, self.colN + 10)
        self.Merge_myCells(ws, cell_1, cell_2)

        self.rowN = self.rowN + 1
        ws.write(self.rowN, self.colN, "Placement #")
        ws.write(self.rowN, self.colN + 1, "Start Date")
        ws.write(self.rowN, self.colN + 2, "End Date")
        ws.write(self.rowN, self.colN + 3, "Placement Name")
        ws.write(self.rowN, self.colN + 4, "Cost type")
        ws.write(self.rowN, self.colN + 5, "Unit Cost")
        ws.write(self.rowN, self.colN + 6, "Planned Cost")
        ws.write(self.rowN, self.colN + 7, "Booked Impressions")
        ws.write(self.rowN, self.colN + 8, "Delivered Impressions")
        ws.write(self.rowN, self.colN + 9, "Delivery%")
        ws.write(self.rowN, self.colN + 10, "Spent")

        self.rowN = self.rowN + 1
        self.colN = self.colN - 2
        df_rows = df.shape[0]
        cell_1 = xl_rowcol_to_cell(self.rowN, self.colN)
        cell_2 = xl_rowcol_to_cell(self.rowN + df_rows, self.colN + 1)
        self.Merge_myCells(ws, cell_1, cell_2)

        self.colN = self.colN + 2
    #Loop through the DF and paste values in summary
        for r in range(df_rows):
            ws.write(self.rowN + r, self.colN, df.iloc[r,1])
            ws.write(self.rowN + r, self.colN + 1, df.iloc[r, 2])
            ws.write(self.rowN + r, self.colN + 2, df.iloc[r, 3])
            ws.write(self.rowN + r, self.colN + 3, df.iloc[r, 4])
            ws.write(self.rowN + r, self.colN + 4, df.iloc[r, 5])
            ws.write(self.rowN + r, self.colN + 5, df.iloc[r, 6])
            ws.write(self.rowN + r, self.colN + 6, df.iloc[r, 7])
            ws.write(self.rowN + r, self.colN + 7, df.iloc[r, 8])

            # get spent & delivery formula
            fm = self.Get_Formulas(str(df.iloc[r, 5]), xl_rowcol_to_cell(self.rowN + r, self.colN + 5), xl_rowcol_to_cell(self.rowN + r, self.colN + 8))
            ws.write_formula(self.rowN + r, self.colN + 10, fm)
            self.fm_delivery = '=' + xl_rowcol_to_cell(self.rowN + r, self.colN + 8) + '/' + xl_rowcol_to_cell(self.rowN + r, self.colN + 7) + ''
            ws.write_formula(self.rowN + r, self.colN + 9, self.fm_delivery)
            print(r, df_rows)
            if r == df_rows - 1:
                self.fm_delivery = '=' + xl_rowcol_to_cell(self.rowN + r + 1, self.colN + 8) + '/' + xl_rowcol_to_cell(self.rowN + r + 1, self.colN + 7) + ''
                ws.write_formula(self.rowN + r + 1, self.colN + 9, self.fm_delivery)

    #Apply Formulas in Sub Total row
        subT_row = self.rowN + df_rows
        cell_1 = xl_rowcol_to_cell(self.rowN, self.colN + 6)
        cell_2 = xl_rowcol_to_cell(subT_row - 1, self.colN + 6)
        ws.write(subT_row, self.colN, "SubTotal")
        ws.write_formula(subT_row, self.colN + 6, '=SUM(' + cell_1 + ':' + cell_2 + ')')

        cell_1 = xl_rowcol_to_cell(self.rowN, self.colN + 7)
        cell_2 = xl_rowcol_to_cell(subT_row - 1, self.colN + 7)
        ws.write_formula(subT_row, self.colN + 7, '=SUM(' + cell_1 + ':' + cell_2 + ')')

        cell_1 = xl_rowcol_to_cell(self.rowN, self.colN + 8)
        cell_2 = xl_rowcol_to_cell(subT_row - 1, self.colN + 8)
        ws.write_formula(subT_row, self.colN + 8, '=SUM(' + cell_1 + ':' + cell_2 + ')')

        cell_1 = xl_rowcol_to_cell(self.rowN, self.colN + 10)
        cell_2 = xl_rowcol_to_cell(subT_row - 1, self.colN + 10)
        ws.write_formula(subT_row, self.colN + 10, '=SUM(' + cell_1 + ':' + cell_2 + ')')

        return self.rowN


    #Function to get Formula for Spent
    def Get_Formulas(self, cost_type_col, unit_cost_col, delivered_number_col):
        if cost_type_col == 'CPM':
            return '=' + delivered_number_col + '/1000*' + unit_cost_col + ''
        else:
            return '=' + delivered_number_col + '*' + unit_cost_col + ''


    #Function to merge cells
    def Merge_myCells(self, ws, cell_1, cell_2):
        ws.merge_range(cell_1 + ':' + cell_2, 'Merged Range')
        return 0

    def CloseWorkbook(self, workbook):
        print('Report Created')
        self.writer.save()
        self.writer.close()
        workbook.close()


if __name__=="__main__":
    #IO_Name = input("Enter IO Name:")
    #IO_ID = int(input("Enter the IO:"))
    #c = Config(IO_Name, IO_ID)
    c = Config("Test",565337)

    #Read Summary Data
    obj = EOC_Summary_Header.Summary_Header(c)
    df_header = EOC_Summary_Header.Summary_Header.read_query_summary(obj)
    #print(df_header)

    obj = EOC_Summary_Detail.Summary_Detail(c)
    df_km_summary = EOC_Summary_Detail.Summary_Detail.read_summary_KM(obj)
    #print(df_km_summary)

    df_sales_summary = EOC_Summary_Detail.Summary_Detail.read_summary_Sales(obj)
    print(df_sales_summary)

    myObj = EocWorkbook(1,1)
    wb = myObj.CreateWorkbook()
    ws = myObj.CreateWorksheet(wb, "Summary")
    r1 = myObj.Print_Header(ws,df_header)
    r2 = myObj.Print_Sales_Summary(ws, df_sales_summary, r1)
    print(r2)
    myObj.CloseWorkbook(wb)

