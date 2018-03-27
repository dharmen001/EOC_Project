import datetime
import xlsxwriter
import pandas as pd
import numpy as np
import EOC_Summary_Header
import EOC_Summary_Detail
import EOC_Daily_Sales
import EOC_Daily_KM
import EOC_AdSize_Sales
from xlsxwriter.utility import xl_rowcol_to_cell
from config import Config

class EocWorkbook():
    def __init__(self, r, c):
        self.rowN = r
        self.colN = c
        self.path = 'C:\zPersonal_Gaurav\EOC Template.xlsx'
        self.writer = pd.ExcelWriter(self.path, engine="xlsxwriter")

    def CreateWorkbook(self):
        workbook = xlsxwriter.Workbook(self.path)
        self.date_format = workbook.add_format({'num_format': 'mm-dd-yyyy'})
        return workbook

    def CreateWorksheet(self, workbook, sheetName):
        worksheet = workbook.add_worksheet(sheetName)
        return worksheet

#**************** Summary and other tabs Header
    def Print_Summary_Header(self, ws, df):
        self.rowN = 1
        self.colN = 1
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

#**************** Summary tab - print Standard Banner and Key Metric Placement wise Table
    def Print_Summary_Table(self, ws, df, rowN, df_daily, creativetype):
        self.colN = 3
        self.rowN =  rowN
        cell_1 = xl_rowcol_to_cell(self.rowN, self.colN)
        cell_2 = xl_rowcol_to_cell(self.rowN, self.colN + 10)
        self.Merge_myCells(ws, cell_1, cell_2, 'Campaign pacing')

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
        self.Merge_myCells(ws, cell_1, cell_2, creativetype)

        self.colN = self.colN + 2
    #Loop through the summary DF and paste values in summary table
        for r in range(df_rows):
            ws.write(self.rowN + r, self.colN, int(df.iloc[r,1]))
            ws.write_datetime(self.rowN + r, self.colN + 1, df.iloc[r, 2], self.date_format)
            ws.write_datetime(self.rowN + r, self.colN + 2, df.iloc[r, 3], self.date_format)
            ws.write(self.rowN + r, self.colN + 3, df.iloc[r, 4])
            ws.write(self.rowN + r, self.colN + 4, df.iloc[r, 5])
            ws.write(self.rowN + r, self.colN + 5, df.iloc[r, 6])
            ws.write(self.rowN + r, self.colN + 6, df.iloc[r, 7])
            ws.write(self.rowN + r, self.colN + 7, df.iloc[r, 8])
        #Add summarized delivered
            if creativetype == 'Standard Banners (Performance/Brand)':
                sum_views = df_daily[(df_daily["PLACEMENT_ID"] == df.iloc[r, 0])]['VIEWS'].sum()
                ws.write(self.rowN + r, self.colN + 8, sum_views)
            else:
                if df.iloc[r, 5] == 'CPM':
                    sum_views = df_daily[(df_daily["PLACEMENT_ID"] == df.iloc[r, 0])]['IMPRESSIONS'].sum()
                    ws.write(self.rowN + r, self.colN + 8, sum_views)
                elif df.iloc[r, 5] == 'CPE':
                    sum_engagements = df_daily[(df_daily["PLACEMENT_ID"] == df.iloc[r, 0])]['ENGAGEMENTS'].sum()
                    ws.write(self.rowN + r, self.colN + 8, sum_engagements)
                elif df.iloc[r, 5] == 'CPCV':
                    sum_cpcv_completions = df_daily[(df_daily["PLACEMENT_ID"] == df.iloc[r, 0])]['CPCV_COUNT'].sum()
                    ws.write(self.rowN + r, self.colN + 8, sum_cpcv_completions)


        # get spent & delivery formula
            #fm = self.Formula_Spend(str(df.iloc[r, 5]), xl_rowcol_to_cell(self.rowN + r, self.colN + 5), xl_rowcol_to_cell(self.rowN + r, self.colN + 8))
            #Spend Formula
            cell_1 = xl_rowcol_to_cell(self.rowN + r, self.colN + 8)
            cell_2 = xl_rowcol_to_cell(self.rowN + r, self.colN + 5)
            self.Formula_Spend(ws, df.iloc[r, 5], self.rowN + r, self.colN + 10 , cell_1, cell_2)

            self.fm_delivery = '=' + xl_rowcol_to_cell(self.rowN + r, self.colN + 8) + '/' + xl_rowcol_to_cell(self.rowN + r, self.colN + 7) + ''
            ws.write_formula(self.rowN + r, self.colN + 9, self.fm_delivery)

            if r == df_rows - 1:
                self.fm_delivery = '=' + xl_rowcol_to_cell(self.rowN + r + 1, self.colN + 8) + '/' + xl_rowcol_to_cell(self.rowN + r + 1, self.colN + 7) + ''
                ws.write_formula(self.rowN + r + 1, self.colN + 9, self.fm_delivery)


    #Apply Formulas in Sub Total row
        total_rows = self.rowN + df_rows                  # check total rows printed
        cell_1 = xl_rowcol_to_cell(self.rowN, self.colN + 6)
        cell_2 = xl_rowcol_to_cell(total_rows - 1, self.colN + 6)
        ws.write(total_rows, self.colN, "SubTotal")
        self.Formula_Sum(ws, total_rows, self.colN + 6, cell_1, cell_2)
        #ws.write_formula(total_rows, self.colN + 6, "=SUM({:s}:{:s})".format(cell_1, cell_2))

        cell_1 = xl_rowcol_to_cell(self.rowN, self.colN + 7)
        cell_2 = xl_rowcol_to_cell(total_rows - 1, self.colN + 7)
        self.Formula_Sum(ws, total_rows, self.colN + 7, cell_1, cell_2)

        cell_1 = xl_rowcol_to_cell(self.rowN, self.colN + 8)
        cell_2 = xl_rowcol_to_cell(total_rows - 1, self.colN + 8)
        self.Formula_Sum(ws, total_rows, self.colN + 8, cell_1, cell_2)

        cell_1 = xl_rowcol_to_cell(self.rowN, self.colN + 10)
        cell_2 = xl_rowcol_to_cell(total_rows - 1, self.colN + 10)
        self.Formula_Sum(ws, total_rows, self.colN + 10, cell_1, cell_2)

        #apply date formats
        cell_1 = xl_rowcol_to_cell(self.rowN, self.colN + 1)
        cell_2 = xl_rowcol_to_cell(total_rows - 1, self.colN + 12)

        return self.rowN + df_rows + 3

#**************** Standard Banner tab - Table 1 -- Summary
    def Print_StandardBanner_Table1_summary(self, wsSales, df, rowN, df_daily, tableHeader):
        self.colN = 1
        self.rowN = rowN
        cell_1 = xl_rowcol_to_cell(self.rowN, self.colN)
        cell_2 = xl_rowcol_to_cell(self.rowN, self.colN + 10)
        self.Merge_myCells(wsSales, cell_1, cell_2, tableHeader)

        self.rowN = self.rowN + 1
        wsSales.write(self.rowN, self.colN, "Placement # - Name")
        wsSales.write(self.rowN, self.colN + 1, "Cost Type")
        wsSales.write(self.rowN, self.colN + 2, "Cost")
        wsSales.write(self.rowN, self.colN + 3, "Booked Impressions")
        wsSales.write(self.rowN, self.colN + 4, "Delivered Impressions")
        wsSales.write(self.rowN, self.colN + 5, "Clicks")
        wsSales.write(self.rowN, self.colN + 6, "CTR")
        wsSales.write(self.rowN, self.colN + 7, "Conversions")
        wsSales.write(self.rowN, self.colN + 8, "Conversion Rate")
        wsSales.write(self.rowN, self.colN + 9, "Spend")
        wsSales.write(self.rowN, self.colN + 10, "eCPA")

        self.rowN = self.rowN + 1
        placement_list = df_daily.PLACEMENT_ID.unique()
        startRow = self.rowN
        endRow = 0

        for pl in placement_list:
            #print(pl)
            df2 = df[df["PLACEMENT_ID"] == pl]
            wsSales.write(self.rowN, self.colN, df2.iloc[0, 1] + ' - ' + df2.iloc[0, 4])
            wsSales.write(self.rowN, self.colN + 1, df2.iloc[0, 5])
            wsSales.write(self.rowN, self.colN + 2, df2.iloc[0, 6])
            wsSales.write(self.rowN, self.colN + 3, df2.iloc[0, 8])

            sum_views = df_daily[(df_daily["PLACEMENT_ID"] == pl)]['VIEWS'].sum()
            wsSales.write(self.rowN, self.colN + 4, sum_views)
            sum_clicks = df_daily[(df_daily["PLACEMENT_ID"] == pl)]['CLICKS'].sum()
            wsSales.write(self.rowN, self.colN + 5, sum_clicks)

            #CTR Formulas
            cell_1 = xl_rowcol_to_cell(self.rowN, self.colN + 5)
            cell_2 = xl_rowcol_to_cell(self.rowN, self.colN + 4)
            self.Formula_CTR(wsSales, self.rowN, self.colN + 6, cell_1, cell_2)
            #wsSales.write_formula(self.rowN, self.colN + 6, "=IFERROR({:s}/{:s},0)".format(cell_1, cell_2))

            sum_conversions = df_daily[(df_daily["PLACEMENT_ID"] == pl)]['CONVERSIONS'].sum()
            wsSales.write(self.rowN, self.colN + 7, sum_conversions)
            cell_1 = xl_rowcol_to_cell(self.rowN, self.colN + 7)
            self.Formula_CTR(wsSales, self.rowN, self.colN + 8, cell_1, cell_2)

            #Spend Formula
            cell_1 = xl_rowcol_to_cell(self.rowN, self.colN + 4)
            cell_2 = xl_rowcol_to_cell(self.rowN, self.colN + 2)
            self.Formula_Spend(wsSales, df2.iloc[0, 5], self.rowN, self.colN + 9 , cell_1, cell_2)



            #eCPA Formula
            cell_1 = xl_rowcol_to_cell(self.rowN, self.colN + 9)
            cell_2 = xl_rowcol_to_cell(self.rowN, self.colN + 7)
            self.Formula_CTR(wsSales, self.rowN, self.colN + 10, cell_1, cell_2)
            endRow = self.rowN
            self.rowN = self.rowN + 1

        # add SubTotal row to the table
        wsSales.write(self.rowN, 1, 'Total')

        cell_1 = xl_rowcol_to_cell(startRow, self.colN + 3)
        cell_2 = xl_rowcol_to_cell(endRow, self.colN + 3)
        self.Formula_Sum(wsSales, self.rowN , self.colN + 3, cell_1, cell_2)

        cell_1 = xl_rowcol_to_cell(startRow, self.colN + 4)
        cell_2 = xl_rowcol_to_cell(endRow, self.colN + 4)
        self.Formula_Sum(wsSales, self.rowN , self.colN + 4, cell_1, cell_2)

        cell_1 = xl_rowcol_to_cell(startRow, self.colN + 5)
        cell_2 = xl_rowcol_to_cell(endRow, self.colN + 5)
        self.Formula_Sum(wsSales, self.rowN , self.colN + 5, cell_1, cell_2)

        cell_1 = xl_rowcol_to_cell(self.rowN, self.colN + 5)
        cell_2 = xl_rowcol_to_cell(self.rowN, self.colN + 4)
        self.Formula_CTR(wsSales, self.rowN, self.colN + 6, cell_1, cell_2)

        cell_1 = xl_rowcol_to_cell(startRow, self.colN + 7)
        cell_2 = xl_rowcol_to_cell(endRow, self.colN + 7)
        self.Formula_Sum(wsSales, self.rowN , self.colN + 7, cell_1, cell_2)

        cell_1 = xl_rowcol_to_cell(self.rowN, self.colN + 7)
        cell_2 = xl_rowcol_to_cell(self.rowN, self.colN + 4)
        self.Formula_CTR(wsSales, self.rowN, self.colN + 8, cell_1, cell_2)

        cell_1 = xl_rowcol_to_cell(startRow, self.colN + 9)
        cell_2 = xl_rowcol_to_cell(endRow, self.colN + 9)
        self.Formula_Sum(wsSales, self.rowN , self.colN + 9, cell_1, cell_2)

        cell_1 = xl_rowcol_to_cell(self.rowN, self.colN + 9)
        cell_2 = xl_rowcol_to_cell(self.rowN, self.colN + 7)
        self.Formula_CTR(wsSales, self.rowN, self.colN + 10, cell_1, cell_2)
        return self.rowN + 1

#**************** Standard Banner tab - Table 2 -- Ad-Size
    def Print_StandardBanner_Table2_adSize(self, wsSales, df, r1, df_adSize, tableHeader):
        self.colN = 1
        self.rowN = r1
        cell_1 = xl_rowcol_to_cell(self.rowN, self.colN)
        cell_2 = xl_rowcol_to_cell(self.rowN, self.colN + 10)
        self.Merge_myCells(wsSales, cell_1, cell_2, tableHeader)

        self.rowN = self.rowN + 1
        wsSales.write(self.rowN, self.colN, "Ad-size")
        wsSales.write(self.rowN, self.colN + 1, "Impressions")
        wsSales.write(self.rowN, self.colN + 2, "Clicks")
        wsSales.write(self.rowN, self.colN + 3, "CTR")
        wsSales.write(self.rowN, self.colN + 4, "Conversions")
        wsSales.write(self.rowN, self.colN + 5, "Conversion Rate")
        wsSales.write(self.rowN, self.colN + 6, "Spend")
        wsSales.write(self.rowN, self.colN + 7, "eCPA")

        self.rowN = self.rowN + 1
        startRow = self.rowN
        endRow = 0

        #********************** apply Look-up of UNIT_COST from summary DF to ad-size DF
        df_adSize['UNIT_PRICE'] = (df_adSize['PLACEMENT_ID']).map(df.set_index('PLACEMENT_ID')['UNIT_COST'])
        df_adSize['SPEND'] = df_adSize['UNIT_PRICE'] / 1000 * df_adSize['VIEWS']
        #print(df_adSize)

        #********************** Group By dataframe dataset by AD-SIZE
        ad_list = df_adSize.MEDIA_SIZE_DESC.unique()

        for ad in ad_list:
            #print(ad)
            df2 = df_adSize[df_adSize["MEDIA_SIZE_DESC"] == ad]
            wsSales.write(self.rowN, self.colN, df2.iloc[0, 1])

            sum_views = df_adSize[(df_adSize["MEDIA_SIZE_DESC"] == ad)]['VIEWS'].sum()
            wsSales.write(self.rowN, self.colN + 1, sum_views)
            sum_clicks = df_adSize[(df_adSize["MEDIA_SIZE_DESC"] == ad)]['CLICKS'].sum()
            wsSales.write(self.rowN, self.colN + 2, sum_clicks)
            sum_conversions = df_adSize[(df_adSize["MEDIA_SIZE_DESC"] == ad)]['CONVERSIONS'].sum()
            wsSales.write(self.rowN, self.colN + 4, sum_conversions)

            # CTR Formulas
            cell_1 = xl_rowcol_to_cell(self.rowN, self.colN + 2)
            cell_2 = xl_rowcol_to_cell(self.rowN, self.colN + 1)
            self.Formula_CTR(wsSales, self.rowN, self.colN + 3, cell_1, cell_2)
            # Conversion Rate Formulas
            cell_1 = xl_rowcol_to_cell(self.rowN, self.colN + 4)
            cell_2 = xl_rowcol_to_cell(self.rowN, self.colN + 1)
            self.Formula_CTR(wsSales, self.rowN, self.colN + 5, cell_1, cell_2)

            # Spend
            sum_spend = df_adSize[(df_adSize["MEDIA_SIZE_DESC"] == ad)]['SPEND'].sum()
            wsSales.write(self.rowN, self.colN + 6, sum_spend)
            # eCPA Formulas
            cell_1 = xl_rowcol_to_cell(self.rowN, self.colN + 6)
            cell_2 = xl_rowcol_to_cell(self.rowN, self.colN + 4)
            self.Formula_CTR(wsSales, self.rowN, self.colN + 7, cell_1, cell_2)

            self.rowN = self.rowN + 1

        #Add Totals
        wsSales.write(self.rowN, self.colN, "Total")
        self.rowN = self.rowN - 1
        cell_1 = xl_rowcol_to_cell(startRow, self.colN + 1)
        cell_2 = xl_rowcol_to_cell(self.rowN, self.colN + 1)
        self.Formula_Sum(wsSales, self.rowN + 1, self.colN + 1, cell_1, cell_2)

        cell_1 = xl_rowcol_to_cell(startRow, self.colN + 2)
        cell_2 = xl_rowcol_to_cell(self.rowN, self.colN + 2)
        self.Formula_Sum(wsSales, self.rowN + 1, self.colN + 2, cell_1, cell_2)

        cell_1 = xl_rowcol_to_cell(self.rowN + 1, self.colN + 2)
        cell_2 = xl_rowcol_to_cell(self.rowN + 1, self.colN + 1)
        self.Formula_CTR(wsSales, self.rowN + 1, self.colN + 3, cell_1, cell_2)

        cell_1 = xl_rowcol_to_cell(startRow, self.colN + 4)
        cell_2 = xl_rowcol_to_cell(self.rowN, self.colN + 4)
        self.Formula_Sum(wsSales, self.rowN + 1, self.colN + 4, cell_1, cell_2)

        cell_1 = xl_rowcol_to_cell(self.rowN + 1, self.colN + 4)
        cell_2 = xl_rowcol_to_cell(self.rowN + 1, self.colN + 1)
        self.Formula_CTR(wsSales, self.rowN + 1, self.colN + 5, cell_1, cell_2)

        cell_1 = xl_rowcol_to_cell(startRow, self.colN + 6)
        cell_2 = xl_rowcol_to_cell(self.rowN, self.colN + 6)
        self.Formula_Sum(wsSales, self.rowN + 1, self.colN + 6, cell_1, cell_2)

        cell_1 = xl_rowcol_to_cell(self.rowN + 1, self.colN + 6)
        cell_2 = xl_rowcol_to_cell(self.rowN + 1, self.colN + 4)
        self.Formula_CTR(wsSales, self.rowN + 1, self.colN + 7, cell_1, cell_2)

        return self.rowN + 2

# **************** Standard Banner tab - Table 3 --Day-wise
    def Print_StandardBanner_Table3_daywise(self, wsSales, df_summary, rowN, df_sales, tableHeader):
        self.rowN = rowN
        self.colN = 1

        cell_1 = xl_rowcol_to_cell(self.rowN + 3, self.colN)
        cell_2 = xl_rowcol_to_cell(self.rowN + 3, self.colN + 10)
        self.Merge_myCells(wsSales,cell_1, cell_2, tableHeader)

        placement_list = df_sales.PLACEMENT_ID.unique()

        #unique placement loop and dataframe filter
        for pl in placement_list:
            df = df_summary[(df_summary['PLACEMENT_ID'] == pl)]
            unit_price = df.iloc[0,6]
            cost_type = df.iloc[0, 5]

            self.rowN = self.rowN + 4
            wsSales.write(self.rowN, self.colN, df.iloc[0,1] + ' - ' + df.iloc[0,4])

            self.rowN = self.rowN + 1
            wsSales.write(self.rowN, self.colN, "Date")
            wsSales.write(self.rowN, self.colN + 1, "Impressions")
            wsSales.write(self.rowN, self.colN + 2, "Clicks")
            wsSales.write(self.rowN, self.colN + 3, "CTR")
            wsSales.write(self.rowN, self.colN + 4, "Conversions")
            wsSales.write(self.rowN, self.colN + 5, "CPA")
            wsSales.write(self.rowN, self.colN + 6, "Spend")

            # For each placement, daily dataframe loop
            df2 = df_sales[df_sales['PLACEMENT_ID'] == pl]
            startRow = self.rowN + 1
            endRow = 0

            for r in range(df2.shape[0]):
                self.rowN = self.rowN + 1
                wsSales.write_datetime(self.rowN, self.colN, df2.iloc[r, 1], self.date_format)
                wsSales.write(self.rowN, self.colN + 1, df2.iloc[r, 2])
                wsSales.write(self.rowN, self.colN + 2, df2.iloc[r, 3])
                wsSales.write(self.rowN, self.colN + 4, df2.iloc[r, 4])

                cell_1 = xl_rowcol_to_cell(self.rowN, self.colN + 2)
                cell_2 = xl_rowcol_to_cell(self.rowN, self.colN + 1)
                self.Formula_CTR(wsSales, self.rowN, self.colN + 3, cell_1, cell_2)

                cell_1 = xl_rowcol_to_cell(self.rowN, self.colN + 1)
                self.Formula_Spend(wsSales, cost_type , self.rowN, self.colN + 6, cell_1, str(unit_price))

                cell_1 = xl_rowcol_to_cell(self.rowN, self.colN + 6)
                cell_2 = xl_rowcol_to_cell(self.rowN, self.colN + 4)
                self.Formula_CTR(wsSales, self.rowN, self.colN + 5, cell_1, cell_2)

                endRow = self.rowN

            #Add each placement-day table TOTALS
            wsSales.write(self.rowN + 1, self.colN, "Total")
            cell_1 = xl_rowcol_to_cell(startRow, self.colN + 1)
            cell_2 = xl_rowcol_to_cell(endRow, self.colN + 1)
            self.Formula_Sum(wsSales, endRow + 1, self.colN + 1, cell_1, cell_2)

            cell_1 = xl_rowcol_to_cell(startRow, self.colN + 2)
            cell_2 = xl_rowcol_to_cell(endRow, self.colN + 2)
            self.Formula_Sum(wsSales, endRow + 1, self.colN + 2, cell_1, cell_2)

            cell_1 = xl_rowcol_to_cell(endRow + 1, self.colN + 2)
            cell_2 = xl_rowcol_to_cell(endRow + 1, self.colN + 1)
            self.Formula_CTR(wsSales, endRow + 1, self.colN + 3, cell_1, cell_2)

            cell_1 = xl_rowcol_to_cell(startRow, self.colN + 4)
            cell_2 = xl_rowcol_to_cell(endRow, self.colN + 4)
            self.Formula_Sum(wsSales, endRow + 1, self.colN + 4, cell_1, cell_2)

            cell_1 = xl_rowcol_to_cell(endRow + 1, self.colN + 6)
            cell_2 = xl_rowcol_to_cell(endRow + 1, self.colN + 4)
            self.Formula_CTR(wsSales, endRow + 1, self.colN + 5, cell_1, cell_2)

            cell_1 = xl_rowcol_to_cell(startRow, self.colN + 6)
            cell_2 = xl_rowcol_to_cell(endRow, self.colN + 6)
            self.Formula_Sum(wsSales, endRow + 1, self.colN + 6, cell_1, cell_2)

        return 0





#*************** FORMULAS AND FUNCTIONS
    #Formula for CTR and Conversion Rate
    def Formula_CTR(self, ws, rowN, colN, cell_1, cell_2):
        ws.write_formula(rowN, colN, "=IFERROR({:s}/{:s},0)".format(cell_1, cell_2))

    #Function to get Formula for Spend
    def Formula_Spend(self, ws, costType, rowN, colN, cell_1, cell_2):
        if costType == "CPM":
            ws.write_formula(rowN, colN, "=IFERROR({:s}/1000*{:s},0)".format(cell_1, cell_2))
        else:
            ws.write_formula(rowN, colN, "=IFERROR({:s}*{:s},0)".format(cell_1, cell_2))

    # Formula for column Sum
    def Formula_Sum(self, ws, rowN, colN, cell_1, cell_2):
        ws.write_formula(rowN, colN, "=SUM({:s}:{:s})".format(cell_1, cell_2))

    #function to merge cells
    def Merge_myCells(self, ws, cell_1, cell_2, creativetype):
        ws.merge_range(cell_1 + ':' + cell_2, creativetype)
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

    #Read Summary Header dataframe
    obj = EOC_Summary_Header.Summary_Header(c)
    df_header = EOC_Summary_Header.Summary_Header.read_query_summary(obj)
    #print(df_header)

    # Read Summary Detail dataframe
    obj = EOC_Summary_Detail.Summary_Detail(c)
    df_km_summary = EOC_Summary_Detail.Summary_Detail.read_summary_KM(obj)
    #print(df_km_summary)
    df_sales_summary = EOC_Summary_Detail.Summary_Detail.read_summary_Sales(obj)
    #print(df_sales_summary)

    #Read sales daily dataframe
    obj = EOC_Daily_Sales.Daily_Sales(c)
    df_sales = EOC_Daily_Sales.Daily_Sales.read_query_summary(obj)
    #print(df_sales)

    obj = EOC_AdSize_Sales.AdSize_Sales(c)
    df_adSzie_sales = EOC_AdSize_Sales.AdSize_Sales.read_query_summary(obj)
    #print(df_adSzie_sales)

    # Read Key Metric daily dataframe
    obj = EOC_Daily_KM.Daily_KeyMetric(c)
    df_km = EOC_Daily_KM.Daily_KeyMetric.read_query_summary(obj)
    #print(df_km)

    myObj = EocWorkbook(1,1)
    wb = myObj.CreateWorkbook()
    ws = myObj.CreateWorksheet(wb, "Summary")
    r1 = myObj.Print_Summary_Header(ws,df_header)


    if df_sales_summary.shape[0] != 0 and df_km_summary.shape[0] != 0:
        r2 = myObj.Print_Summary_Table(ws, df_sales_summary, r1, df_sales, 'Standard Banners (Performance/Brand)')
        r3 = myObj.Print_Summary_Table(ws, df_km_summary, r2, df_km, 'VDX (Display and Instream)')
    else:
        if df_sales_summary.shape[0] != 0:
            r2 = myObj.Print_Summary_Table(ws, df_sales_summary, r1, df_sales, 'Standard Banners (Performance/Brand)')
        elif df_km_summary.shape[0] != 0:
                r2 = myObj.Print_Summary_Table(ws, df_km_summary, r1, df_km, 'VDX (Display and Instream)')


    #add daily sales worksheet
    wsSales = myObj.CreateWorksheet(wb, "Standard banner Campaign detail")
    wsSales.activate()
    r1 = myObj.Print_Summary_Header(wsSales, df_header)

    if df_sales_summary.shape[0] != 0:
        r2 = myObj.Print_StandardBanner_Table1_summary(wsSales, df_sales_summary, r1, df_sales, 'Standard Banner Performance - Summary')
        r3 = myObj.Print_StandardBanner_Table2_adSize(wsSales, df_sales_summary, r2 + 2, df_adSzie_sales, 'Standard Banner Performance - Ad Size Summary')
        r4 = myObj.Print_StandardBanner_Table3_daywise(wsSales, df_sales_summary, r3, df_sales, 'Breakdown By Day + Placement')

    print(r3)
    myObj.CloseWorkbook(wb)

