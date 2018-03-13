import xlsxwriter
import EOC_Summary_Header


class EocWorkbook():
    rowN = 0
    colN = 0

    def CreateWorkbook(self):
        workbook = xlsxwriter.Workbook('C:\zPersonal_Gaurav\EOC Template.xlsx')
        return workbook

    def CreateWorksheet(self, workbook, sheetName):
        worksheet = workbook.add_worksheet(sheetName)
        return worksheet

    def PrintData(self, ws):
        ws.write(self.rowN, self.colN, "Hello Test")
        return self.rowN + 10, self.colN + 10

    def CloseWorkbook(self, workbook):
        print('Report Created')
        workbook.close()

if __name__=="__main__":
    EocWorkbook.rowN=1
    EocWorkbook.colN=1

    wb_Object = EocWorkbook.CreateWorkbook(EocWorkbook)
    ws_Object = EocWorkbook.CreateWorksheet(EocWorkbook,wb_Object,"Summary")
    #EOC_Summary_Header.printData()
    print(EocWorkbook.rowN, EocWorkbook.colN)
    r, c = EocWorkbook.PrintData(EocWorkbook, ws_Object)

    EocWorkbook.rowN = EocWorkbook.rowN + r
    EocWorkbook.colN = EocWorkbook.colN + c
    print(EocWorkbook.rowN, EocWorkbook.colN)
    r, c = EocWorkbook.PrintData(EocWorkbook, ws_Object)

    EocWorkbook.rowN = EocWorkbook.rowN + r
    EocWorkbook.colN = EocWorkbook.colN + c
    print(EocWorkbook.rowN, EocWorkbook.colN)
    EocWorkbook.CloseWorkbook(EocWorkbook,wb_Object)


