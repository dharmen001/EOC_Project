import xlsxwriter
import EOC_Summary_Header


class EocWorkbook():
    def __init__(self,rowN,colN):
        self.rowN = rowN
        self.colN = colN

    def CreateWorkbook(self):
        workbook = xlsxwriter.Workbook('C:\zPersonal_Gaurav\EOC Template.xlsx')
        return workbook

    def CreateWorksheet(self, workbook, sheetName):
        worksheet = workbook.add_worksheet(sheetName)
        return worksheet

    def PrintData(self, ws):
        ws.write(self.rowN, self.colN, "Hello Test")

    def CloseWorkbook(self, workbook):
        print('Report Created')
        workbook.close()

if __name__=="__main__":
    EocWorkbook.rowN=1
    EocWorkbook.colN=1

    wb_Object = EocWorkbook.CreateWorkbook(EocWorkbook)
    ws_Object = EocWorkbook.CreateWorksheet(EocWorkbook,wb_Object,"Summary")
    #EOC_Summary_Header.printData()
    EocWorkbook.PrintData(EocWorkbook, ws_Object)
    EocWorkbook.CloseWorkbook(EocWorkbook,wb_Object)


