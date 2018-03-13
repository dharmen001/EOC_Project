from openpyxl import Workbook

class CreateWorkBook():
    def create_Workbook(self):
        book = Workbook()
        sheet = book.active

        sheet['A1'] = 56
        sheet['A2'] = 43

        book.save("C:/EOC_Project/Bi_Team_Project/Reports/sample.xlsx")

if __name__ == '__main__':


