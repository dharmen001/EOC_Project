import pandas as pd
import numpy as np
import config
from xlsxwriter.utility import xl_rowcol_to_cell

class definition():
    def __init__(self, config):
        self.config = config
    def reading_def(self):
        read_definition = pd.read_excel("C://BiTeam-New-ProjectPython//Bi_Team_Project//EOC_Data//EocCommonSheet.xlsx", header=None)
        return read_definition
    def writing_definition(self):
        read_definition = self.reading_def()
        write_defitntion = read_definition.to_excel(self.config.writer,sheet_name="Definition({})".format(self.config.IO_ID),index=False, header=False)
        return write_defitntion
    def format_definition(self):
        workbook=self.config.writer.book
        worksheet=self.config.writer.sheets["Definition({})".format(self.config.IO_ID)]
        worksheet.insert_image("A1","Exponential.png")
        worksheet.hide_gridlines(2)
        worksheet.set_zoom(100)
        worksheet.set_column("B:B",35)
        worksheet.set_column("C:C",255)

    def main(self):
        self.config.common_columns_summary()
        self.reading_def()
        self.writing_definition()
        self.format_definition()
if __name__ == "__main__":
    pass