import pandas as pd
"""
Class Added
"""

class definition():
    def __init__(self, config):
        self.config = config
        self.logger = self.config.logger
	    
    def reading_def(self):
        read_definition = pd.read_excel("C://BiTeam-New-ProjectPython//Bi_Team_Project//EOC_Data//EocCommonSheet.xlsx", header=None)
        return read_definition
    
    def writing_definition(self):
        read_definition = self.reading_def()
        write_defitntion = read_definition.to_excel(self.config.writer,sheet_name="Definition",index=False, header=False)
        return write_defitntion
    
    def format_definition(self):
        workbook=self.config.writer.book
        worksheet=self.config.writer.sheets["Definition".format(self.config.ioid)]
        worksheet.insert_image("A1","Exponential.png")
        worksheet.hide_gridlines(2)
        worksheet.set_zoom(80)
        worksheet.set_column("B:B",35)
        worksheet.set_column("C:C",255)

    def main(self):
        self.config.common_columns_summary()
        self.reading_def()
        self.writing_definition()
        self.format_definition()
        self.logger.info('EOC for IO - {} Created'.format(self.config.ioid))
if __name__ == "__main__":
    pass