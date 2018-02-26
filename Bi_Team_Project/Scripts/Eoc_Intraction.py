import pandas as pd
import numpy as np
import config
from xlsxwriter.utility import xl_rowcol_to_cell
import xlsxwriter

class Intraction():
    def __init__(self, config):
        self.config = config

    def connect_TFR_Intraction(self):
            sql_KM = "select * from TFR_REP.INTERACTION_DETAIL_MV where IO_ID = {}".format(self.config.IO_ID)
            return sql_KM

    def read_Query_Intraction(self):
        sql_KM = self.connect_TFR_Intraction()
        read_sql_KM = pd.read_sql(sql_KM, self.config.conn)
        return read_sql_KM

    def access_Data_Intraction(self):

        read_sql_KM = self.read_Query_Intraction()

        try:

            KM_Data_ENG = pd.pivot_table(read_sql_KM, index=["PLACEMENT_DESC", "METRIC_DESC","BLAZE_ACTION_TYPE_DESC"],
                                     values=["ENG_INTERACTION"],columns=["BLAZE_TAG_NAME_DESC"], aggfunc=np.sum, fill_value=0, margins=True)
        except ValueError as e:
            pass
        try:
            KM_Data_ENG.index.name = None
            KM_Data_ENG.columns=['_'.join(col).strip() for col in KM_Data_ENG.columns.values]
            KM_Data_ENG.columns=KM_Data_ENG.columns.get_level_values(0)
        except UnboundLocalError as e:
            pass

        try:

            KM_Data_VWR = pd.pivot_table(read_sql_KM,index=["PLACEMENT_DESC", "METRIC_DESC", "BLAZE_ACTION_TYPE_DESC"],
                                     columns=["BLAZE_TAG_NAME_DESC"],
                                     values=["VWR_INTERACTION"], aggfunc=np.sum,fill_value=0, margins=True)
        except ValueError as e:
            pass
        try:
            KM_Data_VWR.index.name=None
            KM_Data_VWR.columns=['_'.join(col).strip() for col in KM_Data_VWR.columns.values]
            KM_Data_VWR.columns=KM_Data_VWR.columns.get_level_values(0)
        except UnboundLocalError as e:
            pass

        try:
            KM_Data_DPE = pd.pivot_table(read_sql_KM,index=["PLACEMENT_DESC", "METRIC_DESC", "BLAZE_ACTION_TYPE_DESC"],
                                   columns=["BLAZE_TAG_NAME_DESC"],
                                   values=["DPE_INTERACTION"],aggfunc=np.sum,fill_value=0, margins=True)
        except ValueError as e:
            pass
        try:
            KM_Data_DPE.index.name=None
            KM_Data_DPE.columns=['_'.join(col).strip() for col in KM_Data_DPE.columns.values]
            KM_Data_DPE.columns=KM_Data_DPE.columns.get_level_values(0)
        except UnboundLocalError as e:
            pass
        try:
            return KM_Data_ENG, KM_Data_VWR, KM_Data_DPE
        except UnboundLocalError as e:
            pass

    def KM_intraction(self):
        try:
            KM_Data_ENG, KM_Data_VWR, KM_Data_DPE = self.access_Data_Intraction()
        except TypeError as e:
            pass
        data_common_columns=self.config.common_columns_summary()

        data_common = data_common_columns[1].to_excel(self.config.writer,
                                                      sheet_name="Intraction Performance({})".format(self.config.IO_ID),
                                                      startcol=0,startrow=7,index=False,header=False)

        try:

            KM_ENG = KM_Data_ENG.to_excel(self.config.writer,sheet_name="Intraction Performance({})".format(self.config.IO_ID)
                                         ,startrow=12, startcol=0, header=True)
        except UnboundLocalError as e:
            pass

        try:

            KM_VWR = KM_Data_VWR.to_excel(self.config.writer,sheet_name="Intraction Performance({})".format(self.config.IO_ID)
                                        , startrow=len(KM_Data_ENG)+16, startcol=0, header=True)
        except UnboundLocalError as e:
            pass

        try:

            KM_DPE = KM_Data_DPE.to_excel(self.config.writer,sheet_name="Intraction Performance({})".format(self.config.IO_ID)
                                      ,startrow=len(KM_Data_ENG)+len(KM_Data_VWR)+20, startcol=0, header=True)
        except UnboundLocalError as e:
            pass

        try:
            return KM_ENG, KM_VWR, KM_DPE, KM_Data_ENG, KM_Data_VWR, KM_Data_DPE
        except UnboundLocalError as e:
            pass

    def formatting_intraction(self):
        try:
            KM_Data_ENG,KM_Data_VWR,KM_Data_DPE = self.access_Data_Intraction()
        except TypeError as e:
            pass
        try:
            number_rows_KM_Data_ENG = KM_Data_ENG.shape[0]
        except UnboundLocalError as e:
            pass
        try:
            number_cols_KM_Data_ENG = KM_Data_ENG.shape[1]+3
        except UnboundLocalError as e:
            pass
        try:
            number_rows_KM_Data_VWR = KM_Data_VWR.shape[0]
        except UnboundLocalError as e:
            pass
        try:
            number_cols_KM_Data_VWR = KM_Data_ENG.shape[1]+3
        except UnboundLocalError as e:
            pass
        try:
            number_rows_KM_Data_DPE = KM_Data_DPE.shape[0]
        except UnboundLocalError as e:
            pass
        try:
            number_cols_KM_Data_DPE = KM_Data_DPE.shape[1]+3
        except UnboundLocalError as e:
            pass
        workbook=self.config.writer.book
        worksheet=self.config.writer.sheets["Intraction Performance({})".format(self.config.IO_ID)]
        worksheet.hide_gridlines(2)
        worksheet.insert_image("A1","Exponential.png")
        worksheet.freeze_panes(13,0)
        worksheet.set_zoom(80)
        format_merge_row=workbook.add_format({"bold":True,"font_color":'#FFFFFF',"align":"left",
                                              "fg_color":"#6495ED"})
        worksheet.merge_range("A7:F7","Intraction Performance",format_merge_row)
        alignment=workbook.add_format({"align":"center"})
        worksheet.merge_range("A12:C12","Engager Metrics", format_merge_row)
        try:
            worksheet.merge_range("A{}:C{}".format(number_rows_KM_Data_ENG+16,number_rows_KM_Data_ENG+16),"Viwer Metrics",format_merge_row)
        except UnboundLocalError as e:
            pass
        try:
            worksheet.merge_range("A{}:C{}".format(number_rows_KM_Data_ENG+number_rows_KM_Data_VWR+20,number_rows_KM_Data_ENG+number_rows_KM_Data_VWR+20),"Deep Metrics",format_merge_row)
        except UnboundLocalError as e:
            pass
        try:
            worksheet.set_column(0,number_cols_KM_Data_ENG,35,alignment)
        except UnboundLocalError as e:
            pass
        full_border=workbook.add_format({"border":1,"border_color":"#000000","align":"center",
                                         "fg_color":"#6495ED","bold":True})
        worksheet.conditional_format("A8:F10",{"type":"no_blanks","format":full_border})

        data_border_style=workbook.add_format({"border":1,"border_color":"#000000"})
        try:
            worksheet.conditional_format(13,3,number_rows_KM_Data_ENG+13,number_cols_KM_Data_ENG,
                                     {"type":"no_blanks","format":data_border_style})
        except UnboundLocalError as e:
            pass
        try:
            worksheet.conditional_format(number_rows_KM_Data_ENG+17,3,number_rows_KM_Data_VWR+number_rows_KM_Data_ENG+16,
                                     number_cols_KM_Data_VWR,{"type":"no_blanks","format":data_border_style})
        except UnboundLocalError as e:
            pass

        try:
            worksheet.conditional_format(number_rows_KM_Data_ENG+number_rows_KM_Data_VWR+21,3,
                                     number_rows_KM_Data_VWR+number_rows_KM_Data_ENG+number_rows_KM_Data_DPE+20,
                                     number_cols_KM_Data_DPE,{"type":"no_blanks","format":data_border_style})
        except UnboundLocalError as e:
            pass
        read_display = pd.read_csv("C://EOC_Project//Bi_Team_Project//EOC_Data//DisplayIOs.csv", index_col=["IO_ID"])
        if self.config.IO_ID in read_display.index:
            worksheet.hide()

    def main(self):
        self.config.common_columns_summary()
        self.connect_TFR_Intraction()
        self.read_Query_Intraction()
        self.access_Data_Intraction()
        self.KM_intraction()
        self.formatting_intraction()

if __name__ == "__main__":
    pass
    #enable it when running for individual file
    #c=config.Config('Dial',565337)
    #o = Intraction(c)
    #o.main()
    #c.saveAndCloseWriter()



