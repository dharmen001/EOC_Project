import pandas as pd
import numpy as np
from xlsxwriter.utility import xl_rowcol_to_cell

class Video():
    def __init__(self, config):
        self.config = config

    def connect_TFR_Video(self):
            sql_KM = "select * from TFR_REP.VIDEO_DETAIL_MV where IO_ID = {}".format(self.config.IO_ID)
            return sql_KM

    def read_Query_Video(self):
        sql_KM = self.connect_TFR_Video()
        read_sql_KM = pd.read_sql(sql_KM, self.config.conn)
        return read_sql_KM

    def access_Data_KM_Video(self):
        read_sql_KM = self.read_Query_Video()

        KM_Data_eng = pd.pivot_table(read_sql_KM, values=["ENG_VIDEO_VIEW_0_PC_COUNT", "ENG_VIDEO_VIEW_25_PC_COUNT",
                                                          "ENG_VIDEO_VIEW_50_PC_COUNT", "ENG_VIDEO_VIEW_75_PC_COUNT",
                                                          "ENG_VIDEO_VIEW_100_PC_COUNT"],
                                        index=["PLACEMENT_ID", "PLACEMENT_DESC", "METRIC_DESC", "FEV_INT_VIDEO_DESC"],
                                        aggfunc = np.sum)
        KM_reset_eng = KM_Data_eng.reset_index()

        try:
            KM_Data_New_eng = KM_reset_eng[["PLACEMENT_ID", "PLACEMENT_DESC", "METRIC_DESC", "FEV_INT_VIDEO_DESC",
                                    "ENG_VIDEO_VIEW_0_PC_COUNT", "ENG_VIDEO_VIEW_25_PC_COUNT",
                                    "ENG_VIDEO_VIEW_50_PC_COUNT", "ENG_VIDEO_VIEW_75_PC_COUNT",
                                    "ENG_VIDEO_VIEW_100_PC_COUNT"]]
        except KeyError:
            KM_Data_New_eng = KM_reset_eng[[]]

        KM_Data_vwr = pd.pivot_table(read_sql_KM,values=["VWR_VIDEO_VIEW_0_PC_COUNT","VWR_VIDEO_VIEW_25_PC_COUNT",
                                                       "VWR_VIDEO_VIEW_50_PC_COUNT","VWR_VIDEO_VIEW_75_PC_COUNT",
                                                       "VWR_VIDEO_VIEW_100_PC_COUNT"],
                                   index=["PLACEMENT_ID","PLACEMENT_DESC","METRIC_DESC","FEV_INT_VIDEO_DESC"],
                                   aggfunc=np.sum)
        KM_reset_vwr = KM_Data_vwr.reset_index()

        try:
            KM_Data_New_vwr=KM_reset_vwr[["PLACEMENT_ID","PLACEMENT_DESC","METRIC_DESC","FEV_INT_VIDEO_DESC",
                                          "VWR_VIDEO_VIEW_0_PC_COUNT","VWR_VIDEO_VIEW_25_PC_COUNT",
                                          "VWR_VIDEO_VIEW_50_PC_COUNT","VWR_VIDEO_VIEW_75_PC_COUNT",
                                          "VWR_VIDEO_VIEW_100_PC_COUNT"]]
        except KeyError:
            KM_Data_New_vwr=KM_reset_vwr[[]]

        KM_Data_DPE = pd.pivot_table(read_sql_KM, values=["DPE_VIDEO_VIEW_0_PC_COUNT", "DPE_VIDEO_VIEW_25_PC_COUNT",
                                                          "DPE_VIDEO_VIEW_50_PC_COUNT", "DPE_VIDEO_VIEW_75_PC_COUNT",
                                                          "DPE_VIDEO_VIEW_100_PC_COUNT"],
                                   index=["PLACEMENT_ID", "PLACEMENT_DESC", "METRIC_DESC", "FEV_INT_VIDEO_DESC"],
                                   aggfunc = np.sum)
        KM_reset_DPE = KM_Data_DPE.reset_index()

        try:
            KM_Data_New_DPE = KM_reset_DPE[["PLACEMENT_ID","PLACEMENT_DESC","METRIC_DESC","FEV_INT_VIDEO_DESC",
                                            "DPE_VIDEO_VIEW_0_PC_COUNT","DPE_VIDEO_VIEW_25_PC_COUNT",
                                            "DPE_VIDEO_VIEW_50_PC_COUNT", "DPE_VIDEO_VIEW_75_PC_COUNT",
                                            "DPE_VIDEO_VIEW_100_PC_COUNT"]]
        except KeyError:
            KM_Data_New_DPE = KM_reset_DPE[[]]

        KM_Data_INT_Eng = pd.pivot_table(read_sql_KM,values=["ENG_MUTE", "ENG_UNMUTE",
                                                             "ENG_PAUSE", "ENG_RESUME",
                                                             "ENG_REWIND", "ENG_REPLAY", "ENG_FULL_SCREEN"],
                                    index =["PLACEMENT_ID", "PLACEMENT_DESC", "METRIC_DESC", "FEV_INT_VIDEO_DESC"],
                                    aggfunc=np.sum)
        KM_reset_INT_Eng = KM_Data_INT_Eng.reset_index()

        try:
            KM_Data_New_INT_Eng = KM_reset_INT_Eng[["PLACEMENT_ID","PLACEMENT_DESC","METRIC_DESC","FEV_INT_VIDEO_DESC",
                                                    "ENG_MUTE", "ENG_UNMUTE", "ENG_PAUSE", "ENG_RESUME",
                                                    "ENG_REWIND", "ENG_REPLAY", "ENG_FULL_SCREEN"]]
        except KeyError:
            KM_Data_New_INT_Eng=KM_reset_INT_Eng[[]]

        KM_Data_INT_vwr=pd.pivot_table(read_sql_KM,values=["VWR_MUTE", "VWR_UNMUTE",
                                                           "VWR_PAUSE", "VWR_RESUME",
                                                           "VWR_REWIND", "VWR_REPLAY","VWR_FULL_SCREEN"],
                                       index=["PLACEMENT_ID", "PLACEMENT_DESC", "METRIC_DESC", "FEV_INT_VIDEO_DESC"],
                                       aggfunc=np.sum)
        KM_reset_INT_vwr=KM_Data_INT_vwr.reset_index()

        try:
            KM_Data_New_INT_vwr=KM_reset_INT_vwr[["PLACEMENT_ID", "PLACEMENT_DESC", "METRIC_DESC", "FEV_INT_VIDEO_DESC",
                                                  "VWR_MUTE", "VWR_UNMUTE", "VWR_PAUSE", "VWR_RESUME",
                                                  "VWR_REWIND", "VWR_REPLAY","VWR_FULL_SCREEN"]]
        except KeyError:
            KM_Data_New_INT_vwr=KM_reset_INT_vwr[[]]

        KM_Data_INT_DPE=pd.pivot_table(read_sql_KM,values=["DPE_MUTE","DPE_UNMUTE",
                                                           "DPE_PAUSE","DPE_RESUME",
                                                           "DPE_REWIND","DPE_REPLAY","DPE_FULL_SCREEN"],
                                       index=["PLACEMENT_ID", "PLACEMENT_DESC", "METRIC_DESC", "FEV_INT_VIDEO_DESC"],
                                       aggfunc=np.sum)
        KM_reset_INT_DPE=KM_Data_INT_DPE.reset_index()

        try:
            KM_Data_New_INT_DPE = KM_reset_INT_DPE[["PLACEMENT_ID", "PLACEMENT_DESC", "METRIC_DESC",
                                                    "FEV_INT_VIDEO_DESC",
                                                    "DPE_MUTE", "DPE_UNMUTE", "DPE_PAUSE", "DPE_RESUME",
                                                    "DPE_REWIND", "DPE_REPLAY", "DPE_FULL_SCREEN"]]
        except KeyError:
            KM_Data_New_INT_DPE = KM_reset_INT_DPE[[]]

        return KM_Data_New_eng, KM_Data_New_vwr, KM_Data_New_DPE, KM_Data_New_INT_Eng,\
        KM_Data_New_INT_vwr, KM_Data_New_INT_DPE

    def access_columns_KM_Video(self):
        KM_Data_New_eng, KM_Data_New_vwr, KM_Data_New_DPE, KM_Data_New_INT_Eng,\
        KM_Data_New_INT_vwr, KM_Data_New_INT_DPE  = self.access_Data_KM_Video()

        accessing_KM_Data_New_eng_columns = KM_Data_New_eng.loc[:, ["PLACEMENT_ID", "PLACEMENT_DESC", "METRIC_DESC",
                                                                        "FEV_INT_VIDEO_DESC",
                                    "ENG_VIDEO_VIEW_0_PC_COUNT", "ENG_VIDEO_VIEW_25_PC_COUNT",
                                    "ENG_VIDEO_VIEW_50_PC_COUNT", "ENG_VIDEO_VIEW_75_PC_COUNT",
                                    "ENG_VIDEO_VIEW_100_PC_COUNT"]]

        accessing_KM_Data_New_vwr_columns = KM_Data_New_vwr.loc[:, ["PLACEMENT_ID","PLACEMENT_DESC","METRIC_DESC","FEV_INT_VIDEO_DESC",
                                          "VWR_VIDEO_VIEW_0_PC_COUNT","VWR_VIDEO_VIEW_25_PC_COUNT",
                                          "VWR_VIDEO_VIEW_50_PC_COUNT","VWR_VIDEO_VIEW_75_PC_COUNT",
                                          "VWR_VIDEO_VIEW_100_PC_COUNT"]]

        accessing_KM_Data_New_DPE_columns = KM_Data_New_DPE.loc[:, ["PLACEMENT_ID","PLACEMENT_DESC","METRIC_DESC","FEV_INT_VIDEO_DESC",
                                            "DPE_VIDEO_VIEW_0_PC_COUNT","DPE_VIDEO_VIEW_25_PC_COUNT",
                                            "DPE_VIDEO_VIEW_50_PC_COUNT", "DPE_VIDEO_VIEW_75_PC_COUNT",
                                            "DPE_VIDEO_VIEW_100_PC_COUNT"]]

        accessing_KM_Data_New_INT_Eng_columns = KM_Data_New_INT_Eng.loc[:, ["PLACEMENT_ID","PLACEMENT_DESC","METRIC_DESC","FEV_INT_VIDEO_DESC",
                                                    "ENG_MUTE", "ENG_UNMUTE", "ENG_PAUSE", "ENG_RESUME",
                                                    "ENG_REWIND", "ENG_REPLAY", "ENG_FULL_SCREEN"]]

        accessing_KM_Data_New_INT_vwr_columns = KM_Data_New_INT_vwr.loc[:, ["PLACEMENT_ID", "PLACEMENT_DESC", "METRIC_DESC", "FEV_INT_VIDEO_DESC",
                                                  "VWR_MUTE", "VWR_UNMUTE", "VWR_PAUSE", "VWR_RESUME",
                                                  "VWR_REWIND", "VWR_REPLAY","VWR_FULL_SCREEN"]]

        accessing_KM_Data_New_INT_DPE_columns = KM_Data_New_INT_DPE.loc[:, ["PLACEMENT_ID", "PLACEMENT_DESC", "METRIC_DESC",
                                                    "FEV_INT_VIDEO_DESC",
                                                    "DPE_MUTE", "DPE_UNMUTE", "DPE_PAUSE", "DPE_RESUME",
                                                    "DPE_REWIND", "DPE_REPLAY", "DPE_FULL_SCREEN"]]

        return accessing_KM_Data_New_eng_columns, accessing_KM_Data_New_vwr_columns, accessing_KM_Data_New_DPE_columns, accessing_KM_Data_New_INT_Eng_columns, accessing_KM_Data_New_INT_vwr_columns, accessing_KM_Data_New_INT_DPE_columns

    def rename_KM_Data_Video(self):
        accessing_KM_Data_New_eng_columns, accessing_KM_Data_New_vwr_columns, accessing_KM_Data_New_DPE_columns, accessing_KM_Data_New_INT_Eng_columns, accessing_KM_Data_New_INT_vwr_columns, accessing_KM_Data_New_INT_DPE_columns = self.access_columns_KM_Video()
        rename_accessing_KM_Data_New_eng_columns = accessing_KM_Data_New_eng_columns.rename(columns={"PLACEMENT_ID":"Placement ID",
                                                                                                     "PLACEMENT_DESC":"Placement#",
                                                                                                     "METRIC_DESC":"Metric",
                                                                                                     "FEV_INT_VIDEO_DESC":"Video Desc",
                                                                                                     "ENG_VIDEO_VIEW_0_PC_COUNT":"ENG video 0 pc",
                                                                                                     "ENG_VIDEO_VIEW_25_PC_COUNT":"ENG video 25 pc",
                                                                                                     "ENG_VIDEO_VIEW_50_PC_COUNT":"ENG video 50 pc",
                                                                                                     "ENG_VIDEO_VIEW_75_PC_COUNT":"ENG video 75 pc",
                                                                                                     "ENG_VIDEO_VIEW_100_PC_COUNT":"ENG video 100 pc"},
                                                                                            inplace=True)
        rename_accessing_KM_Data_New_vwr_columns = accessing_KM_Data_New_vwr_columns.rename(columns={"PLACEMENT_ID":"Placement ID",
                                                                                                     "PLACEMENT_DESC":"Placement#",
                                                                                                     "METRIC_DESC":"Metric",
                                                                                                     "FEV_INT_VIDEO_DESC":"Video Desc",
                                                                                                     "VWR_VIDEO_VIEW_0_PC_COUNT":"VWR video 0 pc",
                                                                                                     "VWR_VIDEO_VIEW_25_PC_COUNT":"VWR video 25 pc",
                                                                                                     "VWR_VIDEO_VIEW_50_PC_COUNT":"VWR video 50 pc",
                                                                                                     "VWR_VIDEO_VIEW_75_PC_COUNT":"VWR video 75 pc",
                                                                                                     "VWR_VIDEO_VIEW_100_PC_COUNT":"VWR video 100 pc"},
                                                                                            inplace=True)
        rename_accessing_KM_Data_New_DPE_columns = accessing_KM_Data_New_DPE_columns.rename(columns={"PLACEMENT_ID":"Placement ID",
                                                                                                     "PLACEMENT_DESC":"Placement#",
                                                                                                     "METRIC_DESC":"Metric",
                                                                                                     "FEV_INT_VIDEO_DESC":"Video Desc",
                                                                                                     "DPE_VIDEO_VIEW_0_PC_COUNT":"DPE video 0 pc",
                                                                                                     "DPE_VIDEO_VIEW_25_PC_COUNT":"DPE video 25 pc",
                                                                                                     "DPE_VIDEO_VIEW_50_PC_COUNT":"DPE video 50 pc",
                                                                                                     "DPE_VIDEO_VIEW_75_PC_COUNT":"DPE video 75 pc",
                                                                                                     "DPE_VIDEO_VIEW_100_PC_COUNT":"DPE video 100 pc"},
                                                                                            inplace=True)
        rename_accessing_KM_Data_New_INT_Eng_columns = accessing_KM_Data_New_INT_Eng_columns.rename(columns={"PLACEMENT_ID":"Placement ID",
                                                                                                         "PLACEMENT_DESC":"Placement#",
                                                                                                         "METRIC_DESC":"Metric",
                                                                                                         "FEV_INT_VIDEO_DESC":"Video Desc",
                                                                                                         "ENG_MUTE":"ENG MUTE",
                                                                                                         "ENG_UNMUTE":"ENG UNMUTE",
                                                                                                         "ENG_PAUSE":"ENG PAUSE",
                                                                                                         "ENG_RESUME":"ENG RESUME",
                                                                                                         "ENG_REWIND":"ENG REWIND",
                                                                                                         "ENG_REPLAY":"ENG REPLAY",
                                                                                                         "ENG_FULL_SCREEN":"ENG FULL SCREEN"},
                                                                                                inplace=True)
        rename_access_KM_Data_New_INT_vwr_columns = accessing_KM_Data_New_INT_vwr_columns.rename(columns={"PLACEMENT_ID":"Placement ID",
                                                                                                      "PLACEMENT_DESC":"Placement#",
                                                                                                      "METRIC_DESC":"Metric",
                                                                                                      "FEV_INT_VIDEO_DESC":"Video Desc",
                                                                                                      "VWR_MUTE":"VWR MUTE",
                                                                                                      "VWR_UNMUTE":"VWR UNMUTE",
                                                                                                      "VWR_PAUSE":"VWR PAUSE",
                                                                                                      "VWR_RESUME":"VWR RESUME",
                                                                                                      "VWR_REWIND":"VWR REWIND",
                                                                                                      "VWR_REPLAY":"VWR REPLAY",
                                                                                                      "VWR_FULL_SCREEN":"VWR FULL SCREEN"},
                                                                                             inplace=True)
        rename_access_KM_Data_New_INT_DPE_columns = accessing_KM_Data_New_INT_DPE_columns.rename(columns={"PLACEMENT_ID":"Placement ID",
                                                                                                      "PLACEMENT_DESC":"Placement#",
                                                                                                      "METRIC_DESC":"Metric",
                                                                                                      "FEV_INT_VIDEO_DESC":"Video Desc",
                                                                                                      "DPE_MUTE":"DPE MUTE",
                                                                                                      "DPE_UNMUTE":"DPE UNMUTE",
                                                                                                      "DPE_PAUSE":"DPE PAUSE",
                                                                                                      "DPE_RESUME":"DPE RESUME",
                                                                                                      "DPE_REWIND":"DPE REWIND",
                                                                                                      "DPE_REPLAY":"DPE REPLAY",
                                                                                                      "DPE_FULL_SCREEN":"DPE FULL SCREEN"},
                                                                                             inplace=True)
        return accessing_KM_Data_New_eng_columns,accessing_KM_Data_New_vwr_columns,accessing_KM_Data_New_DPE_columns,accessing_KM_Data_New_INT_Eng_columns,accessing_KM_Data_New_INT_vwr_columns, accessing_KM_Data_New_INT_DPE_columns

    def write_video_data(self):
        data_common_columns=self.config.common_columns_summary()
        accessing_KM_Data_New_eng_columns,accessing_KM_Data_New_vwr_columns,accessing_KM_Data_New_DPE_columns,\
        accessing_KM_Data_New_INT_Eng_columns,accessing_KM_Data_New_INT_vwr_columns,\
        accessing_KM_Data_New_INT_DPE_columns = self.rename_KM_Data_Video()

        replace_accessing_KM_Data_New_eng_columns_zero = accessing_KM_Data_New_eng_columns.fillna(0)
        replace_accessing_KM_Data_New_vwr_columns_zero = accessing_KM_Data_New_vwr_columns.fillna(0)
        replace_accessing_KM_Data_New_DPE_columns_zero = accessing_KM_Data_New_DPE_columns.fillna(0)
        replace_accessing_KM_Data_New_INT_Eng_columns_zero = accessing_KM_Data_New_INT_Eng_columns.fillna(0)
        replace_accessing_KM_Data_New_INT_vwr_columns_zero = accessing_KM_Data_New_INT_vwr_columns.fillna(0)
        replace_accessing_KM_Data_New_INT_DPE_columns_zero = accessing_KM_Data_New_INT_DPE_columns.fillna(0)


        writing_data_common_columns = data_common_columns[1].to_excel(self.config.writer,
                                                                      sheet_name="Video Performance({})".format(
                                                                      self.config.IO_ID),startcol=0,startrow=7,
                                                                      index=False,header=False)

        writing_accessing_KM_Data_New_eng_columns = replace_accessing_KM_Data_New_eng_columns_zero.to_excel(self.config.writer,
                                                               sheet_name="Video Performance({})".format(
                                                               self.config.IO_ID),startcol=0,startrow=12,
                                                               index=False,header=True)

        writing_accessing_KM_Data_New_vwr_columns = replace_accessing_KM_Data_New_vwr_columns_zero.to_excel(self.config.writer,
                                                                                             sheet_name="Video Performance({})".format(
                                                                                             self.config.IO_ID),
                                                                                             startcol=0,startrow=len(accessing_KM_Data_New_eng_columns)+16,
                                                                                             index=False,header=True)

        writing_accessing_KM_Data_New_DPE_columns = replace_accessing_KM_Data_New_DPE_columns_zero.to_excel(self.config.writer,
                                                                                             sheet_name="Video Performance({})".format(
                                                                                             self.config.IO_ID),
                                                                                             startcol=0,startrow= len(accessing_KM_Data_New_eng_columns)+ len(accessing_KM_Data_New_vwr_columns)+20,
                                                                                             index=False,header=True)

        writing_accessing_KM_Data_New_INT_Eng_columns = replace_accessing_KM_Data_New_INT_Eng_columns_zero.to_excel(self.config.writer,
                                                                                             sheet_name="Video Performance({})".format(
                                                                                                 self.config.IO_ID),
                                                                                             startcol=10,startrow = 12,
                                                                                             index=False,header=True)

        writing_accessing_KM_Data_New_INT_vwr_columns = replace_accessing_KM_Data_New_INT_vwr_columns_zero.to_excel(self.config.writer,
                                                                                             sheet_name="Video Performance({})".format(
                                                                                                 self.config.IO_ID),
                                                                                             startcol=10,startrow= len(accessing_KM_Data_New_INT_Eng_columns)+16,
                                                                                             index=False,header=True)

        writing_accessing_KM_Data_New_INT_DPE_columns = replace_accessing_KM_Data_New_INT_DPE_columns_zero.to_excel(self.config.writer,
                                                                                             sheet_name ="Video Performance({})".format(self.config.IO_ID),
                                                                                             startcol=10, startrow = len(accessing_KM_Data_New_INT_Eng_columns)+len(accessing_KM_Data_New_INT_vwr_columns)+20,
                                                                                             index=False,header=True)
    def formatting_Video(self):
        accessing_KM_Data_New_eng_columns,accessing_KM_Data_New_vwr_columns,accessing_KM_Data_New_DPE_columns,\
        accessing_KM_Data_New_INT_Eng_columns,accessing_KM_Data_New_INT_vwr_columns,\
        accessing_KM_Data_New_INT_DPE_columns=self.rename_KM_Data_Video()

        number_rows_accessing_KM_Data_New_eng_columns = accessing_KM_Data_New_eng_columns.shape[0]
        number_rows_accessing_KM_Data_New_vwr_columns = accessing_KM_Data_New_vwr_columns.shape[0]
        number_rows_accessing_KM_Data_New_DPE_columns = accessing_KM_Data_New_DPE_columns.shape[0]
        number_rows_accessing_KM_Data_New_INT_Eng_columns = accessing_KM_Data_New_INT_Eng_columns.shape[0]
        number_rows_accessing_KM_Data_New_INT_vwr_columns = accessing_KM_Data_New_INT_vwr_columns.shape[0]
        number_rows_accessing_KM_Data_New_INT_DPE_columns = accessing_KM_Data_New_INT_DPE_columns.shape[0]

        workbook=self.config.writer.book
        worksheet=self.config.writer.sheets["Video Performance({})".format(self.config.IO_ID)]
        alignment=workbook.add_format({"align":"center"})
        worksheet.hide_gridlines(2)
        worksheet.insert_image("A1","Exponential.png")
        format_common_column={"header_row":False,"style":"Table Style Medium 2",'autofilter':False}
        worksheet.add_table("A8:F10", format_common_column)
        format_merge_row=workbook.add_format({"bold":True,"font_color":'#FFFFFF',"align":"left",
                                              "fg_color":"#8EE5EE"})
        worksheet.merge_range("A7:I7","Video Performance",format_merge_row)
        worksheet.merge_range("A12:I12","Video Performance - Engager Metrics", format_merge_row)
        worksheet.merge_range("A{}:I{}".format(number_rows_accessing_KM_Data_New_eng_columns+16,number_rows_accessing_KM_Data_New_eng_columns+16),"Video Performance - Viewer Metrics", format_merge_row)
        worksheet.merge_range("A{}:I{}".format(number_rows_accessing_KM_Data_New_eng_columns+number_rows_accessing_KM_Data_New_vwr_columns+20,number_rows_accessing_KM_Data_New_eng_columns+number_rows_accessing_KM_Data_New_vwr_columns+20),"Video Performance - Deep Engager Metrics", format_merge_row)
        worksheet.merge_range("K12:U12","Player Interactions - Engager Metrics", format_merge_row)
        worksheet.merge_range("K{}:U{}".format(number_rows_accessing_KM_Data_New_INT_Eng_columns+16,number_rows_accessing_KM_Data_New_INT_vwr_columns+16),"Player Interactions - Viewer Metrics",format_merge_row)
        worksheet.merge_range("K{}:U{}".format(number_rows_accessing_KM_Data_New_INT_Eng_columns + number_rows_accessing_KM_Data_New_INT_vwr_columns+20,number_rows_accessing_KM_Data_New_INT_Eng_columns + number_rows_accessing_KM_Data_New_INT_vwr_columns+20),"Player Interactions - Deep Engager Metrics",format_merge_row)

        full_border=workbook.add_format({"border":1,"border_color":"#8EE5EE","align":"center",
                                         "fg_color":"#8EE5EE","bold":True})

        data_border_style=workbook.add_format({"border":1,"border_color":"#8EE5EE"})

        worksheet.freeze_panes(13,2)
        worksheet.set_zoom(80)

        for col in range(4, 9):
            cell_location = xl_rowcol_to_cell(number_rows_accessing_KM_Data_New_eng_columns+13, col)
            start_range = xl_rowcol_to_cell(13, col)
            end_range = xl_rowcol_to_cell(number_rows_accessing_KM_Data_New_eng_columns+12, col)
            formula = "=SUM({:s}:{:s})".format(start_range, end_range)
            worksheet.write_formula(cell_location, formula, full_border)
        worksheet.write_string(number_rows_accessing_KM_Data_New_eng_columns+13, 0, "Total",full_border)

        for col in range(4, 9):
            cell_location = xl_rowcol_to_cell(number_rows_accessing_KM_Data_New_eng_columns+number_rows_accessing_KM_Data_New_vwr_columns+17, col)
            start_range = xl_rowcol_to_cell(number_rows_accessing_KM_Data_New_eng_columns+17, col)
            end_range = xl_rowcol_to_cell(number_rows_accessing_KM_Data_New_eng_columns+number_rows_accessing_KM_Data_New_vwr_columns+16, col)
            formula = "=SUM({:s}:{:s})".format(start_range, end_range)
            worksheet.write_formula(cell_location, formula, full_border)
            
        worksheet.write_string(number_rows_accessing_KM_Data_New_eng_columns+number_rows_accessing_KM_Data_New_vwr_columns+17,0,"Total",full_border)

        for col in range(4,9):
            cell_location = xl_rowcol_to_cell(number_rows_accessing_KM_Data_New_eng_columns+number_rows_accessing_KM_Data_New_vwr_columns+number_rows_accessing_KM_Data_New_DPE_columns+21,col)
            start_range = xl_rowcol_to_cell(number_rows_accessing_KM_Data_New_eng_columns+number_rows_accessing_KM_Data_New_vwr_columns+21,col)
            end_range = xl_rowcol_to_cell(number_rows_accessing_KM_Data_New_eng_columns+number_rows_accessing_KM_Data_New_vwr_columns+number_rows_accessing_KM_Data_New_DPE_columns+20,col)
            formula= "=SUM({:s}:{:s})".format(start_range,end_range)
            worksheet.write_formula(cell_location,formula,full_border)

        worksheet.write_string(number_rows_accessing_KM_Data_New_eng_columns+number_rows_accessing_KM_Data_New_vwr_columns+number_rows_accessing_KM_Data_New_DPE_columns+21,0,"Total",full_border)

        for col in range(14,21):
            cell_location=xl_rowcol_to_cell(number_rows_accessing_KM_Data_New_INT_Eng_columns+13,col)
            start_range=xl_rowcol_to_cell(13,col)
            end_range=xl_rowcol_to_cell(number_rows_accessing_KM_Data_New_INT_Eng_columns+12,col)
            formula="=SUM({:s}:{:s})".format(start_range,end_range)
            worksheet.write_formula(cell_location,formula,full_border)
        worksheet.write_string(number_rows_accessing_KM_Data_New_INT_Eng_columns+13,10,"Total",full_border)

        for col in range(14,21):
            cell_location=xl_rowcol_to_cell(number_rows_accessing_KM_Data_New_INT_Eng_columns+number_rows_accessing_KM_Data_New_INT_vwr_columns+17,col)
            start_range=xl_rowcol_to_cell(number_rows_accessing_KM_Data_New_INT_Eng_columns+17,col)
            end_range=xl_rowcol_to_cell(number_rows_accessing_KM_Data_New_INT_Eng_columns+number_rows_accessing_KM_Data_New_INT_vwr_columns+16,col)
            formula="=SUM({:s}:{:s})".format(start_range,end_range)
            worksheet.write_formula(cell_location,formula,full_border)

        worksheet.write_string(number_rows_accessing_KM_Data_New_INT_Eng_columns+number_rows_accessing_KM_Data_New_INT_vwr_columns+17,10,"Total",full_border)

        for col in range(14,21):
            cell_location=xl_rowcol_to_cell(number_rows_accessing_KM_Data_New_INT_Eng_columns+number_rows_accessing_KM_Data_New_INT_vwr_columns+number_rows_accessing_KM_Data_New_INT_DPE_columns+21,col)
            start_range=xl_rowcol_to_cell(number_rows_accessing_KM_Data_New_INT_Eng_columns+number_rows_accessing_KM_Data_New_INT_vwr_columns+21,col)
            end_range=xl_rowcol_to_cell(number_rows_accessing_KM_Data_New_INT_Eng_columns+number_rows_accessing_KM_Data_New_INT_vwr_columns+number_rows_accessing_KM_Data_New_INT_DPE_columns+20,col)
            formula="=SUM({:s}:{:s})".format(start_range,end_range)
            worksheet.write_formula(cell_location,formula,full_border)

        worksheet.write_string(number_rows_accessing_KM_Data_New_INT_Eng_columns+number_rows_accessing_KM_Data_New_INT_vwr_columns+number_rows_accessing_KM_Data_New_INT_DPE_columns+21,10,"Total",full_border)

        worksheet.set_column("A:A",15,alignment)
        worksheet.set_column("B:B",80,alignment)
        worksheet.set_column("C:C",20,alignment)
        worksheet.set_column("D:D",15,alignment)
        worksheet.set_column("E:E",20,alignment)
        worksheet.set_column("F:F",20,alignment)
        worksheet.set_column("G:G",15,alignment)
        worksheet.set_column("H:H",15,alignment)
        worksheet.set_column("I:I",15,alignment)

        worksheet.set_column("K:K",15,alignment)
        worksheet.set_column("L:L",80,alignment)
        worksheet.set_column("M:M",18,alignment)
        worksheet.set_column("N:N",15,alignment)
        worksheet.set_column("O:O",15,alignment)
        worksheet.set_column("P:P",15,alignment)
        worksheet.set_column("Q:Q",15,alignment)
        worksheet.set_column("R:R",15,alignment)
        worksheet.set_column("S:S",15,alignment)
        worksheet.set_column("T:T",15,alignment)
        worksheet.set_column("U:U",15,alignment)

        worksheet.conditional_format("A14:I{}".format(number_rows_accessing_KM_Data_New_eng_columns+13),
                                     {"type":"no_blanks","format":data_border_style})

        worksheet.conditional_format("A{}:I{}".format(number_rows_accessing_KM_Data_New_eng_columns+17,number_rows_accessing_KM_Data_New_eng_columns+number_rows_accessing_KM_Data_New_vwr_columns+17),
                                     {"type":"no_blanks","format":data_border_style})

        worksheet.conditional_format("A{}:I{}".format(number_rows_accessing_KM_Data_New_eng_columns+number_rows_accessing_KM_Data_New_vwr_columns+21,number_rows_accessing_KM_Data_New_eng_columns+number_rows_accessing_KM_Data_New_vwr_columns+number_rows_accessing_KM_Data_New_DPE_columns+21),
                                     {"type":"no_blanks","format":data_border_style})

        worksheet.conditional_format("K14:U{}".format(number_rows_accessing_KM_Data_New_INT_Eng_columns+13),
                                     {"type":"no_blanks","format":data_border_style})

        worksheet.conditional_format("K{}:U{}".format(number_rows_accessing_KM_Data_New_INT_Eng_columns+17,
                                                      number_rows_accessing_KM_Data_New_INT_Eng_columns+number_rows_accessing_KM_Data_New_INT_vwr_columns+17),
                                     {"type":"no_blanks","format":data_border_style})

        worksheet.conditional_format("K{}:U{}".format(number_rows_accessing_KM_Data_New_INT_Eng_columns+number_rows_accessing_KM_Data_New_INT_vwr_columns+21,number_rows_accessing_KM_Data_New_INT_Eng_columns+number_rows_accessing_KM_Data_New_INT_vwr_columns+number_rows_accessing_KM_Data_New_INT_DPE_columns+21),
                                     {"type":"no_blanks","format":data_border_style})

    def main(self):
            self.config.common_columns_summary()
            self.connect_TFR_Video()
            self.read_Query_Video()
            self.access_Data_KM_Video()
            self.access_columns_KM_Video()
            self.rename_KM_Data_Video()
            self.write_video_data()
            self.formatting_Video()

    if __name__=="__main__":
        pass
        #enable it when running for individual file
        #c=config.Config('dial',565337)
        #o = ad_Size(c)
        #o.main()
        #c.saveAndCloseWriter()




















