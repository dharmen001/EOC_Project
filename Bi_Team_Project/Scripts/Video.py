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

        def access_Data_KM_Video(self):
            KM_Data_New_eng, KM_Data_New_vwr, KM_Data_New_DPE, KM_Data_New_INT_Eng,\
            KM_Data_New_INT_vwr, KM_Data_New_INT_DPE  = self.access_Data_KM_Sales_a()

            accessing_KM_Data_New_eng_columns = KM_Data_New_eng.loc[:, []]

            accessing_KM_Data_New_vwr_columns = KM_Data_New_vwr.loc[:, []]

            accessing_KM_Data_New_DPE_columns = KM_Data_New_DPE.loc[:, []]

            accessing_KM_Data_New_INT_Eng_columns = KM_Data_New_INT_Eng.loc[:, []]

            access_KM_Data_New_INT_vwr_columns = KM_Data_New_INT_vwr.loc[:, []]

            access_KM_Data_New_INT_DPE_columns = KM_Data_New_INT_DPE.loc[:, []]

            return accessing_KM_Data_New_eng_columns, accessing_KM_Data_New_vwr_columns, \
                   accessing_KM_Data_New_DPE_columns, accessing_KM_Data_New_INT_Eng_columns, \
                   access_KM_Data_New_INT_vwr_columns, access_KM_Data_New_INT_DPE_columns













