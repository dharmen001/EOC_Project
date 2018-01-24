import pandas as pd
import numpy as np
from xlsxwriter.utility import xl_rowcol_to_cell

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
        KM_Data = pd.pivot_table(read_sql_KM, index=["PLACEMENT_DESC", "METRIC_DESC"], columns=["BLAZE_ACTION_TYPE_DESC",
                                                                                                "BLAZE_TAG_NAME_DESC"],
                                 values= ["ENG_INTERACTION"], aggfunc=np.sum)
        KM_reset=KM_Data.reset_index()
        print KM_reset

    def main(self):
        self.config.common_columns_summary()
        self.connect_TFR_Intraction()
        self.read_Query_Intraction()
        self.access_Data_Intraction()
        #self.KM_Sales_ad_Size()
        #self.rename_KM_Sales_ad_Size()
        #self.adding_vcr_ctr_ad_Size()
        #self.write_KM_Sales_ad_Size()
        #self.formatting_ad_Size()

    if __name__ == "__main__":
        pass
        #enable it when running for individual file
        #c=config.Config('dial',565337)
        #o = ad_Size(c)
        #o.main()
        #c.saveAndCloseWriter()



