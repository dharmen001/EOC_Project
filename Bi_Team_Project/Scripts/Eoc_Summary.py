# coding=utf-8
from __future__ import print_function
import pandas as pd
import numpy as np
from xlsxwriter.utility import xl_rowcol_to_cell
import config


class Summary():
    def __init__(self,config):
        self.config=config

    def connect_TFR_summary(self):

        sql_VDX_summary="select * from (select substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as "'Placement#'", SDATE as "'Start_Date'", EDATE as "'End_Date'", initcap(CREATIVE_DESC)  as "'Placement_Name'",COST_TYPE_DESC as "'Cost_type'",UNIT_COST as "'Unit_Cost'",BUDGET as "'Planned_Cost'",BOOKED_QTY as "'Booked_Imp#Booked_Eng'" from TFR_REP.SUMMARY_MV where (IO_ID = {}) AND (DATA_SOURCE = 'KM') AND CREATIVE_DESC IN(SELECT DISTINCT CREATIVE_DESC FROM TFR_REP.SUMMARY_MV) ORDER BY PLACEMENT_ID) WHERE Placement_Name Not IN ('Pre-Roll - Desktop','Pre-Roll - Desktop + Mobile','Pre-Roll – Desktop + Mobile','Pre-Roll - In-Stream/Mobile Blend','Pre-Roll - Mobile','Pre-Roll -Desktop','Pre-Roll - In-Stream')".format(
            self.config.IO_ID)
        sql_VDX_MV="select substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as "'Placement#'", sum(IMPRESSIONS) as "'Impression'", sum(ENGAGEMENTS) as "'Eng'", sum(DPE_ENGAGEMENTS) as "'Deep'", sum(CPCV_COUNT) as "'Completions'" from TFR_REP.KEY_METRIC_MV WHERE IO_ID = {} GROUP BY PLACEMENT_ID, PLACEMENT_DESC ORDER BY PLACEMENT_ID".format(
            self.config.IO_ID)
        sql_Display_summary="select substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as "'Placement#'", SDATE as "'Start_Date'", EDATE as "'End_Date'", CREATIVE_DESC  as "'Placement_Name'", COST_TYPE_DESC as "'Cost_type'",UNIT_COST as "'Unit_Cost'",BUDGET as "'Planned_Cost'", BOOKED_QTY as "'Booked_Imp#Booked_Eng'" FROM TFR_REP.SUMMARY_MV where IO_ID = {} AND DATA_SOURCE = 'SalesFile' ORDER BY PLACEMENT_ID".format(
            self.config.IO_ID)
        sql_Display_MV="select substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as "'Placement#'", sum(VIEWS) as "'Delivered_Impresion'", sum(CLICKS) as "'Clicks'", sum(CONVERSIONS) as "'Conversion'" from TFR_REP.DAILY_SALES_MV WHERE IO_ID = {} GROUP BY PLACEMENT_ID, PLACEMENT_DESC ORDER BY PLACEMENT_ID".format(
            self.config.IO_ID)
        sql_preroll_summary="select * from (select substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as "'Placement#'", SDATE as "'Start_Date'", EDATE as "'End_Date'", initcap(CREATIVE_DESC)  as "'Placement_Name'",COST_TYPE_DESC as "'Cost_type'",UNIT_COST as "'Unit_Cost'",BUDGET as "'Planned_Cost'",BOOKED_QTY as "'Booked_Imp#Booked_Eng'" from TFR_REP.SUMMARY_MV where (IO_ID = {}) AND (DATA_SOURCE = 'KM') AND CREATIVE_DESC IN(SELECT DISTINCT CREATIVE_DESC FROM TFR_REP.SUMMARY_MV) ORDER BY PLACEMENT_ID) WHERE Placement_Name IN ('Pre-Roll - Desktop','Pre-Roll - Desktop + Mobile','Pre-Roll – Desktop + Mobile','Pre-Roll - In-Stream/Mobile Blend','Pre-Roll - Mobile','Pre-Roll -Desktop','Pre-Roll - In-Stream')".format(
            self.config.IO_ID)
        sql_preroll_mv="select substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as "'Placement#'", sum(IMPRESSIONS) as "'Impression'", sum(CPCV_COUNT) as "'Completions'" from TFR_REP.KEY_METRIC_MV WHERE IO_ID = {} GROUP BY PLACEMENT_ID, PLACEMENT_DESC ORDER BY PLACEMENT_ID".format(
            self.config.IO_ID)

        return sql_VDX_summary,sql_Display_summary,sql_preroll_summary,sql_Display_MV,sql_VDX_MV,sql_preroll_mv

    def read_query_summary(self):
        sql_VDX_summary,sql_Display_summary,sql_preroll_summary,sql_Display_MV,sql_VDX_MV,sql_preroll_mv=self.connect_TFR_summary()
        read_sql_VDX=pd.read_sql(sql_VDX_summary,self.config.conn)
        read_sql_Display=pd.read_sql(sql_Display_summary,self.config.conn)
        read_sql_preroll=pd.read_sql(sql_preroll_summary,self.config.conn)
        read_sql_Display_mv=pd.read_sql(sql_Display_MV,self.config.conn)
        read_sql_VDX_mv=pd.read_sql(sql_VDX_MV,self.config.conn)
        read_sql_preroll_mv=pd.read_sql(sql_preroll_mv,self.config.conn)

        return read_sql_VDX,read_sql_Display,read_sql_preroll,read_sql_Display_mv,read_sql_VDX_mv,read_sql_preroll_mv

    def access_data_summary(self):
        read_sql_VDX,read_sql_Display,read_sql_preroll,read_sql_Display_MV,read_sql_VDX_mv,read_sql_preroll_mv=self.read_query_summary()

        display_first_Summary=read_sql_Display.merge(read_sql_Display_MV,on="PLACEMENT#",how="inner",
                                                     suffixes=('_1','_2'))

        display_first_Table=display_first_Summary[["PLACEMENT#","START_DATE","END_DATE","PLACEMENT_NAME",
                                                   "COST_TYPE","UNIT_COST","PLANNED_COST",
                                                   "BOOKED_IMP#BOOKED_ENG","DELIVERED_IMPRESION"]]

        vdx_second_summary=read_sql_VDX.merge(read_sql_VDX_mv,on="PLACEMENT#",how="inner",suffixes=('_1','_2'))
        vdx_second_Table=vdx_second_summary[["PLACEMENT#","START_DATE","END_DATE","PLACEMENT_NAME",
                                             "COST_TYPE","UNIT_COST","PLANNED_COST","BOOKED_IMP#BOOKED_ENG",
                                             "IMPRESSION","ENG","DEEP","COMPLETIONS"]]

        conditions_ENG=[(vdx_second_Table.loc[:,['COST_TYPE']]=='CPE'),
                        (vdx_second_Table.loc[:,['COST_TYPE']]=='CPE+')]
        choices_ENG=[vdx_second_Table.loc[:,["ENG"]],
                     vdx_second_Table.loc[:,["DEEP"]]]

        try:
            vdx_second_Table['Delivered_Engagements']=np.select(conditions_ENG,choices_ENG,default=0)
        except ValueError as e:
            pass

        conditions_IMP=[(vdx_second_Table.loc[:,['COST_TYPE']]=='CPCV'),
                        (vdx_second_Table.loc[:,['COST_TYPE']]=='CPM')]
        choices_IMP=[vdx_second_Table.loc[:,["COMPLETIONS"]],
                     vdx_second_Table.loc[:,["IMPRESSION"]]]
        try:
            vdx_second_Table['Delivered_Impressions']=np.select(conditions_IMP,choices_IMP,default=0)
        except ValueError as e:
            pass

        try:
            vdx_access_table=vdx_second_Table[["PLACEMENT#","START_DATE","END_DATE","PLACEMENT_NAME",
                                               "COST_TYPE","UNIT_COST","PLANNED_COST",
                                               "BOOKED_IMP#BOOKED_ENG","Delivered_Engagements","Delivered_Impressions"]]
        except KeyError as e:
            vdx_access_table=vdx_second_Table[[]]

        preroll_third_summary=read_sql_preroll.merge(read_sql_preroll_mv,on="PLACEMENT#",how="inner",
                                                     suffixes=('_1','_2'))

        preroll_third_table=preroll_third_summary[["PLACEMENT#","START_DATE","END_DATE","PLACEMENT_NAME","COST_TYPE",
                                                   "UNIT_COST","PLANNED_COST","BOOKED_IMP#BOOKED_ENG","IMPRESSION",
                                                   "COMPLETIONS"]]

        conditions_CPCV=[(preroll_third_table.loc[:,['COST_TYPE']]=='CPCV')]
        choices_CPCV=[preroll_third_table.loc[:,['COMPLETIONS']]]

        try:
            preroll_third_table['Delivered_Impressions']=np.select(conditions_CPCV,choices_CPCV,
                                                                   default=preroll_third_table.loc[:,["IMPRESSION"]])
        except ValueError as e:
            pass

        try:
            preroll_access_table=preroll_third_table[["PLACEMENT#","START_DATE","END_DATE","PLACEMENT_NAME","COST_TYPE",
                                                      "UNIT_COST","PLANNED_COST","BOOKED_IMP#BOOKED_ENG",
                                                      "Delivered_Impressions"]]
        except KeyError as e:
            preroll_access_table=preroll_third_table[[]]

        return display_first_Table,vdx_access_table,preroll_access_table

    def summary_creation(self):
        display_first_Table,vdx_access_table,preroll_access_table=self.access_data_summary()

        try:
            display_first_Table['Delivery%']=display_first_Table['DELIVERED_IMPRESION']/display_first_Table[
                'BOOKED_IMP#BOOKED_ENG']
        except KeyError as e:
            pass
        try:
            display_first_Table['Spend']=display_first_Table['DELIVERED_IMPRESION']/1000*display_first_Table[
                'UNIT_COST']
        except KeyError as e:
            pass

        try:
            display_first_Table["PLACEMENT#"]=display_first_Table["PLACEMENT#"].astype(int)
        except KeyError as e:
            pass

        try:
            vdx_access_table["Delivered_Engagements"]=vdx_access_table["Delivered_Engagements"].astype(int)
        except KeyError as e:
            pass
        try:
            vdx_access_table["Delivered_Impressions"]=vdx_access_table["Delivered_Impressions"].astype(int)
        except KeyError as e:
            pass

        try:
            vdx_access_table["PLACEMENT#"]=vdx_access_table["PLACEMENT#"].astype(int)
        except KeyError as e:
            pass

        try:
            choices_vdx_eng=vdx_access_table["Delivered_Engagements"]/vdx_access_table["BOOKED_IMP#BOOKED_ENG"]
            choices_vdx_cpcv=vdx_access_table["Delivered_Impressions"]/vdx_access_table["BOOKED_IMP#BOOKED_ENG"]
            choices_vdx_eng_spend=vdx_access_table["Delivered_Engagements"]*vdx_access_table["UNIT_COST"]
            choices_vdx_cpcv_spend=vdx_access_table["Delivered_Impressions"]/1000*vdx_access_table["UNIT_COST"]
        except KeyError as e:
            pass

        try:
            mask1=vdx_access_table["COST_TYPE"].isin(['CPE','CPE+'])
            mast2=vdx_access_table["COST_TYPE"].isin(['CPM','CPCV'])
            vdx_access_table['Delivery%']=np.select([mask1,mast2],[choices_vdx_eng,choices_vdx_cpcv],default=0.00)
            vdx_access_table['Spend']=np.select([mask1,mast2],[choices_vdx_eng_spend,choices_vdx_cpcv_spend],
                                                default=0.00)
        except KeyError as e:
            pass

        try:
            vdx_access_table['Delivery%']=vdx_access_table['Delivery%'].replace(np.inf,0.00)
            vdx_access_table['Spend']=vdx_access_table['Spend'].replace(np.inf,0.00)
        except KeyError as e:
            pass

        try:
            preroll_access_table["PLACEMENT#"]=preroll_access_table["PLACEMENT#"].astype(int)
        except KeyError as e:
            pass

        try:
            preroll_access_table['Delivery%']=preroll_access_table["Delivered_Impressions"]/preroll_access_table[
                "BOOKED_IMP#BOOKED_ENG"]
            preroll_access_table['Spend']=preroll_access_table["Delivered_Impressions"]/1000*preroll_access_table[
                "UNIT_COST"]
        except KeyError as e:
            pass

        try:
            preroll_access_table['Delivery%']=preroll_access_table['Delivery%'].replace(np.inf,0.00)
            preroll_access_table['Spend']=preroll_access_table['Spend'].replace(np.inf,0.00)
        except KeyError as e:
            pass

        return display_first_Table,vdx_access_table,preroll_access_table

    def rename_cols_sumary(self):
        display_first_Table,vdx_access_table,preroll_access_table=self.summary_creation()

        display_rename=display_first_Table.rename(columns={"PLACEMENT#":"Placement#",
                                                           "START_DATE":"Start Date","END_DATE":"End Date",
                                                           "PLACEMENT_NAME":"Placement Name",
                                                           "COST_TYPE":"Cost Type","UNIT_COST":"Unit Cost",
                                                           "PLANNED_COST":"Planned Cost",
                                                           "BOOKED_IMP#BOOKED_ENG":"Booked",
                                                           "DELIVERED_IMPRESION":"Delivered_Impressions"},
                                                  inplace=True)

        vdx_rename=vdx_access_table.rename(
            columns={"PLACEMENT#":"Placement#","START_DATE":"Start Date","END_DATE":"End Date",
                     "PLACEMENT_NAME":"Placement Name",
                     "COST_TYPE":"Cost Type","UNIT_COST":"Unit Cost",
                     "PLANNED_COST":"Planned Cost","BOOKED_IMP#BOOKED_ENG":"Booked"},
            inplace=True)

        preroll_rename=preroll_access_table.rename(
            columns={"PLACEMENT#":"Placement#","START_DATE":"Start Date","END_DATE":"End Date",
                     "PLACEMENT_NAME":"Placement Name",
                     "COST_TYPE":"Cost Type","UNIT_COST":"Unit Cost",
                     "PLANNED_COST":"Planned Cost","BOOKED_IMP#BOOKED_ENG":"Booked"},
            inplace=True)

        return display_first_Table,vdx_access_table,preroll_access_table

    def write_summary(self):

        display_first_Table,vdx_access_table,preroll_access_table=self.rename_cols_sumary()
        data_common_columns=self.config.common_columns_summary()

        summary=data_common_columns[1].to_excel(self.config.writer,sheet_name="Summary({})".format(self.config.IO_ID),
                                                startcol=1,
                                                startrow=1,index=False,header=False)
        #offset += len(data_common_columns[1])+5

        """if display_first_Table.empty==True or vdx_access_table.empty == True or preroll_access_table.empty == True:
            pass
        else:
            for df in(display_first_Table, vdx_access_table, preroll_access_table):
                df.to_excel(self.config.writer,sheet_name="Summary({})".format(self.config.IO_ID),
                                                         startcol=3,startrow=offset,
                                                         header=True,index=False)
                offset += len(df)+5"""
        #offset = 8
        check_disp_empty=display_first_Table.empty

        if check_disp_empty==True:
            pass
        else:
            display_write=display_first_Table.to_excel(self.config.writer,
                                                       sheet_name="Summary({})".format(self.config.IO_ID),
                                                       startcol=3,startrow=8,
                                                       header=True,index=False)

        check_vdx_empty=vdx_access_table.empty

        if check_vdx_empty==True:
            pass
        else:
            vdx_write=vdx_access_table.to_excel(self.config.writer,sheet_name="Summary({})".format(self.config.IO_ID),
                                                startcol=3,startrow=len(display_first_Table)+13,
                                                header=True,index=False)

        check_preroll_empty=preroll_access_table.empty

        if check_preroll_empty==True:
            pass
        else:

            preroll_write=preroll_access_table.to_excel(self.config.writer,
                                                        sheet_name="Summary({})".format(self.config.IO_ID),
                                                        startcol=3,
                                                        startrow=len(display_first_Table)+len(vdx_access_table)+18,
                                                        header=True,index=False)

        return display_first_Table,vdx_access_table,preroll_access_table

    def format_summary(self):

        data_common_columns=self.config.common_columns_summary()
        display_first_Table,vdx_access_table,preroll_access_table=self.write_summary()
        #print (len("length of display{}".format(display_first_Table)))
        #print (len("length of vdx{}".format(vdx_access_table)))
        #print (len("length of preroll{}".format(preroll_access_table)))
        workbook=self.config.writer.book
        worksheet=self.config.writer.sheets["Summary({})".format(self.config.IO_ID)]
        worksheet.hide_gridlines(2)
        worksheet.set_row(0,6)
        worksheet.set_column("A:A",2)

        check_disp_empty=display_first_Table.empty
        check_vdx_empty=vdx_access_table.empty
        check_preroll_empty=preroll_access_table.empty

        worksheet.insert_image("H2","Exponential.png",{"url":"https://www.tribalfusion.com"})
        worksheet.insert_image("I2","Client_Logo.png")

        number_rows_commom=data_common_columns[1].shape[0]
        number_cols_common=data_common_columns[1].shape[1]
        number_rows_display=display_first_Table.shape[0]
        number_cols_display=display_first_Table.shape[1]
        number_rows_vdx=vdx_access_table.shape[0]
        number_cols_vdx=vdx_access_table.shape[1]
        number_rows_preroll=preroll_access_table.shape[0]
        number_cols_preroll=preroll_access_table.shape[1]

        forge_colour_info=workbook.add_format()
        forge_colour_info.set_bg_color('#F0F8FF')
        forge_colour_col=workbook.add_format()
        forge_colour_col.set_bg_color('#00B0F0')
        forge_colour_border=workbook.add_format()
        forge_colour_border.set_bg_color('#E7E6E6')

        alignment = workbook.add_format({"align":"center"})

        format_border_bottom=workbook.add_format()
        format_border_bottom.set_bottom(1)

        format_border_right=workbook.add_format()
        format_border_right.set_right(1)

        format_border_left=workbook.add_format()
        format_border_left.set_left(1)

        format_sub=workbook.add_format({"bold":True,"align":"center","num_format":"#,##0"})
        format_subtotal=workbook.add_format({"bold":True,"align":"center"})
        format_sub_num_money=workbook.add_format({"bold":True,"num_format":"$#,###0.00","align":"center"})
        format_sub_num_percent=workbook.add_format({"bold":True,"num_format":"0.00%","align":"center"})
        format_col = workbook.add_format({"num_format":"#,##0"})

        percent_fmt=workbook.add_format({"num_format":"0.00%"})
        money_fmt=workbook.add_format({"num_format":"$#,###0.00"})


        #formatting Columns of IO from B2 to G5
        worksheet.conditional_format("B2:O5",{"type":"blanks","format":forge_colour_info})
        worksheet.conditional_format("B2:O5",{"type":"no_blanks","format":forge_colour_info})


        #formatting columns
        if check_disp_empty==True:
            pass
        else:
            worksheet.conditional_format(number_rows_commom+5,3,number_rows_commom+5,number_cols_display+2,
                                         {"type":"no_blanks","format":forge_colour_col})
        if check_vdx_empty==True:
            pass
        else:
            worksheet.conditional_format(number_rows_commom+number_rows_display+10,3,
                                         number_rows_commom+number_rows_display+10,number_cols_vdx+2,
                                         {"type":"no_blanks","format":forge_colour_col})
        if check_preroll_empty==True:
            pass
        else:
            worksheet.conditional_format(number_rows_commom+number_rows_display+number_rows_vdx+15,3,
                                         number_rows_commom+number_rows_display+number_rows_vdx+15,
                                         number_cols_preroll+2,
                                         {"type":"no_blanks","format":forge_colour_col})


        #formatting money and percent
        if check_disp_empty==True:
            pass
        else:
            worksheet.conditional_format(number_rows_commom+6,8,number_rows_commom+number_rows_display+5,9,
                                         {"type":"no_blanks","format":money_fmt})
            worksheet.conditional_format(number_rows_commom+6,12,number_rows_commom+number_rows_display+5,12,
                                         {"type":"no_blanks","format":percent_fmt})
            worksheet.conditional_format(number_rows_commom+6,13,number_rows_commom+number_rows_display+5,13,
                                         {"type":"no_blanks","format":money_fmt})

        if check_vdx_empty==True:
            pass
        else:
            worksheet.conditional_format(number_rows_commom+number_rows_display+11,13,
                                         number_rows_commom+number_rows_display+number_rows_vdx+10,13,
                                         {"type":"no_blanks","format":percent_fmt})
            worksheet.conditional_format(number_rows_commom+number_rows_display+11,14,
                                         number_rows_commom+number_rows_display+number_rows_vdx+10,14,
                                         {"type":"no_blanks","format":money_fmt})
            worksheet.conditional_format(number_rows_commom+number_rows_display+11,8,
                                         number_rows_commom+number_rows_display+number_rows_vdx+10,9,
                                         {"type":"no_blanks","format":money_fmt})

        if check_preroll_empty==True:
            pass
        else:
            worksheet.conditional_format(number_rows_commom+number_rows_display+number_rows_vdx+16,12,
                                         number_rows_commom+number_rows_display+number_rows_vdx+number_rows_preroll+15,
                                         12,
                                         {"type":"no_blanks","format":percent_fmt})
            worksheet.conditional_format(number_rows_commom+number_rows_display+number_rows_vdx+16,13,
                                         number_rows_commom+number_rows_display+number_rows_vdx+number_rows_preroll+15,
                                         13,
                                         {"type":"no_blanks","format":money_fmt})
            worksheet.conditional_format(number_rows_commom+number_rows_display+number_rows_vdx+16,8,
                                         number_rows_commom+number_rows_display+number_rows_vdx+number_rows_preroll+15,
                                         9,
                                         {"type":"no_blanks","format":money_fmt})


        #formatting columns Booked, Delivered Impressions and Delivered Engagements
        if check_disp_empty==True:
            pass
        else:
            worksheet.conditional_format(number_rows_commom+6,10,number_rows_commom+number_rows_display+5,11,
                                         {"type":"no_blanks","format":format_col})
            """worksheet.conditional_format(number_rows_commom+6,11,number_rows_commom+number_rows_display+5,11,
                                         {"type":"no_blanks","format":format_col})
            worksheet.conditional_format(number_rows_commom+6,13,number_rows_commom+number_rows_display+5,13,
                                         {"type":"no_blanks","format":format_col})"""

        if check_vdx_empty==True:
            pass
        else:
            worksheet.conditional_format(number_rows_commom+number_rows_display+11,10,
                                         number_rows_commom+number_rows_display+number_rows_vdx+10,13,
                                         {"type":"no_blanks","format":format_col})
            """worksheet.conditional_format(number_rows_commom+number_rows_display+11,14,
                                         number_rows_commom+number_rows_display+number_rows_vdx+10,14,
                                         {"type":"no_blanks","format":format_col})
            worksheet.conditional_format(number_rows_commom+number_rows_display+11,8,
                                         number_rows_commom+number_rows_display+number_rows_vdx+10,9,
                                         {"type":"no_blanks","format":format_col})"""

        if check_preroll_empty==True:
            pass
        else:
            worksheet.conditional_format(number_rows_commom+number_rows_display+number_rows_vdx+16,10,
                                         number_rows_commom+number_rows_display+number_rows_vdx+number_rows_preroll+15,
                                         11,
                                         {"type":"no_blanks","format":format_col})
            """worksheet.conditional_format(number_rows_commom+number_rows_display+number_rows_vdx+16,13,
                                         number_rows_commom+number_rows_display+number_rows_vdx+number_rows_preroll+15,
                                         13,
                                         {"type":"no_blanks","format":format_col})
            worksheet.conditional_format(number_rows_commom+number_rows_display+number_rows_vdx+16,8,
                                         number_rows_commom+number_rows_display+number_rows_vdx+number_rows_preroll+15,
                                         9,
                                         {"type":"no_blanks","format":format_col})"""


        #addting subtotal and adding formatting for subtotal
        if check_disp_empty==True:
            pass
        else:
            worksheet.write(number_rows_commom+number_rows_display+6,3,"Subtotal",format_subtotal)

        if check_vdx_empty==True:
            pass
        else:
            worksheet.write(number_rows_commom+number_rows_display+number_rows_vdx+11,3,"Subtotal",format_subtotal)

        if check_preroll_empty==True:
            pass
        else:
            worksheet.write(number_rows_commom+number_rows_display+number_rows_vdx+number_rows_preroll+16,3,"Subtotal",
                            format_subtotal)


        #adding formulas to rows
        if check_disp_empty==True:
            pass
        else:
            worksheet.write_formula(number_rows_commom+number_rows_display+6,9,
                                    '=sum(J{}:J{})'.format(number_rows_commom+7,
                                                           number_rows_commom+number_rows_display+6),
                                    format_sub_num_money)
            worksheet.write_formula(number_rows_commom+number_rows_display+6,10,
                                    '=sum(K{}:K{})'.format(number_rows_commom+7,
                                                           number_rows_commom+number_rows_display+6),format_sub)
            worksheet.write_formula(number_rows_commom+number_rows_display+6,11,
                                    '=sum(L{}:L{})'.format(number_rows_commom+7,
                                                           number_rows_commom+number_rows_display+6),format_sub)
            worksheet.write_formula(number_rows_commom+number_rows_display+6,12,
                                    '=IFERROR((L{}/K{}),0)'.format(number_rows_commom+number_rows_display+7,
                                                                   number_rows_commom+number_rows_display+7),
                                    format_sub_num_percent)
            worksheet.write_formula(number_rows_commom+number_rows_display+6,13,
                                    '=sum(N{}:N{})'.format(number_rows_commom+7,
                                                           number_rows_commom+number_rows_display+6),
                                    format_sub_num_money)

        if check_vdx_empty==True:
            pass
        else:
            worksheet.write_formula(number_rows_commom+number_rows_display+number_rows_vdx+11,9,
                                    '=sum(J{}:J{})'.format(number_rows_commom+number_rows_display+12,
                                                           number_rows_commom+number_rows_display+number_rows_vdx+11),
                                    format_sub_num_money)
            worksheet.write_formula(number_rows_commom+number_rows_display+number_rows_vdx+11,10,
                                    '=sum(K{}:K{})'.format(number_rows_commom+number_rows_display+12,
                                                           number_rows_commom+number_rows_display+number_rows_vdx+11),
                                    format_sub)
            worksheet.write_formula(number_rows_commom+number_rows_display+number_rows_vdx+11,11,
                                    '=sum(L{}:L{})'.format(number_rows_commom+number_rows_display+12,
                                                           number_rows_commom+number_rows_display+number_rows_vdx+11),
                                    format_sub)
            worksheet.write_formula(number_rows_commom+number_rows_display+number_rows_vdx+11,12,
                                    '=sum(M{}:M{})'.format(number_rows_commom+number_rows_display+12,
                                                           number_rows_commom+number_rows_display+number_rows_vdx+11),
                                    format_sub)
            worksheet.write_formula(number_rows_commom+number_rows_display+number_rows_vdx+11,13,
                                    '=IFERROR(sum(L{}:M{})/sum(K{}),0)'.format(
                                        number_rows_commom+number_rows_display+number_rows_vdx+12,
                                        number_rows_commom+number_rows_display+number_rows_vdx+12,
                                        number_rows_commom+number_rows_display+number_rows_vdx+12),
                                    format_sub_num_percent)
            worksheet.write_formula(number_rows_commom+number_rows_display+number_rows_vdx+11,14,
                                    '=sum(O{}:O{})'.format(number_rows_commom+number_rows_display+12,
                                                           number_rows_commom+number_rows_display+number_rows_vdx+11),
                                    format_sub_num_money)

        if check_preroll_empty==True:
            pass
        else:
            worksheet.write_formula(number_rows_commom+number_rows_display+number_rows_vdx+number_rows_preroll+16,9,
                                    '=sum(J{}:J{})'.format(number_rows_commom+number_rows_display+number_rows_vdx+17,
                                                           number_rows_commom+number_rows_display+number_rows_vdx+number_rows_preroll+16),
                                    format_sub_num_money)
            worksheet.write_formula(number_rows_commom+number_rows_display+number_rows_vdx+number_rows_preroll+16,10,
                                    '=sum(K{}:K{})'.format(number_rows_commom+number_rows_display+number_rows_vdx+17,
                                                           number_rows_commom+number_rows_display+number_rows_vdx+number_rows_preroll+16),
                                    format_sub)
            worksheet.write_formula(number_rows_commom+number_rows_display+number_rows_vdx+number_rows_preroll+16,11,
                                    '=sum(L{}:L{})'.format(number_rows_commom+number_rows_display+number_rows_vdx+17,
                                                           number_rows_commom+number_rows_display+number_rows_vdx+number_rows_preroll+16),
                                    format_sub)
            worksheet.write_formula(number_rows_commom+number_rows_display+number_rows_vdx+number_rows_preroll+16,12,
                                    '=IFERROR((L{}/K{}),0)'.format(
                                        number_rows_commom+number_rows_display+number_rows_vdx+number_rows_preroll+17,
                                        number_rows_commom+number_rows_display+number_rows_vdx+number_rows_preroll+17),
                                    format_sub_num_percent)
            worksheet.write_formula(number_rows_commom+number_rows_display+number_rows_vdx+number_rows_preroll+16,13,
                                    '=sum(N{}:N{})'.format(number_rows_commom+number_rows_display+number_rows_vdx+17,
                                                           number_rows_commom+number_rows_display+number_rows_vdx+number_rows_preroll+16),
                                    format_sub_num_money)


        #adding colour to last subtotal to each dataframe
        if check_disp_empty==True:
            pass
        else:
            worksheet.conditional_format(number_rows_commom+number_rows_display+6,3,
                                         number_rows_commom+number_rows_display+6
                                         ,number_cols_display+2,{"type":"blanks","format":forge_colour_border})
            worksheet.conditional_format(number_rows_commom+number_rows_display+6,3,
                                         number_rows_commom+number_rows_display+6
                                         ,number_cols_display+2,{"type":"no_blanks","format":forge_colour_border})
        if check_vdx_empty==True:
            pass
        else:
            worksheet.conditional_format(number_rows_commom+number_rows_display+number_rows_vdx+11,3,
                                         number_rows_commom+number_rows_display+number_rows_vdx+11,number_cols_vdx+2,
                                         {"type":"blanks","format":forge_colour_border})
            worksheet.conditional_format(number_rows_commom+number_rows_display+number_rows_vdx+11,3,
                                         number_rows_commom+number_rows_display+number_rows_vdx+11,number_cols_vdx+2,
                                         {"type":"no_blanks","format":forge_colour_border})
        if check_preroll_empty==True:
            pass
        else:
            worksheet.conditional_format(number_rows_commom+number_rows_display+number_rows_vdx+number_rows_preroll+16,
                                         3,
                                         number_rows_commom+number_rows_display+number_rows_vdx+number_rows_preroll+16,
                                         number_cols_preroll+2,{"type":"blanks","format":forge_colour_border})
            worksheet.conditional_format(number_rows_commom+number_rows_display+number_rows_vdx+number_rows_preroll+16,
                                         3,
                                         number_rows_commom+number_rows_display+number_rows_vdx+number_rows_preroll+16,
                                         number_cols_preroll+2,{"type":"no_blanks","format":forge_colour_border})


        #format bottom border
        if check_disp_empty==True:
            pass
        else:
            worksheet.conditional_format(number_rows_commom+number_rows_display+6,3,
                                         number_rows_commom+number_rows_display+6
                                         ,number_cols_display+2,{"type":"blanks","format":format_border_bottom})

            worksheet.conditional_format(number_rows_commom+number_rows_display+6,3,
                                         number_rows_commom+number_rows_display+6
                                         ,number_cols_display+2,{"type":"no_blanks","format":format_border_bottom})

            """worksheet.conditional_format(number_rows_commom+number_rows_display+6,3,
                                         number_rows_commom+number_rows_display+6
                                         ,number_cols_display+2,{"type":"no_blanks","format":format_border_left})"""
        if check_vdx_empty==True:
            pass
        else:
            worksheet.conditional_format(number_rows_commom+number_rows_display+number_rows_vdx+11,3,
                                         number_rows_commom+number_rows_display+number_rows_vdx+11,number_cols_vdx+2,
                                         {"type":"blanks","format":format_border_bottom})

            worksheet.conditional_format(number_rows_commom+number_rows_display+number_rows_vdx+11,3,
                                         number_rows_commom+number_rows_display+number_rows_vdx+11,number_cols_vdx+2,
                                         {"type":"no_blanks","format":format_border_bottom})

        if check_preroll_empty==True:
            pass
        else:
            worksheet.conditional_format(number_rows_commom+number_rows_display+number_rows_vdx+number_rows_preroll+16,
                                         3,
                                         number_rows_commom+number_rows_display+number_rows_vdx+number_rows_preroll+16,
                                         number_cols_preroll+2,{"type":"blanks","format":format_border_bottom})

            worksheet.conditional_format(number_rows_commom+number_rows_display+number_rows_vdx+number_rows_preroll+16,
                                         3,
                                         number_rows_commom+number_rows_display+number_rows_vdx+number_rows_preroll+16,
                                         number_cols_preroll+2,{"type":"no_blanks","format":format_border_bottom})


        #merge formatting
        format_merge_row=workbook.add_format({"bold":True,"font_color":'#000000',"align":"centre",
                                              "fg_color":"#00B0F0","border":1,"border_color":"#000000"})

        format_merge_row_wrap=workbook.add_format(
            {"bold":True,"font_color":'#000000',"align":"centre",'valign':'vcenter',
             "border":1,'text_wrap':True})

        if check_disp_empty==True:
            pass
        else:
            worksheet.merge_range(number_rows_commom+4,3,number_rows_commom+4,number_cols_display+2,
                                  "Campaign pacing",format_merge_row)

        if check_vdx_empty==True:
            pass
        else:
            worksheet.merge_range(number_rows_commom+number_rows_display+9,3,number_rows_commom+number_rows_display+9,
                                  number_cols_vdx+2,"Campaign pacing",format_merge_row)

        if check_preroll_empty==True:
            pass
        else:
            worksheet.merge_range(number_rows_commom+number_rows_display+number_rows_vdx+14,3,
                                  number_rows_commom+number_rows_display+number_rows_vdx+14,
                                  number_cols_preroll+2,"Campaign pacing",
                                  format_merge_row)


        #wrap and formatting for column B
        if check_disp_empty==True:
            pass
        else:
            worksheet.merge_range(number_rows_commom+6,1,number_rows_commom+number_rows_display+6,2,
                                  "Standard Banners (Performance/Brand)",
                                  format_merge_row_wrap)

        if check_vdx_empty==True:
            pass
        else:
            worksheet.merge_range(number_rows_commom+number_rows_display+11,1,
                                  number_rows_commom+number_rows_display+number_rows_vdx+11,2,
                                  "VDX(Display and Instream)",
                                  format_merge_row_wrap)
        if check_preroll_empty==True:
            pass
        else:
            worksheet.merge_range(number_rows_commom+number_rows_display+number_rows_vdx+16,1,
                                  number_rows_commom+number_rows_display+number_rows_vdx+number_rows_preroll+16,2,
                                  "Standard Pre Roll",
                                  format_merge_row_wrap)


        #Right Border Formatting
        if check_disp_empty==True:
            pass
        else:
            worksheet.conditional_format(number_rows_commom+6,13,
                                         number_rows_commom+number_rows_display+5,13,
                                         {"type":"no_blanks","format":format_border_right})

        if check_vdx_empty==True:
            pass
        else:
            worksheet.conditional_format(number_rows_commom+number_rows_display+11,14,
                                         number_rows_commom+number_rows_display+number_rows_vdx+10,14,
                                         {"type":"no_blanks","format":format_border_right})

        if check_preroll_empty==True:
            pass
        else:
            worksheet.conditional_format(number_rows_commom+number_rows_display+number_rows_vdx+16,13,
                                         number_rows_commom+number_rows_display+number_rows_vdx+number_rows_preroll+15,
                                         13,{"type":"no_blanks","format":format_border_right})


        #Columns setting
        worksheet.set_column("D:O",21,alignment)
        worksheet.set_zoom(90)

        for row in range(5,10):
            if check_disp_empty==True:
                worksheet.set_row(row,None,None,{'hidden':True})
        else:
            pass

        for row in range(5,12):
            if check_disp_empty==True&check_vdx_empty==True:
                worksheet.set_row(row,None,None,{'hidden':True})
        else:
            pass

        for row in range(number_rows_commom+number_rows_display+8,number_rows_commom+number_rows_display+13):
            if check_vdx_empty==True:
                worksheet.set_row(row,None,None,{'hidden':True})
        else:
            pass

    def main(self):
        self.config.common_columns_summary()
        self.connect_TFR_summary()
        self.read_query_summary()
        self.access_data_summary()
        self.summary_creation()
        self.rename_cols_sumary()
        self.write_summary()
        self.format_summary()


if __name__=="__main__":
    pass

    #enable it when running for individual file
    c=config.Config("test",603387)
    o=Summary(c)
    o.main()
    c.saveAndCloseWriter()
