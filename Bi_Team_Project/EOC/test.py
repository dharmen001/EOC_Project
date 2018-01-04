import pandas as pd
IO_ID = int(input("Enter the IO ID:"))
writer = pd.ExcelWriter("{}.xlsx".format(IO_ID),engine="xlsxwriter")
def common_columns_read_write():
    read_common_cols = pd.read_csv("Eociodetails.csv")
    sheet_append = read_common_cols.loc[read_common_cols.IO_ID == IO_ID, "Template_Type"]
    is_long = pd.Series(sheet_append)
    conv = str(is_long)
    Data = read_common_cols.loc[read_common_cols.IO_ID == IO_ID, ["Client_Name", "IO_Name", "Agency_Name","Expo_Account_Manager","Expo_Sales_Contact","Campaign_Status","Campiagn_Report_Date","Currency"]]
    write_data = Data.to_excel(writer,sheet_name=conv, startcol=1,startrow=0,index=False)
    write_data.save()
    #write_data.close()
def main():
    common_columns_read_write()
if __name__== " __main__ ":
    main()