import pandas as pd
import ftplib as ft
import numpy as np
user_input_io = int(input("Enter the IO ID"))
df_Eociodetails = pd.read_csv("C:\BiTeam-New-ProjectPython\Bi_Team_Project\EOC\Eociodetails.csv")
df_Eociodetails_Columns = df_Eociodetails[["Client Name","IO Name","Agency Name","Expo Account Manager","Expo Sales Contact"
                                           ,"Campiagn Report Date","Campaign Status(Live#Ended)","Currency(AUD#NZD#USD#SGD#EURO#GBP#INR#ZAR#MXN#MYR#HKD#CAD#THB#CHF)"]]


table1 = pd.pivot_table(df_Eociodetails_Columns,index= None,columns=["Client Name","IO Name","Agency Name","Expo Account Manager","Expo Sales Contact"
                                           ,"Campiagn Report Date","Campaign Status(Live#Ended)","Currency(AUD#NZD#USD#SGD#EURO#GBP#INR#ZAR#MXN#MYR#HKD#CAD#THB#CHF)"])



#io_Name = Eociodetails["IO Name"]
#sheetName = Eociodetails["Template Type(Summary#Standard banner Campaign detail#VDX Campaigns detail#Standard Preroll Campaign detail#Definitions)"]
#print io_Name
#print sheetName
#Eociodetails_new = Eociodetails.to_excel("C:\BiTeam-New-ProjectPython\Bi_Team_Project\EOC\New.xlsx",sheet_name= "Summary",header= True,index=False)



