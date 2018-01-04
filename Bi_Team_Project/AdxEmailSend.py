import datetime
import win32com.client as win32

outlook = win32.Dispatch('outlook.application')
recipients_to = ["ajeet.jain@exponential.com,yogesh.arora@exponential.com"]
recipients_cc = ["surendra.saxena@exponential.com,gauravk.singh@exponential.com"]
recipients_bcc = ["dharmendra.mishra@exponential.com"]
mail = outlook.createitem(0)

mail.TO = ";".join(recipients_to)
mail.CC = ";".join(recipients_cc)
mail.BCC = "".join(recipients_bcc)
mail.subject = "Adx Data"
mail.importance = 1
mail.Body = "Hi -" \
               "Please find attached required adx data for yesterday" \
            "Thanks, Dharmendra"

Attachment = "C:\\biUi\\tfrSMDataDirectory\\2017\\2017-12\\adx\\"
file = "adxData."
today_Date = datetime.date.today()
Dates_yesterday = today_Date-datetime.timedelta(1)
Dates_daybeforeyesterday = Dates_yesterday-datetime.timedelta(1)
Dates_twodayback = Dates_daybeforeyesterday-datetime.timedelta(1)
Data_file_One = "".join([Attachment,file,str(Dates_daybeforeyesterday),"_",str(Dates_yesterday),".csv"])
mail.Attachments.Add(Data_file_One)
mail.Send()










