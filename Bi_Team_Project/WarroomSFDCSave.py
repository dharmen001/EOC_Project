import os

import att as att
from win32com.client import Dispatch
import datetime as date

outlook = Dispatch("Outlook.Application").GetNamespace("MAPI")
inbox = outlook.GetDefaultFolder("6")
all_inbox = inbox.items
val_date = date.date.today()

sub_today = 'War Room Report with BU Teams'
att_today = 'War Room Report with BU Teams new.csv'
for msg in all_inbox:
    if msg.Subject == sub_today and msg.Attachments == att_today:
        print msg
