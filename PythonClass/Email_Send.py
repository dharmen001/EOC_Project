import smtplib
from string import Template
from email.mime.muiltipart import mimemultipart
from email.mime.text import MIMEText
server = smtplib.SMTP(host= 'smtp-mail.outlook.com',port=587)
server.starttls()
server.login("dharmendra.mishra@exponential.com","sonuchaiwala@123")

def get_contacts(fileName):
    names = []
    email = []