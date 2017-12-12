import ftplib
server = ftplib.FTP()
server.connect("10.29.21.56")
server.login("repgen","reports001")
ser=server.dir("/mnt/svmdev01exporeporting01/crdg2-reporting-bak/data/bidata/Dharmendra")
print ser

