import ftplib
def ftp_lib():
    ftpClient = ftplib.FTP ( '10.29.21.56' )
    ftpClient.login ( 'repgen', 'reports001' )
    data=ftpClient.cwd ( '/mnt/svmdev01exporeporting01/crdg2-reporting-bak/data/bidata' )
