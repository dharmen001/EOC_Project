from __future__ import unicode_literals
import cx_Oracle
import csv
import  ftplib
from datetime import date, timedelta

def uploadFile(filePath):
    # lets upload file to ftp server
    #should be refresh first
    ftpClient = ftplib.FTP ( '10.29.21.56' )
    ftpClient.login ( 'repgen', 'reports001' )
    ftpClient.cwd ( '/mnt/svmdev01exporeporting01/crdg2-reporting-bak/data/bidata/Dharmendra/KM-Video-Interaction/' )
    yesterdayDate = date.today () - timedelta ( days=1 )

    dirName = str ( yesterdayDate.year )
    try:
        fileList = ftpClient.nlst ()
        if dirName in fileList:
            ftpClient.cwd ( dirName )
        else:
            ftpClient.mkd ( dirName )
            ftpClient.cwd ( dirName )
    except ftplib.error_perm:
        ftpClient.mkd ( dirName )
        ftpClient.cwd ( dirName )

    # lets create year month dir
    dirName = str ( yesterdayDate.strftime ( '%Y-%m' ) )
    try:
        fileList = ftpClient.nlst ()
        if dirName in fileList:
            ftpClient.cwd ( dirName )
        else:
            ftpClient.mkd ( dirName )
            ftpClient.cwd ( dirName )
    except ftplib.error_perm:
        ftpClient.mkd ( dirName )
        ftpClient.cwd ( dirName )

    # lets create day dir
    dirName = str ( yesterdayDate.strftime ( '%Y-%m-%d' ) )
    try:
        fileList = ftpClient.nlst ()
        if dirName in fileList:
            ftpClient.cwd ( dirName )
        else:
            ftpClient.mkd ( dirName )
            ftpClient.cwd ( dirName )
    except ftplib.error_perm:
        ftpClient.mkd ( dirName )
        ftpClient.cwd ( dirName )

    f = open(filePath, 'rb')
    fileName = filePath.split('\\')[-1]
    ftpClient.storbinary('STOR {}'.format(fileName), f)
    f.close()
    ftpClient.close()

def createFile(filePath):
    fout = open ( filePath, "wb" )
    writer = csv.writer ( fout )

    conn = cx_Oracle.connect ( "TFR_REP/welcome@10.29.20.76/tfrdb" )
    sql = "select C.day_desc,C.month_desc,B.Buy_ID,B.Buy_Desc,B.Client_ID,B.Client_Desc,B.IO_ID,B.IO_Desc,B.Placement_ID,B.placement_Desc,D.MEDIA_ID,D.MEDIA_DESC,sum(A.VWR_INTERACTION) as VWR_INTERACTION,sum(A.ENG_INTERACTION) as ENG_INTERACTION,sum(A.DPE_INTERACTION) as DPE_INTERACTION from TFR_INT_DETAIL_V1_FACT A\
 Join(select P.BUY_ID,P.BUY_DESC,P.CLIENT_ID,Q.CLIENT_DESC,P.IO_ID,T.IO_DESC,P.CAMPAIGN_ID,R.CAMPAIGN_DESC,P.PLACEMENT_ID,S.PLACEMENT_DESC from TFR_buy_dim P Join TFR_client_dim Q on P.CLIENT_ID=Q.CLIENT_ID Join tfr_IO_Dim T on P.IO_ID= T.IO_ID Join TFR_campaign_dim R on P.CAMPAIGN_ID=R.CAMPAIGN_ID Join TFR_placement_dim S on P.PLACEMENT_ID=S.PLACEMENT_ID) B on A.BUY_ID=B.BUY_ID\
 Join (select X.day_ID,X.day_Desc,X.MONTH_ID,Y.MONTH_Desc from TFR_Day_dim X join TFR_month_Dim Y on X.MONTH_ID=Y.MONTH_ID)C on A.DAY_ID=C.Day_ID\
 Join TFR_MEDIA_DIM D on A.MEDIA_ID=D.MEDIA_ID\
 where C.DAY_ID = (select max(day_id) from tfr_day_dim)-1\
 group by C.day_desc,C.month_desc,B.Buy_ID,B.Buy_Desc,B.Client_ID,B.Client_Desc,B.IO_ID,B.IO_Desc,B.Placement_ID,B.placement_Desc,D.MEDIA_ID,D.MEDIA_DESC"

    #which_IO_ID = int ( input ( "Kindly provide IO ID:" ) )
    try:
        cursor = conn.cursor ()
        cursor.execute ( sql)#, {'The_IO_ID': which_IO_ID} )
    except cx_Oracle.DatabaseError as e:
        error, = e.args
        print("Error while trying to execute cursor.")
        print("Oracle error message is [" + error.message[ :-1 ] + ']')
        print("oracle error code is " + str ( error.code ))
        print ('sql string is [' + sql + ']')
        exit ( error.code )
    writer.writerow ( [ i[ 0 ].strip () for i in cursor.description ] )

    try:
        for row in cursor:
            print row
            writer.writerow ( [ row[ 0 ].date () ] + [i.encode('utf-8') if isinstance(i, unicode) else i for i in row[1:]])

        fout.close ()
    except cx_Oracle.DatabaseError as e:
        error, e.args
        print ("Error while reading from cursor.")
        print ('Oracle error message is [' + error.message[ :-1 ] + ']')
        print ('Oracle error code is ' + str ( error.code ))
        exit ( error.code )
    # file.close()
    cursor.close ()
    conn.close ()

def main():
    filePath = r'C:\\Users\\Dharmendra.Mishra\\Box Sync\\Dharamendra Reports\\Eoc_Automation\\Interactiondetail.csv'
    createFile(filePath)
    uploadFile(filePath)

if __name__ == '__main__':
    main()