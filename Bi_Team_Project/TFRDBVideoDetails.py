from __future__ import unicode_literals
import cx_Oracle
import csv
import  ftplib
#from django.utils.encoding import smart_str, smart_unicode
from datetime import date, timedelta
def uploadFile(filePath):
    # lets upload file to ftp server
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
    sql = "select B.day_desc,c.month_desc,D.Buy_ID,D.Buy_desc,\
    E.MEDIA_ID,E.MEDIA_Desc,\
    F.FEV_MEDIA_SOURCE_ID,F.FEV_MEDIA_SOURCE_desc,\
    G.FEV_MEDIA_SUB_SOURCE_ID,G.FEV_MEDIA_SUB_SOURCE_Desc,\
    H.PRIMARY_VIDEO_LENGTH_ID,H.PRIMARY_VIDEO_LENGTH_desc,\
    I.FEV_INT_VIDEO_ID,I.FEV_INT_VIDEO_DESC,\
    J.IO_ID,J.IO_Desc,\
    K.PLACEMENT_ID,K.PLACEMENT_DESC,\
    Sum(A.MUTE)As MUTE,Sum(A.UNMUTE)As UNMUTE,Sum(A.PAUSE)As PAUSE,Sum(A.RESUME)As RESUME,\
    Sum(A.REWIND)As REWIND,	Sum(A.REPLAY)As REPLAY,	Sum(A.FULL_SCREEN)As FULL_SCREEN,\
    Sum(A.VWR_VIDEO_VIEW_0_PC_COUNT)As VWR_VIDEO_VIEW_0_PC_COUNT,\
    Sum(A.VWR_VIDEO_VIEW_25_PC_COUNT)As VWR_VIDEO_VIEW_25_PC_COUNT,\
    Sum(A.VWR_VIDEO_VIEW_50_PC_COUNT)As VWR_VIDEO_VIEW_50_PC_COUNT,\
    Sum(A.VWR_VIDEO_VIEW_75_PC_COUNT)As VWR_VIDEO_VIEW_75_PC_COUNT,\
    Sum(A.VWR_VIDEO_VIEW_100_PC_COUNT)As VWR_VIDEO_VIEW_100_PC_COUNT,\
    Sum(A.ENG_MUTE)As ENG_MUTE,	Sum(A.ENG_UNMUTE)As ENG_UNMUTE,\
    Sum(A.ENG_PAUSE)As ENG_PAUSE,\
    Sum(A.ENG_RESUME)As ENG_RESUME,\
    Sum(A.ENG_REWIND)As ENG_REWIND,	Sum(A.ENG_REPLAY)As ENG_REPLAY,\
    Sum(A.ENG_FULL_SCREEN)As ENG_FULL_SCREEN,\
    Sum(A.ENG_VIDEO_VIEW_0_PC_COUNT)As ENG_VIDEO_VIEW_0_PC_COUNT,\
    Sum(A.ENG_VIDEO_VIEW_25_PC_COUNT)As ENG_VIDEO_VIEW_25_PC_COUNT,\
    Sum(A.ENG_VIDEO_VIEW_50_PC_COUNT)As ENG_VIDEO_VIEW_50_PC_COUNT,\
    Sum(A.ENG_VIDEO_VIEW_75_PC_COUNT)As ENG_VIDEO_VIEW_75_PC_COUNT,\
    Sum(A.ENG_VIDEO_VIEW_100_PC_COUNT)As ENG_VIDEO_VIEW_100_PC_COUNT,\
    Sum(A.DPE_MUTE)As DPE_MUTE,	Sum(A.DPE_UNMUTE)As DPE_UNMUTE,\
    Sum(A.DPE_PAUSE)As DPE_PAUSE,Sum(A.DPE_RESUME)As DPE_RESUME,\
    Sum(A.DPE_REWIND)As DPE_REWIND,	Sum(A.DPE_REPLAY)As DPE_REPLAY,\
    Sum(A.DPE_FULL_SCREEN)As DPE_FULL_SCREEN,\
    Sum(A.DPE_VIDEO_VIEW_0_PC_COUNT)As DPE_VIDEO_VIEW_0_PC_COUNT,\
    Sum(A.DPE_VIDEO_VIEW_25_PC_COUNT)As DPE_VIDEO_VIEW_25_PC_COUNT,\
    Sum(A.DPE_VIDEO_VIEW_50_PC_COUNT)As DPE_VIDEO_VIEW_50_PC_COUNT,\
    Sum(A.DPE_VIDEO_VIEW_75_PC_COUNT)As DPE_VIDEO_VIEW_75_PC_COUNT,\
    Sum(A.DPE_VIDEO_VIEW_100_PC_COUNT)As DPE_VIDEO_VIEW_100_PC_COUNT from TFR_VIDEO_DETAIL_V1_FACT A\
        join TFR_Day_dim B on A.DAY_ID=B.DAY_ID\
        Join tfr_Month_dim C on B.MONTH_ID=C.MONTH_ID\
        Join tfr_buy_dim D on A.BUY_ID=D.BUY_ID\
        Join tfr_media_dim E on A.MEDIA_ID=E.MEDIA_ID\
        Join TFR_FEV_MEDIA_SOURCE_DIM F on A.FEV_MEDIA_SOURCE_ID=F.FEV_MEDIA_SOURCE_ID\
        Join TFR_FEV_MEDIA_SUB_SOURCE_DIM G on A.FEV_MEDIA_SUB_SOURCE_ID=G.FEV_MEDIA_SUB_SOURCE_ID\
        Join TFR_PRIMARY_VIDEO_LENGTH_DIM H on A.PRIMARY_VIDEO_LENGTH_ID=H.PRIMARY_VIDEO_LENGTH_ID\
        Join TFR_FEV_INT_VIDEO_DIM I on A.FEV_INT_VIDEO_ID=I.FEV_INT_VIDEO_ID\
        Join TFR_IO_dim J on D.IO_ID=J.IO_ID\
        Join TFR_Placement_dim K on D.PLACEMENT_ID=K.PLACEMENT_ID\
        where B.DAY_ID = (select max(day_id) from tfr_day_dim)-1\
        group by B.day_desc,c.month_desc,D.Buy_ID,D.Buy_desc,E.MEDIA_ID,E.MEDIA_Desc,F.FEV_MEDIA_SOURCE_ID,F.FEV_MEDIA_SOURCE_desc,\
    G.FEV_MEDIA_SUB_SOURCE_ID,G.FEV_MEDIA_SUB_SOURCE_Desc,H.PRIMARY_VIDEO_LENGTH_ID,H.PRIMARY_VIDEO_LENGTH_desc,I.FEV_INT_VIDEO_ID,I.FEV_INT_VIDEO_DESC,\
    J.IO_ID,J.IO_Desc ,K.PLACEMENT_ID,K.PLACEMENT_DESC"
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
            writer.writerow ( [ row[ 0 ].date () ] + [i.encode('utf-8') if isinstance(i, unicode) else i for i in row[1:]] )

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
    filePath = r'C:\\Users\Dharmendra.Mishra\Box Sync\Dharamendra Reports\Eoc_Automation\VideoDetail.csv'
    createFile(filePath)
    #uploadFile(filePath)

if __name__ == '__main__':
    main()