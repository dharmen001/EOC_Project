from __future__ import unicode_literals
import cx_Oracle
import csv
import  ftplib
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
    sql = "select C.Month_Desc,B.DAY_DESC,\
        E.IO_ID,E.IO_desc,D.BUY_ID,D.BUY_DESC,\
        H.MEDIA_ID,H.MEDIA_DESC,\
        G.placement_ID,G.placement_desc,\
        J.CREATIVE_DESC,Sum(A.IMPRESSIONS) As IMPRESSIONS,\
        Sum(A.EXPANSIONS) As EXPANSIONS,Sum(A.ENGAGEMENTS) As ENGAGEMENTS,\
        Sum(A.DPE_ENGAGEMENTS) As DPE_ENGAGEMENTS,\
        Sum(A.CPCV_COUNT) As CPCV_COUNT,\
        Sum(A.CPCVE_COUNT) As CPCVE_COUNT,Sum(A.VWR_SKIP_COUNT) As VWR_SKIP_COUNT,\
        Sum(A.VWR_COLLAPSE) As VWR_COLLAPSE,Sum(A.VWR_CLOSE) As VWR_CLOSE,\
        Sum(A.VWR_CLICK_THROUGHS) As VWR_CLICK_THROUGHS,\
        Sum(A.VWR_PLAYER_INTERACTIONS) As VWR_PLAYER_INTERACTIONS,\
        Sum(A.VWR_VIDEO_VIEW_0_PC_COUNT) As VWR_VIDEO_VIEW_0_PC_COUNT,\
        Sum(A.VWR_VIDEO_VIEW_25_PC_COUNT) As VWR_VIDEO_VIEW_25_PC_COUNT,\
        Sum(A.VWR_VIDEO_VIEW_50_PC_COUNT) As VWR_VIDEO_VIEW_50_PC_COUNT,\
        Sum(A.VWR_VIDEO_VIEW_75_PC_COUNT) As VWR_VIDEO_VIEW_75_PC_COUNT,\
        Sum(A.VWR_VIDEO_VIEW_100_PC_COUNT) As VWR_VIDEO_VIEW_100_PC_COUNT,\
        Sum(A.ENG_TOTAL_TIME_SPENT) As ENG_TOTAL_TIME_SPENT,\
        Sum(A.ENG_INTERACTIVE_ENGAGEMENTS) As ENG_INTERACTIVE_ENGAGEMENTS,\
        Sum(A.ENG_AD_INTERACTIONS) As ENG_AD_INTERACTIONS,\
        Sum(A.ENG_CLOSE) As ENG_CLOSE, Sum(A.ENG_CLICK_THROUGHS) As ENG_CLICK_THROUGHS,\
        Sum(A.ENG_PLAYER_INTERACTIONS) As ENG_PLAYER_INTERACTIONS,\
        Sum(A.ENG_VIDEO_VIEW_0_PC_COUNT) As ENG_VIDEO_VIEW_0_PC_COUNT,\
        Sum(A.ENG_VIDEO_VIEW_25_PC_COUNT) As ENG_VIDEO_VIEW_25_PC_COUNT,\
        Sum(A.ENG_VIDEO_VIEW_50_PC_COUNT) As ENG_VIDEO_VIEW_50_PC_COUNT,\
        Sum(A.ENG_VIDEO_VIEW_75_PC_COUNT) As ENG_VIDEO_VIEW_75_PC_COUNT,\
        Sum(A.ENG_VIDEO_VIEW_100_PC_COUNT) As ENG_VIDEO_VIEW_100_PC_COUNT,\
        Sum(A.DPE_TOTAL_TIME_SPENT) As DPE_TOTAL_TIME_SPENT,\
        Sum(A.DPE_INTERACTIVE_ENGAGEMENTS) As DPE_INTERACTIVE_ENGAGEMENTS,\
        Sum(A.DPE_AD_INTERACTIONS) As DPE_AD_INTERACTIONS,\
        Sum(A.DPE_CLOSE) As DPE_CLOSE, Sum(A.DPE_CLICK_THROUGHS) As DPE_CLICK_THROUGHS,\
        Sum(A.DPE_PLAYER_INTERACTIONS) As DPE_PLAYER_INTERACTIONS,\
        Sum(A.DPE_VIDEO_VIEW_0_PC_COUNT) As DPE_VIDEO_VIEW_0_PC_COUNT,\
        Sum(A.DPE_VIDEO_VIEW_25_PC_COUNT) As DPE_VIDEO_VIEW_25_PC_COUNT,\
        Sum(A.DPE_VIDEO_VIEW_50_PC_COUNT) As DPE_VIDEO_VIEW_50_PC_COUNT,\
        Sum(A.DPE_VIDEO_VIEW_75_PC_COUNT) As DPE_VIDEO_VIEW_75_PC_COUNT,\
        Sum(A.DPE_VIDEO_VIEW_100_PC_COUNT) As DPE_VIDEO_VIEW_100_PC_COUNT from TFR_KEY_METRIC_V3_FACT A\
            Join TFR_day_dim B on A.DAY_ID=B.DAY_ID\
            Join TFR_month_dim C on B.Month_ID=C.Month_ID\
            join TFR_buy_dim D on A.Buy_ID=D.Buy_ID\
            Join TFR_IO_Dim E on D.IO_ID=E.IO_ID\
            Join TFR_client_dim F on D.CLIENT_ID=F.CLIENT_ID\
            join TFR_placement_dim G on D.PLACEMENT_ID=G.PLACEMENT_ID\
            JOIN TFR_CREATIVE_DIM J ON G.CREATIVE_ID=J.CREATIVE_ID\
            Join TFR_media_dim H on A.MEDIA_ID=H.MEDIA_ID\
            where B.DAY_ID = (select max(day_id) from tfr_day_dim)-1\
            group by C.Month_Desc,B.DAY_DESC,E.IO_ID,E.IO_desc,D.BUY_ID,D.BUY_DESC,H.MEDIA_ID,\
        H.MEDIA_DESC,G.placement_ID,G.placement_desc,J.CREATIVE_DESC"

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
            writer.writerow ( [ row[ 1 ].date () ] + [i.encode('utf-8') if isinstance(i, unicode) else i for i in row[1:]])

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
    filePath = r'C:\\Users\\Dharmendra.Mishra\\Box Sync\\Dharamendra Reports\\Eoc_Automation\\VideoDetail.csv'
    createFile(filePath)
    uploadFile(filePath)

if __name__ == '__main__':
    main()