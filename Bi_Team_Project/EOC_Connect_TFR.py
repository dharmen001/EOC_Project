import cx_Oracle
import logging
def tfr_Connect():
    conn = cx_Oracle.connect("TFR_REP/welcome@10.29.20.76/tfrdb")
    sql = "select * from TFR_REP.SUMMARY_MV"
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
    except cx_Oracle.DatabaseError as e:
        error = str(e.args)
        logging.info("Error while trying to execute cursor.")
        logging.info("Oracle error message is [" + str(error.message[:-1]) + "]")
        logging.info("Oracle error code is " + str(error.code))
        logging.info("sql string is [" + sql + "]")
        exit(error.code)
    try:
        for row in cursor:
            print (row)
    except cx_Oracle.DatabaseError as e:
        logging.info("Error while reading from cursor.")
        logging.info("Oracle error message is [" + str(error.message[:-1]) + "]")
        logging.info("Oracle error code is" + str(error.code))
        exit(error.code)
    cursor.close()
    conn.close()

def main():
    tfr_Connect()

if __name__ == '__main__':
    main()





