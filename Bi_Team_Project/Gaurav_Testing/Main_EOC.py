import EOC_Summary_Header
#import EOC_Summary_Detail
#import EOC_Daily_Sales
from config import Config


if __name__ == '__main__':
    IO_Name = input("Enter IO Name:")
    IO_ID = int(input("Enter the IO:"))

    c = Config(IO_Name, IO_ID)
    obj_summary = EOC_Summary_Header.Summary_Header(c)
    obj_summary.main()

    """obj_daily = Eoc_Daily.Daily(c)
    obj_daily.main()
    obj_adSize = Eoc_AdSize.ad_Size(c)
    obj_adSize.main()
    obj_Video = Eoc_Video.Video(c)
    obj_Video.main()
    obj_Intraction = Eoc_Intraction.Intraction(c)
    obj_Intraction.main()
    obj_definition = EOC_definition.definition(c)
    obj_definition.main()"""
    c.saveAndCloseWriter()

