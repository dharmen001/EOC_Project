select substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as Placement#, DAY_DESC as Day, sum(VIEWS) as Delivered_Impression, sum(CLICKS) as Clicks, 
sum(CONVERSIONS) as Conversion from TFR_REP.DAILY_SALES_MV WHERE IO_ID = {0} AND TO_CHAR(TO_DATE(DAY_DESC, 'MM/DD/YYYY'),'YYYY-MM-DD') BETWEEN '{1}' AND '{2}' GROUP BY PLACEMENT_ID,PLACEMENT_DESC, DAY_DESC ORDER BY PLACEMENT_ID