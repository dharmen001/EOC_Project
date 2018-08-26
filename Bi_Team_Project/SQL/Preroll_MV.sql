select substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as Placement#,
sum(IMPRESSIONS) as Impression, 
sum(CPCV_COUNT) as Completions 
from TFR_REP.KEY_METRIC_MV WHERE IO_ID = {0} AND TO_CHAR(TO_DATE(DAY_DESC, 'MM/DD/YYYY'),'YYYY-MM-DD') BETWEEN '{1}' AND '{2}' 
GROUP BY PLACEMENT_ID, PLACEMENT_DESC ORDER BY PLACEMENT_ID