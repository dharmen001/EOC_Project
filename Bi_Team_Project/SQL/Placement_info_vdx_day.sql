SELECT substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as Placement#, 
PRODUCT,TO_CHAR(TO_DATE(DAY_DESC, 'MM/DD/YYYY'),'YYYY-MM-DD') as Day,
sum(IMPRESSIONS) as Impressions, sum(ENGAGEMENTS) as Engagements, 
sum(DPE_ENGAGEMENTS) as Dpeengagements, sum(VWR_VIDEO_VIEW_100_PC_COUNT) as View100, 
sum(CPCV_COUNT) as completions, sum(ENG_VIDEO_VIEW_100_PC_COUNT) as Eng100,
sum(DPE_INTERACTIVE_ENGAGEMENTS) as DpeIntractiveEngagements, sum(DPE_VIDEO_VIEW_100_PC_COUNT) as Dpe100 FROM TFR_REP.KEY_METRIC_MV 
WHERE IO_ID = {0} AND TO_CHAR(TO_DATE(DAY_DESC, 'MM/DD/YYYY'),'YYYY-MM-DD') BETWEEN '{1}' AND '{2}' GROUP BY PLACEMENT_ID, PLACEMENT_DESC, DAY_DESC,PRODUCT ORDER BY PLACEMENT_ID