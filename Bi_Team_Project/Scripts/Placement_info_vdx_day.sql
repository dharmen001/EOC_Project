SELECT substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as Placement#, 
PRODUCT,TO_CHAR(TO_DATE(DAY_DESC, 'MM/DD/YYYY'),'YYYY-MM-DD') as Day,
sum(IMPRESSIONS) as Impressions, sum(ENGAGEMENTS) as Engagements, 
sum(DPE_ENGAGEMENTS) as Dpeengagements, sum(CPCV_COUNT) as completions,
sum(VWR_VIDEO_VIEW_100_PC_COUNT) as View100, sum(ENG_VIDEO_VIEW_100_PC_COUNT) as Eng100,sum(DPE_VIDEO_VIEW_100_PC_COUNT) as DPE100,
sum(ENG_CLICK_THROUGHS) as EnggerClickThrough, sum(VWR_CLICK_THROUGHS) as VwrClickThrough, sum(DPE_CLICK_THROUGHS) as DPEClickThrough,
sum(DPE_INTERACTIVE_ENGAGEMENTS) as DpeIntractiveEngagements FROM TFR_REP.EOC_KEY_METRIC_VIEW
WHERE IO_ID = {0} AND TO_CHAR(TO_DATE(DAY_DESC, 'MM/DD/YYYY'),'YYYY-MM-DD') BETWEEN '{1}' AND '{2}' GROUP BY PLACEMENT_ID, PLACEMENT_DESC, DAY_DESC,PRODUCT ORDER BY PLACEMENT_ID