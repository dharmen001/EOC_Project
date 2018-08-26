SELECT substr(PLACEMENT_DESC,1,INSTR(PLACEMENT_DESC, '.', 1)-1) as Placement#,PRODUCT, 
MEDIA_SIZE_DESC as AdSize,sum(IMPRESSIONS) as Impressions, 
sum(ENGAGEMENTS) as Engagements, sum(DPE_ENGAGEMENTS) as DpeEngagements, 
sum(ENG_CLICK_THROUGHS) as EngClickthroughs, sum(DPE_CLICK_THROUGHS) as DpeClickthroughs, 
sum(VWR_CLICK_THROUGHS) as VwrClickthroughs,sum(VWR_VIDEO_VIEW_100_PC_COUNT) as View100, 
sum(ENG_VIDEO_VIEW_100_PC_COUNT) as Eng100,sum(DPE_VIDEO_VIEW_100_PC_COUNT) as Dpe100, 
sum(ENG_TOTAL_TIME_SPENT) as Engtotaltimespent,sum(DPE_TOTAL_TIME_SPENT) as Dpetotaltimespent, 
sum(ENG_INTERACTIVE_ENGAGEMENTS) as EngIntractiveEngagements, sum(DPE_INTERACTIVE_ENGAGEMENTS) as DpeIntractiveEngagements,
sum(CPCV_COUNT) as completions FROM TFR_REP.ADSIZE_KM_MV WHERE IO_ID = {0} AND TO_CHAR(DAY_DESC, 'YYYY-MM-DD') BETWEEN '{1}' AND '{2}' GROUP BY PLACEMENT_ID, 
PLACEMENT_DESC, MEDIA_SIZE_DESC,PRODUCT ORDER BY PLACEMENT_ID