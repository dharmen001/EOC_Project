import numpy as np
from EOC import rename_cols
def adding_column_Delivery():
    summary_new = rename_cols()
    conditions = [(summary_new["Creative Name"] == "Display & Mobile Performance Blend") &
                  (summary_new["Metric"] == "CPM w/ CPA goal") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Display & Mobile Performance Blend") &
                  (summary_new["Metric"] == "CPC") & (summary_new["Unit Cost"] == "CPC"),
                  (summary_new["Creative Name"] == "Display & Mobile Performance Blend") &
                  (summary_new["Metric"] == "View-based CPA") & (summary_new["Unit Cost"] == "CPA"),
                  (summary_new["Creative Name"] == "Display & Mobile Performance Blend") &
                  (summary_new["Metric"] == "CPC") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Display & Mobile Performance Blend") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Display & Mobile Performance Blend") &
                  (summary_new["Metric"] == "Click-based CPA") & (summary_new["Unit Cost"] == "CPA"),
                  (summary_new["Creative Name"] == "Display & Mobile Performance Blend") &
                  (summary_new["Metric"] == "Click-based CPA") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Display & Mobile Performance Blend") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Display & Mobile Performance Blend") &
                  (summary_new["Metric"] == "CPM w/ CPA goal") & (summary_new[ "Unit Cost"] == "CPC"),
                  (summary_new["Creative Name"] == "Expandable Adhesion/IAB Blend (Video)") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Expandable Adhesion/IAB Blend (Video)") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units - Billboard") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units - Desktop + Mobile") &
                  (summary_new["Metric"] == "CPM w/ CPA goal") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units - Desktop + Mobile") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units - Desktop + Mobile") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units - Desktop + Mobile") &
                  (summary_new["Metric"] == "CPC") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units - Desktop + Mobile") &
                  (summary_new["Metric"] == "View-based CPA") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units - Desktop + Mobile") &
                  (summary_new["Metric"] == "View-based CPA") & (summary_new["Unit Cost"] == "CPA"),
                  (summary_new["Creative Name"] == "IAB Units - Desktop + Mobile") &
                  (summary_new["Metric"] == "CPM w/ CPA goal") & (summary_new[ "Unit Cost"] == "CPA"),
                  (summary_new["Creative Name"] == "IAB Units - Desktop + Mobile") &
                  (summary_new["Metric"] == "Click-based CPA") & (summary_new["Unit Cost"] == "CPA"),
                  (summary_new["Creative Name"] == "IAB Units - Desktop + Mobile") &
                  (summary_new["Metric"] == "CPC") & (summary_new[ "Unit Cost"] == "CPC"),
                  (summary_new["Creative Name"] == "IAB Units - Desktop + Mobile") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPC"),
                  (summary_new["Creative Name"] == "IAB Units - Desktop + Mobile") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "IAB Units - Desktop + Mobile") &
                  (summary_new["Metric"] == "Click-based CPA") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units - Desktop + Mobile") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPA"),
                  (summary_new["Creative Name"] == "IAB Units - Desktop + Mobile") &
                  (summary_new["Metric"] == "CPM w/ CPA goal") & (summary_new[ "Unit Cost"] == "CPC"),
                  (summary_new["Creative Name"] == "IAB Units - Desktop + Mobile") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "IAB Units - Half Page") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units - Half Page") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units - Mobile Only") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units - Mobile Only") &
                  (summary_new["Metric"] == "CPM w/ CPA goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units - Mobile Only") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units - Mobile Only") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPCV"),
                  (summary_new["Creative Name"] == "IAB Units (Desktop + Mobile) + Expandable Adhesion (Non-Video)") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units (Desktop + Mobile) + Expandable Adhesion (Non-Video)") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units (Desktop + Mobile) + Expandable Adhesion (Non-Video)") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "IAB Units (Desktop + Mobile) + Expandable Adhesion (Non-Video)") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "IAB Units (Desktop + Mobile) + Expandable Adhesion (Video)") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units (Desktop + Mobile) + Expandable Adhesion (Video)") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units (Desktop + Mobile) + Non-Expanding Adhesion") &
                  (summary_new["Metric"] == "CPM w/ CPA goal") & (summary_new[ "Unit Cost" ] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units (Desktop + Mobile) + Non-Expanding Adhesion") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units (Desktop + Mobile) + Non-Expanding Adhesion") &
                  (summary_new["Metric"] == "CPM w/ CPA goal") & (summary_new[ "Unit Cost"] == "CPA"),
                  (summary_new["Creative Name"] == "IAB Units (Desktop + Mobile) + Non-Expanding Adhesion") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPC"),
                  (summary_new["Creative Name"] == "IAB Units (Desktop + Mobile) + Non-Expanding Adhesion") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPC"),
                  (summary_new["Creative Name"] == "IAB Units (Desktop + Mobile) + Non-Expanding Adhesion") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units (Desktop + Mobile) + Non-Expanding Adhesion") &
                  (summary_new["Metric"] == "CPM w/ CPA goal") & (summary_new["Unit Cost"] == "CPC"),
                  (summary_new["Creative Name"] == "IAB Units (Desktop + Mobile) + Non-Expanding Adhesion") &
                  (summary_new["Metric"] == "CPC") & (summary_new[ "Unit Cost"] == "CPC"),
                  (summary_new["Creative Name"] == "IAB Units (Mobile Only) + Non-Expanding Adhesion") &
                  (summary_new["Metric"] == "CPM w/ CPA goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units (Mobile Only) + Non-Expanding Adhesion") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPC"),
                  (summary_new["Creative Name"] == "IAB Units (Mobile Only) + Non-Expanding Adhesion") &
                  (summary_new["Metric" ] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPC"),
                  (summary_new["Creative Name" ] == "IAB Units (Mobile Only) + Non-Expanding Adhesion") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "IAB Units (Mobile Only) + Non-Expanding Adhesion") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Non-Expanding Adhesion/IAB Blend") &
                  (summary_new["Metric"] == "CPM w/ CPA goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Non-Expanding Adhesion/IAB Blend") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Non-Expanding Adhesion/IAB Blend") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Non-Expanding Adhesion/IAB Blend") &
                  (summary_new["Metric"] == "CPM w/ CPA goal") & (summary_new[ "Unit Cost"] == "CPA"),
                  (summary_new["Creative Name"] == "Pre-roll - Desktop") & (summary_new[ "Metric"] == "CPCV") &
                  (summary_new["Unit Cost"] == "CPCV"),
                  (summary_new["Creative Name"] == "Pre-roll - Desktop") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Pre-roll - Desktop") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Pre-roll - Desktop") & (summary_new[ "Metric"] == "CPCV") &
                  (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "Pre-roll - Desktop") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPCV"),
                  (summary_new["Creative Name"] == "Pre-roll - Desktop") &
                  (summary_new["Metric"] == "CPM w/ CPA goal") & (summary_new[ "Unit Cost"] == "CPCV"),
                  (summary_new["Creative Name"] == "Pre-roll - Desktop") &
                  (summary_new["Metric"] == "CPM w/ CPA goal") & (summary_new[ "Unit Cost" ] == "CPM"),
                  (summary_new["Creative Name"] == "Pre-roll - Desktop" ) & (summary_new[ "Metric"] == "vCPM") &
                  (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Pre-roll - Desktop + Mobile") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Pre-roll - Desktop + Mobile") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Pre-roll - Desktop + Mobile") &
                  (summary_new["Metric"] == "vCPM") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Pre-roll - Mobile") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Pre-roll - Mobile") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPCV"),
                  (summary_new["Creative Name"] == "Pre-roll - Mobile") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Pre-roll - Mobile") & (summary_new[ "Metric"] == "CPCV") &
                  (summary_new["Unit Cost"] == "CPCV"),
                  (summary_new["Creative Name"] == "Pre-roll -Desktop") & (summary_new[ "Metric"] == "CPCV") &
                  (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Pre-roll -Desktop") &
                  (summary_new["Metric"] == "CPM w/ CPA goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Pre-roll -Desktop") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Pre-roll -Desktop") & (summary_new[ "Metric"] == "CPCV") &
                  (summary_new["Unit Cost"] == "CPCV"),
                  (summary_new["Creative Name"] == "Pre-roll -Desktop") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Pre-roll -Desktop") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPCV"),
                  (summary_new["Creative Name"] == "Blend of VDX Rectangle/VDX Leaderboard/VDX Skyscraper/VDX Half"
                                                   "page/VDX Billboard") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost" ] == "CPM"),
                  (summary_new["Creative Name"] == "Blend of VDX Rectangle/VDX Leaderboard/VDX Skyscraper/VDX Half"
                                                   "page/VDX Billboard") &
                  (summary_new["Metric"] == "CPE") & (summary_new[ "Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "Blend of VDX Rectangle/VDX Leaderboard/VDX Skyscraper/VDX Half"
                                                   "page/VDX Billboard") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "Blend of VDX Rectangle/VDX Leaderboard/VDX Skyscraper/VDX Half"
                                                   "page/VDX Billboard") &
                  (summary_new["Metric"] == "CPE+") & (summary_new[ "Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "Blend of VDX Rectangle/VDX Leaderboard/VDX Skyscraper/VDX Half"
                                                   "page/VDX Billboard") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost" ] == "CPE"),
                  (summary_new["Creative Name"] == "Blend of VDX Rectangle/VDX Leaderboard/VDX Skyscraper/VDX Half "
                                                   "page/VDX Billboard") &
                  (summary_new["Metric"] == "vCPM") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Billboard") & (summary_new[ "Metric"] == "CPE") &
                  (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Billboard") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Billboard") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Billboard") & (summary_new[ "Metric"] == "CPE+") &
                  (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Billboard") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Billboard") & (summary_new["Metric"] == "vCPM") &
                  (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Billboard") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Display Blend") & (summary_new["Metric"] == "CPE") &
                  (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Display Blend") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Display Blend") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Display Blend") & (summary_new[ "Metric"] == "CPE+") &
                  (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Display Blend") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Halfpage") & (summary_new[ "Metric"] == "CPE") &
                  (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Halfpage") & (summary_new[ "Metric"] == "CPE+") &
                  (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Halfpage") & (summary_new[ "Metric"] == "CPM Branding") &
                  (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Halfpage") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Halfpage") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Halfpage") & (summary_new["Metric"] == "CPM Branding") &
                  (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX In-Stream") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX In-Stream") & (summary_new["Metric"] == "CPE") &
                  (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX In-Stream") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX In-Stream") & (summary_new["Metric"] == "CPCV") &
                  (summary_new["Unit Cost"] == "CPCV"),
                  (summary_new["Creative Name"] == "VDX In-Stream") & (summary_new["Metric"] == "CPE+") &
                  (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX In-Stream" ) & (summary_new[ "Metric" ] == "CPE") &
                  (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX In-Stream" ) & (summary_new[ "Metric" ] == "CPCV") &
                  (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX In-Stream") & (summary_new[ "Metric"] == "vCPM") &
                  (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX In-Stream") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX In-Stream") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX In-Stream") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new["Unit Cost"] == "CPCV"),
                  (summary_new["Creative Name"] == "VDX Leaderboard") & (summary_new["Metric"] == "CPE") &
                  (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Leaderboard") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Mobile Adhesion Banner") &
                  (summary_new["Metric"] == "CPE") & (summary_new[ "Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Mobile Adhesion Banner") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Mobile Adhesion Banner") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Mobile Adhesion Banner") &
                  (summary_new["Metric"] == "CPE+") & (summary_new[ "Unit Cost" ] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Mobile Adhesion Banner" ) &
                  (summary_new["Metric"] == "CPE") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Mobile Adhesion Banner") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Mobile Adhesion Banner") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Mobile IAB") & (summary_new["Metric"] == "CPE") &
                  (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Mobile IAB") & (summary_new["Metric"] == "CPE+") &
                  (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Mobile IAB") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Mobile IAB") & (summary_new["Metric"] == "vCPM") &
                  (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Mobile IAB") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name" ] == "VDX Mobile Leaderboard") &
                  (summary_new["Metric"] == "CPE") & (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Mobile Rectangle") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Mobile Rectangle") & (summary_new["Metric"] == "CPE") &
                  (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Rectangle") &
                  (summary_new["Metric"] == "CPM Branding") & (summary_new[ "Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Rectangle") &
                  (summary_new["Metric"] == "CPM w/ CTR goal") & (summary_new["Unit Cost"] == "CPM"),
                  (summary_new["Creative Name"] == "VDX Rectangle") & (summary_new["Metric"] == "CPE") &
                  (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Rectangle") & (summary_new["Metric"] == "CPE+") &
                  (summary_new["Unit Cost"] == "CPE"),
                  (summary_new["Creative Name"] == "VDX Skyscraper") & (summary_new["Metric"] == "CPE") &
                  (summary_new["Unit Cost"] == "CPE")]
    choices = [ "Delivered Impressions/Booked Engagements#Booked Impressions",
                "Sales_Clicks/Booked Engagements#Booked Impressions",
                "Conversions/Booked Engagements#Booked Impressions",
                "Delivered Impressions/Booked Engagements#Booked Impressions",
                "Delivered Impressions/Booked Engagements#Booked Impressions",
                "Conversions/Booked Engagements#Booked Impressions",
                "Delivered Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Sales_Clicks/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Delivered Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Delivered Impressions/Booked Engagements#Booked Impressions",
                "Delivered Impressions/Booked Engagements#Booked Impressions",
                "Conversions/Booked Engagements#Booked Impressions",
                "Conversions/Booked Engagements#Booked Impressions",
                "Conversions/Booked Engagements#Booked Impressions",
                "Sales_Clicks/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "Delivered Impressions/Booked Engagements#Booked Impressions",
                "Conversions/Booked Engagements#Booked Impressions",
                "Sales_Clicks/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Delivered Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Completions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Delivered Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Conversions/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Sales_Clicks/Booked Engagements#Booked Impressions",
                "Sales_Clicks/Booked Engagements#Booked Impressions",
                "Delivered Impressions/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Delivered Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Conversions/Booked Engagements#Booked Impressions",
                "Completions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "Completions/Booked Engagements#Booked Impressions",
                "Completions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Completions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Completions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Completions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Completions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Deep Engagements/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Deep Engagements/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "Deep Engagements/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "Deep Engagements/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Completions/Booked Engagements#Booked Impressions",
                "Deep Engagements/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "Completions/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Deep Engagements/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "Deep Engagements/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "KM_Impressions/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions",
                "Deep Engagements/Booked Engagements#Booked Impressions",
                "Delivered Engagements/Booked Engagements#Booked Impressions"]

    summary_new["Delivery"] = np.select(conditions, choices)
    print summary_new
