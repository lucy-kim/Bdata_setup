*get the first visit date and last visit date for each client Epi ID

cd /home/hcmg/kunhee/Labor/Bayada_data

use visit_allqtrs.dta, clear
rename visit_inhouse lov0
rename tot_time tot_time0
rename visit_points visit_points0
destring workerID, replace

*merge with the service code data
merge 1:m epiid workerID visitdate yr qtr using visit_svccode_time


rename _merge _m_svccode

assert visitd!=.
assert epiid!=.
collapse (min) fvd = visitd (max) lvd = visitd, by(epiid)

* do this restriction after merging with episode-level data!!!
*if first visit date is July 2011, then drop the episodes because we don't have all the visits for those episodes (this restriction may drop episodes whose real start is in July 2011)
*drop if month(fvd)==7 & year(fvd)==2011

*if last visit date is Dec 2015, drop the episodes because we don't have all the visits for those episodes (this restriction may drop episodes whose real end is in Dec 2015)


compress
save first_last_vd, replace
