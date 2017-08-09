*merge the visit-level data with visit service code-level data that contain visit code & time

loc path /home/hcmg/kunhee/Labor/Bayada_data
cd `path'

use visit_allqtrs.dta, clear
rename visit_inhouse lov0
rename tot_time tot_time0
rename visit_points visit_points0
destring workerID, replace

*merge with the service code data
merge 1:m epiid workerID visitdate yr qtr using visit_svccode_time

*drop 2011 data or 2015 Q4 data
drop if yr==2011 & _merge==1
/*drop if qtr==4 & yr==2015 & _merge==2*/
tab _merge
tab yr
tab qtr if yr==2015
tab _merge if yr==2015
tab yr if _merge!=3

/* sort epiid visitdate_e */
/* bys epiid: egen max = max(_merge) */
/* bys epiid: egen min = min(_merge) */
/* tab max min */
/* tab max if _merge==2 */
/* tab min if _merge==2 */
/* tab qtr if yr==2015 & _merge==2 */

/* preserve */
/* keep if _merge==2 */
/* merge m:1 epiid using masterclientadmiss2, gen(_m_epi) */
/* keep if _m_epi==3 */
/* restore */

/* drop min max */

sort workerID visitdate visittime_e

list workerID visitdate visittime_e epiid discipline jobcode description visittype visit_inhouse* *driven visit_travel in 1/1

rename _merge _m_svccode

rename visit_inhouse_hrs lov
lab var lov "Length of visit in hours from the service code"
lab var discipline "Visit discipline from the service code"
lab var productivity "Visit productivity point from the service code"
lab var description "Service code description"
lab var jobcode "Job code from the service code"
lab var billable "Billable Y/N from the service code"
lab var payable "Payable Y/N from the service code"
lab var payrolltransmit "Payroll Transmittal Group from the service code"
lab var oasis "OASIS Y/N from the service code"
lab var minexpectedvisit "Min Expected Visit Time (minutes) from the service code"
lab var visittime_e "Visit time from the service code"
lab var lov0 "Length of visit in hours from the original visit data"
lab var tot_time0 "Total Time Hours from the original visit data"
lab var _m_svccode "_merge when merging the raw visit-level data w/ service code data"
lab var visit_points0 "Visit point from the original visit data"
lab var visit_points "Visit point from the service code"

*for 710 observations that are only in the old original visit-level data but not in the new service code data, fill in values
foreach v of varlist lov visit_points tot_time {
    replace `v' = `v'0 if `v'==. & _m_svccode==1
}

foreach v of varlist epiid workerID lov visit_points {
    di "`v'"
    assert `v'!=.
}
count if visittime_e==. & _m_svccode!=1
count if visitd==. & _m_svccode!=1

foreach v of varlist discipline jobcode {
    assert `v'!="" if _m_svccode!=1
}

compress
save visit_codelevel, replace
