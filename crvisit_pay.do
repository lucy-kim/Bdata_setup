*create a visit-level data containing workers' weekly pay, weekly number of visit points worked, employmenet status, productivity quota, job title for each worker-visit (code from crworker_panel.do)

loc path /home/hcmg/kunhee/Labor/Bayada_data
cd `path'
set linesize 150

*--------------------
* Set up
*--------------------
*create visit-level data with additional visit-level characteristics: service code, time, and employee characteristics, office ID
do crvisit_base

*create weekly pay data for each worker for each week
do crpay_byworker_byweek

*----------------------------------------
* create worker panel data
*----------------------------------------
*create worker-office-week level visit count data
use visit_worker_chars.dta, clear

/* *restrict to episode IDs that can be matched to the base sample of client episodes */
/* merge m:1 epiid using epi_hosp_base */
/* *0.4M has _m=1 ; 4.6M has _m=3; no _m=2 obs -> drop unmatched obs */
/* keep if _merge==3 */
/* drop _merge */

*how many workers and what type of workers do not get assigned a workerID (workerID_es) used in the historical status/pay data?
egen tag = tag(payrollno)
count if tag==1
*6321 unique workers

count if tag==1 & workerID_es==""
*20% or 1289 workers do not get assigned the workerID_es; 80% or 6321 workers get assigned the ID

tab status_hchb if tag==1 & workerID_es==""
*most workers are either "EXEMPT" (28%) or "NOT EMPLOYEE-CONTRACTOR" (70%)

tab jobdisc_hchb if tag==1 & workerID_es==""
*all across the board: PT (26%), RN, OT (15%), HHA (12%), ADMN (11%) etc.

tab jobdisc_hchb if workerID_es=="" & tag==1 & status_hchb=="EXEMPT"
tab jobtitle if workerID_es=="" & tag==1 & status_hchb=="EXEMPT"
tab jobdisc_hchb if workerID_es=="" & tag==1 & status_hchb=="NOT EMPLOYEE-CONTRACTOR"

tab jobtitle if workerID_es=="" & tag==1
*field clinician (70%), clinical manager (8%), clinical associate (5%), director (4%), senior living program manager (4%)

*what about matched workers?
tab status_hchb if workerID_es!="" & tag==1
*none of the matched workers are "CONTRACTOR"; 2 workers are "EXEMPT"

tab jobdisc_hchb if workerID_es!="" & tag==1
*all across the board: PT (26%), RN, OT (15%), HHA (12%), ADMN (11%) etc.

tab jobtitle if workerID_es!="" & tag==1
*field clinician (99.91%)


*---------------

  *merge with the worker-office-week level pay data
use visit_worker_chars.dta, clear
rename monday startdate
merge m:1 payrollno startdate offid_nu discipline using pay_byworker_byweek
*857K has _m=1; 120K has _m=2; 4.5M has _m=3

*create a fake payrollno so that for those workers who have non-missing worker ID but missing payrollno, I can fill in the missing values of payrollno
count if regexm(payrollno, "P")
replace payrollno = workerID + "P" if payrollno=="" & workerID!=""
assert payrollno!=""

sort payrollno startdate visitdate visittime
list payrollno workerID startdate visitdate status jobtitle _merge in 900000/900030

*restrict to the date ranges in which we have visit-level data; for each worker-office, drop weeks that precede the first matched week or that follow the last matched week
bys payrollno: egen first = min(startd) if _merge!=2
bys payrollno: egen first2 = min(first)
bys payrollno: egen last = max(startd) if _merge!=2
bys payrollno: egen last2 = max(last)
drop if startd < first2 | startd > last2

tab _merge
*there are still 8K obs w/ _m==2

*how many workers and what type of workers do not get assigned a workerID (workerID_es) used in the historical status/pay data?
sort payrollno startd offid_nu
*list payrollno startd offid_nu payrate s_pay* wnp _merge in 1/50
*tab payrollno if _m==1 in 1/10000
*list payrollno startd offid_nu payrate s_pay* wnp _merge if payrollno=="388310"
*list payrollno startd offid_nu payrate s_pay* wnp _merge if payrollno=="329612"

*Some workers have weekly pay information available for some weeks but not others
capture drop minm maxm
bys payrollno : egen minm = min(_merge)
bys payrollno : egen maxm = max(_merge)
tab minm maxm
*minm = maxm = 1 (BAD): 243K visits by workers all of whose work weeks are not matched to weekly pay data
*minm = 1, maxm = 2: 1263 obs ;
*minm = 1, max = 3 (GOOD but INCOMPLETE): 3.9M visits by workers some of whose work weeks are not matched to weekly pay data*minm = 2, max = 3 (GOOD but WEIRD): 2.7M visits by workers all of whose work weeks are matched to weekly pay data but who got paid on weeks they didn't work
*minm = 2, max = 3: 781K obs
*minm = 3, max = 3 (GOOD) : 0.445M visits by workers all of whose work weeks are matched to weekly pay data

*Some workers have no weekly pay information available for all weeks they worked (i.e. minm==maxm & maxm==1)
capture egen tag = tag(payrollno)
tab minm maxm if tag==1

*1,026 workers have weekly pay information available even on weeks they didn’t work (i.e. minm==2 & maxm==3)

*tab payrollno if minm==maxm & maxm==1
tab payrollno if minm==2 & maxm==3

tab _merge
rename _merge _m_wkpay
lab var _m_wkpay "_merge when merging visit count data w/ weekly pay data by worker-office-week"

drop minm maxm first* last* tag
rename startdate monday
compress

*weekly number of visit points a worker works
bys payrollno offid_nu monday: egen np_byww = sum(visit_points)

*add date_hired, date_terminated, termination_status, home office ID
merge m:1 workerID using staff_chars, keepusing(homeoffid auth_soc_recert zipcode age female date_hired_e date_term_e active) keep(1 3) nogen

sort payrollno monday visitdate
compress
save visit_pay, replace

/* tempfile tmp2 */
/* save `tmp2' */




    *---------------
/*merge with the worker-week level historical employment status data
use `tmp2', clear
merge m:1 payrollno monday using `path'/empl_status
sort payrollno monday

*restrict to the period for which the worker appears in the visit data
bys payrollno: egen min = min(monday) if _merge!=2
bys payrollno: egen max = max(monday) if _merge!=2
bys payrollno: egen min2 = max(min)
bys payrollno: egen max2 = max(max)
bys payrollno: drop if monday < min2 | monday > max2

*drop if purely from the employment status data but status is missing
drop if _merge==2 & status==""

drop min* max* dup

bys payrollno: egen min = min(_merge)
bys payrollno: egen max = max(_merge)
tab min max
*there are 47K worker-office-monday obs that are not assigned any status for all weeks - may be EXEMPT or CONTRACTOR
tab min max if tag==1

tab payrollno if min==1 & max==3 in 1/1000
*list payrollno monday status _merge if payrollno=="100282"

*fill in the missing values of status if the worker has missing values in status for the first few weeks
gsort payrollno -monday
bys payrollno: replace status = status[_n-1] if status==""

*are there more workers who have missing status while they have matched status obs for some weeks? NOPE
assert status!="" if max==3

*fill in the employment status
bys payrollno: egen min = min(_merge)
bys payrollno: egen max = max(_merge)
count
count if status=="" & min==max & max==1
codebook status_hchb if status=="" & min==max & max==1
tab status_hchb if status=="" & min==max & max==1

*fill in the status reported in the cross-sectional HCHB data
replace status = status_hchb if status=="" & min==max & max==1

preserve
keep if min==max & max==1 & status==""
drop status_hchb
merge m:1 payrollno using staff_chars2, keep(1 3) keepusing(status_hchb) nogen
replace status = status_hchb if status=="" & min==max & max==1
assert status!=""
drop status_hchb
tempfile filled2
save `filled2'

restore
drop if min==max & max==1 & status==""
append using `filled2'
assert status!=""

tab status

*standardize the status categories
replace status = "NON EXEMPT-PT" if status=="NON EXEMPT PART TIME"
replace status = "PTP" if status=="PART TIME PROFESSIONAL (HOURLY OFFICES)"
replace status = "PTN" if status=="PART TIME NON PROFESSIONAL (HOURLY OFFCS)"
replace status = "FT" if status=="FULL TIME (HOURLY OFFICES)"
replace status = "SFT" if status=="HOSPICE FULL TIME"
replace status = "NON-EXEMPT-HR" if status=="NON EXEMPT HOURLY"
replace status = "VFT" if status=="VISIT FULL TIME"
replace status = "VPB" if status=="VISIT PART-TIME W/BENEFITS"
replace status = "VPD" if status=="VISIT PER DIEM"
tab status


*tag salaried & piece-rate workers: define a worker as permanent only if salaried, though she is not full-time (e.g. part-time)
gen salaried = status=="VFT" | status=="VPB" | status=="VPC" | status=="SFT" | status=="SPB" | status=="SPC" | status=="EXEMPT"
gen piecerate = 1 - salaried
*/
/* gen temp = status=="CONTRACTOR" | status=="VPD" | status=="SPD" | status=="STATN" | status=="STATP" | status=="PTP" | status=="PTN" | status=="FT" | status=="NON EXEMPT-PT" | status=="NON-EXEMPT-HR" */
/* *tricky statuses: STATN, STATP, PTN, PTP, FT, Non-exempt PT/HR */

rename _merge _m_emplstatus
lab var _m_emplstatus "_merge when merging with historical empl status data"

*drop _m_emplstatus = 2 obs
drop if _m_emplstatus == 2
drop min max
*---------------
*merge with the worker-week level quota data
merge m:1 workerID_es monday using `path'/productivity_quota, keep(1 3)

*are the workers who are unmatched to the productivity quota data not guaranteed pay workers?
tab status if _merge==1
tab status if _merge==3
bys payrollno: egen min = min(_merge)
bys payrollno: egen max = max(_merge)
tab min max
tab status if min==1 & max==1
*90% of them are VPD

tab payrollno if _merge==1 & status=="VFT" in 1/10000

sort payrollno monday
*list payrollno monday status quota* wnp wnv payrate pay_* _merge if payrollno=="100462"

rename _merge _m_quota
lab var _m_quota "_merge when merging with historical productivity quota data"
drop min max

/* list payrollno monday offid_nu job status wnp_bywow wpayr_bywow pay_* quota* in 1050/1100 */
/* list payrollno monday offid_nu job status wnp_bywow wpayr_bywow pay_* quota* if payroll=="11-11972" */

*---------------
/* *merge w/ staff characteristics data */
/* merge m:1 payrollno using staff_chars2, keep(1 3) nogen */

/* *if status is missng, use the status from HCHB data */
/* count if status=="" */
/* replace status = status_hchb if status=="" */
/* replace status = "VFT" if status=="VISIT FULL TIME" */
/* replace status = "VPB" if status=="VISIT PART-TIME W/BENEFITS" */
/* replace status = "VPC" if status=="VISIT PART-TIME W/O BENEFITS" */
/* replace status = "VPD" if status=="VISIT PER DIEM" */
/* replace status = "STATP" if status=="STAT PROFESSIONAL (HOURLY OFFICES)" */
/* replace status = "FT" if status=="FULL TIME (HOURLY OFFICES)" */
/* replace status = "SFT" if status=="HOSPICE FULL TIME" */
/* replace status = "SPB" if status=="HOSPICE PART-TIME W/BENEFITS" */
/* replace status = "SPD" if status=="HOSPICE PER DIEM" */
/* replace status = "NON-EXEMPT-HR" if status=="NON EXEMPT HOURLY" */
/* replace status = "NON EXEMPT-PT" if status=="NON EXEMPT PART TIME" */
/* replace status = "PTN" if status=="PART TIME NON PROFESSIONAL (HOURLY OFFCS)" */
/* replace status = "PTP" if status=="PART TIME PROFESSIONAL (HOURLY OFFICES)" */


    *merge with the historical job title data by payrollno-week
merge m:1 payrollno monday using jobtitle_chng, keep(1 3)
rename job job_wkpay
lab var job_wkpay "Job reported for each week from the weekly pay data (recoded; roles played)"
rename job_title job
lab var job "Job title reported in the historical job title data"

bys payrollno: egen min = min(_merge)
bys payrollno: egen max = max(_merge)
tab min max

*fill in the missing values of status if the worker has missing values in status for the first few weeks
gsort payrollno -monday
bys payrollno: replace job = job[_n-1] if job==""

*are there more workers who have missing job title while they have matched job title obs for some weeks? NOPE
assert job!="" if max==3

gsort payrollno monday
*list payrollno monday job status quota* wnp wnv payrate pay_* _merge if payrollno=="100462"

/* tab payrollno if job=="" */
/* list payrollno monday job status quota* wnp wnv payrate pay_* _merge if payrollno=="373760" */

rename _merge _m_job
lab var _m_job "_merge when merging with historical job title data"

*for workers who cannot be matched to historical empl status or job title data, fill in the missing values in those variables using the cross-sectional staff chars data
count
count if min==max & max==1 & job==""
*13% of our obs have missing job title

*first fill in the jobs reported in weekly pay data
replace job = job_wkpay if job_wkpay!="" & job=="" & min==max & max==1

preserve
keep if min==max & max==1 & job==""
merge m:1 payrollno using staff_chars2, keep(1 3) keepusing(jobdisc) nogen
replace job = jobdisc_hchb if job==""
assert job!=""
drop jobdisc_hchb
tempfile filled
save `filled'

restore
drop if min==max & max==1 & job==""
append using `filled'
assert job!=""

*redefine the job discipline
capture drop disc
gen discipline = "ST" if job=="ST"

replace disc = "SN" if regexm(job, "BSN") | regexm(job, "RN") | job=="MSN" | job=="RD" | job=="TV" | job=="SAX" | job=="LPN"

replace disc = "PT" if job=="PT" | job=="MPT" | job=="DPT" | job=="BSPT" | job=="PTA"

replace disc = "OT" if job=="OCCUPATIONAL THERAPIST" | job=="OT" | job=="OTR" | job=="OTRL" | job=="OTA"

replace disc = "HHA" if regexm(job, "HHA") | regexm(job,"CNA")
replace disc = "MSW" if job=="MSW"

replace disc = "Oth" if job=="CT" | job=="FS"

replace disc = "SN" if job=="ADMN"

assert disc!=""

compress
save `path'/worker_panel, replace





*how many workers have different pay rates on the same week?
sort payrollno monday date
bys payrollno monday payrate: gen a = 1 if _n==1
bys payrollno monday: egen sa = sum(a)
count
count if sa > 1

*---------------
*before merging with the worker-office-week level pay data, aggregate the pay data to the level of worker-week
use pay_byworker_byweek, clear

collapse (sum) s_pay_* , by(payrollno startdate njobs noffices job)

foreach l in "pay_BONUS" "pay_OVERTIME" "pay_PTO" "pay_REGULAR" "pay_SALARY" "pay_TRANSPORTATION" {
  rename s_`l' `l'
}
rename startdate monday
compress
save pay_byworker_byweek2, replace

*---------------
  *merge the worker-week-level pay data with the worker-day-week level employment status & visit count data
use `tmp', clear
merge m:1 payrollno monday using pay_byworker_byweek2

*---------------
*1) number of visits provided by each worker-office on each week;
*2) employment status (salaried or piece-rate) info
*3) visit quota for salaried staff

use an1_v, clear

*get start (Mon) and end dates (Sun) of the week to which the visit date belongs
gen day = dow(visitdate)
gen startdate = visitdate - 6 if day==0
forval d = 1/6 {
  loc d2 = `d' - 1
  replace startdate = visitdate - `d2' if day==`d'
}
format startdate %d
drop nv_ nw_ totnv day

collapse (sum) wnp_bywow = visit_points, by(payrollno temp quota quotacat status job offid_nu startdate)

duplicates tag payro offid_nu startd, gen(dup)
tab dup

sort payr startd offid job temp
list if payr=="100357"

*reshape wide to get weekly # visit points per worker-week-office on a permanent and piece-rate status since some worker-offices work on both status on the same week
list payr startd offid job temp quota* status job dup if dup > 0 in 1/100
reshape wide wnp_bywow, i(payrollno offid_nu startdate) j(temp)
rename wnp_bywow0 wnp_bywow_pm
label var wnp_bywow_pm "Weekly # visit points worked under permanent for worker-office-week level"
rename wnp_bywow1 wnp_bywow_pr
label var wnp_bywow_pr "Weekly # visit points worked under piece-rate for worker-office-week level"

duplicates tag payro offid_nu startd, gen(dup)
assert dup==0
drop dup

/* *for 0.5% worker-office-status-week obs who have both piece-rate and non-piece-rate status on the same week-office, choose the status for which there are more visits */
/* bys payro startd offid_nu: egen aa = max(wnp) */
/* gen b = wnp==aa */
/* drop if dup > 0 & b==0 */
/* drop dup aa b */
/* duplicates tag payro offid_nu startd, gen(dup) */
/* tab dup */

/* *for still 0.03% worker-office-status-week obs who have both statuses & have same number of visits provided under either status, use the status recorded for the next week */
/* sort payroll offid startd temp */

list if payr=="383648"

tempfile wnp
save `wnp'

*merge with the pay by worker-discipline-office-week data
use `wnp', clear
merge 1:1 payrollno startd offid_nu using pay_byworker_byweek
sort payrollno startd offid_nu

*drop the week start date obs if they are beyond the range reported in the visit-level data
sum startd if _m==3
tab startd if startd==20359
drop if startd > mdy(9,28,2015) & _m==2

drop if _m==2 & startd < mdy(9,26,2011)


*if a _m==2 obs is right after a _m==1 or _m==3 obs, it means that the pay is provided for the previous week but the worker didn't work on the week the pay came out. so add the pay to the previous obs


*change s_pay to the sum of overtime, regular, and salary
drop s_pay
egen s_pay = rowtotal(s_pay_OVERTIME s_pay_REGULAR s_pay_SALARY)
gen pay_pp = s_pay/wnp_bywow
label var pay_pp "Pay per visit point = sum of overtime, regular, and salary"

*---------------
  *2) employment status (salaried or piece-rate) info



list offid_nu startd wnp_bywow* s_pay_* _m if payr=="100124"

list job offid_nu startd wnp_bywow* s_pay_* _m if payr=="383648"

use worker_panel, clear
drop if year(monday)==2011
sort payrollno monday offid_nu

egen tag = tag(payrollno monday offid_nu)
bys payrollno: egen nn = sum(tag)

list payrollno monday offid_nu wnp pay* salaried discipline status nn in 1/50
