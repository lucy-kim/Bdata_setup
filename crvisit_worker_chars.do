*create visit-level data with worker characteristics (employment status, job title as of the visit date) attached

set linesize 150
local path /home/hcmg/kunhee/Labor/Bayada_data
cd `path'
local mvar admissionclie

*create worker-office-week level visit count data
use staff_visit_office, clear

*get start (Mon) date of the week to which the visit date belongs
gen day = dow(visitdate)
gen monday = visitdate - 6 if day==0
forval d = 1/6 {
  loc d2 = `d' - 1
  replace monday = visitdate - `d2' if day==`d'
}
format monday %d
drop day
gen sunday = monday + 6
format sunday %d

*get payrollno since several worker IDs map to one payrollno
tostring workerID, replace
merge m:1 workerID using staff_chars, keep(1 3) nogen keepusing(payrollno)

*Missing values in payrollno (-> drop them)
*tab workerID if payrollno==""
count if payrollno==""
count
/* drop if payrollno=="" */

*get the employee ID used in the historical empl status data
merge m:1 payrollno using payrollno_workerIDes_xwalk, keep(1 3) nogen

tempfile init
save `init'

*----------------
*merge with the worker-week level historical employment status data
use `init', clear
merge m:1 workerID_es payrollno monday sunday using empl_status_weekly, keep(1 3) gen(_m_status)
*4.9M has _m=3; 0.5M has _m=1

*what is the last match date?
tab monday if _m_status==3 & year(monday)==2015 & month(monday)>8
*12/21/2015

*merge with the worker-week level historical productivity data
/*list visit_points productivity if visit_points!=. & productivity!=. & visit_points!=productivity
sum visit_points productivity if visit_points!=. & productivity!=. & visit_points==productivity*/
drop productivity
merge m:1 workerID_es payrollno monday sunday using productivity_weekly, keep(1 3) gen(_m_productivity)

*merge with the worker-week level historical salary data
destring workerID_es, replace
merge m:1 workerID_es payrollno monday sunday using pay_byww_sa, keep(1 3) gen(_m_salary)

*missing values in status
gen nostatus = status==""

*fill in the missing values of status if the worker has missing values in status for the first few weeks
gsort payrollno -visitdate
bys payrollno: replace status = status[_n-1] if status==""
count if status==""
tab status if nostatus==1

*fill in the missing values of status if the worker has missing values in status for the last few weeks
gsort payrollno visitdate
bys payrollno: replace status = status[_n-1] if status==""
count if status==""
tab status if nostatus==1

*for still missing values in status, get it from HCHB data
tab status
merge m:1 workerID using staff_chars, keep(1 3) nogen keepusing(status_hchb)

*standardize the status_hchb categories
replace status_hchb = "NON EXEMPT-PT" if status_hchb=="NON EXEMPT PART TIME"
replace status_hchb = "PTP" if status_hchb=="PART TIME PROFESSIONAL (HOURLY OFFICES)"
replace status_hchb = "PTN" if status_hchb=="PART TIME NON PROFESSIONAL (HOURLY OFFCS)"
replace status_hchb = "FT" if status_hchb=="FULL TIME (HOURLY OFFICES)"
replace status_hchb = "SFT" if status_hchb=="HOSPICE FULL TIME"
replace status_hchb = "SPD" if status_hchb=="HOSPICE PER DIEM"
replace status_hchb = "NON-EXEMPT-HR" if status_hchb=="NON EXEMPT HOURLY"
replace status_hchb = "VFT" if status_hchb=="VISIT FULL TIME"
replace status_hchb = "VPB" if status_hchb=="VISIT PART-TIME W/BENEFITS"
replace status_hchb = "VPC" if status_hchb=="VISIT PART-TIME W/O BENEFITS"
replace status_hchb = "VPD" if status_hchb=="VISIT PER DIEM"
tab status_hchb

*how many obs did I interpolate the status?
tab nostatus if status!=""
*1% or 50K obs

*if status was missing and the interpolated status indicates non-salary positions while status_hchb indicates salary position & (productivity is non-missing & > 0 | salary is non-missing & > 0), then use the status_hchb
tab status status_hchb if nostatus==1 & status!="" & status_hchb!=status
*most people are VPD in HCHB but VFT from the interpolated data; next largest observations are the other way around

*Case 1: interpolated = VFT but HCHB = VPD
gen possalary = (productivity!=. & productivity >0) |  (salary!=. & salary > 0)
loc cond "status=="VFT" & status_hchb=="VPD" & nostatus==1 & status!="" & status_hchb!=status"
count if possalary==0 & `cond'
* 6335/ 7038 contradictions
*tab payrollno if status=="VFT" & status_hchb=="VPD" & nostatus==1 & status!="" & status_hchb!=status & possalary==0

*when status is interpolated, if salary or productivity is missing or 0, make status = VPD
replace status = status_hchb if possalary==0 & `cond'

*Case 2: interpolated = VPD & HCHB = VFT; VPD is correct b/c productivity or salary is missing / 0
loc cond "status=="VPD" & status_hchb=="VFT" & nostatus==1 & status!="" & status_hchb!=status"
tab possalary if `cond'
*all of them have no postive salary or productivity -> should be VPD, & no change required

*Case 3: interpolated = VFT / VPC / VPB & HCHB = VPD
loc cond "(status=="VFT" | status=="VPC" | status=="VPB") & status_hchb=="VPD" & nostatus==1 & status!="" & status_hchb!=status"
tab possalary if `cond'
*60% have positive salary or productivity; change the status to VPD for the remaining 40%
replace status = status_hchb if possalary==0 & `cond'

*still missing values in status
count if status==""
*0.4M obs
tab status_hchb if status==""
*55% are EXEMPT, 42% Contractor
replace status = status_hchb if status==""
assert status!="" if payrollno!=""
assert payrollno=="" if status==""
*if payrollno is missing, status is missing too.

*create a fake payrollno so that for those workers who have non-missing worker ID but missing payrollno, I can fill in the missing values of payrollno
count if regexm(payrollno, "P")
replace payrollno = workerID + "P" if payrollno=="" & workerID!=""
assert payrollno!=""

*tag salaried & piece-rate workers: define a worker as permanent only if salaried, though she is not full-time (e.g. part-time)
tab status
gen salaried = status=="VFT" | status=="VPB" | status=="VPC" | status=="SFT" | status=="SPB" | status=="SPC" | status=="EXEMPT" | status=="NON-EXEMPT-HR"
gen piecerate = 1 - salaried

*tag office & field workers
gen officew = status=="EXEMPT" | status=="NON EXEMPT-PT" | status=="NON-EXEMPT-HR"
replace officew = . if status==""
gen fieldw = 1 - officew
assert fieldw==. if officew==.

*tag office - SA & PR workers
gen officew_sa = officew==1 & salaried==1
gen officew_pr = officew==1 & piecerate==1

*tag field - SA & PR workers
gen fieldw_sa = fieldw==1 & salaried==1
gen fieldw_pr = fieldw==1 & piecerate==1

*tag field - contractor workers
gen contractor = status=="NOT EMPLOYEE-CONTRACTOR"

foreach v of varlist fieldw_* officew_* contractor {
    replace `v' = . if status==""
}

lab var status "Status from the historical empl status data (interpolated if nostatus==1)"
lab var nostatus "=1 if worker-week has no status assigned from the historical empl status data"
lab var status_hchb "Status from the HCHB worker chars data"
lab var salary "Salary for guranteed pay workers"
lab var productivity "Productivity expected for each worker-week"
lab var salaried "=1 if one of VFT, VPB, VPC, SFT, SPB, SPC, Exempt, or Non-exempt-HR"
lab var piecerate "=1 if not salaried"
lab var possalary "=1 if non-miss productivity > 0 | non-miss salary > 0"
lab var officew "=1 if office worker; Exempt Non-exempt-pt Non-exempt-hr"
lab var fieldw "=1 for field worker"
lab var officew_sa "=1 for salaried office worker"
lab var officew_pr "=1 for piece-rate office worker"
lab var fieldw_sa "=1 for salaried field worker"
lab var fieldw_pr "=1 for piece-rate field worker"
lab var contractor "=1 for contractor field worker"
lab var prodcat "Category of productivity goal (visit (99.98%) or hourly)"

*for missing values in the visit discipline, use the major discipline for which the worker provided most visits
count if discipline==""
bys payrollno discipline: gen n = _N
bys payrollno : egen maxn = max(n)
bys payrollno : gen most = discipline if maxn==n
gsort payrollno -most
bys payrollno: replace most = most[_n-1] if most==""
count if most==""
replace discipline = most if discipline==""
count if discipline==""
*1 obs still missing - but this guy provides only 1 visit & has no discipline records
drop n maxn
rename most majordisc
lab var majordisc "unique major discipline for each payrollno; provided most visits in that disc"

*1 correction of major discipline: payrollno=="252377" having 2 major disciplines
bys payrollno majordisc: gen i = _n==1
bys payrollno: egen si = sum(i)
tab payrollno if si > 1
replace majordisc = "ST" if payrollno=="252377"
drop si i

*merge with daily pay rate data
tostring workerID_es, replace
merge m:1 visitdate workerID_es payrollno using payrate_daily, keep(1 3) keepusing(payrate) gen(_m_payrate)
tab status if payrate==.
tab status if payrate!=.
lab var payrate "Pay rate for each worker-day"

assert visitdate!=.

*drop 1 obs for 1 contractor we don't have a discipline for
list if majordisc==""
drop if majordisc==""

tempfile tmp
save `tmp'

*fill in missing values in office ID using the most frequent office ID for each worker
use `tmp', clear
count if offid_nu==.
keep payrollno offid_nu
bys payrollno offid_nu: gen n = _N
bys payrollno: egen max = max(n)
keep if n==max
duplicates drop
duplicates tag payrollno, gen(dup)
tab dup
list if dup > 0
drop if dup > 0 & offid_nu==.

merge m:1 payrollno using staff_chars2, keep(1 3) keepusing(homeoffid*)
drop dup
duplicates tag payrollno, gen(dup)

preserve
use office, clear
keep offid*
drop offid
rename offid0 offid
tempfile office
save `office'
restore

list if dup > 0
rename homeoffid offid
rename offid_nu offid_nu0
merge m:1 offid using `office', keep(1 3) nogen keepusing(offid_nu)
list if dup > 0
replace offid_nu0 = offid_nu if dup > 0 & offid_nu!=.
replace offid_nu0 = 2 if offid=="002" & dup > 0
duplicates drop
drop dup offid offid_nu
duplicates tag payrollno, gen(dup)
list if dup > 0
replace offid_nu = . if dup > 0
drop n max _merge dup
duplicates drop
rename offid_nu0 offid_nu_freq
tempfile freqoffice
save `freqoffice'

use `tmp', clear
merge m:1 payrollno using `freqoffice', keep(1 3) nogen
replace offid_nu_freq = offid_nu if offid_nu_freq==.
count if offid_nu_freq==.

gsort epiid visitdate
bys epiid: replace offid_nu_freq = offid_nu_freq[_n-1] if offid_nu_freq >= .
bys epiid offid_nu_freq: gen n = _N
bys epiid: egen maxn = max(n)
gen a = offid_nu_freq if n==maxn
gsort epiid -a
bys epiid: replace a = a[_n-1] if a >= .

sort epiid a
bys epiid a: gen i = _n==1
bys epiid: egen si = sum(i)
tab si

gen office_missing = offid_nu==.
lab var office_missing "Office ID is missing in original data"

replace offid_nu = a if offid_nu==. & si==1
assert offid_nu!=.
drop offid_nu_freq n maxn a i si

compress
save `path'/visit_worker_chars, replace








*----------------
    /* preserve
do /home/hcmg/kunhee/Labor/crjobtitle_chng_daily
restore

*merge with the historical job title data by payrollno-week
merge m:1 workerID_es date monday using `path'/jobtitle_chng_daily, keep(1 3)
rename job_title job
lab var job "Job title reported in the historical job title data"

bys payrollno: egen min = min(_merge)
bys payrollno: egen max = max(_merge)
tab min max

*fill in the missing values of status if the worker has missing values in status for the first few weeks
gsort payrollno -date
bys payrollno: replace job = job[_n-1] if job=="" & job[_n-1]!=""

*are there more workers who have missing job title while they have matched job title obs for some weeks? NOPE
assert job!="" if max==3

/* gsort payrollno monday */
*list payrollno monday job status quota* wnp wnv payrate pay_* _merge if payrollno=="100462"

/* tab payrollno if job=="" */
/* list payrollno monday job status quota* wnp wnv payrate pay_* _merge if payrollno=="373760" */

rename _merge _m_job
lab var _m_job "_merge when merging with historical job title data"

*for workers who cannot be matched to historical empl status or job title data, fill in the missing values in those variables using the cross-sectional staff chars data
count
count if min==max & max==1 & job==""
*13% of our obs have missing job title

*use the job code reported in the visit-level service code data
count if jobcode=="" & job=="" & min==max & max==1
replace job = jobcode if jobcode!="" & job=="" & min==max & max==1

/*for 72 obs with still missing values in job title, use fill in with jobs reported in weekly pay data
count if min==max & max==1 & job==""
*merge with the worker-office-week level pay data
rename monday startdate
rename job job0
merge m:1 payrollno startdate offid_nu using pay_byworker_byweek, keep(1 3) nogen keepusing(job)
rename job job_wkpay
rename job0 job
replace job = job_wkpay if job_wkpay!="" & job=="" & min==max & max==1
*6 filled*/

assert jobdisc_hchb!="" if job=="" & min==max & max==1
replace job = jobdisc_hchb if job=="" & min==max & max==1 & jobdisc_hchb!=""

assert job!=""

*redefine the discipline of visit using the job titles for missing values in discipline
count if discipline==""
tab job

capture drop disc2
gen disc2 = "ST" if job=="ST"
replace disc2 = "SN" if regexm(job, "BSN") | regexm(job, "RN") | job=="MSN" | job=="TV" | job=="SAX" | job=="LPN"
replace disc2 = "PT" if job=="PT" | job=="MPT" | job=="DPT" | job=="BSPT" | job=="PTA"
replace disc2 = "OT" if job=="OCCUPATIONAL THERAPIST" | job=="OT" | job=="OTR" | job=="OTRL" | job=="OTA"
replace disc2 = "HHA" if regexm(job, "HHA") | regexm(job,"CNA")
replace disc2 = "MSW" if job=="MSW"
/* replace disc2 = "Oth" if job=="CT" | job=="FS" | job=="RD"  */
    replace disc2 = "SN" if job=="ADMN"
replace disc2 = "FS" if job=="FS"
replace disc2 = "RD" if job=="RD"

replace discipline = disc2 if discipline=="" & disc2!=""
lab var disc2 "Discipline redefined based on the worker's historical job title"

assert discipline!=""
assert job!=""

drop min max

*-----------------------------------
    *merge with pay rate data by worker-office-visit date
*get start (Mon) date of the week to which the visit date belongs
capture rename startdate monday

*merge with daily worker's pay rate data by visit date
merge m:1 workerID_es monday date using `path'/payrate, keep(1 3) gen(_m_payrate)
lab var payrate "Historical default pay rate for each worker"
rename date visitdate_e

compress
save visit_worker_chars, replace */
