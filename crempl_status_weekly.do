*create daily data showing each worker's employment arrangement on that day

loc path /home/hcmg/kunhee/Labor/Bayada_data
cd `path'/staff_CSV

loc f "Bayada Employee History File_EE Identifiers"
insheet using "`f'.csv", clear comma names
rename count workerID
rename v2 payrollno
rename v3 sysdes
lab var sysdes "Subsystem description"
drop if workerID=="WorkerId"

*there are duplicates in terms of workerID & payrollno but not in sysdes
duplicates drop workerID payrollno, force
compress
tempfile id
save `id'
*the worker ID here is different from the worker ID in the visit-level data

loc f "Bayada Employee History File_Benefit Class"
insheet using "`f'.csv", clear comma names
drop v1
rename count workerID_es
rename v3 status
rename v4 status_std
rename v5 status_endd
drop if workerID=="Worker Id"

foreach v of varlist status_std status_endd {
  replace `v' = "" if `v'=="NULL"
  split `v', p("/")
  replace `v'3 = "20" + `v'3
  destring `v'*, replace
  gen `v'_e = mdy(`v'1, `v'2, `v'3)
  format `v'_e %d
  drop `v'1 `v'2 `v'3 `v'
}
drop if status=="" & status_std==.
drop if status=="" | status=="."

compress
tempfile status
save `status'

*-------------------------------
*get the payrollno's appearing in the visit-level data so that I can restrict to workers that show up in the visit-level data
cd `path'
use staff_visit_office, clear
assert workerID!=.
tostring workerID, replace
merge m:1 workerID using staff_chars, keep(3) keepusing(payrollno) nogen
keep payrollno
assert payrollno!=""
duplicates drop

*merge with crosswalk b/w payrollno & workerID_es
merge 1:1 payrollno using payrollno_workerIDes_xwalk, keep(1 3)
*5033 _m=3 ; 1289 _m=1
keep if _merge==3
drop _merge

duplicates tag workerID_es, gen(d)
assert d==0
drop d

compress
tempfile workersinvisit
save `workersinvisit'

*merge with status data by the workerID_es
use `workersinvisit', clear
merge 1:m workerID_es using `status', keep(3) nogen
tempfile merges
save `merges'

*---------
*create weekly panel of workers showing the employment status for each worker-week
use `merges', clear
sort workerID status_std
rename status_std startd
rename status_end endd

*for some workers, there are entries of status only for one day; if the previous (by date) obs before this entry has the end date same as the start date in the next obs after this entry, then drop this one-day status obs
gen same = startd==endd
sort workerID startd endd
bys worker: drop if same==1 & endd[_n-1]==startd[_n+1]

*drop obs if the one-day status is different from the preceding or next status but the preceding and next statuses are equal
bys worker: drop if same==1 & endd[_n-1]==startd & startd[_n+1]==endd & status[_n-1]==status[_n+1] & status[_n-1]!=status
drop same

*recode the enddate of a worker's status if the next startdate is same as the end date
gen endd2 = endd
sort workerID startd endd
bys workerID: replace endd2 = endd2-1 if endd==startd[_n+1]
format endd2 %d
drop endd
rename endd2 endd

*if end date is before the start date, correct it
replace endd = mdy(1,1, 2016) if endd==.
gen gap = endd - startd + 1
count if gap <= 0
tab workerID if gap <= 0

sort workerID startd endd
bys workerID: drop if gap <=0 & endd[_n-1]==startd[_n+1] & status[_n-1]==status[_n+1] & status!=status[_n-1]
bys workerID: drop if gap <=0 & _n==1 & startd==startd[_n+1] & gap[_n+1]>0
bys workerID: drop if gap <= 0 & startd[_n+1]==startd
bys workerID: drop if gap <= 0 & endd[_n-1]==startd[_n+1]

*recode the enddate of a worker's status if the next startdate is same as the end date
gen endd2 = endd
sort workerID startd endd
bys workerID: replace endd2 = endd2-1 if endd==startd[_n+1]
format endd2 %d
drop endd
rename endd2 endd

drop gap
duplicates drop

*manually correct
sort workerID startd endd
bys workerID status startd: egen m = max(endd)
bys workerID status startd: drop if m!=endd
drop m

*recode the enddate of a worker's status if the next startdate is same as the end date
gen endd2 = endd
sort workerID startd endd
bys workerID: replace endd2 = endd2-1 if endd==startd[_n+1]
format endd2 %d
drop endd
rename endd2 endd

*count if enddate is after the next line's start date; if so and the next line's end date is one day before the start date in the obs after next line, then correct the end date
bys worker: gen after = endd >= startd[_n+1]
bys worker: replace endd = startd[_n+1] - 1 if after==1 & endd[_n+1]+1==startd[_n+2]
drop after

tempfile bf
save `bf'

*expand to daily obs
use `bf', clear
gen gap = endd - startd + 1
expand gap
sort worker startd endd
bys worker startd: gen date = startd if _n==1
bys worker startd: replace date = date[_n-1] + 1 if _n > 1
format date %d

*quality check
bys worker: egen a = max(endd)
bys worker: egen b = max(date)
assert a==b
tab workerID if a!=b
drop a b gap startd endd

*get start (Mon) date of the week to which the effective start date belongs
gen day = dow(date)
gen monday = date - 6 if day==0
forval d = 1/6 {
  loc d2 = `d' - 1
  replace monday = date - `d2' if day==`d'
}
format monday %d
drop day

*for the same worker-day, are there multiple statuses recorded?
duplicates tag worker date, gen(dup)
assert dup==0
drop dup

tempfile wk
save `wk'

*the monday of the previous week before the week to which the effective start date belongs is the week corresponding to the new status
use `wk', clear
replace monday = monday - 7
gen sunday = monday + 6
format sunday %d

*-------------------------
*for each worker-week, keep only 1 status

*first, are there many worker-week pairs that have multiple statuses?
sort worker monday date
bys worker monday status: gen i = _n==1
bys worker monday : egen si = sum(i)
bys worker monday: gen j = _n==1
tab si if j==1
*3369 worker-weeks have > 1 statuses (0.4%) - reassuring b/c worker-weeks reporting multiple statuses are anomalies

preserve
sort worker monday date
keep if si > 1
*assign the last status as the status for that week

*flag whenever a status changes within a worker-week
bys workerID monday: gen ch = _n==1
bys workerID monday: replace ch = 1 if status!=status[_n-1]
bys workerID monday: gen sch = sum(ch)
bys workerID monday: egen a = max(sch)
keep if sch==a
keep payrollno worker status monday sunday
duplicates drop

duplicates tag worker monday, gen(d)
assert d==0
drop d

tempfile multiple
save `multiple'
restore

drop if si > 1
keep payrollno worker status monday sunday
duplicates drop
append using `multiple'

duplicates tag worker monday, gen(d)
assert d==0
drop d

bys workerID: egen wk1_emplst = min(monday)
bys workerID: egen wklast_emplst = max(monday)
format wk1_empl %d
format wklast_empl %d
lab var wk1_empl "First week recorded in the historical empl status data for the worker"
lab var wklast_empl "Last week recorded in the historical empl status data for the worker"

sort workerID monday
compress

save `path'/empl_status_weekly, replace




*------------ drop below
  *merge the status data with daily pay data
use `paydaily', clear
merge m:1 workerID monday using `statusfinal'
*1,152 obs has _m=1, 230K obs has _m=2, 4.5M obs has _m=3

bys workerID: egen wk1_emplst = min(monday)
bys workerID: egen wklast_emplst = max(monday)
format wk1_empl %d
format wklast_empl %d
lab var wk1_empl "First week recorded in the historical empl status data for the worker"
lab var wklast_empl "Last week recorded in the historical empl status data for the worker"
rename _merge merge_payrate_status
lab var merge_payrate_status "_merge when merging hist pay rate data w/ hist empl status"
/* *keep only the overlapping range of weeks that appear in both the historical pay rate data and historical empl status data */
/* bys workerID: egen min = min(monday) if _m==3 */
/* bys workerID: egen max = max(monday) if _m==3 */
/* format min %d */
/* format max %d */
/* gsort workerID -_m */
/* foreach v of varlist min max { */
/*   bys workerID: replace `v' = `v'[_n-1] if `v' >= . */
/* } */
/* keep if monday >= min & monday <= max */
/* drop min max _m */
sort workerID monday date

append using `nomatch_inemplst'
gen nohiststatus = _m==1
lab var nohiststatus "=1 if the payrollno is not in the historical empl status data"
drop sysdes _m

compress
save `path'/empl_status, replace



*what is the monday date for each week to which the start date belongs?
gen day = dow(startd)
gen monday = startd - 6 if day==0
forval d = 1/6 {
  loc d2 = `d' - 1
  replace monday = startd - `d2' if day==`d'
}
format monday %d
drop day





*------------ drop below

*worker ID & payrollno are same but have different worker IDs in visit data -> reshape wide
drop dup _m
sort workerID workerID_visit
bys workerID: gen j = _n
reshape wide workerID_visit num, i(payrollno workerID sysdes) j(j)

*merge with historical changes in status
merge 1:m workerID using `status', keep(1 3) nogen

gen _merge = 3

tempfile tmp2
save `tmp2'

*append the _m=1 workers which didn't appear in the empl status data
use `tmp2', clear
append using `nomatch_inemplst'

*recode status of _m=1 guys using the acronyms based on the value of worker_category
replace status = "VFT" if worker_cate=="VISIT FULL TIME" & status==""
replace status = "PTN" if (worker_cate=="PART TIME NON PROFESSIONAL (HOURLY OF.."| worker_cate=="PART TIME NON PROFESSIONAL (HOURLY OFFCS)" ) & status==""
replace status = "PTP" if worker_cate=="PART TIME PROFESSIONAL (HOURLY OFFICES)" & status==""
replace status = "FT" if worker_cate=="FULL TIME (HOURLY OFFICES)" & status==""
replace status = "NON-EXEMPT-HR" if worker_cate=="NON EXEMPT HOURLY" & status==""
replace status = "NON EXEMPT-PT" if worker_cate=="NON EXEMPT PART TIME" & status==""
replace status = "SFT" if worker_cate=="HOSPICE FULL TIME" & status==""
replace status = "CONTRACTOR" if worker_cate=="NOT EMPLOYEE-CONTRACTOR" & status==""
replace status = "EXEMPT" if worker_cate=="EXEMPT" & status==""
drop worker_cate

*for workers still with missing values in status, merge with the HCHB worker data to get the status
preserve
keep if status==""
drop worker_cate
rename workerID workerID_es
rename workerID_visit1 workerID
merge m:1 workerID using `path'/hchb_worker, gen(m1) keep(1 3)
assert m1==3
replace status = "VPD" if m1==3 & worker_cate=="VISIT PER DIEM"
replace status = "CONTRACTOR" if worker_cate=="NOT EMPLOYEE-CONTRACTOR" & status=="" & m1==3
replace status = "EXEMPT" if worker_cate=="EXEMPT" & status=="" & m1==3
assert status!=""
keep payrollno-num
rename workerID workerID_visit1
rename workerID_es workerID
tempfile missingst
save `missingst'
restore

drop if status==""
append using `missingst'

assert status!=""
tab status
rename _merge merge_es
lab var merge_es "From merging the previous worker data with the new empl status data"
lab var num "Total number of visits by the visit worker ID"
lab var status_std "Worker status start date"
lab var status_endd "Worker status end date"
lab var workerID "Worker ID from the empl status data"
lab var workerID_visit1 "Worker ID appearing in visit-level data"
lab var workerID_visit2 "Worker ID 2 appearing in visit-level data"
lab var status "Worker status"

sort workerID status_std status_endd

*some workers have rows where status start date is later than status end date
assert status_std <= status_endd
gen a = status_std > status_endd
bys payrollno: egen b = max(a)
count if b==1
*list if b==1
tab status if a==1
*for now, drop the rows where start date is later than end date because that period is usually covered by other rows
drop if a==1
drop a b

*drop duplicates in all vars
duplicates drop

tempfile tmp
save `tmp'

*expand to daily obs
use `tmp', clear
sort payrollno status_std status_endd
gen d = status_endd - status_std

*if there are multiple status listed for the same start date & one of the status is only for one day, then delete it
duplicates tag payrollno status_std, gen(dup)
drop if d==0 & dup > 0

*if the first status end date is 11/2/2012 & the next status has a start date before the first status end date, then the first status end date is wrong. -> recode the first status end date as the second status start date
bys payrollno: gen x = status_std ==status_end[_n-1] if _n>1
bys payrollno: replace status_endd = status_std[_n+1] if x==. & x[_n+1]==0 & x[_n+2]==1 & status_endd == mdy(11,2,2012)

*for some workers, one status's period overlaps with a different status's period -> recode the status end date for the status that has a different status listed during that period
assert status_std <= status_endd
sort payroll status_std status_endd
bys payrol: gen nn = _N
drop x
bys payrollno: gen x = status_std < status_endd[_n-1] & status_std[_n+1]==status_endd+1 & nn > 1 & status_endd[_n-1] !=. & status_std[_n+1]!=. & status_endd!=.
count if x==1
list if x==1
*only one obs
bys payrollno: replace status_endd = status_std[_n+1]-1 if x[_n+1]==1 & status==status[_n+1]

*drop if the status changes the next day but the start date is recorded as the end date of the previous status
sort payrollno status_std status_endd
bys payrollno: replace status_std = status_std + 1 if status_end[_n-1]==status_std

drop d dup x nn
tempfile t
save `t'

use `t', clear

*for 3 workers, correct status date ranges
drop if status=="VPD" & status_endd==mdy(3,6,2013) & payrollno=="234060"
replace status_std = status_std + 1 if status_std==mdy(3,1,2013) & payrollno=="234060"
drop if status_std == mdy(12,22,2011) & payr=="257368"
replace status_std = mdy(12,22,2011) if status_endd==mdy(8,2,2012) & payr=="257368"
drop if status_std==mdy(1,22,2013) & payr=="338511"
replace status_std = status_std + 1 if status_std==mdy(6,28,2013) & payr=="338511"

replace status_endd = mdy(12,1,2015) if status_endd ==.
gen d = status_endd - (status_std - 1)
expand d
sort payrollno status_std status_endd
bys payrollno status_std status_end: gen i = status_std if _n==1
bys payrollno status_std status_end: replace i = i[_n-1] + 1 if _n > 1
format i %d

*drop any rows showing same status on the same day
drop status_std status_endd d
duplicates drop

duplicates tag workerID_visit1 i, gen(dup)
tab dup
drop dup

*get the status start and end date for each worker-status
sort payro i
capture drop k
bys payroll: gen k = 1 if _n==1
bys payroll: replace k = k[_n-1] + 1*(status!=status[_n-1]) if _n > 1
bys payroll k: egen status_std_e = min(i)
bys payroll k: egen status_endd_e = max(i)
format status_std_e %d
format status_endd_e %d

drop i k
duplicates drop

save `path'/empl_status, replace



/* *want to merge with the all-staff data to get demographic info & employment status data for some worker who didn't appear in the empl status data; but in the all-staff data, some workers appear with different workerIDs (visit workerID) though they are same persons. so keep one */
/* use `tmp2', clear */
/* rename workerID workerID_emplst */
/* rename workerID_visit1 workerID */

/* merge m:1 workerID using `path'/staff, gen(m2) */

/* preserve */
/* keep if m2==3 */
/* tempfile m3 */
/* save `m3' */
/* restore */

/* drop if m2==3 */
/* drop age-m2 */
/* rename workerID workerID_visit1 */
/* rename workerID_visit2 workerID */
/* merge m:1 workerID using `path'/staff, gen(m3) */
