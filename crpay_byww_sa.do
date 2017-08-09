*create weekly pay data for each worker-week for salaried workers (later merge these data with the worker-week-office-discipline-level pay data by payrollno)

set linesize 150
local path /home/hcmg/kunhee/Labor/Bayada_data

cd `path'/pay_CSV
loc f "EmployeeWeeklyGuarantee.txt"

import delimit "`f'", clear
*the worker ID in these data are the same worker IDs reported in the historical employment status / job title data (workerID_es)
rename ï workerID_es
rename effectivestartdate start
rename effectiveenddate end
rename value salary

*extract dates from effective start and end date vars
foreach v of varlist start end {
    replace `v' = "" if `v'=="NULL"
    gen `v'_d = substr(`v', 1, 10)
    split `v'_d, p("-")
}

foreach v of varlist *_d? {
    destring `v', replace
}

foreach v in start end {
    gen `v'd = mdy(`v'_d2, `v'_d3, `v'_d1)
    format `v'd %d
}
drop start_* end_*

*note the salary applies to visits provided on the week spanning from the start date to the end date

*is the end date mostly sunday?
gen dow = dow(startd)
tab dow
*no all over the week, mostly on Thursday
drop dow start end

tempfile salary
save `salary'

*----------------------------
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

*merge with salary data by the workerID_es
use `workersinvisit', clear
destring workerID_es, replace
merge 1:m workerID_es using `salary', keep(3) nogen
tempfile merges
save `merges'

*-------------------------
use `merges', clear

*for some workers, there are entries of status only for one day; if the previous (by date) obs before this entry has the end date same as the start date in the next obs after this entry, then drop this one-day status obs
gen same = startd==endd
sort workerID startd endd
bys worker: drop if same==1 & endd[_n-1]==startd[_n+1]

*recode the enddate of a worker's status if the next startdate is same as the end date
gen endd2 = endd
sort workerID startd endd
bys workerID: replace endd2 = endd2-1 if endd==startd[_n+1]
format endd2 %d
drop endd
rename endd2 endd

*if end date is before the start date, correct it
replace endd = mdy(12,1, 2016) if endd==.
gen gap = endd - startd + 1
count if gap <= 0
*27 obs - they all have start date = end date
drop if same==1 & gap <= 0

drop gap
duplicates drop

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
bys worker monday salary: gen i = _n==1
bys worker monday : egen si = sum(i)
bys worker monday: gen j = _n==1
tab si if j==1
*2573 worker-week obs (0.6%) have multiple salary values reported

preserve
sort worker monday date
keep if si > 1

*assign the last status as the status for that week
*flag whenever a status changes within a worker-week
bys workerID monday: gen ch = _n==1
bys workerID monday: replace ch = 1 if salary!=salary[_n-1]
bys workerID monday: gen sch = sum(ch)
bys workerID monday: egen a = max(sch)
keep if sch==a
keep payrollno worker salary monday sunday
duplicates drop

duplicates tag worker monday, gen(d)
assert d==0
drop d

tempfile multiple
save `multiple'
restore

drop if si > 1
keep payrollno worker salary monday sunday
duplicates drop
append using `multiple'

duplicates tag worker monday, gen(d)
assert d==0
drop d

sort workerID monday
compress
cd `path'
save pay_byww_sa, replace
