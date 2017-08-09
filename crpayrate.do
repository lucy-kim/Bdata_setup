*create data for each worker's historical pay rate

loc path /home/hcmg/kunhee/Labor/Bayada_data
cd `path'/staff_CSV

loc f "Bayada Employee History File_Pay Rate"
insheet using "`f'.csv", clear comma names
drop v1
rename count workerID_es
rename v3 payrate
rename v4 pay_std
rename v5 pay_endd
drop if workerID=="Worker Id"

foreach v of varlist pay_std pay_endd {
  replace `v' = "" if `v'=="NULL"
  split `v', p("/")
  replace `v'3 = "20" + `v'3
  destring `v'*, replace
  gen `v'_e = mdy(`v'1, `v'2, `v'3)
  format `v'_e %d
  drop `v'1 `v'2 `v'3 `v'
}
drop if payrate==""
destring payrate, replace ig("$" "-" "$-" ",")

rename pay_std_e startd
rename pay_endd_e endd
list in 1/10
des

duplicates drop

compress
tempfile payrate
save `payrate'

*-------------------------
*get the payrollno's appearing in the visit-level data so that I can restrict to workers that show up in the visit-level data
cd `path'
use staff_visit_office, clear
assert workerID!=.
tostring workerID, replace
merge m:1 workerID using staff_chars, keep(1 3) keepusing(payrollno)
keep if _merge==3
drop _merge
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

* link data by workerID_es
use `payrate', clear
merge m:1 workerID_es using `workersinvisit', keep(3) nogen

*are there duplicates?
duplicates tag workerID_es startd endd, gen(d)
tab d

*for the same workerID_es startd endd cell, one obs has nonmissing payrate and another missing payrate, drop missing pay rate
gen miss = payrate==.
bys workerID_es startd endd: egen mm = mean(miss)
drop if d > 0 & mm < 1 & mm > 0

tempfile merges
save `merges'

*create weekly data for each worker
use `merges', clear
replace endd = mdy(12,1,2016) if endd==.

sort workerID startd endd
gen same = startd==endd
tab same

rename payrate value

*for some workers, there are multiple entries of different pay rates for one day -> use the last recorded pay rate value
bys workerID_es: egen ss = sum(same)
sort workerID startd endd
bys workerID_es: drop if same==1 & endd[_n-1]==startd & startd[_n+1]==endd & value[_n-1]==value[_n+1] & value[_n-1]!=value

*drop if the pay rate is only for 1 day and the end of the previous period and the start of the next period are the same day
bys workerID_es: drop if same==1 & startd==endd[_n-1] & startd[_n+1]==endd

*drop if the pay rate is recorded only for one day and the next entry of quota covers that day and lasts for a longer time
bys workerID_es: gen num = _N
gen gap = endd - startd + 1
bys workerID_es: drop if same==1 & startd[_n+1]==endd & gap[_n+1] > gap & num > 1

*if having the same start date, then choose the value that lasts longer
sort worker startd gap
bys worker startd: drop if startd==startd[_n+1] & gap < gap[_n+1]

*manually correct 2 workers who are later found to have duplicates by worker-day: workerID_es = 1105682, 1147087
drop if worker=="1105682" & startd==mdy(2,12,2013) & endd==mdy(2,15,2013)
drop if worker=="1105682" & startd==mdy(2,15,2013) & endd==mdy(2,18,2013) & value==34.21

drop if worker=="1147087" & startd==mdy(1,11,2013) & endd==mdy(1,22,2013)
drop if worker=="1147087" & startd==mdy(1,22,2013) & endd==mdy(4,11,2013)
drop if worker=="1147087" & startd==mdy(4,11,2013) & endd==mdy(6,5,2013)

*recode the enddate of a worker if the next startdate is same as the end date
gen endd2 = endd
bys workerID_es: replace endd2 = endd2-1 if endd==startd[_n+1]
format endd2 %d
drop endd
rename endd2 endd
drop gap d miss mm same ss num

gen gap = endd - startd + 1
assert gap >=1

tempfile bf
save `bf'


*expand to daily obs
use `bf', clear
expand gap
sort worker startd endd

bys worker startd endd : gen date = startd if _n==1
bys worker startd endd : replace date = date[_n-1] + 1 if _n > 1
format date %d
drop gap startd endd
duplicates drop

*now is there only one productivity category per worker-day?
duplicates tag worker date , gen(dd)
assert dd==0
drop dd

*get start (Mon) and end dates (Sun) of the week to which the visit date belongs
gen day = dow(date)
gen monday = date - 6 if day==0
forval d = 1/6 {
  loc d2 = `d' - 1
  replace monday = date - `d2' if day==`d'
}
format monday %d
drop day

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
bys worker monday value: gen i = _n==1
bys worker monday : egen si = sum(i)
bys worker monday: gen j = _n==1
tab si if j==1
*1% or 6K obs have > 1 pay rate values during the week

drop i si j

sort worker monday date
rename value payrate
rename date visitdate_e

compress
save `path'/payrate_daily, replace





*-------drop code below


bys worker: egen a = max(endd)
bys worker: egen b = max(date)
assert a==b
drop a b startd endd



rename workerID_es workerID
merge 1:m workerID using `payrate', keep(1 3) nogen
duplicates drop

*create a weekly panel
rename pay_std startd
rename pay_end endd
sort workerID startd endd

*if worker has different pay rate recorded for the same start date, then drop the obs whose end date is not missing (b/c the obs w/ missing end dates mean that the pay rate is still valid) if there is a missing end date correponding to the start date
gen miss = endd==.
bys worker startd: egen a = sum(miss)
bys worker startd: gen n = _N
bys worker startd: drop if n > 1 & a >= 1 & n > a & miss==0
drop a miss n
duplicates drop

*for some workers, there are entries of pay only for one day; for obs with same start dates, if the other obs has a longer effective date range, then drop it
gen same = startd==endd
sort workerID startd endd
tab same
replace endd = mdy(1,1, 2016) if endd==.
gen gap = endd - startd
sort worker startd gap
bys worker startd: gen n = _N
bys worker startd: egen maxgap = max(gap)
bys worker startd: drop if n > 1 & gap < maxgap

*manually correct some workers by looking up the time of change of pay rate in the raw data
drop if payrollno=="256906" & startd==mdy(1,22,2013)
replace endd = mdy(6,3,2013) if payrollno=="256906" & endd == mdy(6,5,2013)
drop if payrollno=="234238" & startd==mdy(2,13,2013)
drop if payrollno=="234238" & payrate!=34.21 & startd==mdy(2,15,2013)

/* manually correct by looking up the time of pay rate changes in the raw file */
/* drop if same==1 & worker=="1133368" */

/* bys worker: drop if same==1 & endd[_n-1]==startd[_n+1] & startd==startd[_n+1] */
/* bys worker: replace endd = startd[_n+1] - 1 if same[_n+1]==1 & endd==startd[_n+2] & endd==startd[_n+1] */
/* bys worker: replace startd = endd[_n-1] + 1 if same[_n-1]==1 & startd==endd[_n-2]+1 & startd==endd[_n-1] */
/* bys worker: drop if same[_n+1]==1 & endd==startd[_n+2] & endd==startd[_n+1]  */
/* *drop obs if the one-day status is different from the preceding or next status but the preceding and next statuses are equal */
/* sort workerID startd endd */
/* bys worker: drop if same==1 & endd[_n-1]==startd & startd[_n+1]==endd & payrate[_n-1]==payrate[_n+1] & payrate[_n-1]!=payrate */
drop same n maxgap gap

duplicates drop

*recode the enddate of a worker's pay rate if the next startdate is same as the end date
gen endd2 = endd
sort workerID startd endd
bys workerID: replace endd2 = endd2-1 if endd==startd[_n+1]
format endd2 %d
drop endd
rename endd2 endd

*if end date is before the start date, correct it
gen gap = endd - startd + 1
assert gap > 0

*expand to daily obs
drop if payrate==. & startd==.
*the 16 obs have pay rate and effective date ranges all missing.
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

*get start (Mon) and end dates (Sun) of the week to which the visit date belongs
gen day = dow(date)
gen monday = date - 6 if day==0
forval d = 1/6 {
  loc d2 = `d' - 1
  replace monday = date - `d2' if day==`d'
}
format monday %d
drop day

*create worker-week-day level pay rate data
rename workerID workerID_es
compress
sort workerID_es monday date

*there are two workers who have different wage rates on the same day-week
duplicates drop
duplicates tag workerID_es monday date, gen(dup)
assert dup==0
drop dup

save `path'/payrate, replace



/* *leave only one obs per worker-week */
/* sort worker monday date  */
/* bys worker monday payrate: gen a = _N */
/* drop date */
/* duplicates drop */

/* *for the same worker-week, are there multiple pay rates recorded? */
/* duplicates tag worker monday, gen(dup) */
/* tab dup */
/* *2% (12K obs) have multiple pay rates recorded on the same week */

/* *if the worker-week has 2-3 different pay rates, use the status that accounts for more days on that week */
/* use `wk2', clear */
/* list if dup > 0 in 1/1000 */
/* bys worker monday: egen maxa = max(a) */
/* gen double q2 = payrate if maxa==a */
/* sort worker monday payrate */
/* bys worker monday: replace q2 = q2[_n-1] if dup > 0 & q2 >= .   */
/* drop if payrate!=q2 & dup > 0 */
/* drop dup */
/* duplicates tag worker monday, gen(dup) */
/* tab dup */
/* tab workerID if dup > 0 */

/* *if the worker had equal # days of different statuses on the same week (17 obs), then manually choose the pay rate by looking up the effective time of change */
