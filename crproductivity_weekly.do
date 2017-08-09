*create daily data showing the productivity quota for each worker & keep only the workers I can match with the workers appearing in the visit-level data

loc path /home/hcmg/kunhee/Labor/Bayada_data
cd `path'/staff_CSV

*visit productivity data
insheet using EmployeeMinimumVisitProductivity.txt, tab names clear
drop if value==.
foreach v of varlist effective* {
  gen `v'2 = substr(`v',1,10)
}
rename effectivestartdate2 sd
rename effectiveenddate2 ed
foreach v of varlist sd ed {
  replace `v' = "" if `v'=="NULL"
  split `v', p("-")
}
destring sd? ed?, replace
gen startd = mdy(sd2, sd3, sd1)
gen endd = mdy(ed2, ed3, ed1)
format startd %d
format endd %d
drop sd ed sd1-sd3 ed1-ed3
drop effectivestartdate effectiveenddate
rename Worker workerID_es

tempfile productivity
save `productivity'

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
merge 1:m workerID_es using `productivity', keep(3) nogen
tempfile merges
save `merges'


*for some workers, there are multiple entries of different quotas for one day -> use the last recorded quota value
use `merges', clear
replace endd = mdy(12,1,2016) if endd==.

sort workerID startd endd
gen same = startd==endd
bys workerID_es: egen ss = sum(same)
bys workerID_es: drop if same==1 & endd[_n-1]==startd & startd[_n+1]==endd & value[_n-1]==value[_n+1] & value[_n-1]!=value

*drop if the quota is recorded only for one day and the next entry of quota covers that day and lasts for a longer time
bys workerID_es: gen num = _N
gen gap = endd - startd + 1
bys workerID_es: drop if same==1 & startd[_n+1]==endd & gap[_n+1] > gap & num > 1

*drop if the quota is recorded only for one day and the former entry of productivity covers that day and lasts for a longer time
bys workerID_es: drop if same==1 & endd[_n-1]==startd & gap[_n-1] > gap & num > 1

*drop if the quota is recorded only for one day and the next entry of productivity covers that day and lasts for a longer time
bys workerID_es: drop if same==1 & endd==startd[_n+1] & gap[_n+1] > gap & num > 1

*there are some workers who have a break in the history of quota (workerID_esID=="1199461") or has quota only for one day and never appears again (workerID_esID=="1202394") -> drop the latter worker & drop the one day obs for the former worker
duplicates drop
list if same==1
bys workerID_es : egen ms = mean(same)
list if ms >0 & ms <1
drop if same==1

drop same ss num gap

*recode the enddate of a worker if the next startdate is same as the end date
gen endd2 = endd
bys workerID_es: replace endd2 = endd2-1 if endd==startd[_n+1]
format endd2 %d
drop endd
rename endd2 endd
gen gap = endd - startd + 1
assert gap >= 1
drop gap  ms

gen productivity = "visit"
assert startd!=.
assert endd!=.

tempfile visitprod
save `visitprod'

*--------------------------------------------------------
*hourly productivity data
cd `path'/staff_CSV
insheet using EmployeeMinimumHourlyProductivity.txt, tab names clear
drop if value==.
foreach v of varlist effective* {
  gen `v'2 = substr(`v',1,10)
}
rename effectivestartdate2 sd
rename effectiveenddate2 ed
foreach v of varlist sd ed {
  replace `v' = "" if `v'=="NULL"
  split `v', p("-")
}
destring sd? ed?, replace
gen startd = mdy(sd2, sd3, sd1)
gen endd = mdy(ed2, ed3, ed1)
format startd %d
format endd %d
drop sd ed effective* sd1-sd3 ed1-ed3

*keep only workers appearing in the visit data
rename Worker workerID_es
merge m:1 workerID_es using `workersinvisit', keep(3) nogen
tempfile merges
save `merges'

*for some workers, there are multiple entries of different quotas for one day -> use the last recorded quota value
use `merges', clear
replace endd = mdy(12,1,2016) if endd==.

sort workerID startd endd
gen same = startd==endd
assert same==0
*no same day obs
gen gap = endd - startd + 1
assert gap >= 1
drop same gap

*recode the enddate of a worker if the next startdate is same as the end date
gen endd2 = endd
bys workerID_es: replace endd2 = endd2-1 if endd==startd[_n+1]
format endd2 %d
drop endd
rename endd2 endd

gen productivity = "hourly"
assert startd!=.
assert endd!=.

*append with the visit productivity quota data
append using `visitprod'

sort workerID startd endd
rename productivity prodcat
rename value productivity

tempfile bf
save `bf'

*check whether a worker reports two different productivity levels on the same week
use `bf', clear

*count if same worker has quota categories visit and hourly together
bys worker prodcat: gen a = 1 if _n==1
bys worker: egen aa = sum(a)
count if aa > 1
drop a

*if the end date and start date are same for different categories of productivity, recode the end date
sort worker startd endd
gen endd2 = endd
bys workerID_es: replace endd2 = endd2-1 if endd==startd[_n+1]
format endd2 %d
drop endd
rename endd2 endd

*manully correct workerID_es=="1155741" b/c for the overlapping period of time with indefinite end dates, report both visit & hourly productivity (she is hospice worker: SFT/SPD)
*drop hourly productivity
drop if workerID=="1155741" & prodcat=="hourly"

*drop if start and end dates are same & there is 0 productivity
sort worker startd endd productivity
bys workerID: drop if productivity==0 & aa > 1 & startd==startd[_n+1] & endd==endd[_n+1]

*recode end dates for a productivity obs if there is another entry with the same start date but different end date
sort worker startd endd
bys workerID: replace startd = endd[_n-1] + 1 if startd==startd[_n-1] & endd[_n-1] < endd


*expand to daily obs
gen gap = endd - startd + 1
assert gap >= 1
expand gap
sort worker startd endd prodcat

bys worker startd endd prodcat: gen date = startd if _n==1
bys worker startd endd prodcat: replace date = date[_n-1] + 1 if _n > 1
format date %d
* list if workerID=="1179149" & month(startd)==3 & year(startd)==2015

*for the same period, if the productivity for visit, say, is 0 but quota for hourly is not 0, then drop the visit quota and use the hourly quota, & vice versa
sort worker date
bys worker date: egen mprod = mean(productivity)
bys worker date: drop if productivity==0 & aa > 1 & mprod > 0
*list if worke=="1191631" | worker=="1191780"

*now is there only one productivity category per worker-day?
duplicates tag worker date , gen(dd)
assert dd==0
drop dd mprod aa gap

bys worker: egen a = max(endd)
bys worker: egen b = max(date)
assert a==b
drop a b startd endd


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
bys worker monday productivity: gen i = _n==1
bys worker monday : egen si = sum(i)
bys worker monday: gen j = _n==1
tab si if j==1
*1943 (0.45%) worker-weeks only have > 1 productivity values

preserve
sort worker monday date
keep if si > 1
*assign the last status as the status for that week

*flag whenever a productivity value changes within a worker-week
bys workerID monday: gen ch = _n==1
bys workerID monday: replace ch = 1 if productivity!=productivity[_n-1]
bys workerID monday: gen sch = sum(ch)
bys workerID monday: egen a = max(sch)
keep if sch==a
keep payrollno worker productivity monday sunday prodcat
duplicates drop

duplicates tag worker monday, gen(d)
assert d==0
drop d

tempfile multiple
save `multiple'
restore

drop if si > 1
keep payrollno worker productivity monday sunday prodcat
duplicates drop
append using `multiple'

duplicates tag worker monday, gen(d)
assert d==0
drop d

sort worker monday

compress
save `path'/productivity_weekly, replace
