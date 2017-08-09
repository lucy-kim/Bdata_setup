*create cross-sectional staff characteristics data with (one obs per worker) using the HCHB worker-level data and staff.dta

loc path /home/hcmg/kunhee/Labor/Bayada_data

*start with the crosswalk between worker ID (in visit data) and payroll ID
cd `path'/pay_CSV
loc f "pay\Bayada Clinical Workers_Payroll - GP ID.csv"
insheet using "`f'", clear comma names
rename workerid workerID
rename payrollno payrollno_gp

*merge with workers in the HCHB data
tostring workerID, replace
merge 1:1 workerID using `path'/hchb_worker

keep workerID* payrollno* _m worker_category primary_job workerlast workerfirst workertype auth_soc_recert admiss_coord eval_producti pay_prodbonus homeoffid effectivefrom jobtitle overtimepay contractor
assert payrollno==payrollno_gp if _m==3
list if payrollno!=payrollno_gp & _m==3
replace payrollno_gp = payrollno if payrollno!=payrollno_gp & _m==3
drop if payrollno=="" & _m==2
replace payrollno_gp = payrollno if payrollno_gp==""
drop payrollno _m
rename payrollno_gp payrollno

*restrict to workers showing up in the visit data
merge 1:1 workerID using `path'/uniqworker, keep(3) nogen
*only 2 worker has _m==2

* merge with the all-staff all-period snapshot data to get more worker-level info on the employment status & job title
rename worker_category status_hchb
rename primary_jobcode jobtitle_hchb

merge m:1 workerID using `path'/staff, gen(m1) keep(1 3) keepusing(age female race city state zipcode primary_jobcode worker_status jobtitle primary_workerclass worker_category date_hired date_term)
rename worker_status active
lab var active "Is the worker active, separated, etc.?"
replace status_hchb = worker_cate if status_hchb==""
replace jobtitle_hchb = primary_job if jobtitle_hchb==""
drop primary_job worker_cate m1
rename jobtitle_hchb jobdisc_hchb

*convert effective from date to stata data format
split effective, p("/")
replace effectivefrom3 = "20" + effectivefrom3
replace effectivefrom3 = "" if effectivefrom3=="20"
destring effectivefrom?, replace
gen empstartdate = mdy(effectivefrom1, effectivefrom2, effectivefrom3)
format empstartdate %d
drop effective*
lab var empstartdate "Date hired; year=2000 means employed before 2011"

compress
save `path'/staff_chars, replace

*--------
  *drop duplicates by payrollno : since a payrollno is mapped to multiple workerID's
duplicates tag payrollno, gen(dup)
tab dup
sort payrollno workerID
list if dup > 0

*choose one worker that provided more visits per payrollno
bys payrollno: egen max = max(num)
drop if num < max
drop dup max
duplicates tag payrollno, gen(dup)
list if dup==1
drop if dup > 0 & jobdis=="PTA"
drop dup
duplicates tag payrollno, gen(dup)
assert dup==0
drop dup

compress
save `path'/staff_chars2, replace
