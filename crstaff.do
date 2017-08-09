*create clinical staff data from CSV files

set linesize 100
local stdatapath /home/hcmg/kunhee/Labor/Bayada_data/
cd `stdatapath'/staff_CSV

insheet using "staff\Bayada Clinical Workers_Sheet3.csv", clear comma
drop if v1=="All Workers - All Services - All Time Periods"

lab var v1 "Worker ID"
lab var v2 "Age"
lab var v3 "Gender"
lab var v4 "Race"
lab var v5 "City"
lab var v6 "State"
lab var v7 "Zip Code"
lab var v8 "Date Hired"
lab var v9 "Date Terminated"
lab var v10 "Primary Job Code"
lab var v11 "Worker Status"
lab var v12 "Job Title"
lab var v13 "Primary Worker Class"
lab var v14 "Employment Type"
lab var v15 "Exempt"
lab var v16 "Worker Category"
lab var v17 "Full Time"

rename v1 workerID
rename v2 age
rename v3 gender
rename v4 race
rename v5 city
rename v6 state
rename v7 zipcode
rename v8 date_hired
rename v9 date_term
rename v10 primary_jobcode
rename v11 worker_status
rename v12 jobtitle
rename v13 primary_workerclass
rename v14 employtype
rename v15 exempt
rename v16 worker_category
rename v17 fulltime

drop if regexm(workerID, "Worker ID")

*destring a few vars
destring age, replace
gen byte female = gender=="Female"
replace female = . if gender=="N/A"
gen byte white = race=="WHITE"
replace white = . if race=="OPT-OUT"|race=="UNKNOWN"

codebook date_*
  foreach l in "hired" "term" {
    split date_`l', p(/)
    gen mo = date_`l'1
    gen day = date_`l'2
    gen yr = date_`l'3
    replace yr = "20"+yr if substr(yr,1,1)!="8"&substr(yr,1,1)!="9"
    replace yr = "19"+yr if substr(yr,1,1)=="8"|substr(yr,1,1)=="9"
    replace yr = "" if date_`l'==""
    destring mo day yr, replace
    gen date_`l'_e = mdy(mo,day,yr)
    format date_`l'_e %d
    drop date_`l'? date_`l'
    drop mo day yr
  }
lab var date_hired "Date Hired"
lab var date_term "Date Terminated"
lab var white "=1 if worker is white"
lab var female "=1 if worker is female"

gen byte fulltime2 = fulltime=="Full Time"
drop fulltime
rename fulltime2 fulltime
lab var fulltime "=1 if worker is full-time"

gen byte exempt2 = exempt=="Exempt"
drop exempt
rename exempt2 exempt
lab var exempt "=1 if worker is exempt"

*Note for variable meanings ----------------------------------------------------------------
  * primary_jobcode: HHA, LPN, PT, RN
* worker_status: active, (in)voluntarily separated
* jobtitle: filed clinician, clinical manager, director
* primary_workerclass: field vs office
* employtype: hourly, none, per visit (99%), salaried
* worker_category: visit per-diem, non employee-contractor, visit full-time, exempt
*-------------------------------------------------------------------------------------------

saveold "`stdatapath'/staff", replace v(12) 

