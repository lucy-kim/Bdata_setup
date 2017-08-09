*create episode-admission-level data containing patient information e.g. race, for each admission

set linesize 150
local path /home/hcmg/kunhee/Labor/Bayada_data
cd `path'
local mvar admissionclientsocid

use epi_hosp, clear

*for obs with non-missing office ID but missing state, fill in the state using office data
preserve
use office, clear
keep offid_nu addr_st addr_zip

replace addr_zip = substr(addr_zip,1,5)
destring addr_zip , replace

duplicates drop
tempfile st
save `st'
restore

*first, just add office state & ZIP code for all
drop addr_st
merge m:1 offid_nu using `st', keep(1 3)
assert offid_nu==. if _merge==1
drop _merge

*merge with race data
merge m:1 clientid socdate_e `mvar' using Rev_Race_v2.dta, keep(1 3) nogen
foreach v of varlist american asian black hispa native white {
  replace `v' = 0 if `v'==.
}
egen race = rowtotal(american asian black hispa native white)
gen norace = race==0

*3058 obs have no race identified
foreach v of varlist american asian black hispa native white {
  replace `v' = . if norace==1
}
gen asian2 = asian==1 | nativehawaii==1
lab var asian2 "=1 if Asian or native Hawaiian"
lab var norace "=1 if no race is identified for the patient"

sort `mvar' epidate2
bys `mvar': gen i = _n==1
sum american asian* black hispa native white norace if i==1
*82% of admissions white; 12% black; 5% hispanicor; 3% no race
drop i

*-----------------------------
*merge with inpatient facility discharged last 14 days
count
merge m:1 `mvar' clientid using DB3_M1000_v2, keep(1 3)
count

gen facility = "Hosp" if inpatdc_acute ==1
replace facility = "SNF" if inpatdc_snf==1
replace facility = "Non-inpat" if inpatdc_na==1
replace facility = "Inpat Rehab" if inpatdc_rehab==1
replace facility = "LTC hosp" if inpatdc_ltchosp==1
replace facility = "LT nursing" if inpatdc_ltnur==1
replace facility = "Psych" if inpatdc_psych==1
replace facility = "Oth" if inpatdc_oth==1
/* assert facility!="" if _merge==3 */
*1 contradiction
count if facility==""

*how many admissions have >1 inpat facilities listed?
sort `mvar' facility
bys `mvar': gen i = _n==1
bys `mvar' facility: gen j = _n==1
bys `mvar': egen ninpat = sum(j)
tab ninpat if i==1
assert ninpat==1
drop ninpat j _merge
tab facility if i==1
drop i
*37% admissions from hospital; 22% from SNF; 30% from non-inpatient

*-----------------------------
*merge with inpatient facility discharge date
list clientid socdate2 epidate2 hospdate dcdate2 if clientid==100004
preserve
use inpat_dcdate, clear
duplicates tag clientid socdate_e, gen(dup)
gen earlier = inpat_dcdate <= socdate_e
sort clientid socd inpat
/*list in 1/30*/
drop if dup > 0 & earlier==0
drop dup
duplicates tag clientid socdate_e, gen(dup)
count if dup > 0
*28 contradictions: pick the later inpat DC date
bys clientid socdate: egen max = max(inpat)
format max %d
drop if dup > 0 & inpat!=max
drop dup max

*are there any admissions for which inpatient DC date is later than the SOC date?
tab earlier
*6253 obs (3%) have earlier=0

lab var earlier "=1 if inpatient DC date <= SOC date_e (=0 is bad)"

tempfile inpat
save `inpat'
restore

count
merge m:1 clientid socdate_e using `inpat', keep(1 3)
*192974 obs have _m=3; 88199 have _m=1

*if not matched to inpatient DC date data, then code the facility as "Non-inpat"
replace facility = "Non-inpat" if _merge==1 & facility==""

tab facility if _merge==1
*93% are non-inpatient but 7% have inpatient facility

count if facility!="Non-inpat" & _merge==1
*5814 obs have inpatient DC but have missing inpatient DC date

drop _merge

*collapse to the episode-admission level data by dropping multiple hosp date obs
drop priorcond* inpatdc_*
gen hospi = hospdate!=.
bys `mvar': egen nhosp = sum(hospi)
bys `mvar': egen firsthospdate = min(hospdate)
format firsthospdate %d
drop hospdate hospi
duplicates drop

capture drop dup
duplicates tag epiid, gen(dup)
assert dup==0
drop dup

gen hashosp = nhosp > 0
lab var firsthospdate "First hospitalization date during an episode if it occurred"
lab var hashosp "=1 if Have at least 1 hospitalization during an episode"
lab var nhosp "Number of hospitalizations in an episode"

*--------------------

*merge with the diagnosis indicators for applicable conditions subject to HRRP penalty
merge m:1 clientid socdate_e using HRRPdiag
*194K have _m=3; 80K have _m=1; 24,470 have _m=2
drop if _merge==2
drop _merge

des pneu*

compress
save `path'/precomorbid, replace

*add comorbidity vars
do /home/hcmg/kunhee/Labor/crcomorbidity

cd `path'
use precomorbid, clear
merge m:1 `mvar' clientid socdate_e using comorbidity, keep(1 3) nogen

compress
save client_chars, replace
