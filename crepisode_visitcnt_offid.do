*create episode-level data that contain office ID and visit counts, to be merged with visit-level data by episode ID

loc path /home/hcmg/kunhee/Labor/Bayada_data
cd `path'/visit_CSV

insheet using "visit\Visits by Office and Recertification Episodes_Epis by Office w Visit Totals.csv", clear comma names

*get numeric office ID
preserve
use `path'/office, clear
keep offid0 offid_nu addr_st
drop if offid0==""
duplicates drop
tempfile xwalk
save `xwalk'
restore

*merge with numeric office ID
rename branchcode offid0
tab offid0

merge m:1 offid0 using `xwalk'
*65 obs (offid_nu=366 + other office IDs) has _m==2; all others 362515 obs have _m=3; no _m=1 obs
drop if _m==2
drop _m

rename totalvisitcount visitcnt
lab var visitcnt "Visit count by episode-officeID"

format epiid %10.0g

*add Q4 office ID for each episode ID
insheet using "Clients 2012 to 2015 with Office ID_Q4 2015 Episodes.csv", clear comma names
drop if regexm(v2, "Branch")
rename q4 offid0
destring v3, gen(epiid)
drop v2 v3
merge m:1 offid0 using `path'/office, keepusing(offid_nu addr_st) keep(1 3)
*7 obs office ID 631 has _m=1; 25K have _m=3
drop if _m==1
drop _m

compress
save `path'/episode_visitcnt_offid, replace
