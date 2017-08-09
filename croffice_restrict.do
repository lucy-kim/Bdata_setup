*create office-level data on office's tenure to restrict to established or new offices

loc path /home/hcmg/kunhee/Labor/Bayada_data
cd `path'
loc gph /home/hcmg/kunhee/Labor/gph
loc reg /home/hcmg/kunhee/Labor/regresults
set matsize 11000

use office_flow, clear

*for one "pending" office 617 that has missing value in open date, put july 1, 2015 which is the first month it has visits
replace opendate = mdy(7,1,2015) if opendate==. & ym_first==ym(2015,7)

*create office tenure as of the start of the sample period

/* *get the real start date of each office by merging with weekly office panel data
preserve
use weekly_officepanel, clear
collapse (sum) allepi, by(offid_nu monday)
drop if allepi==0
collapse (min) monday, by(offid_nu)
tempfile epistart
save `epistart'
restore

*merge
merge 1:1 offid_nu using `epistart', nogen

*get the real start date for each office
egen realstartwk = rowmin(monday opendate)
format realstartwk %d
list offid_nu monday realstart opendate ym_* if realstart!=opendate */

gen realstartwk = opendate
capture drop lbizto2012
gen lbizto2012 = (mdy(1,1,2012) - realstartwk + 1)/365
lab var lbizto2012 "Office tenure in years as of 1/1/2012"
sum lbizto2012

gen officetenure = (mdy(8,17,2015) - realstartwk + 1)/365
replace officetenure = (closeddate - realstartwk + 1)/365 if closeddate!=.
lab var officetenure "Office tenure as of 8/17/2015 or up to the closure"
sum officetenure

/* *tag established offices = 1 if the office has been around for >= 2 year as of the start of sample period
capture drop established
gen established = lbizto2012 >= 4
tab established
lab var established "= 1 if office has been around for >= 4 year as of 1/1/2012, start of smpl period"
*41 offices established */

*tag offices which started during or after 2012
gen openaft2012 = realstartwk >= mdy(1,1,2012)
tab openaft2012
lab var openaft2012 "=1 if offices started during or after 2012"
*45 offices opened in or after 2012

*create closed before the end of the sample period
tab closeddate
gen closedbf2015sep = closeddate!=. & closeddate <= mdy(8,17,2015)
lab var closedbf2015sep "=1 if offices closed before 8/17/2015"

*get indicator for whether the office is senior living office & split status
preserve
use office, clear
keep offid_nu officefullname startuptype
keep if offid_nu!=.
gen seniorliv = regexm(officefullname, "Senior Living")
keep seniorliv offid_nu startuptype officefullname
duplicates drop
duplicates tag offid_nu, gen(dup)
assert dup==0
drop dup
tempfile senior
save `senior'
restore

merge 1:1 offid_nu using `senior', keep(1 3) nogen

compress
save office_restrict0, replace


*----old code below------------

*get distances between every combination of offices
do crbwofficedistance

*check what is the distribution of distances between offices if < 500 miles
use bwofficedistance, clear
sum mi_to if _m==3

*what is the closest neighbor office for each office?
keep if _merge==3
sort zip mi_to
bys zip: keep if _n==1
drop _merge zip1
rename zip zip_office
rename zip2 zip
sum mi_to, de
*median is 21 miles, mean 38 miles

tempfile closestneighbor
save `closestneighbor'

*create all possible combinations of offices
use office_restrict0, clear
keep offid_nu
foreach v of varlist offid_nu {
    gen `v'2 = `v'
}
fillin offid_nu offid_nu2
drop if _fillin==0
drop _fillin
tempfile pairs
save `pairs'

*merge with office-level data to see which offices are closest neighbors for each office
use office_restrict0, clear
rename addr_zip zip
split zip, p("-")
drop zip zip2
duplicates drop
rename zip1 zip
keep offid_nu zip
tempfile office_zip
save `office_zip'

use `pairs', clear
merge m:1 offid_nu using `office_zip', nogen
rename offid_nu offid_nu0
rename offid_nu2 offid_nu
rename zip zip0
merge m:1 offid_nu using `office_zip', nogen

rename zip0 zip1
rename zip zip2
rename offid_nu0 offid_nu1
rename offid_nu offid_nu2

merge m:1 zip1 zip2 using bwofficedistance, gen(m2)
replace mi_to = 0 if m2==1 & zip1==zip2
assert mi_to!=. if m2==1
drop m2 zip

*add real start week to check if the paired office actually exists at the time the office starts
rename offid_nu1 offid_nu
merge m:1 offid_nu using office_restrict0, keepusing(realstartwk) nogen
rename offid_nu offid_nu0
rename realstartwk realstartwk0
rename offid_nu2 offid_nu
merge m:1 offid_nu using office_restrict0, keepusing(realstartwk seniorliv) nogen

*create active indicator = 1 if the paired office is around after the real start date for each office
sort offid_nu0 offid_nu
gen active = realstartwk <= realstartwk0

*don't count senior living offices as existing Bayada offices
*within 10, 20, 30, 40, ..., miles, respectively, how many other Bayada offices are there?
forval x = 1/12 {
    loc rad = `x'*5
    gen count = mi_to <= `rad' & mi_to!=. & active==1 & seniorliv==0
    bys offid_nu0: egen within`rad' = sum(count)
    capture drop count
}

keep offid_nu0 within* realstartwk0
/* keep offid_nu0 within10-within120 realstartwk0 */
duplicates drop

*reshape long
reshape long within, i(offid_nu0 realstartwk0) j(rad)
rename within nneighbors

*merge with new & established office definition purely by age
rename offid_nu0 offid_nu
merge m:1 offid_nu using office_restrict0, keepusing(addr_st con established openaft2012 lbizto2012) nogen

tempfile tmp
save `tmp'

*if I set X = 10, say, how many offices among are going to be considered as
use `tmp', clear
drop if offid_nu==120
compress
outsheet using offices_withinXmi.csv, replace comma

*restrict to offices that opened after the sample period began
keep if openaft2012==1
bys rad: sum nneighbors

collapse (mean) Mean = nneighbors (sd) StdDev = nneighbors (p50) Median = nneighbors (max) Max = nneighbors, by(rad)
outsheet using `reg'/summ_nneighbors.csv, replace comma names


*count offices that had no offices within X miles
use `tmp', clear
drop if offid_==120

*restrict to offices that opened after the sample period began
gen realnew = openaft==1 & nneighbors==0
*nneighbors==0 &
gen new = openaft==1
gen estab2 = lbizto2012 >= 2
collapse (sum) realnew new estab2, by(rad)
list


*for now, define an office as established if it has 1+ years of tenure as of 1/1/2012
use `tmp', clear
replace established = .
replace established = lbizto2012 >= 2
rename established realestab

*define an office as ``new'' if it has no office within 20 miles of radius
keep if rad==15
count if nneighbors==0 & openaft==1
/* gen new = nneighbors==0 & openaft2012==1 */
gen new = nneighbors==0 & openaft==1


/* *keep only either new or established offices
keep if new==1 | established==1
assert established + new==1
keep offid_nu established
rename established realestab
lab var realestab "=1 if 2+ yrs as of 1/1/2012; =0 if opened post-2012 & no exist offices<=20 miles" */

drop if offid_nu==120

merge 1:1 offid_nu using office_restrict0, keep(3) nogen
drop established
rename realestab established
tab closedbf established
*6 closed; 4 new & 2 established
*59 total; 19 new; 40 estab

compress
save office_restrict, replace

*----------------- Analysis
use office_restrict, clear
tab closedbf established
bys established: sum officetenure



*-----------------
*plot # new offices by quarter
use office_restrict, clear
drop if offid_nu==120
assert realstartwk!=.
gen openq = yq(year(realstartwk), quarter(realstartwk))
gen closedq = yq(year(closeddate), quarter(closeddate))

keep offid_nu openq closedq
gen i = 1

*number of offices that opened by quarter
preserve
collapse (sum) nfirms = i, by(openq)
format openq %tq

tw bar nfirms openq, xti("Quarter") yti("Number of offices that opened")
graph save `gph'/nfirms_byqtr, replace asis
graph export nfirms_byqtr.png, replace
restore

*number of offices that closed by quarter
collapse (sum) nfirms = i, by(closedq)
format closedq %tq

tw bar nfirms closedq if closedq!=., xti("Quarter") yti("Number of offices that closed") ysc(r(0 2)) ylab(0(1)2)
graph save `gph'/nfirmsclosed_byqtr, replace asis
