*track the entry and exit flow of offices

do crvisit
do crvisit_svccode_time
do crvisit_codelevel
do crepisode_visitcnt_offid
do crstaff_visit_office

*get the first and last visit date for each office from the visit data
use staff_visit_office, clear

*drop visits that appeared only in 2015 Q4 or any visits that are not matched to the crosswalk between episode ID and office ID data
drop if offid_nu==.

*what is the earliest and last visit date for each office?
assert offid_nu!=.
bys offid_nu: egen first = min(visitdate)
bys offid_nu: egen last = max(visitdate)

gen ym_first = ym(year(first), month(first))
gen ym_last = ym(year(last), month(last))
format ym_first %tm
format ym_last %tm

keep offid_nu ym_*
    duplicates drop
count
*106 offices

merge 1:m offid_nu using office
*119 obs has _m==3; 65 has _m=2
drop if _m==2
drop _m

tempfile tmp
save `tmp'

use `tmp', clear
*list offid_nu ym_* provnum addr_str addr_city addr_st addr_zip opendate closeddate officestatus
keep offid_nu ym_* opendate closeddate officestatus addr*
duplicates drop
duplicates tag offid_nu, gen(dup)
assert dup==0
drop dup

*group offices into 2 X 2 categories: stayers - entrants/incumbents, exiters - entrants/incumbents (mutually exclusive)
tab ym_last
count
*106 offices

gen office_status = .

*stayers - incumbents: offices whose last visit dates are in or after sept 2015 & whose first visit dates are in or before jan 2012
    *offices whose last visit dates are in nov or dec 2015 are incumbents b/c i don't include visits appearing in Q4 2015
tab offid_nu if ym_last > ym(2015,8) & ym_first <= ym(2012,1)
*51 offices
replace office_status = 1 if ym_last > ym(2015,8) & ym_first <= ym(2012,1)

*exiters - incumbents: offices whose last visit dates are before sept 2015 & whose first visit dates are in or before jan 2012
tab offid_nu if ym_last < ym(2015,9) & ym_first <= ym(2012,1)
*9 offices
replace office_status = 2 if ym_last < ym(2015,9) & ym_first <= ym(2012,1)

*stayers - entrants: offices whose last visit dates are in or after sept 2015 & whose first visit dates are after jan 2012
tab offid_nu if ym_last > ym(2015,8) & ym_first > ym(2012,1)
*41 offices
replace office_status = 3 if ym_last > ym(2015,8) & ym_first > ym(2012,1)

*exiters - entrants: offices whose last visit dates are before sept 2015 & whose first visit dates are after jan 2012
tab offid_nu if ym_last < ym(2015,9) & ym_first > ym(2012,1)
*5 offices
replace office_status = 4 if ym_last < ym(2015,9) & ym_first > ym(2012,1)
assert office_status!=.

lab define st 1 "Incumbent-Stayer" 2 "Incumbent-Exiter" 3 "Entrant-Stayer" 4 "Entrant-Exiter"
lab values office_status st
tab office_status

lab var office_status "Office classified into 4 groups by incumbent / stayer"
rename officestatus office_active
lab var office_active "From office data: Active, Closed, Closing, Pending"
lab var ym_first "First month of visit for the office"
lab var ym_last "First month of visit for the office"
lab var opendate "Office open date from office data"
lab var closeddate "Office closed date from office data"

order offid_nu office_status opendate closeddate addr_*


gen con = addr_st=="NC"|addr_st=="VT"|addr_st=="NJ"|addr_st=="MD"|addr_st=="HI"
tab con office_status
list if office_status==2 & con==1

sort office_status offid_nu
compress
save office_flow, replace
outsheet using office_flow.csv, comma names replace


*analysis-----
tab addr_st

forval x = 1/4 {
di "`x'"
preserve
keep if office_status==`x'
tab addr_st
restore
}

/*--------
use office_historical, clear
tab ym_last

*group offices into 2 X 2 categories: stayers - entrants/incumbents, exiters - entrants/incumbents (mutually exclusive)

*stayers - incumbents: have last admission month 8/2015 & first admission 1/2012
tab ym_first if ym_last==ym(2015,8)
*49 offices has first admission 1/2012

*stayer - entrants: have last admission month 8/2015 & first admission later than 1/2012
tab ym_first if ym_last==ym(2015,8)
*91 - 49 = 42 offices

*exiter - incumbent: have last admission month before 8/2015 but first admission 1/2012
count if ym_last < ym(2015,8)
*13 offices

tab ym_first if ym_last < ym(2015, 8)
*7 /13 offices have first admission 1/2012

*exiters - incumbent: have last admission month before 8/2015 but first admission after 1/2012
tab ym_first if ym_last < ym(2015, 8)
*6 offices have first admission later than 1/2012
*/
