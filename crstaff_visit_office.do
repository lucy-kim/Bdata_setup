*link the visit-level data with episode-level data that contain visit counts and office ID by episode ID (old file name = link_staff_visit_office.do)

set linesize 120
local home /home/hcmg/kunhee/Labor
cd `home'/Bayada_data

use visit_codelevel.dta, clear

*link with episode-level data that contain visit counts and office ID by episode ID
merge m:1 epiid using episode_visitcnt_offid
*25K obs has _m=2, 35K obs has _m=1; 5,319,113 matched

tempfile tmp
save `tmp'

*Get office ID for _merge =1 visit obs

*get admission ID for the unmatched episode IDs
keep if _merge==1
keep epiid visitdate
sort epiid visitdate
bys epiid: gen i = _n==1
keep if i==1
keep epiid
merge 1:1 epiid using epiid_admitID, keep(1 3) nogen

merge m:1 admissionclie using officeID_foradmitID, keep(1 3) nogen

keep epiid offid_nu
duplicates drop

rename offid_nu newoffid_nu
tempfile nooffice
save `nooffice'

use `tmp', clear
merge m:1 epiid using `nooffice', gen(m2)
assert m2==3 if _merge==1
replace offid_nu = newoffid_nu if m2==3 & offid_nu==.
count if offid_nu==.
*612 obs have missing office ID values

*only in 2015, missing office ID
tab yr if offid_nu==.
tab qtr if offid_nu==. & yr==2015
*a majority of _m==1 obs is for 2015 Q4

tab offid_nu if _merge==2
* 20% in office 163 , 17% in office 269
tab _merge if offid0=="163"
tab addr_st if _merge==2

preserve
keep if _merge==2
keep epiid offid0 visitcnt
duplicates drop
outsheet using epiid_notinvisit.csv, comma names replace
restore

*since I don't have visit records for episodes that only appear in the office-episode-level visit count data, drop those episodes
drop if _merge==2
drop _merge m2 newoffid_nu

count if offid_nu==.
*episode IDs that appeared only in 2015 Q4 visit data do not have office IDs

compress
save staff_visit_office, replace


/* *there are 264K missing values in the office ID; fill in these by using the office ID reported in the admission data */
/* preserve */
/* keep if offid_n==. */
/* drop offid* */
/* merge m:1 epiid using epi_hosp_base, keep(1 3) keepusing(offid*) nogen */
/* tempfile filled */
/* save `filled' */
/* restore */

/* drop if offid_n==. */
/* append using `filled' */

/* *for the remaining 264K visits that do not get assigned office ID, use the office ID for which workers mostly provide visits? */
/* *for each worker ID, what is the most frequently appearing office ID? */
/* bys workerID offid_nu: gen x = _N */
/* bys workerID: egen xx = max(x) */
/* list workerID epiid offid_nu x xx in 1/30 */
/* gen freqoffid = offid_nu if xx==x */
/* gsort workerID -freq */
/* bys workerID: replace freqof = freqof[_n-1] if freqof >= . */

/* *tag episode IDs that have missing office IDs */
/* gen missing = offid_nu==. */
/* count if missing==1 */
/* bys epiid freqof: gen y = _N */
/* bys epiid: egen yy = max(y) */

/* preserve */
/* keep if missing==1 */
/* sort epiid visitdate visittime */
/* list epiid workerID offid_nu freq y yy in 1/100 */
/* list  */


*-----------------------
  /*check if visit counts are really sum of number of visits per episode ID
bys epiid: gen m = _N
list epiid m visitcnt visitdate in 100000/100030

*for episodes with visit dates near july 2011, we don't have visit-level data for visits made prior to july 2011, so visit count is greater than the sum of the visit observations by episode ID
*/
