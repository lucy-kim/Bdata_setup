*create office-day level data showing
*employment: # new hires, # quitters (voluntary, involuntary), # total workers on each day,
*demand: # visits in each discipline, # new episodes, # existing episodes, # discharged episodes on each day

loc path /home/hcmg/kunhee/Labor/Bayada_data
cd `path'
loc mvar admissionclie
loc d "SN"

*collapse the daily worker panel to the office-day level for employment data: show in each obs separate status for each office-day & have different columns show different disciplines
use daily_workerpanel, clear

sort offid_nu visitdate payrollno
bys offid_nu visitdate payrollno: gen i = _n==1
replace i = . if worked==0

*tag workers by discipline
gen nw_active_`d' = i

*tag new hires by discipline
gen nw_nh_`d' = i if newhire==1 & visitdate==esd2

*tag quitters by discipline
gen nw_quit_`d' = i if attrited==1 & visitdate==etd2 & fired==0

*tag layoffs by discipline
gen nw_layoff_`d' = i if attrited==1 & visitdate==etd2 & fired==1

*tag absent workers
gen nw_shabs_`d' = absent_short==1
gen nw_longabs_`d' = absent_long==1
gen nw_mediabs_`d' = absent_medi==1

list payrollno visitdate offid_nu leave esd2 etd2 absent* linact status if payrollno=="100046"

des nw_*
loc unit "offid_nu visitdate status"
collapse (sum) nw_*, by(`unit')

tempfile empl
save `empl'

*aggregate across several statuses to get the office-day level count of office workers, field workers, salaried workers, piece-rate workers


/*nw_all = i nw_field = j nw_sa = salaried nw_pr = piecerate nw_vac_all = vacation nw_vac_field = vac2 nw_vac_safield = vac3 nw_safield = safield nw_prfield = prfield nw_absent*  nw_nh_all = newhiredate nw_nh_field = nhfield nw_nh_prfield = nh_pr nw_nh_safield = nh_sa nw_quit_pr = quit_pr nw_quit_sa = quit_sa nw_fired_field = fireddate nw_saf_fired nw_prf_fired , by(offid_nu visitdate discipline)*/


/**for each office-day, fraction of inactive workers who have 7+ day long vacation and continuosly employed
gen iaf_all = 100*nw_vac_all / nw_all
gen iaf_field = 100*nw_vac_field / nw_field
*Fraction of inactive salaried workers
gen iaf_safield = 100*nw_vac_safield / nw_sa
*fraction of piece-rate field workers among all field workers
gen frac_prfield = 100*nw_prfield / nw_field
*fraction of workers who work for multiple offices during the same week

gen frac_absent_field = 100*nw_absent_field / nw_field
gen frac_absent_safield = 100*nw_absent_safield / nw_sa*/

*---------------------

*create for each office-day, number of new patients (i.e. episodes) starting home health care on that day (i.e. first visit is that day) as a fraction of all patients
*for each worker, tag if the worker is inactive (i.e. doesn't provide any visit) for X days

*create # new episodes for each office-day first
use visit_worker_chars, clear
assert visitdate!=.
assert offid_nu!=.

sort offid_nu epiid visitdate
*episode start date
bys offid_nu epiid : egen first = min(visitdate)
keep if first==visitdate
keep offid_nu epiid visitdate
duplicates drop

*for each office-visitdate, # episodes with first visits on that day?
gen i = 1
collapse (sum) newepi = i, by(offid_nu visitdate)
tempfile newepi
save `newepi'

*------------------------

*create # all existing (except new) episodes for each office-day first
use visit_worker_chars, clear
*for each office, first visit date & last visit date
bys offid_nu epiid: egen fvd = min(visitdate)
bys offid_nu epiid: egen lvd = max(visitdate)
gen g = lvd - fvd + 1
keep offid_nu epiid fvd lvd g
duplicates drop
expand g
sort offid_nu epiid
bys offid_nu epiid: gen visitdate_e = fvd + _n - 1
bys offid_nu epiid: egen a = max(visitdate)
assert a == lvd

keep offid_nu visitdate epiid
duplicates drop
gen i = 1
collapse (sum) allepi = i, by(offid_nu visitdate)

merge 1:1 offid_nu visitdate using `newepi'
*88K _m=3; 22K _m=1; no _m=2
*_m=1 means that there are no new episode on that day
sort offid_nu visitd
format visitd %d

*create # non-new episodes for each office-day
replace newepi = 0 if _merge==1 & newepi==.
gen notnewepi = allepi - newepi

*create # new episodes as a percentage of # existing episodes for each office-day
gen newepi_pct = 100*newepi / notnewepi

drop _merge

tempfile newepi_pct
save `newepi_pct'

*------------------------
*create # visits in each discipline-salaried for each office-day
use visit_worker_chars, clear

*restrict to SN visits
keep if discipline=="SN"
/* *drop 1 obs with missing discipline
drop if discipline=="" */

gen i = 1
collapse (sum) nv_SN = i, by(offid_nu visitdate status)

*tag 2385 obs for which the worker status is missing
gen missing = status==""
bys missing: egen nn = sum(nv)
tab nn
*1405 visits have no status assigned; ~5M visits have status assigned
replace status = "unknown" if status==""
drop missing nn
assert status!=""

/*------------------------+-----------------------------------
                 EXEMPT |     63,164        8.04        8.04
                     FT |        153        0.02        8.06
          NON EXEMPT-PT |      2,777        0.35        8.41
          NON-EXEMPT-HR |      1,653        0.21        8.62
NOT EMPLOYEE-CONTRACTOR |     50,357        6.41       15.03
                    PTN |     17,884        2.28       17.31
                    PTP |      4,846        0.62       17.92
                    SFT |        113        0.01       17.94
                    SPB |        143        0.02       17.96
                    SPC |          5        0.00       17.96
                    SPD |        791        0.10       18.06
                  STATN |         63        0.01       18.07
                  STATP |         19        0.00       18.07
                    VFT |    249,080       31.70       49.77
                    VPB |    101,297       12.89       62.66
                    VPC |     57,794        7.36       70.01
                    VPD |    234,429       29.83       99.85
                unknown |      1,197        0.15      100.00
------------------------+-----------------------------------*/

/* *reshape wide so that each obs is office-day-status
rename nv nv_

reshape wide nv_, i(offid_nu visitdate status) j(discipline) string

foreach v of varlist nv* {
    replace `v' = 0 if `v'==.
}

foreach d in "FS" "HHA" "MSW" "OT" "PT" "RD" "SN" "ST" {
    lab var nv_`d' "# visits provided on office-day for `d'"
} */

tempfile nv
save `nv'

*------------------------
*create an office-day level base data
use visit_worker_chars, clear
keep offid_nu visitdate
collapse (min) min = visitdate (max) max = visitdate, by(offid_nu)
gen t = max - min + 1
expand t
sort offid_nu

gen day = .
bys offid_nu  : replace day = min if _n==1
bys offid_nu  : replace day = day[_n-1] +1 if _n > 1
rename day visitdate_e
drop t min max
format visitdate %d
sum visitdate

*merge w/ the # new episodes (as a percentage of #  existing episodes) for each office-day
merge 1:1 offid_nu visitdate using `newepi_pct'
*1028 _m=1; 0.1M have _m=3

foreach v of varlist allepi newepi notnewepi newepi_pct {
    replace `v' = 0 if _m==1 & `v'==.
}
drop _merge

*merge w/ office-day level # visits in each discipline, status
merge 1:m offid_nu visitdate using `nv'
*4696 obs have _m=1; 388K _m=3; no _m=2
sum visitdate

foreach v of varlist nv_* {
    replace `v' = 0 if _m==1 & `v'==.
}
drop _merge

*merge with employment for each office-day
merge 1:1 offid_nu visitdate status using `empl'
*4696 have _m=1; 215K have _m=2; 388K have _m=3
*_m=2 means that no visits were provided by workers in that status
*_m=1 means there are no visits in all the disciplines

foreach v of varlist nw_* {
    replace `v' = 0 if _m==1 & `v'==.
}
foreach v of varlist nv_* {
    replace `v' = 0 if _m==2 & `v'==.
}
foreach v of varlist *epi* {
    gsort offid_nu visitd -`v'
    bys offid_nu visitd: replace `v' = `v'[_n-1] if `v' >= .
}
drop _merge

sort offid_nu visitd status

compress
save daily_officepanel, replace
