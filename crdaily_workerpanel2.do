*Create daily panel of each worker who ever works in our sample from 2012 - Q3 2015
*create office-worker-daily panel

loc path /home/hcmg/kunhee/Labor/Bayada_data
cd `path'
loc mvar admissionclie
loc min 7
loc max 14
loc unit offid_nu payrollno discipline visitdate

*for each worker, tag if the worker is inactive (i.e. doesn't provide any visit) for X days
use visit_worker_chars, clear

assert visitdate!=.

*for worker, create active_q42015 = 1 if the worker still provides some visits in Dec of 2015
assert payrollno!=""
gen i = 1
bys payrollno visitdate: egen tv = sum(i)
gen x = tv > 0 & visitdate > mdy(9,30,2015)
bys payrollno: egen max = max(x)
gen active_q42015 = max > 0
drop i tv x max

/* *since I do not have office IDs for a majority of visits in 2015 Q4, drop Q4 2015
drop if yr==2015 & qtr==4 */

tempfile t
save `t'

*get 2011 visit data to get an indicator for working before 2012 for each workerID in each office?
use visit_allqtrs, clear
keep if yr==2011
keep workerID epiid
duplicates drop

*get office ID
merge m:1 epiid using episode_visitcnt_offid, keepusing(offid_nu) keep(1 3) nogen

assert workerID!=""
assert offid_nu!=.
keep workerID offid_nu
duplicates drop

*merge with workerID-payrollno xwalk
merge m:1 workerID using staff_chars, keep(1 3) keepusing(payrollno)
*only 6 _m=1; 2K _m=3
replace payrollno = workerID + "P" if _m==1
drop _merge
gen active_q42011 = 1

*drop office ID
drop offid_nu
duplicates drop

tempfile active_q42011
save `active_q42011'

*merge the visit-level data with the info on whether the worker is active in 2011
use `t', clear
merge m:1 workerID payrollno using `active_q42011', keep(1 3)
replace active_q42011 = 0 if _merge==1
assert active_q42011!=.
drop _merge

tempfile t
save `t'

*-------------------
/**what is the HHI of visit share in each discipline for each worker-office?
use `t', clear
gen i = 1
collapse (sum) i, by(payrollno offid_nu discipline)
bys payrollno offid_nu: egen tnv = sum(i)
gen sh2 = (i/tnv)^2
preserve
collapse (sum) hhi = sh2 , by(payrollno offid_nu tnv)
sum hhi, de
count if hhi < 1
list if hhi < 1*/

*-------------------
*identify whether a worker is a new hire, separated
use `t', clear

assert payrollno!=""
bys payrollno : egen fvd = min(visitd)
bys payrollno : egen lvd = max(visitd)
format fvd %d
format lvd %d
lab var fvd "First ever visit for payrollno during 1/1/2012-12/31/2015"
lab var lvd "Last ever visit for payrollno during 1/1/2012-12/31/2015"

*worker = new hire if the worker was not active during 2011
gen newhire_cs = active_q42011==0 & fvd >= mdy(1,1,2012)
tab newhire
lab var newhire "=1 if not active during 2011 (active_q42011==0) & 1st visit >= 1/1/2012"

*active_q42015 = 1 means the worker is still working on or after 10/1/2015

*worker = separated if not working for any offices in Oct-Dec 2015
capture drop attrited_cs
gen attrited_cs = active_q42015==0
*gen attrited_cs = lvd < mdy(10,5,2015) & active_q42015==0
tab attrited
lab var attrited "=1 if not active during Q4 2015"

*tag worker as attrited if they had inactivity period for 6 months for any office
sort payrollno visitdate offid_nu
bys payrollno: gen gap = visitdate[_n+1] - visitdate
tab gap
loc xx 90
gen gt`xx' = gap > `xx' & gap!=.
bys payrollno visitdate : egen mgt`xx' = max(gt`xx')
gen attrityes = mgt`xx'==1
replace attrityes = 1 if attrited_cs==1 & visitdate==lvd

*tag worker as a new hire if they come back after a period of inactivity of 2 months for any office
*create the hiring date
capture drop hiringyes
gen hiringyes = visitdate==fvd & newhire_cs==1
sort payrollno visitdate offid_nu gap
drop gt`xx' mgt`xx'
gen gt`xx' = gap[_n-1] > `xx' & gap[_n-1]!=.
bys payrollno visitdate: egen mgt`xx' = max(gt`xx')
bys payrollno: replace hiringyes = 1 if mgt`xx'==1

foreach v of varlist attrityes hiringyes {
    assert `v'!=.
    tab `v'
}
lab var attrityes "=1 if lvd & no work in Q42015 | inactive >90 days after"
lab var hiringyes "=1 if fvd & no work in Q42011 | after >90-day inactivity"

*get the employment start & termination (if applicable) dates
gen esd = visitdate if hiringyes==1
sort payrollno visitdate offid_nu
bys payrollno: replace esd = esd[_n-1] if esd>=.

capture drop etd
gen etd = visitdate if attrityes==1
gsort payrollno -visitdate offid_nu
bys payrollno: replace etd = etd[_n-1] if etd>=.
lab var esd "employment start date"
lab var etd "employment termination date"
format esd %d
format etd %d

sort payrollno visitdate offid_nu
list payrollno offid_nu visitdate newhire hiringyes gap gt`xx' mgt`xx' esd etd attrited attrityes if payrollno=="100352"

tempfile beforedelete
save `beforedelete'

*are there a lot of workers who quit and come back?
bys payrollno : egen a = sum(hiringyes) if mgt`xx'==1

*create office-worker-daily panel
use `beforedelete', clear
collapse (min) fvd_o = visitdate (max) lvd_o = visitdate (min) wk1_emplst, by(offid_nu payrollno workerID fvd lvd active_q42015 active_q42011 attrited newhire esd etd)
duplicates tag offid_nu payrollno, gen(dup)
tab dup
*there are 16 obs with dup > 0 ; these have different worker IDs ; after merging with staff chars data by workerID-payrollno, keep only one obs per office-payrollno
drop dup

*merge w/ cross-sectional worker chars data
merge m:1 workerID payrollno using staff_chars, keep(1 3) nogen

*can I get employment start date for non-new hires?
*use wk1_emplst (First week recorded in the historical empl status data for the worker) & date_hired_e & empstartdate

*if not new hire, get an employment start date using other sources: from historical empl status data
preserve
keep if newhire==0

*since HCHB empl start date = 1/1/2000 is not accurate, don't assign these dates
tab empstartdate, sort
replace empstartdate = . if empstart==mdy(1,1,2000)

capture drop earliest
egen earliest = rowmin(date_hired wk1_emplst empstartdate fvd)
count if earliest==.
replace earliest = . if earliest!=. & earliest > fvd
assert earliest <= fvd if earliest!=.

replace esd = earliest if earliest!=. & esd==.
count if esd==.
drop earliest

tempfile notnewhire
save `notnewhire'
restore

drop if newhire==0
append using `notnewhire'
drop empstartdate date_hired wk1_emplst workerlast workerfirst num

foreach v of varlist auth_soc_recert-pay_prodbonus overtimepay {
     gen `v'2 = `v'=="Y" & `v'!=""
     replace `v'2 = . if `v'==""
     assert `v'2 == 0 if `v'=="N" & `v'!=""
     drop `v'
     rename `v'2 `v'
}
lab var auth_soc_recert "Has authority to do SOC/RECT assessment?"
lab var admiss_coordi "Admission Coordinator"
lab var eval_producti "Evaluate Productivity"
lab var pay_prodbonus "Pay Productivity Bonus y/n"
lab var overtimepay "overtime pay"

*for attriting workers, tag whether it's voluntary or not
gen fired = attrited==1 & active=="T - INVOLUNTARILY SEPARATED"
replace fired = . if attrited==0
lab var fired "=1 if worker fired"

gen attrit_invol = attrited==1 & (active=="T - INVOLUNTARILY SEPARATED" | active=="D - DECEASED" | active=="OFFICE CLOSURE - INACTIVE" | active=="OFFICE SEPARATION")
replace attrit_invol = . if attrited==0
lab var attrit_invol "=1 if worker involuntary separated (e.g. fired, office closure)"

tab fired
tab attrit_invol
tab active if attrit_invol==1 & fired==0

tempfile pre
save `pre'

*want to get one obs per office-payrollno
use `pre', clear
drop dup
duplicates tag offid_nu payrollno, gen(dd)
tab dd
*16 obs
drop workertype homeoffid jobdisc_hchb status_hchb primary_wor jobtitle workertype active

*if there are multiple worker IDs mapped to a same office ID-payrollno, pick the earlier of empl start date & the later of empl term date & recode newhire & separated indicators
preserve
keep if dd > 0

sort offid_nu payrollno workerID
list offid_nu payrollno workerID esd etd fvd* lvd*

foreach v of varlist esd fvd {
    bys payrollno: egen z = min(`v')
    drop if `v'!=z
    drop z
}
foreach v of varlist etd lvd {
    bys payrollno: egen z = max(`v')
    drop if `v'!=z
    drop z
}
foreach v of varlist fvd_o {
    bys offid_nu payrollno: egen z = min(`v')
    drop if `v'!=z
    drop z
}
foreach v of varlist lvd_o {
    bys offid_nu payrollno: egen z = max(`v')
    drop if `v'!=z
    drop z
}

*if office ID-payrollno has some variable = 1 at least once, count it as having 1
foreach v of varlist active_* newhire attrited age  auth_soc_recert-pay_prodbonus overtimepay fired attrit_invol date_term {
    bys offid_nu payrollno: egen z = max(`v')
    drop if `v'!=z
    drop z
}

replace race = "" if race=="OPT-OUT"
loc v race
gsort payrollno -`v'
bys payrollno: replace `v' = `v'[_n-1] if `v'==""

loc v female
gsort payrollno -`v'
bys payrollno: replace `v' = `v'[_n-1] if `v'>=.

*one worker have two names, one female and male -> choose a female
drop if female==0 & payrollno=="134"

drop workerID dd
duplicates drop

duplicates tag offid_nu payrollno, gen(dd)
assert dd==0
drop dd

tempfile onlyone
save `onlyone'
restore

drop if dd > 0
append using `onlyone'
drop workerID dd

*calculate tenure (i.e. length of employment) for people we have both employment start date & termination date
gen lemp = etd - esd + 1
assert lemp==. if etd==. | esd==.
assert lemp > 0

replace lemp = lemp/30
lab var lemp "# months (30-day blocks) in employment if nonmissing empl start & term dates"

compress
save worker_tenure, replace

*---------------------------------------------------------
*create a daily panel for each worker-office
use worker_tenure, clear

*create the length of days on which the worker appears (including non-work days)
gen t = lvd_o - fvd_o + 1

keep offid_nu payrollno fvd_o lvd_o t
duplicates drop
expand t
sort payrollno offid_nu
bys payrollno offid_nu  : gen day = fvd + _n - 1
rename day visitdate_e
drop t
format visitd %d

duplicates tag offid_nu payrollno visitdate, gen(dup)
assert dup==0
drop dup

tempfile owdpanel
save `owdpanel'

*for each worker-office-day, create 0/1 indicator of working on that day
use `t', clear
keep payrollno visitdate_e offid_nu status discipline distance_driven time_driven visit_travel_cost visit_points monday lov active_* monday sunday majordisc salary productivity prodcat possalary salaried piecerate officew* fieldw* contractor payrate lov

gen i = 1

assert visitdate!=.
assert payrollno!=""

collapse (sum) dnv = i visit_points distance_driven time_driven visit_travel_cost lov (mean) mlov = lov, by(payrollno active_* offid_nu status monday sunday visitdate_e majordisc discipline salary productivity prodcat possalary salaried piecerate officew* fieldw* contractor payrate)

duplicates tag offid_nu payrollno visitdate, gen(dd)
tab dd
*dd > 0 for 0.32% obs since they provide visits in multiple disciplines
drop dd

tempfile worklog
save `worklog'

*create 0/1 indicator of being inactive for X days for each worker and coming back after 7 days
*apply this rule only for workers who don't quit & who appear both before and after the 7-day inactive period
use `owdpanel', clear
merge 1:m offid_nu payrollno visitdate using `worklog'
assert _merge!=2
gen worked = _merge==3

format lvd_o %d
lab var lvd_o "Last visit date for payrollno offid_nu"
format fvd_o %d
lab var fvd_o "First visit date for payrollno offid_nu"

format visitdate %d
sum visitdate
tab visitd if visitd==`r(max)'
*last visit date is 9/30/2015

*fill in missing values in indicator for whether active in Q4 2015 for unmatched obs
loc v active_q42015
gsort payrollno -`v'
bys payrollno: replace `v' = `v'[_n-1] if `v'>=.
assert `v'!=.

*get start (Mon) date of the week to which the visit date belongs
drop monday sunday
gen day = dow(visitdate)
gen monday = visitdate - 6 if day==0
forval d = 1/6 {
  loc d2 = `d' - 1
  replace monday = visitdate - `d2' if day==`d'
}
format monday %d
drop day
gen sunday = monday + 6
format sunday %d

*on days unmatched, put # visit = 0
foreach v of varlist dnv distance_driven time_driven visit_travel_cost lov mlov visit_points {
    replace `v' = 0 if `v'==. & _merge==1
}

*create absence indicator = 1 on days if worker doesn't work in any office
bys payrollno visitdate: egen snv = sum(dnv)
gen absent = snv==0
lab var snv "# visits for payrollno-visitdate across all offices & disciplines"
lab var absent "=1 if worker had no visits in any office on that visitdate"

*create worked_oo = 1 if worked==0 & only worked in another office
gen worked_oo = worked==0 & snv > 0

*on the same day, does a worker work in multiple offices? - yes (33% of worker-days have >=2 offices; 25% w/ 2 offices)
sort payrollno visitd offid_nu
bys payrollno visitdate offid_nu: gen k = _n==1
bys payrollno visitdate: egen sk = sum(k)
bys payrollno visitdate: gen i = _n==1
tab sk if i==1
gen work_manyoffice = sk > 1
drop k sk i
lab var work_manyoffice "=1 if worker-day works for multiple offices"
/*list payrollno visitdate offid_nu absent worked* snv in 1/30*/

*make sure a worker-day-office is either not working, working in other office, or working in the current office
egen a = rowtotal(worked worked_oo absent)
assert a==1
drop a

lab var worked_oo "=1 if worked in other office but not in the current office"
lab var worked "=1 if worked in the current office"
lab var snv "Total # visit for payrollno visitdate"
lab var absent "=1 if worker-day has no visits (snv==0)"
lab var active_q42015 "=1 if still working in Q4 2015"

gen nowork = 1-worked

*fill in missing values in status for unmatched obs
foreach v of varlist status prodcat {
    gsort payrollno monday -`v'
    bys payrollno monday: replace `v' = `v'[_n-1] if `v'=="" & _merge==1

    *for weeks that have no status or prodcat, use previous weeks' values
    gsort payrollno visitdate -`v'
    bys payrollno: replace `v' = `v'[_n-1] if `v'=="" & _merge==1
}
assert status!=""
count if prodcat==""

loc v majordisc
gsort payrollno -`v'
bys payrollno: replace `v' = `v'[_n-1] if `v'=="" & _merge==1

foreach v of varlist productivity salary-contractor payrate {
    gsort payrollno monday -`v'
    bys payrollno monday: replace `v' = `v'[_n-1] if `v' >= . & _merge==1
}

drop _merge

tempfile prevac
save `prevac'

*------------------------

*flag if the worker didn't work for X days for each offices for vacation
use `prevac', clear
keep payrollno visitdate absent
duplicates drop

*create a block sequence number where each block is a period of working or not working on each worker-day

*tag whenever absent changes from 1 to 0 or vice versa for each worker (not worker-office)
sort payrollno visitdate
capture drop ch
bys payrollno : gen ch = _n==1
bys payrollno : replace ch = ch + 1 if absent!=absent[_n-1] & _n > 1

capture drop seq
bys payrollno : gen seq = ch + _n if ch==1
bys payrollno : replace seq = seq[_n-1] if absent==absent[_n-1]
assert seq!=.

bys payrollno seq: gen linact = _N if absent==1

sort payrollno visitd
gen vacation = linact >= `min' & linact <= `max'
drop absent ch seq

tempfile wdlevel
save `wdlevel'

*merge the worker-office-day level data w/ worker-day level data showing vacation indicator
use `prevac', clear
merge m:1 payrollno visitdate using `wdlevel', nogen

*replace vacation = 0 if the worker is on vacation for one office but working in another office
replace vacation = 0 if worked_oo==1 & vacation==1

*create short absence indicator = 1 if absent but not on vacation
gen absent_short = absent==1 & vacation==0 & linact < `min'

*create long absence indicator = 1 if the worker is still working in some office in Q4 2015 but taking a long absence from the other office
gen absent_long = absent==1 & vacation==0 & linact > `max'
sum linact if absent_short==1
sum linact if absent_long==1

*make sure a worker-day-office is either absent for a short time, on vacation, working in other office, or working in the current office
egen a = rowtotal(worked worked_oo absent_short absent_long vacation)
assert a==1
drop a

compress
tempfile tmp
save `tmp'

*merge with cross-sectional worker_tenure data that contains other worker chars vars
use `tmp', clear
rename contractor contractor_ind
merge m:1 offid_nu payrollno using worker_tenure, keep(1 3) nogen

lab var fired "=1 if the worker is fired"
lab var linact "Length of inactivity in days for the worker"
lab var vacation "=1 if linact >= `min' & <= `max' where linact = # consecutive days of absence"
lab var visitdate "each day for worker-office"
lab var nowork "= 1 - worked"
lab var absent_short "=1 if not working in the office for < `min' days"
lab var absent_long "=1 if not working in the office for > `max' days"
lab var officew "=1 if office worker; Exempt Non-exempt-pt Non-exempt-hr"

*on each week , what is the tenure up to that point?
gen tenure = visitdate - esd + 1
replace tenure = tenure / 30
lab var tenure "Time-varying tenure (in months) up to the day for each worker (across offices)"

compress
save daily_workerpanel, replace








/**---------------------- OLD CODE below


*is the worker new employee? As of 30jan2012, if the worker-office's first visit is 30jan2012 (monday) or later, then flag as new employee
format fvd %d
*tab fvd_wo
lab var fvd "First visit date for payrollno offid_nu "

gen newhire = fvd >= mdy(1,30,2012)
tab newhire
lab var newhire "Worker-office new hire starting on or after 1/30/2012 (Mon)"

gen newhired = visitdate==fvd & newhire==1
lab var newhired "=1 on day when worker-office newly hired"

gen newhiredate = fvd if newhire==1
/*visitdate==fvd & newhire==1*/
lab var newhiredate "date of hiring when worker-office is newly hired"
format newhiredate %d


*active_q42015 = 1 means the worker is still working on or after 10/1/2015

*create quit = 1 on day when the worker-office quits
gen terminated = visitdate==lvd & active_q42015==0
lab var terminated "=1 on day when worker-office terminated employment"

*termination date
gen termdate = lvd if terminated==1
format termdate %d
lab var termdate "date of empl termination when worker-office ended employment"
gsort payrollno offid_nu -termdate
bys payrollno offid_nu: replace termdate = termdate[_n-1] if termdate >= .




/**as of 8/31/2015 (Monday), is the worker still active? if the last visit date is in Sept 2015, code the worker as active
capture drop inactive
gen inactive = lvd < mdy(8,31,2015)
tab inactive
lab var inactive "Worker-office-status quit on or before 8/30/2015 (Sun)"*/



*create salaried vs piece-rate positions
gen salaried = status=="VFT" | status=="VPB" | status=="VPC" | status=="SFT" | status=="SPB" | status=="SPC" | status=="EXEMPT"

replace salaried = . if status==""

gen piecerate = 1 - salaried

*tag office workers
gen officew = status=="EXEMPT" | status=="NON EXEMPT-PT" | status=="NON-EXEMPT-HR"
replace officew = . if status==""

drop _merge

lab var worked "=1 if the worker worked on that day-office-discipline"
lab var salaried "=1 if status = VFT VPC VPB SFT SPC SPB or Exempt"
lab var piecerate "= 1 - salaried"
lab var officew "=1 if office worker; Exempt Non-exempt-pt Non-exempt-hr"


*get a major discipline for each worker-office-day
preserve
use `t', clear
keep payrollno offid_nu discipline
bys payrollno offid_nu : gen a = _N
bys payrollno offid_nu  discipline: gen b = _N
gen c = b/a
bys offid_nu payrollno  : egen d = max(c)

gen e = disc if d==c
gsort offid_nu payrollno -e
bys offid_nu payrollno : replace e = e[_n-1] if e==""
assert e!=""

*one worker 240423 works as SN in some offices & as HHA in other offices; in office 576, works same # visits as SN and HHA -> recode as SN since she provided more visits as SN
replace e = "SN" if payrollno=="240423" & offid_nu==576
drop a b c d disci
rename e discipline
lab var disci "Major discipline for payrollno offid_nu"
duplicates drop
tempfile disc
save `disc'
restore

merge m:1 offid_nu payrollno using `disc', nogen

*flag if the worker didn't work for X days for each offices

*create a block sequence number where each block is a period of working or not working

*tag whenever worked = 0 changes to 1 or vice versa
sort offid_nu payrollno visitdate
capture drop ch
bys offid_nu payrollno : gen ch = _n==1
bys offid_nu payrollno : replace ch = ch + 1 if worked!=worked[_n-1] & _n > 1

capture drop seq
bys offid_nu payrollno : gen seq = ch + _n if ch==1
bys offid_nu payrollno : replace seq = seq[_n-1] if worked==worked[_n-1]
assert seq!=.

bys offid_nu payrollno seq: gen linact = _N if nowork==1


*want to analyze the distribution of the length of inactivity period for each worker-office
preserve
/*list offid_nu payrollno visitdate worked ch seq nowork linact in 50/100*/
keep if nowork==1
keep if ch==1
keep offid_nu payrollno linact seq status visitdate officew salaried discipline

*for all statuses
bys disci: sum linact, de

keep if discipline=="SN" | discipline=="PT" | discipline=="HHA"

*for selective status
bys disci: sum linact if officew==1, de
bys disci: sum linact if officew==0, de
bys disci: sum linact if officew==0 & salaried==1, de
bys disci: sum linact if officew==0 & salaried==0, de

gen yr = year(visitd)
gen mo = month(visitd)
gen day = day(visitd)

compress
outsheet using vaclength.csv, replace comma names
restore

sort offid_nu payrollno visitd
gen vacation = linact >= `min' & linact <= `max'
*if nowork==1

*replace vacation = 0 if the worker is on vacation for one office but working in another office
replace vacation = 0 if worked_oo==1 & vacation==1

*create short absence indicator = 1 if absent but not on vacation
gen absent_short = absent==1 & vacation==0 & linact < `min'

*create long absence indicator = 1 if the worker is still working in some office in Q4 2015 but taking a long absence from the other office
gen absent_long = absent==1 & vacation==0 & linact > `max'
sum linact if absent_short==1
sum linact if absent_long==1

*make sure a worker-day-office is either absent for a short time, on vacation, working in other office, or working in the current office
egen a = rowtotal(worked worked_oo absent_short absent_long vacation)
assert a==1
drop a

*add worker characteristics
merge m:1 payrollno using staff_chars2, keep(1 3)
*6321 workers _m=3 ; 268 workers _m=2 ; 59 workers _m=1 (these have P in the payrollno)
drop _merge

*if inactive & separation reason is involuntarily separated, then code as fired
tab active if active_q42015==0
gen fired = active_q42015==0 & active=="T - INVOLUNTARILY SEPARATED"
gen fireddate = visitdate==lvd & fired==1

drop ch
lab var fired "=1 if the worker-office is fired"
lab var fireddate "=1 on day the worker-office is fired"
lab var seq "Index of time blocks during which the worker-office is working (non-working)"
lab var linact "Length of inactivity in days for the worker-office-discipline"
lab var worked "=1 if the worker worked on that day-office-discipline"
lab var vacation "=1 if linact >= `min' & <= `max'"
lab var visitdate "each day for worker-office-discipline"
lab var nowork "= 1 - worked"


compress
save daily_workerpanel, replace




*---- remove code below


*collapse to the office-day level
sort offid_nu visitdate payrollno
bys offid_nu visitdate payrollno: gen i = _n==1

*tag all workers except office workers whose status = Exempt, Non-exempt-HR /PT
gen j = i
replace j = . if status=="EXEMPT" | status=="NON EXEMPT-PT" | status=="NON-EXEMPT-HR"

*tag workers who have a vacation = 1 as missing if the status is office worker status
gen vac2 = vacation
replace vac2 = . if vacation==1 & (status=="EXEMPT" | status=="NON EXEMPT-PT" | status=="NON-EXEMPT-HR")

*tag workers who have a vacataion = 1 as missing if the status is office worker status or piece-rate status
gen vac3 = vac2
replace vac3 = . if vac2==1 & piecerate==1

*tag worker who is salaried & field worker
gen safield = salaried==1
replace safield = . if status=="EXEMPT" | status=="NON EXEMPT-PT" | status=="NON-EXEMPT-HR"

*tag worker who is piece-rate & field worker
gen prfield = piecerate==1
replace prfield = . if status=="EXEMPT" | status=="NON EXEMPT-PT" | status=="NON-EXEMPT-HR"

collapse (sum) nw_all = i nw_field = j nw_sa = salaried nw_pr = piecerate nw_vac_all = vacation nw_vac_field = vac2 nw_vac_safield = vac3 nw_safield = safield nw_prfield = prfield, by(offid_nu visitdate)

*for each office-day, fraction of inactive workers who have 7+ day long vacation and continuosly employed
gen iaf_all = nw_vac_all / nw_all
gen iaf_field = nw_vac_field / nw_field
*Fraction of inactive salaried workers
gen iaf_safield = nw_vac_safield / nw_sa
*fraction of piece-rate field workers among all field workers
gen frac_prfield = nw_prfield / nw_field
*fraction of workers who work for multiple offices during the same week

tempfile vacation
save `vacation'

*create for each office-day, number of new patients (i.e. episodes) starting home health care on that day (i.e. first visit is that day) as a fraction of all patients
use `t', clear

*create # new episodes for each office-day first
sort offid_nu epiid visitdate
bys offid_nu epiid : egen first = min(visitdate)
keep if first==visitdate
keep offid_nu epiid visitdate
sort offid_nu visitdate epiid
duplicates drop

*for each office-visitdate, # episodes with first visits on that day?
collapse (count) epiid, by(offid_nu visitdate )
rename epiid newepi
tempfile newepi
save `newepi'

*create # all existing (except new) episodes for each office-day first
use `t', clear
*for each office, first visit date & last visit date
bys offid_nu epiid: egen fvd = min(visitdate)
bys offid_nu epiid: egen lvd = max(visitdate)
gen g = lvd - fvd + 1
keep offid_nu epiid fvd lvd g
duplicates drop
expand g
sort offid_nu epiid
bys offid_nu epiid: gen visitdate_e = fvd + _n - 1
sort offid_nu visitdate epiid
collapse (count) allepi = epiid, by(offid_nu visitdate)
merge 1:1 offid_nu visitdate using `newepi'
*84K _m=3; 22K _m=1; no _m=2
sort offid_nu visitd

*create # non-new episodes for each office-day
replace newepi = 0 if _merge==1 & newepi==.
gen notnewepi = allepi - newepi

*create ratio of # new episodes to # existing episodes for each office-day
gen r_new_oldepi = newepi / notnewepi
drop _merge

tempfile r_new_oldepi
save `r_new_oldepi'

*--------------------
*create office-day level data containing only the potential instrument variables to analyze them separately
use `r_new_oldepi', clear

merge 1:1 offid_nu visitdate using `vacation'
*1746 have _m=1; 80 have _m=2; 105K have _m=3

keep if _m==3
drop _m
sort offid_nu visitd
rename visitd day

compress
save instr, replace*/
