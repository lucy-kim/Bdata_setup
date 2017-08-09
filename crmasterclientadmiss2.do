*create a master data of client-episode-admissions to link with staff and visit data
*code mainly from crdemand_bycell.do

set linesize 150
local path /home/hcmg/kunhee/Labor/Bayada_data
cd `path'
local mvar admissionclientsocid

*start with the base Master file
use "Rev_Master DB_v2", clear
duplicates drop

*drop admissions starting before 1/1/2012
drop if socdate < mdy(1,1,2012)

*convert Episode start date to a date variable
rename episodestartd epidate
split epidate, p("/")
replace epidate3 = "20"+epidate3
destring epidate?, replace float
gen epidate_e = mdy(epidate1, epidate2, epidate3)
format epidate_e %d
drop epidate? epidate
label var epidate_e "Episode start date"

*1167 obs in the Master DB have missing values in the SOC date
count if socdate==.

*some client IDs with missing SOC dates have another episodes with non-missing SOC dates that are only a few days later than the episode date in the obs with missing SOC dates: e.g. clientid 315249
sort clientid epidate
gen missing = socdate==.
bys clientid: egen max = max(missing)
/*list `mvar' clientid socdate epidate if max==1*/

*merge with visit-level data to keep valid episodes that also appear in the visit-level data if the client ID has missing value in SOC date
preserve
use visit_worker_chars, clear
keep epiid
duplicates drop
tempfile epi
save `epi'
restore

merge m:1 epiid using `epi', keep(1 3)
count if _m==1 & socdate==.
*1144 obs
*if the client ID with missing values in SOC dates and non-matched episodes have any matched episodes, then drop them
bys clientid: egen a = mean(_m)
count if a > 1 & a < 3 & socdate==. & _m==1
drop if a > 1 & a < 3 & socdate==. & _m==1

*check client ID that has missing SOC date have only one episode date associated with the missing SOC date
sort clientid `mvar' epidate
assert epidate!=.
bys clientid missing epidate: gen j = _n==1
bys clientid missing: egen sj = sum(j)
replace sj = . if missing==0
tab sj
tab clientid if sj > 1 & sj!=.
*1158 observations have only one episode date appearing for the obs with missing values in socdate; but 4 clients have multiple episode dates appearing for the obs with missing values in socdate

list clientid `mvar' admitmrnsocid epiid socdate epidate missing oasistype j sj if sj > 1 & sj!=.

*from the manual examination: for client IDs with missing values in SOC dates & multiple episodes, if the episode dates are > 60 days apart, then code SOC date = episode date b/c each episode is a separate admission; all the above clients are these cases
replace socdate = epidate if socdate==. & sj > 1 & sj!=.

*check if clientid with missing values in SOC date has any other admissions without missing SOC ID
sort clientid `mvar'
bys clientid `mvar': gen k = _n==1
bys clientid: egen sk = sum(k)
tab sk if max==1
*96% of client IDs with missing values in SOC date has only 1 episode; 31 obs have > 1 episodes
count if max==1 & sk > 1
assert _merge==3 if max==1 & sk > 1

sort clientid epidate
list `mvar' clientid socdate epidate epiid if max==1 & sk > 1

*since these client IDs have different episode IDs when SOC date is missing from when they have a non-missing SOC date, just treat them as different admission by putting SOC date = epidate
replace socdate = epidate if socdate==.
assert socdate!=.
drop _merge-sk
count

*for obs with newly added SOC date values, re-create the admission-SOC ID var
gen a = socd + 21916
tostring clientid, gen(cc)
tostring a, gen(b)
gen id = cc + b
destring id, replace
format id %11.0f
*replace `mvar' = id if clientid==`mvar'
replace `mvar' = id if `mvar'==clientid
drop a-id cc

*44,321 obs are duplicates on all vars except for OASIS type
duplicates tag clientid socdate `mvar' admitmrnsocid clientmedica epiid epidate age locationtype payor icdcode category *count los female , gen(dupall)
count if dupall>0
tab oasistype if dupall >0
tab dupall
duplicates drop clientid socdate `mvar' admitmrnsocid clientmedica epiid epidate age locationtype payor icdcode category *count los female dup*, force
drop dupall
*22,383 obs deleted
/* list clientid socdate `mvar' oasistype dup* if dupall>0 in 1/100 */
/* list clientid socdate `mvar' oasistype icdcode if clientid==5854 */

tempfile t0
save `t0'

*--------------------------------------------------
*merge with number of recertifications
*--------------------------------------------------
use `t0', clear
assert clientid!=.
assert socdate!=.
tostring clientid, replace
merge m:1 `mvar' socdate clientid using clientrecert
*278K have _m=3; 2K have _m=1; 37K have _m=2
*_m=2 obs have a `mvar' that has either _m=1 or _m=3 obs

sum socdate if _merge==2
*drop among _m=2 obs, if the socdate is before 1/1/2012 or after 8/25/2015 (the last SOC date in the Master DB file)
drop if _merge==2 & (socdate < mdy(1,1,2012) | socdate > mdy(8,24,2015))
tab _merge
*there are still 37,182 obs with _merge==2

/* *for unmatched (_merge==1) obs and any missing values in # recert var, assume that they have no recertified episodes */
/* replace epi_recert = 0 if epi_recert == . & */

rename _merge _m_nrecert
lab var _m_nrecert "_merge when merging with # recertification per client admission data"

*--------------------------------------------------
*merge with episode ID-admission ID xwalk to get a full list of episode IDs per admission ID
*--------------------------------------------------
*process a list of admissionID - episode ID xwalk
preserve
use epiid_admitID, clear
keep epiid `mvar'
duplicates drop
destring `mvar', replace
tempfile epiid_admitID
save `epiid_admitID'
restore

merge m:1 `mvar' epiid using `epiid_admitID'

*for the same admission ID, if there is only _merge==2, drop the admission
gen is2 = _merge==2
bys `mvar': egen sis2 = sum(is2)
bys `mvar': gen nn = _N
drop if nn==sis2
drop j-sk is2-nn max missing

*for added admissions with missing values in episode ID, merge separately with admission-episode ID xwalk by admission ID only
preserve
keep if epiid==.
drop epiid
merge 1:m `mvar' using `epiid_admitID', keep(1 3) nogen
count if epiid==.
*only 12 obs
tempfile missingepiid
save `missingepiid'
restore

drop if epiid==.
append using `missingepiid'

*further fill in missing values of episode ID for 4 obs
count if epiid==.

tempfile missingepiid2
save `missingepiid2'

use `missingepiid2', clear
keep if epiid==.
duplicates drop
destring clientid, replace
rename epidate epidate0

*merge with recertification data
merge 1:m clientid socdate using recert_roc, keep(1 3) keepusing(epiid_new epidate) gen(m2)
*only 1 matched
replace epidate0 = epidate_e if epidate_e!=. & epidate0==.
replace epiid = epiid_new if epiid_new!=. & epiid==.
drop epiid_new epidate_e
rename epidate0 epidate_e

preserve
keep if m2==3
drop m2
tempfile m3
save `m3'
restore

*drop 11 obs with no matched episode ID - can't find it in casemix weight data or any other data
use `missingepiid2', clear
drop if epiid==.
destring clientid, replace
append using `m3'
drop _merge
assert epiid!=.

*get episode start date by using visit data
count if epidate==.
merge m:1 epiid using first_last_vd, keep(1 3) nogen
replace epidate = fvd if epidate==. & fvd!=.
count if epidate==.

*----------------------------------------------------------------
*merge with recertification & ROC episode ID for each admission
*----------------------------------------------------------------
foreach v of varlist clientid socdate {
    count if `v'==.
    gsort `mvar' -`v'
    bys `mvar': replace `v' = `v'[_n-1] if `v' >=.
    count if `v'==.
}

count
rename epidate epidate0
rename epiid epiid_new
merge m:1 clientid socdate epiid_new using recert_roc, gen(m2)
*203,449 has _m=1; 6,146 has _m=2; 70,692 has _m=3

tab socdate if m2==2
*drop among _m=2 obs, if the socdate is before 1/1/2012 or after 8/25/2015 (the last SOC date in the Master DB file)
drop if m2==2 & (socdate < mdy(1,1,2012) | socdate > mdy(8,24,2015))
tab m2
*there are still 4794 obs with _m==2 -> includes re-certified episodes

*create a concatenated ID = client ID + SOC date for missing values
tostring clientid, gen(c)
tostring socdate, gen(s)
gen a = c + s
destring a, replace
format a %11.0f
replace a = a + 21916
replace `mvar' = a if `mvar'==.
drop a c s
assert `mvar'!=.
rename epiid_new epiid
assert epiid!=.

*replace the episode date with the new episode date
replace epidate0 = epidate_e if epidate_e!=. & epidate0==.
count if epidate0==.
*there are still 3467 episode IDs with missing values in Epi date (these only come from the xwalk of admission ID-episode ID)
drop epidate_e
rename epidate0 epidate_e

*fill in missing values in variables in obs purely coming from the re-certification data using values for those variables in the original episode-level data downstream for the same admission-SOC ID
bys `mvar': egen y = max(m2)
bys `mvar': egen yy = min(m2)
count if y==2 & yy==2
tab y yy
*4,770 obs are for admissions that do not exist in the original admissions data.
drop y yy

*for example
/* tab clientid if y==2 & yy==2 in 1/50 */
/* list clientid `mvar' socdate epiid epidate oasistype* _merge if clientid==5848 */

*don't drop episodes purely coming from the re-certification data since most of them are matched to the visit-level data
/* preserve */
/* keep if  y==2 & yy==2 */
/* keep epiid oasistype_new */
/* merge 1:1 epiid using episode_visitcnt_offid */
/* *11,889/12,549 obs get matched to the episode-level data containing office ID (only 660 obs has _m=1) -> have to keep these episode IDs */
/* restore */

tempfile tmp
save `tmp'

use `tmp', clear
*fill in all other variables for m2=2 guys
sort clientid `mvar' epiid
*list clientid `mvar' epid _m age if clientid==20846

foreach v of varlist age los female {
    bys clientid `mvar': replace `v' = `v'[_n-1] if `v' >=.
}
gsort clientid `mvar' -epiid
foreach v of varlist age los female {
    bys clientid `mvar': replace `v' = `v'[_n-1] if `v' >=.
}
sort clientid `mvar' epiid
foreach v of varlist locationtype icdcode payor category admitmrnsocid clientmedical {
    bys clientid `mvar': replace `v' = `v'[_n-1] if `v' =="" & `v'[_n-1]!=""
}
gsort `mvar' -epiid
foreach v of varlist locationtype icdcode payor category admitmrnsocid clientmedical {
    bys clientid `mvar': replace `v' = `v'[_n-1] if `v' =="" & `v'[_n-1]!=""
}

*for re-certification episodes purely coming from the re-certification data, fill in age, female, and other vars if the corresponding clientid has other episodes
sort clientid `mvar' epiid
foreach v of varlist age female {
    bys clientid: replace `v' = `v'[_n-1] if `v' >=.
}
foreach v of varlist locationtype payor admitmrnsocid clientmedical {
    bys clientid: replace `v' = `v'[_n-1] if `v' =="" & `v'[_n-1]!=""
}

*since I randomly dropped observations when there are duplicate values in terms of all variables except the oasis type, the original oasis type var may have wrong values -> replace the original OASIS type values with the OASIS type values from the recertification data
replace oasistype = oasistype_new if oasistype=="" & oasistype_new!=""
replace oasistype = oasistype_new if oasistype_new!="" & oasistype_new != oasistype
count if oasistype==""
drop oasistype_new

rename m2 _m_recert
lab var _m_recert "_merge when merging with recertification episode data"

tempfile tmp2
save `tmp2'

*--------------------------------------------------
* merge with case-mix weights by episode-admission
*--------------------------------------------------
use `tmp2', clear
rename epidate_e epidate0
merge m:1 `mvar' socdate_e epiid using casemixwgt, gen(m2)
*264,911 has _m=3; 50,688 has _m=2; 8,995 has _m=1

replace epidate0 = epidate_e if epidate0==. & epidate_e!=.
drop epidate_e
rename epidate0 epidate_e

sum socdate if m2==2
*drop among _m=2 obs, if the socdate is before 1/1/2012 or after 8/25/2015 (the last SOC date in the Master DB file)
drop if m2==2 & (socdate < mdy(1,1,2012) | socdate > mdy(8,24,2015))
tab m2
*there are still 3,607 obs with _merge==2

sort `mvar' socdate_e epiid

*get clientID
assert clientid!=. if m2!=2
tostring `mvar', gen(a) format(%11.0f)
gen l = length(a)
tab l

gen b = ""
forval x = 9/11 {
    loc y = `x'-5
    replace b = substr(a,1,`y') if l==`x'
}
destring b, replace
assert clientid==b if m2!=2
replace clientid = b if clientid==.
drop l b a

*list clientid `mvar' socdate epiid epidate cmw payortype _merge in 1/50

*drop if the admission-SOC ID doesn't exist in the existing client admissions data
sort `mvar' socdate_e epiid
bys `mvar': egen a = min(_merge)
bys `mvar': egen b = max(_merge)
tab a b
count if a==b & a==2
drop a b

*fill in all other variables for _m=2 guys
sort clientid `mvar' epiid
foreach v of varlist age los female {
    bys clientid `mvar': replace `v' = `v'[_n-1] if `v' >=.
}
gsort clientid `mvar' -epiid
foreach v of varlist age los female {
    bys clientid `mvar': replace `v' = `v'[_n-1] if `v' >=.
}
sort clientid `mvar' epiid
foreach v of varlist locationtype icdcode payor category admitmrnsocid clientmedical {
    bys clientid `mvar': replace `v' = `v'[_n-1] if `v' =="" & `v'[_n-1]!=""
}
gsort `mvar' -epiid
foreach v of varlist locationtype icdcode payor category admitmrnsocid clientmedical {
    bys clientid `mvar': replace `v' = `v'[_n-1] if `v' =="" & `v'[_n-1]!=""
}

*fill in age, female, and other vars if the corresponding clientid has other episodes
sort clientid `mvar' epiid
foreach v of varlist age female {
    bys clientid: replace `v' = `v'[_n-1] if `v' >=.
}
foreach v of varlist locationtype payor admitmrnsocid clientmedical {
    bys clientid: replace `v' = `v'[_n-1] if `v' =="" & `v'[_n-1]!=""
}

rename m2 _m_cmw
lab var _m_cmw "_merge when merging w/ case-mix weights data by episode ID & admission-SOC ID"

gsort `mvar' -epi_recert
foreach v of varlist _m_nrecert epi_recert {
    bys `mvar': replace `v' = `v'[_n-1] if `v' >=.
}

*-----------------
*merge again with episode-admission ID xwalk for newly added obs
merge m:1 `mvar' epiid using `epiid_admitID'

*for the same admission ID, if there is only _merge==2, drop the admission
gen is2 = _merge==2
bys `mvar': egen sis2 = sum(is2)
bys `mvar': gen nn = _N
drop if nn==sis2

drop _merge

foreach v of varlist clientid socdate {
    gsort `mvar' -`v'
    bys `mvar': replace `v' = `v'[_n-1] if `v' >=.
}

tempfile tmp3
save `tmp3'

*-----------------
*get office ID: merge with the episode-level office ID data
use `tmp3', clear
merge m:1 epiid using episode_visitcnt_offid
*361K obs have _m==3; 7759 obs have _m==1; 71K obs have _m==2 (there are 71K episodes I'm excluding in the client admissions data!!!)

drop if _merge==2

rename offid_nu offid_nu0

preserve
use officeID_foradmitID, clear
keep `mvar' offid_nu
duplicates drop
destring `mvar', replace
tempfile offid
save `offid'
restore

merge m:1 `mvar' using `offid', gen(m2) keep(1 3)
*1215 obs have m2 =1 ; 276K have m2=3
count if offid_nu0==. & offid_nu!=.
replace offid_nu0 = offid_nu if offid_nu0==. & offid_nu!=.
drop m2 offid_nu
rename offid_nu0 offid_nu

*for 3,416 obs that have _m==1, create office ID from the clientmedica var; first 2 or 3 nuemric/ alphabet (3digit) digits of 14-digit clientmedica are office ID
replace clientmedica = trim(clientmedica)
gen l = length(clientmedica)
tab l
*0 for re-certified episodes without matched admission-SOC ID & 12 - 14 characters

tempfile pre
save `pre'

preserve
*keep if _merge==3
keep if offid_nu!=.
drop _merge l
*assert offid_nu!=.
tempfile matched
save `matched'
restore

use `pre', clear
keep if offid_nu==.
drop _merge offid0 offid_nu addr_st
tab l
gen offid0 = substr(clientmedica,1,3) if l==14
replace offid0 = substr(clientmedic, 1,2) if l==13
replace offid0 = substr(clientmedic, 1,1) if l==12
tab offid0

*get numeric office ID
preserve
use office, clear
keep offid0 offid_nu addr_st
drop if offid0==""
duplicates drop
tempfile xwalk
save `xwalk'
restore

compress
merge m:1 offid0 using `xwalk', keep(1 3)
*135 obs have _m=1; 3,281 have _m=3

assert clientmedical=="" if _merge==1
assert offid0!="" if _merge==3
assert offid_nu!=. if _merge==3
drop _merge l

gen miss = offid_nu==.
bys clientid: egen a = mean(miss)
assert a==0
drop miss a

*append
append using `matched'
assert offid_nu!=.

tempfile tmp4
save `tmp4'

*-----------------
*there are multiple payer sources indicated for an episode;
use `tmp4', clear

duplicates tag clientid socdate `mvar' admitmrnsocid clientmedica epiid epidate age locationtype icdcode category *count los female offid* addr_st _m_recert visitcnt cmw episodetype , gen(dup)
count if dup > 0
*there are 1321 / 280,691 obs

sort epiid payortype
bys epiid payortype: gen i = _n==1
bys epiid: egen npayer = sum(i)

*for each episode, how many payers are indicated?
sort epiid payortype
bys epiid: gen k = _n==1
tab npayer if k==1
*among 276,444 episodes, 99.8% have 1 payer indicated; 661 episode have > 1 payers ; 1 have 1 payer

list epidate payor if npayer==3

tab payor if npayer==2 & k==1
*-----------------
*merge with insurance plan data
preserve
*keep only primary insurance for each episode so that each epiid has only 1 obs
use planid, clear
keep if payerorder=="Primary"
duplicates tag epiid, gen(dup)
tab dup
*64 obs have 2 primary payers for each episode

*since 99.9% obs usually have only 1 primary payer, for not just pick randomly 1 payer per episode
duplicates drop epiid, force
drop dup

tempfile planid
save `planid'
restore

assert epiid!=.
merge m:1 epiid using `planid', keep(1 3)
*8515 have _m=1 ; 269K have _m=3

*Palmetto & CGS & National Government Services are payers for 99% of Medicare FFS episodes
*tab payername if npayer==1 & payortype=="MEDICARE", sort

*are there any dual eligibles?
gen medicaid = regexm(payor, "MEDICAID")
bys epiid: egen mean = mean(medicaid)
*medicaid = 0 means Medicare

*if 0 < mean < 1 or (mean==1 & age >= 65), then flag the episode as dual eligible
gen dual = (mean > 0 & mean < 1 & npayer > 1) | (mean==1 & age >= 65)
*dual = 1 for 1%

drop dup i k _merge mean

*create Medicare Advantage indicator
gen ma = payor=="MANAGED - MEDICARE" | payor=="PPS - NON MEDICARE"

*create per-visit-paid MA & per-episode-paid MA indicator
gen ma_visit = payor=="MANAGED - MEDICARE"
gen ma_epi = payor=="PPS - NON MEDICARE"

*create Medicare FFS indicator
gen mcr_ffs = payor=="MEDICARE"

*create managed medicaid indicator
gen mcd_mng = payor=="MANAGED - MEDICAID"

drop payortype

*since some episodes reported multiple payer types, collapse these insurance indicators to episode level
foreach v of varlist medicaid dual ma ma_visit ma_epi mcr_ffs mcd_mng {
    bys epiid: egen max_`v' = max(`v')
    drop `v'
    rename max_`v' `v'
}

lab var offid0 "alpha office ID"
lab var medicaid "=1 if the episode is paid for by (managed) Medicaid"
lab var dual "=1 if the episode is paid by Medicaid+care or (Medicaid & age >=65)"
lab var npayer "Num payer types indicated for the episode"
lab var ma "=1 if episode is paid for by Medicare Advantage"
lab var ma_epi "=1 if episode is paid for by per-episode-paid Medicare Advantage"
lab var ma_visit "=1 if episode is paid for by per-visit-paid Medicare Advantage"
lab var mcr_ffs "=1 if episode is paid for by Medicare FFS"
lab var mcd_mng "=1 if episode is paid for by managed Medicaid"

duplicates drop

duplicates tag epiid, gen(dup)
tab dup
*there are 48 contradictions
sort epiid clientid epidate
list clientid `mvar' epiid epidate visitcnt offid0 _m_recert _m_cmw if dup > 0
count

*merge with a list of admissionID - episode ID xwalk
preserve
use epiid_admitID, clear
keep epiid `mvar'
duplicates drop
destring `mvar', replace
tempfile epiid_admitID
save `epiid_admitID'
restore

merge 1:1 `mvar' epiid using `epiid_admitID', keep(1 3)
tab _merge if dup > 0
list clientid `mvar' epiid epidate _merge dup if dup > 0
drop if dup > 0 & _merge==1
drop dup _merge
duplicates tag epiid, gen(dup)
assert dup==0
drop dup

tempfile demand_start
save `demand_start'


*----------------------------------------------------------------
*have to know when patients exit -- merge with deaths data
*----------------------------------------------------------------
merge m:1 `mvar' using deaths, keep(1 3)
*967 have _m=3 ; 313,043 have _m=1; 0 have _m=2

replace death = 0 if death==. & _merge==1
drop _merge

*--------------------------------------------------------------------
*have to know when patients exit -- merge with discharge date data
*--------------------------------------------------------------------
merge m:1 `mvar' using dischrg
*269,914 obs have _m=3; 47,280 have _m=1; 6,392 obs have _m=2
drop if _merge==2
drop _merge

/* gen dc_est = dcdate==. */

/* *if DC date is missing but the LOS is non-missing, make the DC date be the SOC date + LOS -1 */
/* count if los==. */
/* count if dcdate==. */
/* *1158 obs have missing values in LOS; 25K obs have missing values in DC date */
/* gen d = socdate + los - 1 */
/* format d %d */
/* *assert dcdate==d if dcdate!=.  */
/* *70 contradictions */
/* list socdate epidate dcdate los d if dcdate!= socdate + los - 1 & dcdate!=. */

/* sum los if dcdate==. */
/* replace dcdate = d if dcdate==. & d!=. */
/* drop d */
/* replace dc_est = 0 if dcd==. */
/* lab var dc_est "=1 if the DC date is imputed" */

tempfile t
save `t'

*--------------------------------------------------
*revise the DC and SOC dates
*--------------------------------------------------
use `t', clear
*get the first visit date and last visit date for each client Epi ID
assert epiid!=.

drop fvd lvd
merge 1:1 epiid using first_last_vd, keep(1 3)
*1832 obs has _m==1 ; 274,612 obs has _m==3

bys `mvar': egen nomatch_admiss = min(_merge)
bys `mvar': egen maxm = max(_merge)
tab nomatch maxm
tab _m_cmw if _merge==1
*tab `mvar' if nomatch_admiss==1 & year(socdate)==2015

*drop admissions if at least one of their episodes are not matched to visit-level dataset
count if epidate==.
sort `mvar' epidate
bys `mvar': gen i = _n==1
count if i==1
count if (maxm!=3 | nomatch!=3) & i==1
*1826
keep if maxm==3 & nomatch==3
list `mvar' clientid socdate epidate epi_recert _merge nomatch maxm episodetype if maxm!=3 | nomatch!=3 in 1/10000
count
assert fvd!=.

*create a new episode date that makes sure the first visit date is on or after the episode date
assert epidate!=.
count if fvd < epidate & epidate!=. & fvd!=.
*199 obs
gen epidate2 = epidate_e
replace epidate2 = fvd if fvd < epidate_e & epidate_e!=. & fvd!=.
format epidate2 %d

*change the SOC date to the first episode date
bys `mvar': egen b = min(epidate2)
gen socdate2 = socdate_e
gen bad = fvd < epidate_e & epidate_e!=. & fvd!=.
bys `mvar': egen maxx = max(bad)
replace socdate2 = b if maxx==1
*257 obs changed
format socdate2 %d
count if socdate_e != socdate2 & fvd < epidate_e & epidate_e!=. & fvd!=.
*187 obs
/*list `mvar' socdate* epidate* dcdate_e if socdate_e != socdate2 & fvd < epidate_e & epidate_e!=. & fvd!=. */

assert socdate2 <= fvd

*check if the number of episodes in an admission matches the number of episodes there should be using the # recertifications + 1
sort `mvar' epiid
bys `mvar' epiid: gen a = _n==1
bys `mvar': egen nepi = sum(a)


*since there are admissions for which I may not have all the episodes, use the # days under HHC to get the # episodes if the # days under HHC indicates a different # episodes than # episode IDs we have
bys `mvar': egen llvd = max(lvd)
gen daysinHH = llvd - socdate2 + 1
gen nepi2 = ceil(daysinHH/60)
count if nepi!=nepi2
forval x = 1/21 {
    replace nepi = nepi2 if nepi==`x' & daysinHH > `x'*60
}
tab nepi if i==1

*if epi_recert==. & nepi==1 & SOC date!= Episode start date, drop
count if epi_recert==.
*77
/* drop if epi_recert==. & nepi==1 & socdate_e!=epidate_e
*3 */

*fill in missing values of epi_recert using # episodes we have in data
forval n = 1/13 {
    replace epi_recert = `n'-1 if nepi==`n' & epi_recert==.
}
assert epi_recert!=.

gen totepi = epi_recert + 1

count
*273676
count if nepi==totepi
*267759
count if nepi!=totepi
*5917
count if nepi > totepi
*5765 - these contain episodes that started after 10/10/2015 when the # re-certification data obtained
count if nepi < totepi
*152 - this is bad ; we are missing some episodes in the admission

*drop admissions if I miss any episode
count if nepi < totepi & i==1
*104
drop if nepi < totepi
assert nepi >= totepi

sort `mvar' socdate_e epidate_e

*create a new DC date that uses last visit date if the last visit date is after the original DC date (impossible to get a visit after discharged)
format llvd %d
count if llvd > dcd & dcd!=.
*194 obs
count if dcd==.
*64219 obs
*list `mvar' socd epid dcd lvd llvd fvd if  llvd > dcd & dcd!=. & dc_est==1

gen dcdate2 = dcdate_e
replace dcdate2 = llvd if dcdate_e < llvd & dcdate_e!=. & llvd!=.
format dcdate2 %d
assert dcdate2 >= llvd if dcdate_e!=.
assert dcdate2==. if dcdate_e==.

/* *have to change the DC date for all the episodes in the admission, not just one episode */
/* gen a = lvd > dcdate_e & dcdate_e!=. | dcdate_e==. */
/* bys `mvar': egen aa = max(a) */
/* replace dcd = llvd if aa==1 & llvd!=. */
/* count if dcd==. */


rename _merge _m_fvd_lvd
lab var _m_fvd_lvd "_merge by epiid with first & last visit dates per epi from visit codelevel"
lab var totepi "Number of episodes under an admission = # recertifications from data +1"
lab var nepi "Number of episodes under an admission I have in data"
lab var daysinHH "= last visit date in the admission - socdate2 + 1"

drop i a maxm nomatch llvd b bad maxx nepi2

lab var dcdate2 "Recoded: last visit in the admission if non-missing DC date before last visitdate"
lab var epidate2 "Recoded: first viistdate in the episode if first visitdate < epidate"
lab var socdate2 "Recoded: first epidate2 in the admission if first visitdate < epidate"


/* compress */
/* tempfile merged_DC */
/* save `merged_DC' */

/* *there are obs whose dc date is after the next soc date, e.g. clientid==114399 & epiid ==176587 -> make the DC date to last visit date of that Epi ID */
/* sort clientid socdate2 epidate2 dcdate */
/* bys clientid: replace dcdate = lvd if dcdate > socdate2[_n+1] & socdate2[_n+1]!=. & `mvar'!=`mvar'[_n+1] */

/* *count if the SOC date is 60+ days before the episode start date */
/* bys `mvar': egen a = min(epidate2) */
/* count if socdate2 <= a - 60 */
/* list `mvar' epiid epidate2 socdate2 dcd if socdate2 < a - 60 in 1/1000 */
/* list `mvar' epiid epidate2 socdate2 dcd los fvd lvd if `mvar' ==651940518 */
/* drop a */

*create episode end date
capture drop epienddate
gen epienddate = epidate2 + 59
assert epienddate!=.
format epienddate %d
lab var epienddate "The end date for a 60-day episode"

destring clientid, replace
foreach v of varlist clientid `mvar' epiid {
    di "`v'"
    assert `v'!=.
}
assert offid_nu!=.

tempfile final
save `final'
*----------------------------------------------------------------
*add patient ZIP code
*----------------------------------------------------------------
use `final', clear
assert epidate_e!=.
gen yr = year(epidate_e)

tostring clientid, replace
merge m:1 clientid yr using client_zip
*306,179 obs have _m=3; 61,082 have _m=1; 117,617 have _m=2

*if the clientID has both unmatched and matched obs with ZIP code data, then use the matched ZIP code for the unmatched obs
bys clientid: egen a = min(_merge)
bys clientid: egen b = max(_merge)
count if a==b & b==2
count if b==3
count if b==2 & a==1
count if a==b & b==1

*-zipcode
gsort clientid epidate_e
bys clientid: replace zipcode = zipcode[_n-1] if zipcode >= .

gsort clientid -epidate_e
bys clientid: replace zipcode = zipcode[_n-1] if zipcode >= .

count if zipcode==.
*& a==1 & _merge==1

preserve
keep if zipcode==.
drop zipcode
keep clientid yr
duplicates drop
replace yr = yr - 1
merge 1:m clientid yr using client_zip, keep(1 3) nogen
tempfile zip_l1
save `zip_l1'

keep if zipcode==.
keep clientid yr
duplicates drop
replace yr = yr + 2
merge 1:m clientid yr using client_zip, keep(1 3) nogen
tempfile zip_lead1
save `zip_lead1'

keep if zipcode==.
keep clientid
duplicates drop
merge 1:m clientid using client_zip, keep(1 3) nogen
tempfile zip_0
save `zip_0'
restore

rename zipcode zip0
replace yr = yr - 1
merge m:1 clientid yr using `zip_l1', gen(m2)
replace zip0 = zipcode if m2==3 & zip0==.
drop m2 zipcode

replace yr = yr +2
merge m:1 clientid yr using `zip_lead1', gen(m2)
replace zip0 = zipcode if m2==3 & zip0==.
drop m2 zipcode

merge m:1 clientid using `zip_0', gen(m2)
replace zip0 = zipcode if m2==3 & zip0==.
drop m2 zipcode

drop if _merge==2
drop _merge a b yr
rename zip0 patzip
lab var patzip "Patient ZIP code"

destring clientid, replace

compress
save masterclientadmiss2, replace




*----------------
*how many episodes are there for each admission?
use masterclientadmiss2, clear
duplicates tag `mvar' epiid, gen(dup)
assert dup==0
drop dup

bys `mvar': gen nepi = _N

*how many episodes should there be for each admission based on the SOC date (& DC date, if available)?
bys `mvar': egen lasteed = max(epienddate)
bys `mvar': egen firstepid = min(epidate2)
gen l = (lasteed - firstepid + 1)/60
tab l

*

sort `mvar' epidate_e
bys `mvar': gen i = _n==1
count if i==1 & l > nepi


*1368/224676 admissions: we have fewer episodes than there should be
list `mvar' if i==1 & l > nepi in 10000/101000
list `mvar' epiid socdate* epidate* dcdate* l nepi if l < nepi
*these are actually correct; l is just not integer

/* *----------------------------------------------------------------------------- */
/* *hospitalization - merge with hospitalization data  */
/* *----------------------------------------------------------------------------- */
/*   use `merged_DC', clear */
/* count */

/* *since some obs have first episode date not equal to SOC date, create a new episode date var where the first episode date is same as SOC date */
/* bys `mvar': egen f = min(epidate_e) */
/* count if socdate_e!=f */
/* gen epi2 = epidate_e */
/* replace epi2 = socdate_e if socdate_e!=f */

/* *create clientID + episode date var to merge with hospitalization data  */
/* tostring epi2, gen(socroc) */
/* gen client_socrocid = clientid + socroc */
/* destring client_socrocid, replace */
/* format client_socrocid %11.0f */
/* replace client_socrocid = client_socrocid + 21916 */
/* drop socroc epi2 */

/* destring clientid, replace */
/* count */
/* merge m:1 client_socrocid clientid using hosp */
/* count */

/* *drop client IDs that only come from the hosp data (i.e. _merge==2) */
/* sort clientid socdate_e epidate_e socrocdate hospdate */
/* bys clientid: egen a = max(_merge) */
/* bys clientid: egen aa = min(_merge) */
/* count if a==2 & aa==2 */
/* *2.3K obs only appear in the hosp data -> they have hosp date mostly before 1/1/2012 or 1 on 1/8/2015 -> drop them */
/* list `mvar' clientid epiid _m socdate epidate socrocdate hospdate dcdat los if a==2 & aa==2 in 1/1000 */
/* tab socrocd if a==2 & aa==2 */
/* *7 obs with socrocdate in 2012-2015 */
/* /\* list if a==2 & aa==2 & year(socrocd)==2015 *\/ */
/* /\* list if clientid=="257713" *\/ */
/* drop if a==2 & aa==2 */
/* drop a aa */

/* *drop 18 obs where hosp date is in 2020 (error) */
/* drop if year(hospdate) > 2015 & hospdate!=. */

/* tempfile tmp */
/* save `tmp' */
/* count */

/* *I want to create a patient-episode-admission level dataset and note whether the hosp occurred in each episode */
/* *-> not a problem for most guys with only one episode during an admission */

/* *assign the admission-SOC ID in the previous line to the _merge=2 SOC/ROC - hospitalization event if the hosp occurred before the next episode start date */
/* use `tmp', clear */
/* replace epidate_e = socrocdate if _merge==2 */
/* drop socrocdate */
/* sort clientid epidate_e hospdate */

/* *drop ROC & hosp observations whose SOC/ROC date is before the first SOC date of the associated clientid */
/* bys clientid: egen fs = min(socdate) */
/* drop if epidate_e < fs */

/* loc cond ( _merge==2 & hospdate <= dcdate[_n-1]+1 & epidate >= epidate[_n-1] & hospdate > epidate[_n-1] & dcdate[_n-1]!=. & dcdate==.) */
/* foreach v of varlist `mvar' socdate epiid offid_nu dcdate { */
/*   bys clientid: replace `v' = `v'[_n-1] if `cond' */
/* } */

/* loc cond ( _merge==2 & hospdate <= dcdate[_n-1]+1 & epidate >= epidate[_n-1] & hospdate > epidate[_n-1] & dcdate[_n-1]!=. ) */
/* foreach v of varlist `mvar' socdate epiid offid_nu { */
/*   bys clientid: replace `v' = `v'[_n-1] if `cond' */
/* } */

/* *even if the episode date & hospdate is not before the previous DC date, assign the previous epiID since DC date may be wrong */
/* loc cond (_merge==2 & dcdate==. & dc_est[_n-1]==1 & epidate >= epidate[_n-1] & hospdate > epidate[_n-1]) */
/* foreach v of varlist `mvar' socdate epiid offid_nu dcdate { */
/*   bys clientid: replace `v' = `v'[_n-1] if `cond' */
/* } */

/* *there are 1.1K obs with ROC date is before the DC date but the second hosp date is later than the latest DC date - then ignore that the second hosp date is later than the DC date and just assign the previous epiid, `mvar', etc. */
/* gsort clientid epidate hospdate */
/* loc cond (_merge==2 & dcdate==. & epidate_e <= dcdate[_n-1] & dcdate[_n-1]!=.) */
/* foreach v of varlist `mvar' socdate epiid offid_nu dcdate { */
/*   bys clientid: replace `v' = `v'[_n-1] if `cond' */
/* } */
/* *bys clientid: replace dcdate = dcdate[_n-1] if _merge==2 & dcdate==. & hospdate <= dcdate[_n-1] + 30 & hospdate > dcdate[_n-1] & dcdate[_n-1]!=. */

/* codebook epiid */
/* *39 obs have missing values in epiid */

/* *manually drop some weird observations */
/* *clientid 71427 has epidate 12/15/2012 & 12/17/2012 -> drop one with 12/17/2012 since that epiid has no visits associated in the visit-level data */
/* drop if epiid==97837 */

/* *some observations have hospitalization date occurring after the next epidate */
/* loc cond (_merge==2 & dcdate==. & hospdate <= dcdate[_n+1] & hospdate >= epid[_n+1]) */
/* foreach v of varlist `mvar' socdate epiid offid_nu dcdate { */
/*   bys clientid: replace `v' = `v'[_n+1] if `cond' */
/* } */

/* *manually correct 1 obs: clientid 49627 hosp date 2/16/2013 belong to epiid 158924 */
/* replace epiid = 158924 if clientid==49627 & epiid==. & hospdate==mdy(2,16,2013) */
/* sort clientid epiid socd */
/* foreach v of varlist `mvar' socdate offid_nu dcdate { */
/*   bys clientid: replace `v' = `v'[_n-1] if `v'>=. & clientid==49627 */
/* } */
/* sort clientid epid hospd */

/* *manually correct 1 obs: clientid 16616 has only 1 viist on 2/24/2012, epidate */
/* drop if clientid==16616 & _merge==2 */

/* *manually drop 1 obs: clientid 6351 has epidate 7/9/2013 but there is no visit during that period */
/* drop if clientid==6351 & epiid==. */

/* *clientid 124769 has DC date 5/1/2013 but has hosp date 5/9/2013 & the last visit is 4/26/2013 -> drop it */
/* *similarly, clientid 198430 has DC date 7/2/2015 but has hosp date 7/19/2015 & the last visit is 7/1/2015 -> drop it */
/* *similar cases: clientid 205711 */

/* *manually correct 1 obs: clientid 269924 has last visit on 4/19/2015, so assign the previous epiid */
/* replace epiid = 487979 if clientid==269924 & epiid==. & hospdate==mdy(4,23,2015) */
/* sort clientid epiid socd */
/* foreach v of varlist `mvar' socdate offid_nu dcdate { */
/*   bys clientid: replace `v' = `v'[_n-1] if `v'>=. & clientid==269924 */
/* } */
/* count if epiid==. */
/* assert _merge==2 if epiid==. */
/* drop if epiid==. */

/* gen _m_fromhosp = _merge */
/* drop _merge */

/* foreach v of varlist `mvar' socdate epiid offid_nu  { */
/*   assert `v'!=. */
/* } */

/* tempfile tmp2 */
/* save `tmp2' */
/* count */

/* *------------------   */
/* *in some episode obs, hosp date that belong to some later episodes belong to earlier episodes -> move to the corresponding episodes */

/* use `tmp4', clear */

/* *create a new SOC date variable that changes the SOC date to the first visit date if the first visit date is before the SOC date */
/* count if fvd < socd */
/* gen socdate2 = socd */
/* replace socdate2 = fvd if fvd < socdate_e */
/* format socdate2 %d */
/* assert socdate2 <= fvd */
/* assert dcdate >= lvd */

/* *create a new episode date that makes sure the first visit date is on or after the episode date */
/* count if fvd < epidate */
/* gen epidate2 = epid */
/* replace epidate2 = fvd if fvd < epidate_e */
/* format epidate2 %d */

/* sort clientid socdate2 epidate2 hospdate dcdate */
/* /\* bys clientid socdate: replace hd = hospdate if hospdate >= epidate & hospdate < epidate[_n+1] & hospdate!=. *\/ */

/* gen dayofhosp = hospdate - epidate2 + 1 */

/* sort clientid socdate2 epidate2 hospdate dayofhosp dcdate */
/* foreach v of varlist hospdate dayofhosp { */
/*   bys clientid socdate2: replace `v' = `v'[_n-1] if `v' >=. */
/* } */

/* sort clientid socdate2 hospdate epidate2 dcdate */
/* bys clientid socdate2 : gen gap = epidate2 - epidate2[_n-1] */
/* bys clientid socdate2 hospdate: replace gap = 0 if _n==1 */
/* bys clientid socdate2 hospdate: gen cgap = gap if _n==1 */
/* bys clientid socdate2 hospdate: replace cgap = sum(gap) */
/* bys clientid socdate2 hospdate: replace cgap = cgap + 1 if _n!=1 */

/* gen hd = . */
/* bys clientid socdate2 hospdate: replace hd = hospdate if dayofhosp >= cgap & dayofhosp < cgap[_n+1]  */

/* sort clientid socdate2 epidate2 hospdate dcdate */
/* bys clientid socdate2: replace hd = hospdate if hospdate >= epidate2 & hospdate < epidate2[_n+1] & hospdate!=. */

/* gen hospoccur = hospdate!=. */
/* count if hospoccur==1 */
/* count if hd!=. */
/* capture drop ss sshd hdi */
/* bys clientid socdate2: egen ss = sum(hospoccur) */
/* gen hdi = hd!=. */
/* bys clientid socdate2: egen sshd = sum(hdi) */
/* count if ss!=sshd */

/* capture drop ss sshd hdi */
/* bys clientid socdate2: egen ss = sum(hospoccur) */
/* gen hdi = hd!=. */
/* bys clientid socdate2: egen sshd = sum(hdi) */
/* count if ss!=sshd */

/* /\* list clientid socdate `mvar' epiid epidate hospdate los dayofhosp *gap hd hospoccur if ss!=sshd in 1/10000 *\/ */

/* *it appears that the difference in the number of hospitalization incidents is due to error with the original record */
/* drop hospoccur hospdate *gap ss* hdi hospreason* */
/* format hd %d */
/* rename hd hospdate_e */
/* /\* forval i = 1/7 { *\/ */
/* /\*   rename x_hospreason`i' hospreason`i' *\/ */
/* /\* } *\/ */

/* *there are some obs with hospitalization listed in the previous episode while it should be in the next episode */
/* sort clientid socdate2 epidate2 hospd */
/* gen nextepid = epidate2[_n+1] if hospd!=. & clientid==clientid[_n+1] */
/* gen g = nextepid - hospd + 1 */
/* format nextepid %d */
/* sum g, de */
/* count if g < 0 */
/* gen next2epid = epidate2[_n+2] if hospd!=. & clientid==clientid[_n+2] */
/* gen g2 = next2epid - hospd + 1 */
/* gen next3epid = epidate2[_n+3] if hospd!=. & clientid==clientid[_n+3] */
/* gen g3 = next3epid - hospd + 1 */

/* bys clientid: replace hospdate = hospdate[_n-1] if g[_n-1] < 0 & g2[_n-1] > 0 & hospdate[_n-1]!=. & hospd==. */
/* replace hospd = . if g < 0 & g2 > 0 */
/* bys clientid: replace hospd = . if g < 0 & g2 < 0 & hospd[_n+1]==hospd */
/* *for clientid = 283436 */
/* bys clientid: replace hospd = . if g < 0 & g2 < 0 & hospd[_n+1]==hospd - 1 */
/* *for clientid = 96106 */
/* bys clientid: replace hospd = . if g < 0 & g2 < 0 & hospd[_n+2]==hospd */
/* *clientid = 33495 */

/* drop g* next* */
/* forval k = 1/5 { */
/*   sort clientid socdate2 epidate2 hospd */
/*   gen n`k' = epidate2[_n+`k'] - hospd + 1 if hospd!=. & clientid==clientid[_n+`k'] */
/* } */
/* sum n1, de */
/* count if n1 < 0 */

/* replace hospd = hospd[_n-2] if n1[_n-2] < 0 & n2[_n-2] < 0 & n3[_n-2] > 0 & hospd==. & clientid==clientid[_n-2] */
/* replace hospd = . if n1 < 0 & n2 < 0 & n3 > 0 */
/* replace hospd = hospd[_n-3] if n1[_n-3] < 0 & n2[_n-3] < 0 & n3[_n-3] <0 & n4[_n-3] > 0 & hospd==. & clientid==clientid[_n-3] */
/* replace hospd = . if n1 < 0 & n2 < 0 & n3 <0 & n4 > 0 */
/* replace hospd = hospd[_n-4] if n1[_n-4] < 0 & n2[_n-4] < 0 & n3[_n-4] <0 & n4[_n-4] < 0 & n5[_n-4] > 0 & hospd==. & clientid==clientid[_n-4] */
/* replace hospd = . if n1 < 0 & n2 < 0 & n3 <0 & n4 < 0 & n5 > 0 */
/* drop n? */

/* sort clientid socdate2 epidate2 hospd */
/* gen n = epidate2[_n+1] - hospd + 1 if hospd!=. & clientid==clientid[_n+1] */
/* assert n >= 0 */
/* drop n */

/* *fill in missing values of payor type if for the same admission ID, payor type info is available */
/* gsort `mvar' -payortype */
/* bys `mvar': replace payortype = payortype[_n-1] if payortype >= "" & payortype[_n-1]!="" */
/* tempfile tmp */
/* save `tmp' */

/* *there are some admission IDs with missing values in payor type -> merge with the original admission data to get the payor type */
/* use `tmp', clear */
/* keep if payort=="" */
/* keep `mvar' */
/* duplicates drop */
/* merge 1:m `mvar' using "Rev_Master DB_v2", keep(3) keepusing(payortype) nogen */
/* rename payortype pp */
/* duplicates drop */
/* sort `mvar' */
/* *there are Multiple sources indicated; if MANAGED - MEDICARE & PPS-non Meidcare are both indicated, choose managed-Medicare; */
/* duplicates tag `mvar', gen(dup) */
/* capture drop *medi */
/* gen medi = pp=="MANAGED - MEDICARE" */
/* bys `mvar': egen amedi = max(medi) */
/* drop if amedi==1 & dup > 0 & medi==0 */

/* *if Medicare & Medicaid indicated, choose Medicare */
/* capture drop *medi */
/* gen medi = regexm(pp,"MEDICARE") */
/* bys `mvar': egen amedi = max(medi) */
/* drop if amedi==1 & dup > 0 & medi==0 */

/* *if both Managed medicaid & just medicaid included, choose managed Medicaid (2 obs) */
/* capture drop *medi */
/* gen medi = pp=="MANAGED - MEDICAID" */
/* bys `mvar': egen amedi = max(medi) */
/* drop if amedi==1 & dup > 0 & medi==0 */

/* drop dup */
/* duplicates tag `mvar', gen(dup) */
/* assert dup==0 */
/* drop dup *meddrop pp */

/* * I want to know 1) if during the episode, a hospitalization occurred; and 2) if the patient returend, what is the length of stay in hospital */

/* merge 1:m `mvar' using `tmp', nogen */
/* replace payortype = pp if payortype=="" */
/* assert payortype!="" */
/* drop pp */

/* *there are some missing values in branch_st */
/* codebook branch_st */
/* gsort `mvar' -branch_st */
/* bys `mvar': replace branch_st = branch_st[_n-1] if branch_st[_n-1]!="" & branch_st=="" */
/* count if branch_st=="" */
/* tempfile tmp */
/* save `tmp' */

/* use `tmp', clear */
/* keep if branch_st=="" */
/* keep offid_nu */
/* duplicates drop */
/* merge 1:m offid_nu using office_location, keep(3) nogen keepusing(branch_st) */
/* rename branch_st st2 */

/* *merge back with the admission level data */
/* merge 1:m offid_nu using `tmp', nogen */
/* replace branch_st = st2 if branch_st=="" & st2!="" */
/* assert branch_st!="" */
/* drop st2 */

/* * I want to know 1) if during the episode, a hospitalization occurred; and 2) if the patient returend, what is the length of stay in hospital */






/* save masterclientadmiss, replace */
