*1) add additional variables, e.g. risk of hospitalization, smoking, etc.,
*OASIS vars : I should use variables available in both C & C1 versions
*2) create improvement or stabilization in outcome vars using the HomeHealthOutcomeMeasuresTable.pdf in
*https://www.cms.gov/Medicare/Quality-Initiatives-Patient-Assessment-Instruments/HomeHealthQualityInits/Downloads/Home-Health-Measures-Tables-Updated-March-7-2016.zip
/**Use outcomes used to compute star ratings:
4. Improvement in Ambulation
5. Improvement in Bed Transferring
6. Improvement in Bathing
7. Improvement in Pain Interfering With Activity
8. Improvement in Shortness of Breath
9. Acute Care Hospitalization*/
*the algorithm for creating the improvement/stabilization indicators found in "Patient Outcome Measures Revision 4.pdf" from
*https://www.cms.gov/Medicare/Quality-Initiatives-Patient-Assessment-Instruments/HomeHealthQualityInits/Downloads/Technical-Documentation-of-OASIS-Based-Measures-for-OASIS-C1-ICD-9-and-OASIS-C1-ICD-10-2014_09_30-[ZIP-2MB]-.zip

cd /home/hcmg/kunhee/Labor/Bayada_data
loc mvar admissionclientsocid
loc id `mvar' clientid socdate_e

* outcome variables -----------------

*management of oral medications
use DB4_M2020_v2.dta, clear

loc x oralmed
foreach v of varlist `x'* {
    assert `v'!=.
    rename `v' `v'_soc
}
merge 1:1 `mvar' client socdate using DB8_M2020_v2, keep(3) nogen

gen yr = year(socdate)
bys yr: sum
keep if yr >= 2012 & yr <= 2015
drop yr

keep if `x'_na_soc==0 & `x'_na==0

*create a var that equals 1, 2, 3,... for each possible answer to rank
foreach l in "_soc" "" {
    gen `x'`l' = .
    replace `x'`l' = 0 if oralmed_indep`l'==1
    replace `x'`l' = 1 if oralmed_prep`l'==1
    replace `x'`l' = 2 if oralmed_reminder`l'==1
    replace `x'`l' = 3 if oralmed_unable`l'==1
}
gen imprv_`x' = `x' < `x'_soc if `x'_soc > 0
gen stabl_`x' = `x' <= `x'_soc if `x'_soc < 3

keep imprv stabl `id'

tempfile oralmed
save `oralmed'

*--------------------------------
*improvement in ambulation (1860) & bed transferring (1850)
use "Rev_M1850 - 1860_v2.dta", clear

loc v1 ambul
loc v2 trnsf
foreach v of varlist `v1'* `v2'* {
    rename `v' `v'_soc
}

merge 1:1 `mvar' clientid socdate using "DB7_M1860_v2.dta", keep(3) nogen

merge 1:1 `mvar' clientid socdate using "DB7_M1850_v2.dta", keep(3) nogen

gen yr = year(socdate)
bys yr: sum
keep if yr >= 2012 & yr <= 2015
drop yr

*create one var that equals 1, 2, 3,... for each possible answer to rank
foreach l in "_soc" "" {
    gen `v1'`l' = .
    replace `v1'`l' = 0 if `v1'_able`l'==1
    replace `v1'`l' = 1 if `v1'_onedevice`l'==1
    replace `v1'`l' = 2 if `v1'_twodevice`l'==1
    replace `v1'`l' = 3 if `v1'_assist`l'==1
    replace `v1'`l' = 4 if `v1'_chairfastwh`l'==1
    replace `v1'`l' = 5 if `v1'_chairfastnowh`l'==1
    replace `v1'`l' = 6 if `v1'_bedfast`l'==1
}
foreach l in "_soc" {
    gen `v2'`l' = .
    replace `v2'`l' = 0 if `v2'_able==1
    replace `v2'`l' = 1 if `v2'_minimal==1
    replace `v2'`l' = 2 if `v2'_bear==1
    replace `v2'`l' = 3 if `v2'_nobear==1
    replace `v2'`l' = 4 if `v2'_turn==1
    replace `v2'`l' = 5 if `v2'_noturn==1
}
foreach l in "" {
    gen `v2'`l' = .
    forval k = 0/5 {
        replace `v2'`l' = `k' if `v2'`k'==1
    }
}

gen imprv_`v2' = `v2' < `v2'_soc if `v2'_soc > 0
gen stabl_`v2' = `v2' <= `v2'_soc if `v2'_soc < 5

gen imprv_`v1' = `v1' < `v1'_soc if `v1'_soc > 0
gen stabl_`v1' = `v1' <= `v1'_soc if `v1'_soc < 6

keep imprv* stabl* `id'

tempfile amb
save `amb'

*--------------------------------
*ability to bath herself
use "Rev_M1820 - 1830_v2.dta", clear
drop dress*

loc x bath
foreach v of varlist `x'* {
    rename `v' `v'_soc
}

merge 1:1 `mvar' clientid socdate using "DB7_M1830_v2.dta", keep(3) nogen

foreach l in "_soc" "" {
    gen `x'`l' = .
    forval k = 0/5 {
        replace `x'`l' = `k' if `x'`k'==1
    }
}

gen imprv_`x' = `x' < `x'_soc if `x'_soc > 0
gen stabl_`x' = `x' <= `x'_soc if `x'_soc < 6

keep imprv* stabl* `id'

gen yr = year(socdate)
bys yr: sum
keep if yr >= 2012 & yr <= 2015
drop yr

tempfile bath
save `bath'

*--------------------------------
*frequency of pain
use "DB2_M1242 - 1300_v2.dta", clear
drop PU*

loc x freqpain
foreach v of varlist `x'* {
    rename `v' `v'_soc
}

merge 1:1 `mvar' clientid socdate using "DB6_M1242_v2.dta", keep(3) nogen

foreach l in "_soc" "" {
    gen `x'`l' = .
    replace `x'`l' = 0 if `x'_no`l'==1
    replace `x'`l' = 1 if `x'_noaffect`l'==1
    replace `x'`l' = 2 if `x'_ledaily`l'==1
    replace `x'`l' = 3 if `x'_daily`l'==1
    replace `x'`l' = 4 if `x'_alltime`l'==1
}

gen imprv_`x' = `x' < `x'_soc if `x'_soc > 0
gen stabl_`x' = `x' <= `x'_soc if `x'_soc < 4

keep imprv* stabl* `id'

gen yr = year(socdate)
bys yr: sum
keep if yr >= 2012 & yr <= 2015
drop yr

tempfile pain
save `pain'

*--------------------------------
*when Short of Breath?
use "DB2_M1400 - 1410_v2.dta", clear
drop respira*

loc x whendyspneic
foreach v of varlist `x'* {
    rename `v' `v'_soc
}

merge 1:1 `mvar' clientid socdate using "DB6_M1400_v2.dta", keep(3) nogen

foreach l in "_soc" "" {
    gen `x'`l' = .
    replace `x'`l' = 0 if `x'_n`l'==1
    replace `x'`l' = 1 if `x'_walk`l'==1
    replace `x'`l' = 2 if `x'_moderate`l'==1
    replace `x'`l' = 3 if `x'_minimal`l'==1
    replace `x'`l' = 4 if `x'_rest`l'==1
}

gen imprv_`x' = `x' < `x'_soc if `x'_soc > 0
gen stabl_`x' = `x' <= `x'_soc if `x'_soc < 4

keep imprv* stabl* `id'

gen yr = year(socdate)
bys yr: sum
keep if yr >= 2012 & yr <= 2015
drop yr

tempfile dyspnea
save `dyspnea'



* risk variables -----------------

*add overall status & risk of hospitalization
use "DB3_M1032 - 1033 - 1034_v2.dta", clear
gen yr = year(socdate)
bys yr: sum overallst_uk
drop riskhosp_frail riskhosp_exha riskhosp_manyer riskhosp_diff riskhosp_weig
keep if yr >= 2012 & yr <= 2015
drop yr
tempfile hosprisk
save `hosprisk'
*--------------------------------
*high risk factors (M1036) - includes smoking, alcohol dependency
use "DB3_M1036 - 1200_v2.dta", clear
rename admissionclie `mvar'
gen yr = year(socdate)
bys yr: sum
keep if yr >= 2012 & yr <= 2015
drop yr
tempfile riskfactor
save `riskfactor'
*--------------------------------
*prior conditions (M1018) prior to Medical or Treatment Regimen Change or Inpatient Stay Within Past 14 Days
use DB3_M1000_v2.dta
keep prior* `mvar' clientid socdate
gen yr = year(socdate)
bys yr: sum
keep if yr >= 2012 & yr <= 2015
drop yr
tempfile priorcond
save `priorcond'
*--------------------------------
*add living situation variables (M1100)
use DB3_M1100_v2.dta, clear
gen yr = year(socdate)
bys yr: sum
keep if yr >= 2012 & yr <= 2015
drop yr

*patient lives alone
gen livealone = living_alatc==1 | living_alna==1 |living_aloa==1 | living_alrd==1 | living_alrn==1
gen livecon = living_coatc==1 | living_cona==1 | living_cooa==1 | living_cord==1 | living_corn==1
gen liveoth = living_othatc==1 | living_othna==1 | living_othoa==1 | living_othrd==1 | living_othrn==1
gen noassist = living_alna==1 | living_cona==1 | living_othna==1

drop living_*

tempfile living
save `living'
*--------------------------------
*cognitive functioning
use "DB2_M1615 - 1700_v2.dta", clear
drop whenincont*

*merge with when confused/anxious variables
merge 1:1 `mvar' clientid socdate using "DB2_M1710 - 1720_v2.dta", nogen

gen yr = year(socdate)
bys yr: sum
keep if yr >= 2012 & yr <= 2015
drop yr

gen mentalfine = cogni_depend!=1 & whenconfuse_na!=1 & whenanxious_na!=1

tempfile cognitive
save `cognitive'

*--------------------------------
*depression screening
use Rev_M1730_v2, clear
rename admissionclie `mvar'

gen yr = year(socdate)
bys yr: sum
keep if yr >= 2012 & yr <= 2015
drop yr

*create 0/1 variable for whether depressed at all or not
gen depressed = 0 if phq2int_not==1 & phq2down_not==1

*depressed = 1 if phq2int* ==1 at all or phq2down==1 at all
foreach v of varlist phq2*_gthalf phq2*_every phq2*_several {
    replace depressed = 1 if `v'==1
}
count if depressed == .

drop depress_* phq*

collapse (max) depressed , by(`id')

tempfile depressed
save `depressed'

*--------------------------------
*behavioral problems (1740)
local file "Rev_M1740 - 1745"
use "`file'_v2", clear

gen freqbehav = freqbehav_ge1day ==1 | freqbehav_manywk==1 | freqbehav_manymo == 1

keep behav_impdm behav_memd freqbehav `mvar' clientid socdate

gen yr = year(socdate)
bys yr: sum
keep if yr >= 2012 & yr <= 2015
drop yr

tempfile behavprob
save `behavprob'
*--------------------------------

*how much receive care from others?
/* use DB4_M2100_v2.dta, clear

use DB4_M2102_v2.dta, clear */

*receive care from others?
use DB4_M2110_v2.dta

gen ADLhelp_rarely = oftenADLa_no==1 | oftenADLa_lt1==1 if oftenADLa_uk==0

gen yr = year(socdate)
bys yr: sum
keep if yr >= 2012 & yr <= 2015
drop yr

keep ADLhelp_rarely `id'

tempfile help
save `help'

/* *ability to do daily activity of living prior to HHC
use "DB4_M1900_v2.dta" */
 

*----------------------------
*merge all the variables

use client_chars, clear
foreach file in "oralmed" "amb" "bath" "pain" "dyspnea" "hosprisk" "riskfactor" "priorcond" "living" "cognitive" "depressed" "behavprob" "help" {
    di "`file'"
    merge m:1 `id' using ``file'', keep(1 3) nogen
}
compress
save client_chars2, replace

/*
*----------------------------
*merge the additional variables with the episode-level handoffs data
use handoff, clear
foreach file in "oralmed" "amb" "bath" "pain" "dyspnea" "hosprisk" "riskfactor" "priorcond" "living" "cognitive" "depressed" "behavprob" "help" {
    di "`file'"
    merge 1:1 `id' using ``file'', keep(1 3) nogen
}
compress
save handoff, replace

use handoff_pd, clear

*fill in missing values for no-visit days
sort epiid visitdate
foreach v of varlist `mvar' socdate_e {
    bys epiid: replace `v' = `v'[_n-1] if `v' >=.
    assert `v'!=.
}

foreach file in "oralmed" "amb" "bath" "pain" "dyspnea" "hosprisk" "riskfactor" "priorcond" "living" "cognitive" "depressed" "behavprob" "help" {
    di "`file'"
    merge m:1 `id' using ``file'', keep(1 3) nogen
}
compress
save handoff_pd, replace*/
