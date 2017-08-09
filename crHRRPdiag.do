*use M1010 client's first inpatient diagnosis code (ICD-9) & tag conditions subject to the Hospital Readmissions Reduction Program

loc path /home/hcmg/kunhee/Labor/Bayada_data
cd `path'

use inpat_dx, clear

*use both inpatdiag & inpat_dx vars to identify a patient who has applicable conditions

*ami
tab inpat_dx if substr(inpat_dx,1,3)=="410"
gen ami = substr(inpat_dx,1,3)=="410"
tab ami

*hf
gen hf = substr(inpat_dx,1,3)=="428"
replace hf = 1 if inpat_dx=="402.01"| inpat_dx=="402.11"| inpat_dx=="402.91"| inpat_dx=="404.01"| inpat_dx=="404.03"| inpat_dx=="404.11"| inpat_dx=="404.13"| inpat_dx=="404.91"| inpat_dx=="404.93"

*pneumonia
tab inpat_dx if substr(inpat_dx,1,3)=="480"|substr(inpat_dx,1,3)=="481"|substr(inpat_dx,1,3)=="482"| substr(inpat_dx,1,3)=="483"|substr(inpat_dx,1,3)=="485"|substr(inpat_dx,1,3)=="486"|inpat_dx=="487"|inpat_dx=="488.11"
gen pneu = substr(inpat_dx,1,3)=="480"|substr(inpat_dx,1,3)=="481"|substr(inpat_dx,1,3)=="482"| substr(inpat_dx,1,3)=="483"|substr(inpat_dx,1,3)=="485"|substr(inpat_dx,1,3)=="486"|inpat_dx=="487"|inpat_dx=="488.11"

*expanded definition of pneumonia starting in FY 2016
tab inpat if substr(inpat,1,3)=="507"
gen pneu2 = inpat_dx=="507.0" | inpat_dx=="507"
tab inpat if substr(inpat,1,3)=="038" | substr(inpat,1,3)=="38." | substr(inpat,1,3)=="995"
gen pneu3 = inpat_dx=="995.91" | substr(inpat,1,3)=="038" | substr(inpat,1,3)=="38."

gen pneu_new = pneu2==1 | (pneu3==1 & (pneu2==1 & pneu==1))
drop pneu2 pneu3

*COPD
*has primary diagnosis of COPD
gen copd =  inpat_dx=="491.21"|inpat_dx=="491.22"|inpat_dx=="491.8"|inpat_dx=="491.9"|inpat_dx=="492.8"|inpat_dx=="493.2"|inpat_dx=="493.21"|inpat_dx=="493.22"|inpat_dx=="496"

*has both primary diagnosis of respiratory failure + secondary diagnosis of AECOPD
gen rf = inpat_dx=="518.81" | inpat_dx=="518.82" |inpat_dx=="518.84" |inpat_dx=="799.1"
gen ae = inpat_dx=="491.21"|inpat_dx=="491.22"|inpat_dx=="493.21"|inpat_dx=="493.22"
replace copd = 1 if rf==1 & ae==1

foreach v of varlist rf ae {
    bys clientid socdate: egen max`v' = max(`v')
}
replace copd = 1 if maxrf==1 & maxae==1

*Total hip arthroplasty / Total knee arthroplasty; it seems that we don't have procedural codes -- COME BACK TO THIS
/*tab inpat_dx if regexm(inpat_dx, "81[.]")*/


*if a client ID-SOC date have 1 in at least one obs for each diagnosis indicators, then assign the admission to having that condition
foreach v of varlist hf pneu pneu_new ami copd {
    bys clientid socdate: egen max`v' = max(`v')
}
drop ami hf pneu copd rf ae maxrf maxae inpat_dx pneu_new

foreach l in "hf" "pneu" "ami" "copd" "pneu_new" {
    rename max`l' `l'
}
duplicates drop

compress
save HRRPdiag, replace
