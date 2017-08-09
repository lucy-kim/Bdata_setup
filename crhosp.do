*create hospitalization data before merging with client admission base data

loc path /home/hcmg/kunhee/Labor/Bayada_data
cd `path'
local mvar admissionclientsocid

use "Rev_Hosp & Reason_v2", clear
format `mvar' %11.0f
duplicates tag `mvar', gen(dup)
*list if dup > 0 in 1/30

*it appears that obs with the same admission-SOC ID & hosp date appears >1 once for different hosp reasons
duplicates tag `mvar' clientid oasistype hospcou socrocd hospdat, gen(dupp)
tab dup
tab dupp
assert dupp > 0 if dup > 0
*6 contradictions
list if dupp ==0 & dup > 0

*for the 6 contradictions, the OASIS type differs and hosp date & other vars are same except for one clientid=38713
drop if dupp ==0 & dup > 0 & oasistype!="Resumption of Care"
  
*reshape wide
rename hospitalizationreason hospreason
bys `mvar' clientid oasistype socroc hospd: gen ii = _n
list in 1/20
reshape wide hospreason, i(`mvar' clientid oasistype socroc hospd) j(ii)

*drop hosp reason & remove duplicates on other var values
duplicates drop
duplicates tag `mvar', gen(duppp)
assert duppp==0
drop dup* oasistype hospcount

rename `mvar' client_socrocid

save hosp, replace

tostring clientid, replace
tostring epidate_e, gen(socroc)
gen id = client + socroc
list `mvar' id clientid socroc in 1/10
destring id, replace
format id %11.0f
replace id = id + 21916

/* gen yr=year(socroc) */
/* gen mo=month(socroc) */
/* gen yr2=year(hospd) */
/* gen mo2=month(hospd) */
/* drop if yr<2012 | (yr==2015 & mo>6) */
/* drop if yr2<2012 | (yr2==2015 & mo2>6) */
/* drop yr* mo* */

