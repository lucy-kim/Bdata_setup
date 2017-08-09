*create Charlson Index and Elixhauser index by looking at the ICD-9-CM codes for home health care diagnosis & inpatient diagnosis (for patients with prior inpatient stays) in the patient data (referred to crcharlson_comorbid.do)

cd /home/hcmg/kunhee/Labor/Bayada_data
capture ssc install charlson
capture ssc install elixhauser
loc mvar admissionclie~d

*create data containing all relevant ICD-9-CM codes for the beginning of admission from the inpatient diagnosis codes data & ICD-9-CM code for using HHC reported in the OASIS data

*keep ICD9 code & category for the first episode only for each admission
use precomorbid, clear
keep `mvar' epiid epidate2 clientid socdate_e icdcode category
duplicates drop

sort `mvar' epidate2
bys `mvar': gen n = _N
bys `mvar': drop if n > 1 & _n > 1
drop n

merge 1:m clientid socdate_e using inpat_dx
*149K have _m=3; 17K have _m=2; 67K have _m=1
drop if _m==2
drop _merge

*unify the prefix of ICD code vars
rename icdcode icd1
rename inpat_dx icd2

*convert the ICD-9-CM codes with dots to a version of code without dot; for XX.YY it should be 0XXYY
split icd2, p(".")
gen l2 = length(icd21)
tab icd2 if icd2!="" & l2==1
replace icd2 = "00" + icd2 if icd2!="" & l2==1
tab icd2 if icd2!="" & l2==2
replace icd2 = "0" + icd2 if icd2!="" & l2==2
tab icd2 if icd2!="" & l2==4

split icd1, p(".")
gen l1 = length(icd11)
tab icd1 if icd1!="" & l1==1
replace icd1 = "00" + icd1 if icd1!="" & l1==1
tab icd1 if icd1!="" & l1==2
replace icd1 = "0" + icd1 if icd1!="" & l1==2
/*tab icd1 if icd1!="" & l1==3*/

drop l1 l2 icd2? icd1?

gen aicd1 = subinstr(icd1, ".", "", .)
gen aicd2 = subinstr(icd2, ".", "", .)

*create Charlson score based on the ICD9 code for HHC, inpatient DX codes
*use Enhanced ICD-9-CM and put index(e)

preserve
charlson , index(e) idvar(`mvar') wtchrl cmorb diagprfx(aicd)
tempfile charlson
save `charlson'
restore

preserve
elixhauser , index(e) idvar(`mvar') cmorb diagprfx(aicd) smelix
tempfile elix
save `elix'
restore

keep clientid `mvar' category socdate
duplicates drop
merge 1:1 `mvar' using `charlson', nogen
merge 1:1 `mvar' using `elix', nogen

* add data All DX categories
merge 1:1 clientid `mvar' socdate_e using "Rev_All DX Categories_v2.dta", keep(1 3) nogen

compress
save comorbidity, replace
