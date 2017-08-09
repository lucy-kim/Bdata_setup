*clean each individual data file from the client admissin DB4

local stdatapath /home/hcmg/kunhee/Labor/Bayada_data
set linesize 170
cd `stdatapath'
local mvar admissionclientsocid

*------------------------------------------
local file "DB4_M1010"
use "`file'", clear
rename date socdate
drop note
rename m1010 inpatdiag
lab var inpatdiag "Inpatient Diagnosis ICD-9CM for hosp w/i last 14 days; first of several codes"

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

drop if `mvar'==.
assert `mvar'!=.
save "`file'_v2", replace
*------------------------------------------
*local file "DB4_M1018" -- redundant with "DB3_M1000", so skip
*------------------------------------------
local file "DB4_M1030"
use "`file'", clear
rename v1 `mvar'
rename v2 clientid
rename v3 socdate

rename v4 therapy_enteral
rename v5 therapy_iv
rename v6 therapy_none
rename v7 therapy_parenteral

lab var therapy_enteral "M1030 Therapies client receives Enteral Nutrition"
lab var therapy_iv "M1030 Therapies client receives Intravenous, Infusion"
lab var therapy_none "M1030 Therapies client receives None Of The Above"
lab var therapy_parenteral "M1030 Therapies client receives Parenteral Nutrition"

drop if _n<3
destring therapy*, replace
foreach v of varlist therapy*{
  replace `v' = 0 if `v'==.
}

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

destring `mvar' clientid, replace
drop if `mvar'==.
assert `mvar'!=.
save "`file'_v2", replace
*------------------------------------------
local file "DB4_M1870 - 1880 - 1890"
use "`file'", clear
rename v1 `mvar'
rename v2 clientid
rename v3 socdate

rename v4 phone_ansdiffcall
rename v5 phone_anssome
rename v6 phone_able
rename v7 eat_indep
rename v8 eat_setup
rename v9 cook_indep
rename v10 eat_oral
rename v11 phone_spec
rename v12 phone_na
rename v13 phone_unable
rename v14 phone_listen
rename v15 eat_assist
rename v16 cook_unable
rename v17 cook_unablereg
rename v18 eat_tube
rename v19 eat_unable

lab var phone_ansdiffcall "M1890 Ability to Use Telephone Able To Answer The Telephone But Has Difficulty Placing Calls"
lab var phone_anssome "M1890 Ability to Use Telephone Able To Answer The Telephone Only Some Of The Time"
lab var phone_able "M1890 Ability to Use Telephone Able To Dial Numbers And Answere Calls Appropriately"
lab var eat_indep "M1870 Ability to feed self meals and snacks safely Able To Independently Feed Self"
lab var eat_setup "M1870 Ability to feed self meals and snacks safely Able to Independently Feed Self with Meal Setup or Intermittent Assistance or liquid diet"
lab var cook_indep "M1880 Ability to Plan and Prepare Light Meals  Able To Independently Plan And Prepare All Light Meals"
lab var eat_oral "M1870 Ability to feed self meals and snacks safely Able To Take In Nutrients Orally"
lab var phone_spec "M1890 Ability to Use Telephone Able To Use A Specially Adapted Telephone"
lab var phone_na "M1890 Ability to Use Telephone N/A - Patient Does Not Have A Telephone"
lab var phone_unable "M1890 Ability to Use Telephone Totally Unable To Use The Telephone"
lab var phone_listen "M1890 Ability to Use Telephone Unable To Answer The Telephone At All, But Can Listen If Assisted"
lab var eat_assist "M1870 Ability to feed self meals Unable To Feed Self And Must Be Assisted Or Supervised Throughout"
lab var cook_unable "M1880 Ability to Plan and Prepare Light Meals Unable To Prepare Any Light Meals"
lab var cook_unablereg "M1880 Ability to Plan and Prepare Light Meals  Unable To Prepare Light Meals On A Regular Basis"
lab var eat_tube "M1870 Ability to feed self meals and snacks safely Unable To Take In Nutrients Orally And Is Fed Through Tube Or Gastrostomy"
lab var eat_unable "M1870 Ability to feed self meals and snacks safely Unable To Take In Nutrients Orally Or By Tube Feeding" 

drop if _n<3
destring eat* cook* phone*, replace
foreach v of varlist eat* cook* phone* {
  replace `v' = 0 if `v'==.
}

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

destring `mvar' clientid, replace
drop if `mvar'==.
assert `mvar'!=.
save "`file'_v2", replace
*------------------------------------------
local file "DB4_M1900"
use "`file'", clear
rename v1 `mvar'
rename v2 clientid
rename v3 socdate
drop v7 v14 v21

assert (`mvar'==v15 & clientid==v16 & socdate==v17) & (`mvar'==v22 & clientid==v23 & socdate==v24)
assert (`mvar'==v8 & clientid==v9 & socdate==v10)
drop v15-v17 v22-v24 v8-v10

rename v4 priorADL_scdep
rename v5 priorADL_scindep
rename v6 priorADL_schelp
rename v11 priorADL_ambdep
rename v12 priorADL_ambindep
rename v13 priorADL_ambhelp
rename v18 priorADL_trnsfdep
rename v19 priorADL_trnsfindep
rename v20 priorADL_trnsfhelp
rename v25 priorADL_hhtdep
rename v26 priorADL_hhtindep
rename v27 priorADL_hhthelp

lab var priorADL_scdep "M1900a Prior Functioning ADL/IADL Self-Care Dependent"
lab var priorADL_scindep "M1900a Prior Functioning ADL/IADL Self-Care Independent"
lab var priorADL_schelp "M1900a Prior Functioning ADL/IADL Self-Care Needed Some Help"
lab var priorADL_ambdep "M1900a Prior Functioning ADL/IADL Ambulation Dependent"
lab var priorADL_ambindep "M1900a Prior Functioning ADL/IADL Ambulation Independent"
lab var priorADL_ambhelp "M1900a Prior Functioning ADL/IADL Ambulation Needed Some Help"
lab var priorADL_trnsfdep "M1900a Prior Functioning ADL/IADL Transfer Dependent"
lab var priorADL_trnsfindep "M1900a Prior Functioning ADL/IADL Transfer Independent"
lab var priorADL_trnsfhelp "M1900a Prior Functioning ADL/IADL Transfer Needed Some Help"
lab var priorADL_hhtdep "M1900a Prior Functioning ADL/IADL Household tasks Dependent"
lab var priorADL_hhtindep "M1900a Prior Functioning ADL/IADL Household tasks Independent"
lab var priorADL_hhthelp "M1900a Prior Functioning ADL/IADL Household tasks Needed Some Help"

drop if _n<3
destring priorADL*, replace
foreach v of varlist priorADL* {
  replace `v' = 0 if `v'==.
}

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

destring `mvar' clientid, replace
drop if `mvar'==.
assert `mvar'!=.
save "`file'_v2", replace
*------------------------------------------
local file "DB4_M1910"
use "`file'", clear
rename v1 `mvar'
rename v2 clientid
rename v3 socdate

rename v4 fallsriskassess_n
rename v5 fallsriskassess_ynr
rename v6 fallsriskassess_yr

lab var fallsriskassess_n "M1910 Had a multi-factor Falls Risk Assessment? No"
lab var fallsriskassess_ynr "M1910 Had a multi-factor Falls Risk Assessment? Yes, doesn't indicate a risk"
lab var fallsriskassess_yr "M1910 Had a multi-factor Falls Risk Assessment? Yes, does indicate a risk"

drop if _n<4
destring fallsriskassess*, replace
foreach v of varlist fallsriskassess* {
  replace `v' = 0 if `v'==.
}

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

destring `mvar' clientid, replace
drop if `mvar'==.
assert `mvar'!=.
save "`file'_v2", replace
*------------------------------------------
local file "DB4_M2020"
use "`file'", clear
rename v1 `mvar'
rename v2 clientid
rename v3 socdate

rename v4 oralmed_indep
rename v5 oralmed_prep
rename v6 oralmed_reminder
rename v7 oralmed_na
rename v8 oralmed_unable

lab var oralmed_indep "M2020 Management of Oral Meds Able To Independently Take The Correct Medication And Proper Dosage"
lab var oralmed_prep "M2020 Management of Oral Meds Able To Take Medication At Correct Times If Dosages Are Prepared"
lab var oralmed_reminder "M2020 Management of Oral Meds Able to Take Medication at Correct Times if given Reminders by Someone Else"
lab var oralmed_na "M2020 Management of Oral Meds N/A - No Oral Medications Prescribed"
lab var oralmed_unable "M2020 Management of Oral Meds Unable To Take Medication Unless Administered By Someone Else"

drop if _n<3
destring oralmed*, replace
foreach v of varlist oralmed* {
  replace `v' = 0 if `v'==.
}

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

destring `mvar' clientid, replace
drop if `mvar'==.
assert `mvar'!=.
save "`file'_v2", replace
*------------------------------------------
local file "DB4_M2030"
use "`file'", clear
rename v1 `mvar'
rename v2 clientid
rename v3 socdate

rename v4 injmed_indep
rename v5 injmed_prep
rename v6 injmed_reminder
rename v7 injmed_na
rename v8 injmed_unable

lab var injmed_indep "M2030 Management of Injectible Meds Able To Independently Take The Correct Medication And Proper Dosage"
lab var injmed_prep "M2030 Management of Injectible Meds Able To Take Medication At Correct Times If Dosages Are Prepared"
lab var injmed_reminder "M2030 Management of Injectible Meds Able to Take Medication at Correct Times if given Reminders by Someone Else"
lab var injmed_na "M2030 Management of Injectible Meds N/A - No Injectible Medications Prescribed"
lab var injmed_unable "M2030 Management of Injectible Meds Unable To Take Medication Unless Administered By Someone Else"

drop if _n<3
destring injmed*, replace
foreach v of varlist injmed* {
  replace `v' = 0 if `v'==.
}

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

destring `mvar' clientid, replace
drop if `mvar'==.
assert `mvar'!=.
save "`file'_v2", replace
*------------------------------------------ RESUME HERE
local file "DB4_M2040"
use "`file'", clear
rename v1 `mvar'
rename v2 clientid
rename v3 socdate

gen byte poralmed_dep = (v4=="1"|v12=="1")
gen byte poralmed_indep = (v5=="1"|v13=="1")
gen byte poralmed_na = (v6=="1"|v14=="1")
gen byte poralmed_help = (v7=="1"|v15=="1")
gen byte pinjmed_dep = (v8=="1"|v16=="1")
gen byte pinjmed_indep = (v9=="1"|v17=="1")
gen byte pinjmed_na = (v10=="1"|v18=="1")
gen byte pinjmed_help = (v11=="1"|v19=="1")

lab var poralmed_dep "C:M2040a - Prior Medication Management with Oral Medication Dependent"
lab var poralmed_indep "C:M2040a - Prior Medication Management with Oral Medication Independent"
lab var poralmed_na "C:M2040a - Prior Medication Management with Oral Medication NA - Not Applicable"
lab var poralmed_help "C:M2040a - Prior Medication Management with Oral Medication Needed Some Help"
lab var pinjmed_dep "C:M2040a - Prior Medication Management with Injectible Medication Dependent"
lab var pinjmed_indep "C:M2040a - Prior Medication Management with Injectible Medication Independent"
lab var pinjmed_na "C:M2040a - Prior Medication Management with Injectible Medication NA - Not Applicable"
lab var pinjmed_help "C:M2040a - Prior Medication Management with Injectible Medication Needed Some Help"

*recode var to missing value if corresponding var in C (C1) has no values in all categories
drop if _n==1|_n==2
destring v4-v19, replace
egen s = rowtotal(v4-v7)
egen p = rowtotal(v8-v11)
egen s1 = rowtotal(v12-v15)
egen p1 = rowtotal(v16-v19)

foreach v of varlist poralmed_* {
  replace `v' = . if (s==0 & s1==0)|s>1|s1>1
}
foreach v of varlist pinjmed_* {
  replace `v' = . if (p==0 & p1==0)|p>1|p1>1
}

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

keep `mvar' clientid socdate poralmed_* pinjmed_*
destring `mvar' clientid, replace
assert `mvar'!=.
save "`file'_v2", replace
*------------------------------------------
local file "DB4_M2100"
use "`file'", clear
rename v1 `mvar'
rename v2 clientid
rename v3 socdate

gen byte typassistADL_nav = v4=="1"
gen byte typassistADL_av = v5=="1"
gen byte typassistADL_tr = v6=="1"
gen byte typassistADL_un = v7=="1"|v9=="1"
gen byte typassistADL_na = v8=="1"

gen byte typassistIADL_nav = v10=="1"
gen byte typassistIADL_av = v11=="1"
gen byte typassistIADL_tr = v12=="1"
gen byte typassistIADL_un = v13=="1"|v15=="1"
gen byte typassistIADL_na = v14=="1"

gen byte typassistmed_nav = v16=="1"
gen byte typassistmed_av = v17=="1"
gen byte typassistmed_tr = v18=="1"
gen byte typassistmed_un = v19=="1"|v21=="1"
gen byte typassistmed_na = v20=="1"

gen byte typassistproc_nav = v22=="1"
gen byte typassistproc_av = v23=="1"
gen byte typassistproc_tr = v24=="1"
gen byte typassistproc_na = v26=="1"
gen byte typassistproc_un = v27=="1"|v25=="1"

gen byte typassistequ_nav = v28=="1"
gen byte typassistequ_av = v29=="1"
gen byte typassistequ_tr = v30=="1"
gen byte typassistequ_na = v32=="1"
gen byte typassistequ_un = v33=="1"|v31=="1"

gen byte typassistsafe_nav = v34=="1"
gen byte typassistsafe_av = v35=="1"
gen byte typassistsafe_tr = v36=="1"
gen byte typassistsafe_na = v38=="1"
gen byte typassistsafe_un = v39=="1"|v37=="1"

gen byte typassistadv_nav = v40=="1"
gen byte typassistadv_av = v41=="1"
gen byte typassistadv_tr = v42=="1"
gen byte typassistadv_na = v44=="1"
gen byte typassistadv_un = v45=="1"|v43=="1"

lab var typassistADL_nav "C:M2100a-Types of Assist-ADL Activities Assistance Needed, But No Caregiver(s) Available"
lab var typassistADL_av "C:M2100a-Types of Assist-ADL Activities CG Currently Provides Assistance" 
lab var typassistADL_tr "C:M2100a-Types of Assist-ADL Activities CG Need Training/ Supportive Services To Provide Assistance" 
lab var typassistADL_na "C:M2100a-Types of Assist-ADL Activities No Assistance Needed In This Area" 
lab var typassistADL_un "C:M2100a-Types of Assist-ADL Activities CG not likely to provide assistance OR it is unclear if they will provide assistance" 

lab var typassistIADL_nav "C:M2100b-Types of Assist-IIADL Activities Assistance Needed, But No CG Available"
lab var typassistIADL_av "C:M2100b-Types of Assist-IADL Activities CG Currently Provides Assistance" 
lab var typassistIADL_tr "C:M2100b-Types of Assist-IADL Activities CG Need Training/ Supportive Services To Provide Assistance" 
lab var typassistIADL_na "C:M2100b-Types of Assist-IADL Activities No Assistance Needed In This Area" 
lab var typassistIADL_un "C:M2100b-Types of Assist-IADL Activities CG not likely to provide assistance OR it is unclear if they will provide assistance" 

lab var typassistmed_nav "C:M2100c-Types of Assist-Medication Administration Assistance Needed, But No CG Available"
lab var typassistmed_av "C:M2100c-Types of Assist-Medication Administration CG Currently Provides Assistance" 
lab var typassistmed_tr "C:M2100c-Types of Assist-Medication Administration CG Need Training/ Supportive Services To Provide Assistance" 
lab var typassistmed_na "C:M2100c-Types of Assist-Medication Administration No Assistance Needed In This Area" 
lab var typassistmed_un "C:M2100c-Types of Assist-Medication Administration CG not likely to provide assistance OR it is unclear if they will provide assistance" 

lab var typassistproc_nav "C:M2100d-Types of Assist-Medical Procedures Assistance Needed, But No CG Available"
lab var typassistproc_av "C:M2100d-Types of Assist-Medical Procedures CG Currently Provides Assistance" 
lab var typassistproc_tr "C:M2100d-Types of Assist-Medical Procedures CG Need Training/ Supportive Services To Provide Assistance" 
lab var typassistproc_na "C:M2100d-Types of Assist-Medical Procedures No Assistance Needed In This Area" 
lab var typassistproc_un "C:M2100d-Types of Assist-Medical Procedures CG not likely to provide assistance OR it is unclear if they will provide assistance" 

lab var typassistequ_nav "C:M2100e-Types of Assist-Management of Equipment Assistance Needed, But No CG Available"
lab var typassistequ_av "C:M2100e-Types of Assist-Management of Equipment CG Currently Provides Assistance" 
lab var typassistequ_tr "C:M2100e-Types of Assist-Management of Equipment CG Need Training/ Supportive Services To Provide Assistance" 
lab var typassistequ_na "C:M2100e-Types of Assist-Management of Equipment No Assistance Needed In This Area" 
lab var typassistequ_un "C:M2100e-Types of Assist-Management of Equipment CG not likely to provide assistance OR it is unclear if they will provide assistance" 

lab var typassistsafe_nav "C:M2100f-Types of Assist-Supervision and Safety Assistance Needed, But No CG Available"
lab var typassistsafe_av "C:M2100f-Types of Assist-Supervision and Safety CG Currently Provides Assistance" 
lab var typassistsafe_tr "C:M2100f-Types of Assist-Supervision and Safety CG Need Training/ Supportive Services To Provide Assistance" 
lab var typassistsafe_na "C:M2100f-Types of Assist-Supervision and Safety No Assistance Needed In This Area" 
lab var typassistsafe_un "C:M2100f-Types of Assist-Supervision and Safety CG not likely to provide assistance OR it is unclear if they will provide assistance" 

lab var typassistadv_nav "C:M2100g-Types of Assist-Advocacy Assistance Needed, But No CG Available"
lab var typassistadv_av "C:M2100g-Types of Assist-Advocacy CG Currently Provides Assistance" 
lab var typassistadv_tr "C:M2100g-Types of Assist-Advocacy CG Need Training/ Supportive Services To Provide Assistance" 
lab var typassistadv_na "C:M2100g-Types of Assist-Advocacy No Assistance Needed In This Area" 
lab var typassistadv_un "C:M2100g-Types of Assist-Advocacy CG not likely to provide assistance OR it is unclear if they will provide assistance" 

*recode var to missing value if corresponding var in C (C1) has no values in all categories
drop if _n==1|_n==2
destring v4-v45, replace
egen s = rowtotal(v4-v9)
egen p = rowtotal(v10-v15)
egen s1 = rowtotal(v16-v21)
egen p1 = rowtotal(v22-v27)
egen p2 = rowtotal(v28-v33)
egen p3 = rowtotal(v34-v39)
egen p4 = rowtotal(v40-v45)

foreach v of varlist typassistADL_* {
  replace `v' = . if s==0|s>1
}
foreach v of varlist typassistIADL_* {
  replace `v' = . if p==0|p>1
}
foreach v of varlist typassistmed* {
  replace `v' = . if s1==0|s1>1
}
foreach v of varlist typassistproc* {
  replace `v' = . if p1==0|p1>1
}
foreach v of varlist typassistequ* {
  replace `v' = . if p2==0|p2>1
}
foreach v of varlist typassistsafe* {
  replace `v' = . if p3==0|p3>1
}
foreach v of varlist typassistadv* {
  replace `v' = . if p4==0|p4>1
}

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

keep `mvar' clientid socdate typassist*
destring `mvar' clientid, replace
assert `mvar'!=.
save "`file'_v2", replace
*------------------------------------------
local file "DB4_M2102"
use "`file'", clear
rename v1 `mvar'
rename v2 clientid
rename v3 socdate

gen byte typassistADL_nav = v4=="1"
gen byte typassistADL_na = v5=="1"
gen byte typassistADL_un = v6=="1"
gen byte typassistADL_av = v7=="1"
gen byte typassistADL_tr = v8=="1"

gen byte typassistIADL_nav = v9=="1"
gen byte typassistIADL_na = v10=="1"
gen byte typassistIADL_un = v11=="1"
gen byte typassistIADL_av = v12=="1"
gen byte typassistIADL_tr = v13=="1"

gen byte typassistmed_nav = v14=="1"
gen byte typassistmed_na = v15=="1"
gen byte typassistmed_un = v16=="1"
gen byte typassistmed_av = v17=="1"
gen byte typassistmed_tr = v18=="1"

gen byte typassistproc_nav = v19=="1"
gen byte typassistproc_na = v20=="1"
gen byte typassistproc_un = v21=="1"
gen byte typassistproc_av = v22=="1"
gen byte typassistproc_tr = v23=="1"

gen byte typassistequ_nav = v24=="1"
gen byte typassistequ_na = v25=="1"
gen byte typassistequ_un = v26=="1"
gen byte typassistequ_av = v27=="1"
gen byte typassistequ_tr = v28=="1"

gen byte typassistsafe_nav = v29=="1"
gen byte typassistsafe_na = v30=="1"
gen byte typassistsafe_un = v31=="1"
gen byte typassistsafe_av = v32=="1"
gen byte typassistsafe_tr = v33=="1"

gen byte typassistadv_nav = v34=="1"
gen byte typassistadv_na = v35=="1"
gen byte typassistadv_un = v36=="1"
gen byte typassistadv_av = v37=="1"
gen byte typassistadv_tr = v38=="1"

lab var typassistADL_nav "C1:M2102a-Types of Assist-ADL Activities Assistance Needed, But No CG Available"
lab var typassistADL_na "C1:M2102a-Types of Assist-ADL Activities CG No Assistance Needed In This Area" 
lab var typassistADL_un "C1:M2102a-Types of Assist-ADL Activities CG Not Likely To Provide Assistance OR it is unclear if they will provide assistance"
lab var typassistADL_av "C1:M2102a-Types of Assist-ADL Activities Currently Provides Assistance"  
lab var typassistADL_tr "C1:M2102a-Types of Assist-ADL Activities Need Training/ Supportive Services To Provide Assistance" 

lab var typassistIADL_nav "C1:M2102b-Types of Assist-IADL Activities Assistance Needed, But No CG Available"
lab var typassistIADL_na "C1:M2102b-Types of Assist-IADL Activities CG No Assistance Needed In This Area" 
lab var typassistIADL_un "C1:M2102b-Types of Assist-IADL Activities CG Not Likely To Provide Assistance OR it is unclear if they will provide assistance"
lab var typassistIADL_av "C1:M2102b-Types of Assist-IADL Activities Currently Provides Assistance"  
lab var typassistIADL_tr "C1:M2102b-Types of Assist-IADL Activities Need Training/ Supportive Services To Provide Assistance" 

lab var typassistmed_nav "C1:M2102c-Types of Assist-Medication Administration Assistance Needed, But No CG Available"
lab var typassistmed_na "C1:M2102c-Types of Assist-Medication Administration CG No Assistance Needed In This Area" 
lab var typassistmed_un "C1:M2102c-Types of Assist-Medication Administration CG Not Likely To Provide Assistance OR it is unclear if they will provide assistance"
lab var typassistmed_av "C1:M2102c-Types of Assist-Medication Administration Currently Provides Assistance"  
lab var typassistmed_tr "C1:M2102c-Types of Assist-Medication Administration Need Training/ Supportive Services To Provide Assistance" 

lab var typassistproc_nav "C1:M2102d-Types of Assist-Medical Procedures Assistance Needed, But No CG Available"
lab var typassistproc_na "C1:M2102d-Types of Assist-Medical Procedures CG No Assistance Needed In This Area" 
lab var typassistproc_un "C1:M2102d-Types of Assist-Medical Procedures CG Not Likely To Provide Assistance OR it is unclear if they will provide assistance"
lab var typassistproc_av "C1:M2102d-Types of Assist-Medical Procedures Currently Provides Assistance"  
lab var typassistproc_tr "C1:M2102d-Types of Assist-Medical Procedures Need Training/ Supportive Services To Provide Assistance" 

lab var typassistequ_nav "C1:M2102e-Types of Assist-Management of Equipment Assistance Needed, But No CG Available"
lab var typassistequ_na "C1:M2102e-Types of Assist-Management of Equipment CG No Assistance Needed In This Area" 
lab var typassistequ_un "C1:M2102e-Types of Assist-Management of Equipment CG Not Likely To Provide Assistance OR it is unclear if they will provide assistance"
lab var typassistequ_av "C1:M2102e-Types of Assist-Management of Equipment Currently Provides Assistance"  
lab var typassistequ_tr "C1:M2102e-Types of Assist-Management of Equipment Need Training/ Supportive Services To Provide Assistance" 

lab var typassistsafe_nav "C1:M2102f-Types of Assist-Supervision and Safety Assistance Needed, But No CG Available"
lab var typassistsafe_na "C1:M2102f-Types of Assist-Supervision and Safety CG No Assistance Needed In This Area" 
lab var typassistsafe_un "C1:M2102f-Types of Assist-Supervision and Safety CG Not Likely To Provide Assistance OR it is unclear if they will provide assistance"
lab var typassistsafe_av "C1:M2102f-Types of Assist-Supervision and Safety Currently Provides Assistance"  
lab var typassistsafe_tr "C1:M2102f-Types of Assist-Supervision and Safety Need Training/ Supportive Services To Provide Assistance" 

lab var typassistadv_nav "C1:M2102g-Types of Assist-Advocacy Assistance Needed, But No CG Available"
lab var typassistadv_na "C1:M2102g-Types of Assist-Advocacy CG No Assistance Needed In This Area" 
lab var typassistadv_un "C1:M2102g-Types of Assist-Advocacy CG Not Likely To Provide Assistance OR it is unclear if they will provide assistance"
lab var typassistadv_av "C1:M2102g-Types of Assist-Advocacy Currently Provides Assistance"  
lab var typassistadv_tr "C1:M2102g-Types of Assist-Advocacy Need Training/ Supportive Services To Provide Assistance" 

*recode var to missing value if corresponding var in C (C1) has no values in all categories
drop if _n==1|_n==2
destring v4-v38, replace
egen s = rowtotal(v4-v8)
egen p = rowtotal(v9-v13)
egen s1 = rowtotal(v14-v18)
egen p1 = rowtotal(v19-v23)
egen p2 = rowtotal(v24-v28)
egen p3 = rowtotal(v29-v33)
egen p4 = rowtotal(v34-v38)

foreach v of varlist typassistADL_* {
  replace `v' = . if s==0|s>1
}
foreach v of varlist typassistIADL_* {
  replace `v' = . if p==0|p>1
}
foreach v of varlist typassistmed* {
  replace `v' = . if s1==0|s1>1
}
foreach v of varlist typassistproc* {
  replace `v' = . if p1==0|p1>1
}
foreach v of varlist typassistequ* {
  replace `v' = . if p2==0|p2>1
}
foreach v of varlist typassistsafe* {
  replace `v' = . if p3==0|p3>1
}
foreach v of varlist typassistadv* {
  replace `v' = . if p4==0|p4>1
}

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

keep `mvar' clientid socdate typassist*
destring `mvar' clientid, replace
assert `mvar'!=.
save "`file'_v2", replace
*------------------------------------------
local file "DB4_M2110"
use "`file'", clear
rename v1 `mvar'
rename v2 clientid
rename v3 socdate

rename v4 oftenADLa_day
rename v5 oftenADLa_no
rename v6 oftenADLa_1to2
rename v7 oftenADLa_lt1
rename v8 oftenADLa_ge3
rename v9 oftenADLa_uk

lab var oftenADLa_day "How often client receive ADL/IADL assist? At Least Daily"
lab var oftenADLa_no "How often client receive ADL/IADL assist? No Assistance Received"
lab var oftenADLa_1to2 "How often client receive ADL/IADL assist? 1-2 times/week"
lab var oftenADLa_lt1 "How often client receive ADL/IADL assist? Received but less often than weekly"
lab var oftenADLa_ge3 "How often client receive ADL/IADL assist? >=3 / week"
lab var oftenADLa_uk "How often client receive ADL/IADL assist? UK- Unknown"

drop if _n<3
destring oftenADLa_*, replace
foreach v of varlist oftenADLa_* {
  replace `v' = 0 if `v'==.
}

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

destring `mvar' clientid, replace
drop if `mvar'==.
assert `mvar'!=.
save "`file'_v2", replace
*------------------------------------------
*Postpone this file for now. The data are presented in a wrong way.
/*local file "DB4_M2250"
use "`file'", clear
rename v1 `mvar'
rename v2 clientid
rename v3 socdate

rename v4 

lab var "Does physician-ordered plan of care include: "
destring `mvar' clientid, replace
drop if `mvar'==.
assert `mvar'!=.
save "`file'_v2", replace*/
*------------------------------------------
