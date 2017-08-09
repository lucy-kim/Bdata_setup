*clean each individual data file from the client admission DB

local stdatapath /home/hcmg/kunhee/Labor/Bayada_data
set linesize 200
cd `stdatapath'
local mvar admissionclientsocid

*first, work on Master DB data ------------------------------
local file "Rev_Master DB"
use "`file'", clear

*matching variable: admissionclientsocid
sort clientid `mvar'
sum clientid
*cilentid has 6-digit max & we know socdate converted should be at most 5 digit
tostring `mvar', gen(id) format(%11.0f)
gen double id2 = real(id)
assert id2==`mvar'
*`mvar' should be 11 digit long!
drop id2 id
format `mvar' %11.0f
list clientid `mvar' socdate in 1/20
order clientid `mvar'
assert `mvar'!=.

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

*make gender variable into a binary var
gen female = (gender=="FEMALE")
drop gender
label var female "Is a client female?"

*location type
tab locationtype
*error if locationtype=="INPATIENT HOSPITAL"|locationtype=="INPATIENT HOSPICE FACILITY"|locationtype=="LONG TERM CARE HOSPITAL"|locationtype=="psychiatric facility"
replace locationtype = "ASSISTED LIVING FACILITY" if regexm(locationtype, "NURSING LONG TERM CARE")|regexm(locationtype,"SKILLED NURSING FACILITY")

*payor type
tab payortype

*length of stay
rename averagelengthofstay los
destring los, replace ig(",")
save "`file'_v2", replace

* Hospitalization & Reason ------------------------------
local file "Rev_Hosp & Reason"
use "`file'", clear
assert `mvar'!=.

*convert SOC-ROC date to a date variable
split socrocdate, p("/")
replace socrocdate3 = "20"+socrocdate3
destring socrocdate?, replace float
gen socrocdate_e = mdy(socrocdate1, socrocdate2, socrocdate3)
format socrocdate_e %d
drop socrocdate? socrocdate
label var socrocdate_e "SOC/ROC date"

*convert hospitalization date to a date variable
split hospdate, p("/")
replace hospdate3 = "20"+hospdate3
destring hospdate?, replace float
gen hospdate_e = mdy(hospdate1, hospdate2, hospdate3)
format hospdate_e %d
drop hospdate? hospdate
label var hospdate_e "Hospitalization date"

save "`file'_v2", replace

*Race ------------------------------
local file "Rev_Race"
use "`file'", clear
drop if `mvar'==.
assert `mvar'!=.

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

save "`file'_v2", replace

*Quality incidents ------------------------------
local file"Rev_Quality Incidents"
use "`file'", clear
assert `mvar'!=.
capture drop if `mvar'==.
assert `mvar'!=.
drop v24

egen totQI = rowtotal(attendedfalls-usererrorrela)

save "`file'_v2", replace

*------------------------------
  local file "DB2_M1302 - 1306"
use "`file'", clear
rename v1 `mvar'
rename v2 clientid
rename v3 socdate
/* destring `mvar', gen(a) force */
/* list `mvar' if a>=. */

  gen byte risk_PU = (v5=="1" | v9=="1")
gen byte has_PU = (v7=="1" | v11=="1")

*recode var to missing value if corresponding var in C (C1) has no values in all categories
drop if _n==1|_n==2
destring v4-v11, replace
egen risk = rowtotal(v4 v5)
egen has = rowtotal(v6 v7)
egen riskc1 = rowtotal(v8 v9)
egen hasc1 = rowtotal(v10 v11)

replace risk_PU = . if (risk==0&riskc1==0)|risk>1|riskc1>1
replace has_PU = . if (has==0 & hasc1==0)|has>1|hasc1>1

keep `mvar' clientid socdate *_PU
label var risk_PU "C/C1:M1302 - Risk of Developing Pressure Ulcers Y/N"
label var has_PU "C/C1:M1306 - Patient Has At Least 1 Unhealed PU At Stage 2 Or Higher Y/N"

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

destring `mvar' clientid, replace
assert `mvar'!=.
save "`file'_v2", replace
*------------------------------
local file "DB2_M1242 - 1300"
use "`file'", clear
rename v1 `mvar'
rename v2 clientid
rename v3 socdate

gen byte freqpain_alltime = (v4=="1" | v12=="1")
gen byte freqpain_daily = (v5=="1" | v13=="1")
gen byte freqpain_ledaily = (v6=="1" | v14=="1")
gen byte freqpain_no = (v7=="1" | v15=="1")
gen byte freqpain_noaffect = (v8=="1" | v16=="1")

gen byte PUassess_no = (v9=="1" | v17=="1")
gen byte PUassess_yeval = (v10=="1" | v18=="1")
gen byte PUassess_ytool = (v11=="1" | v19=="1")

label var freqpain_alltime "C/C1:M1242 - Frequency of Pain: All Of The Time"
label var freqpain_daily "C/C1:M1242 - Frequency of Pain: Daily, But Not Constantly"
label var freqpain_ledaily "C/C1:M1242 - Frequency of Pain: Less Often Than Daily"
label var freqpain_no "C/C1:M1242 - Frequency of Pain:  No Pain"
label var freqpain_noaffect "C/C1:M1242 - Frequency of Pain:  Pain, but does not affect activity"
label var PUassess_no "C/C1:M1300 - Pressure Ulcer Assessment: No Assessment Conducted"
label var PUassess_yeval "C/C1:M1300 - Pressure Ulcer Assessment: Yes, Based on an Evaluation of Clinical Factors"
label var PUassess_ytool "C/C1:M1300 - Pressure Ulcer Assessment: Yes, Using a Standardized Tool"

*recode var to missing value if corresponding var in C (C1) has no values in all categories
drop if _n==1|_n==2
destring v4-v19, replace
egen freq = rowtotal(v4-v8)
egen PU = rowtotal(v9-v11)
egen freq1 = rowtotal(v12-v16)
egen PU1 = rowtotal(v17-v19)

foreach v of varlist freqpain_* {
  replace `v' = . if (freq==0 & freq1==0)
}
foreach v of varlist PUassess_* {
  replace `v' = . if (PU==0 & PU1==0)
}

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

keep `mvar' clientid socdate freqpain* PUassess*
destring `mvar' clientid, replace
assert `mvar'!=.
save "`file'_v2", replace
*------------------------------
local file "DB2_M1230 - 1240"
use "`file'", clear
rename v1 `mvar'
rename v2 clientid
rename v3 socdate

gen byte speechst_able = (v4=="1"|v13=="1")
gen byte speechst_minimal = (v5=="1"|v14=="1")
gen byte speechst_moderate = (v6=="1"|v15=="1")
gen byte speechst_nonresp = (v7=="1"|v16=="1")
gen byte speechst_severe = (v8=="1"|v17=="1")
gen byte speechst_unable = (v9=="1"|v18=="1")
gen byte painassess_no = (v10=="1"|v19=="1")
gen byte painassess_y= (v11=="1"|v20=="1")
gen byte painassess_ysevere = (v12=="1"|v21=="1")

lab var speechst_able "C/C1:M1230 - Speech Status Able To Express Complex Ideas"
lab var speechst_minimal "C/C1:M1230 - Speech Status Minimal Difficulty"
lab var speechst_moderate "C/C1:M1230 - Speech Status Moderate Difficulty"
lab var speechst_nonresp "C/C1:M1230 - Speech Status Nonresponsive Or Unable To Speak"
lab var speechst_severe "C/C1:M1230 - Speech Status Severe Difficulty"
lab var speechst_unable "C/C1:M1230 - Speech Status Unable To Express Basic Needs"
lab var painassess_no "C/C1:M1240 - Standardized Pain Assessment No Standardized Assessment Conducted"
lab var painassess_y "C/C1:M1240 - Standardized Pain Assessment Yes, And It Does Not Indicate Severe Pain"
lab var painassess_ysevere "C/C1:M1240 - Standardized Pain Assessment Yes, And It Indicates Severe Pain"

*recode var to missing value if corresponding var in C (C1) has no values in all categories
drop if _n==1|_n==2
destring v4-v21, replace
egen s = rowtotal(v4-v9)
egen p = rowtotal(v10-v12)
egen s1 = rowtotal(v13-v18)
egen p1 = rowtotal(v19-v21)

foreach v of varlist speechst_* {
  replace `v' = . if (s==0 & s1==0)
}
foreach v of varlist painassess_* {
  replace `v' = . if (p==0 & p1==0)
}

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

keep `mvar' clientid socdate speechst* painassess*
destring `mvar' clientid, replace
assert `mvar'!=.
save "`file'_v2", replace
*------------------------------
local file "DB2_M1210 - 1220"
use "`file'", clear
rename v1 `mvar'
rename v2 clientid
rename v3 socdate

gen byte hearst_adeq = (v4=="1"|v13=="1")
gen byte hearst_mild = (v5=="1"|v14=="1")
gen byte hearst_severe = (v6=="1"|v15=="1")
gen byte hearst_uk = (v7=="1"|v16=="1")
gen byte verbal_rare = (v8=="1"|v17=="1")
gen byte verbal_some = (v9=="1"|v18=="1")
gen byte verbal_uk = (v10=="1"|v9=="1")
gen byte verbal_y = (v11=="1"|v20=="1")
gen byte verbal_usually = (v12=="1"|v21=="1")

lab var hearst_adeq "C/C1:M1210 - Hearing Status Adequate Hearing"
lab var hearst_mild "C/C1:M1210 - Hearing Status Mildly to Moderately Impaired"
lab var hearst_severe "C/C1:M1210 - Hearing Status Severely Impaired"
lab var hearst_uk "C/C1:M1210 - Hearing Status UK - Unable to Assess"
lab var verbal_rare "C/C1:M1220 - Understanding of Verbal Content Rarely/Never Understands"
lab var verbal_some "C/C1:M1220 - Understanding of Verbal Content Sometimes Understands"
lab var verbal_uk "C/C1:M1220 - Understanding of Verbal Content UK - Unable to assess understanding"
lab var verbal_y "C/C1:M1220 - Understanding of Verbal Content Understands"
lab var verbal_usually "C/C1:M1220 - Understanding of Verbal Content Usually Understands"

*recode var to missing value if corresponding var in C (C1) has no values in all categories
drop if _n==1|_n==2
destring v4-v21, replace
egen s = rowtotal(v4-v7)
egen p = rowtotal(v8-v12)
egen s1 = rowtotal(v13-v16)
egen p1 = rowtotal(v17-v21)

foreach v of varlist hearst_* {
  replace `v' = . if (s==0 & s1==0)
}
foreach v of varlist verbal_* {
  replace `v' = . if (p==0 & p1==0)
}

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

keep `mvar' clientid socdate hearst* verbal*
destring `mvar' clientid, replace
assert `mvar'!=.
save "`file'_v2", replace
*------------------------------
local file "Rev_M1850 - 1860"
use "`file'", clear
rename v1 `mvar'
rename v2 clientid
rename v3 socdate

gen byte trnsf_able = (v4=="1"|v17=="1")
gen byte trnsf_minimal = (v5=="1"|v18=="1")
gen byte trnsf_bear = (v6=="1"|v19=="1")
gen byte trnsf_nobear = (v7=="1"|v20=="1")
gen byte trnsf_turn = (v8=="1"|v21=="1")
gen byte trnsf_noturn = (v9=="1"|v22=="1")
gen byte ambul_able = (v10=="1"|v23=="1")
gen byte ambul_assist = (v11=="1"|v24=="1")
gen byte ambul_bedfast = (v12=="1"|v25=="1")
gen byte ambul_chairfastnowh = (v13=="1"|v26=="1")
gen byte ambul_chairfastwh = (v14=="1"|v27=="1")
gen byte ambul_onedevice = (v15=="1"|v28=="1")
gen byte ambul_twodevice = (v16=="1"|v29=="1")

lab var trnsf_able "C:M1850 - Transferring Able To Independently Transfer"
lab var trnsf_minimal "C:M1850 - Transferring Transfers With Minimal Human Assistance"
lab var trnsf_bear "C:M1850 - Transferring Unable To Transfer Self But Able To Bear Weight"
lab var trnsf_nobear "C:M1850 - Transferring Unable To Transfer Self And Unable To Bear Weight"
lab var trnsf_turn "C:M1850 - Transferring Bedfast, Unable To Transfer But Able To Turn And Position Self"
lab var trnsf_noturn "C:M1850 - Transferring Bedfast, Unable To Transfer And Unable To Turn And Position Self"
lab var ambul_able "C:M1860 - Ambulation Able To Independently Walk On Even Or Uneven Surfaces"
lab var ambul_assist "C:M1860 - Ambulation Able To Walk Only With Supervision Or Assistance Of Another"
lab var ambul_bedfast "C:M1860 - Ambulation Bedfast, Unable To Ambulate Or Be Upright In Chair"
lab var ambul_chairfastnowh "C:M1860 - Ambulation Chairfast, Unable To Ambulate And Unable To Wheel Self"
lab var ambul_chairfastwh"C:M1860 - Ambulation Chairfast, Unable To Ambulate But Able To Wheel Self"
lab var ambul_onedevice "C:M1860 - Ambulation Requires Use of a one-handed Device to Walk Alone"
lab var ambul_twodevice "C:M1860 - Ambulation Requires Use of a two-handed Device to Walk Alone"

*recode var to missing value if corresponding var in C (C1) has no values in all categories
drop if _n==1|_n==2
destring v4-v29, replace
egen s = rowtotal(v4-v9)
egen p = rowtotal(v10-v16)
egen s1 = rowtotal(v17-v22)
egen p1 = rowtotal(v23-v29)

foreach v of varlist trnsf_* {
  replace `v' = . if (s==0 & s1==0)
}
foreach v of varlist ambul_* {
  replace `v' = . if (p==0 & p1==0)
}

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

keep `mvar' clientid socdate trnsf* ambul*
destring `mvar' clientid, replace
assert `mvar'!=.
save "`file'_v2", replace
*------------------------------
  local file "DB2_M1307 - 1320"
use "`file'", clear
rename v1 `mvar'
rename v2 clientid
rename v3 socdate

gen byte nes2PU_develop = (v4=="1")
gen byte nes2PU_na = (v5=="1")
gen byte nes2PU_waspresent = (v6=="1"|v12=="1")
gen byte PUst_partial = (v7=="1"|v13=="1")
gen byte PUst_fully = (v8=="1"|v14=="1")
gen byte PUst_na = (v9=="1"|v15=="1")
gen byte PUst_newly = (v10=="1"|v16=="1")
gen byte PUst_notheal = (v11=="1"|v17=="1")

lab var nes2PU_develop "C:M1307 - Non-epithelialized Stage II Pressure Ulcer Developed since the most recent SOC/ROC assessment (Not in C1)"
lab var nes2PU_na "C:M1307 - Non-epithelialized Stage II Pressure Ulcer NA -Not present at discharge (Not in C1)"
lab var nes2PU_waspresent "C:M1307 - Non-epithelialized Stage II Pressure Ulcer Was present at the most recent SOC/ROC assessment"
lab var PUst_partial "C/C1:M1320 - Status Of Most Problematic Pressure Ulcer Early/Partial Granulation"
lab var PUst_fully "C/C1:M1320 - Status Of Most Problematic Pressure Ulcer Fully Granulating"
lab var PUst_na "C/C1:M1320 - Status Of Most Problematic Pressure Ulcer N/A - No Observable Pressure Ulcer"
lab var PUst_newly "C/C1:M1320 - Status Of Most Problematic Pressure Ulcer Newly Epithelialized"
lab var PUst_notheal "C/C1:M1320 - Status Of Most Problematic Pressure Ulcer Not Healing"

*recode var to missing value if corresponding var in C (C1) has no values in all categories
drop if _n==1|_n==2
destring v4-v17, replace
egen s = rowtotal(v4-v6)
egen p = rowtotal(v7-v11)
egen s1 = rowtotal(v12)
egen p1 = rowtotal(v13-v17)

foreach v of varlist nes2PU_* {
  replace `v' = . if (s==0 & s1==0)
}
foreach v of varlist nes2PU_develop nes2PU_na {
  replace `v' = . if s==0 & (s1>0|p1>0)
}
foreach v of varlist PUst_* {
  replace `v' = . if (p==0 & p1==0)
}

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

keep `mvar' clientid socdate nes2PU* PUst*
destring `mvar' clientid, replace
assert `mvar'!=.
save "`file'_v2", replace
*------------------------------
local file "DB2_M1322 - 1324"
use "`file'", clear
rename v1 `mvar'
rename v2 clientid
rename v3 socdate

gen byte ns1PU_ge4 = (v4=="1"|v14=="1")
gen byte ns1PU_1 = (v5=="1"|v15=="1")
gen byte ns1PU_3 = (v6=="1"|v16=="1")
gen byte ns1PU_2 = (v7=="1"|v17=="1")
gen byte ns1PU_0 = (v8=="1"|v18=="1")
gen byte PUstg_na = (v9=="1"|v19=="1")
gen byte PUstg_1 = (v10=="1"|v20=="1")
gen byte PUstg_2 = (v11=="1"|v21=="1")
gen byte PUstg_3 = (v12=="1"|v22=="1")
gen byte PUstg_4 = (v13=="1"|v23=="1")

lab var ns1PU_ge4 "C/C1:M1322 - No. of Stage 1 Pressure Ulcers Four Or More"
lab var ns1PU_1 "C/C1:M1322 - No. of Stage 1 Pressure Ulcers One"
lab var ns1PU_2 "C/C1:M1322 - No. of Stage 1 Pressure Ulcers Two"
lab var ns1PU_3 "C/C1:M1322 - No. of Stage 1 Pressure Ulcers Three"
lab var ns1PU_0 "C/C1:M1322 - No. of Stage 1 Pressure Ulcers Zero"
lab var PUstg_na "C/C1:M1322 - C:M1324 - Stage of Most Problematic Pressure Ulcer N/A"
lab var PUstg_1 "C/C1:M1322 - C:M1324 - Stage of Most Problematic Pressure Ulcer Stage 1"
lab var PUstg_2 "C/C1:M1322 - C:M1324 - Stage of Most Problematic Pressure Ulcer Stage 2"
lab var PUstg_3 "C/C1:M1322 - C:M1324 - Stage of Most Problematic Pressure Ulcer Stage 3"
lab var PUstg_4 "C/C1:M1322 - C:M1324 - Stage of Most Problematic Pressure Ulcer Stage 4"

*recode var to missing value if corresponding var in C (C1) has no values in all categories
drop if _n==1|_n==2
destring v4-v23, replace
egen s = rowtotal(v4-v8)
egen p = rowtotal(v9-v13)
egen s1 = rowtotal(v14-v18)
egen p1 = rowtotal(v19-v23)

foreach v of varlist ns1PU_* {
  replace `v' = . if (s==0 & s1==0)
}
foreach v of varlist PUstg_* {
  replace `v' = . if (p==0 & p1==0)
}

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

keep `mvar' clientid socdate ns1PU* PUstg*
destring `mvar' clientid, replace
assert `mvar'!=.

save "`file'_v2", replace
*------------------------------
local file "DB2_M1330 - 1332"
use "`file'", clear
rename v1 `mvar'
rename v2 clientid
rename v3 socdate

gen byte hasSU_n = (v4=="1"|v12=="1")
gen byte hasSU_both = (v5=="1"|v13=="1")
gen byte hasSU_obs = (v6=="1"|v14=="1")
gen byte hasSU_unobs = (v7=="1"|v15=="1")
gen byte nSU_ge4 = (v8=="1"|v16=="1")
gen byte nSU_1 = (v9=="1"|v17=="1")
gen byte nSU_3 = (v10=="1"|v18=="1")
gen byte nSU_2 = (v11=="1"|v19=="1")

lab var hasSU_n "C/C1:M1330 - Does This Patient Have A Stasis Ulcer No"
lab var hasSU_both "C/C1:M1330 - Does This Patient Have A Stasis Ulcer Yes, Patient Has Both Observable And Unobservable Stasis Ulcers"
lab var hasSU_obs "C/C1:M1330 - Does This Patient Have A Stasis Ulcer Yes, Patient Has Observable Stasis Ulcers Only"
lab var hasSU_unobs "C/C1:M1330 - Does This Patient Have A Stasis Ulcer Yes, Patient Has Unobservable Stasis Ulcers Only (Known But Not Observable Due To Non-Removable Dressing)"
lab var nSU_ge4 " C/C1:M1332 - No. of Stasis Ulcer Four Or More"
lab var nSU_1 " C/C1:M1332 - No. of Stasis Ulcer One"
lab var nSU_3 " C/C1:M1332 - No. of Stasis Ulcer Three"
lab var nSU_2 " C/C1:M1332 - No. of Stasis Ulcer Two"

*recode var to missing value if corresponding var in C (C1) has no values in all categories
drop if _n==1|_n==2
destring v4-v19, replace
egen s = rowtotal(v4-v7)
egen p = rowtotal(v8-v11)
egen s1 = rowtotal(v12-v15)
egen p1 = rowtotal(v16-v19)

foreach v of varlist hasSU_* {
  replace `v' = . if (s==0 & s1==0)
}
foreach v of varlist nSU_* {
  replace `v' = . if (p==0 & p1==0) & hasSU_n==0
  assert `v'==0 if (p==0 & p1==0) & hasSU_n==1
}

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

keep `mvar' clientid socdate hasSU* nSU*
destring `mvar' clientid, replace
assert `mvar'!=.
save "`file'_v2", replace
*------------------------------
local file "DB2_M1334 - 1340"
use "`file'", clear
rename v1 `mvar'
rename v2 clientid
rename v3 socdate

gen byte SUst_partial = (v4=="1"|v11=="1")
gen byte SUst_fully = (v5=="1"|v12=="1")
gen byte SUst_newly = (v6=="1")
gen byte SUst_notheal = (v7=="1"|v13=="1")
gen byte hasSW_n =(v8=="1"|v14=="1")
gen byte hasSW_unobs =(v9=="1"|v15=="1")
gen byte hasSW_y =(v10=="1"|v16=="1")

lab var SUst_partial "C/C1:M1334 - Status Of Most Problematic Stasis Ulcer Early/Partial Granulation"
lab var SUst_fully "C/C1:M1334 - Status Of Most Problematic Stasis Ulcer Fully Granulating"
lab var SUst_newly "C:M1334 - Status Of Most Problematic Stasis Ulcer  Newly Epithelialized (Not in C1)"
lab var SUst_notheal "C/C1:M1334 - Status Of Most Problematic Stasis Ulcer Not Healing"
lab var hasSW_n " C/C1:M1340 - Does This Patient Have A Surgical Wound No"
lab var hasSW_unobs " C/C1:M1340 - Does This Patient Have A Surgical Wound Known Or Likely But Not Observable Due To Non-Removable Dressing"
lab var hasSW_y " C/C1:M1340 - Does This Patient Have A Surgical Wound Yes, Patient Has At Least One (Observable) Surgical Wound"

*recode var to missing value if corresponding var in C (C1) has no values in all categories
drop if _n==1|_n==2
destring v4-v16, replace
egen s = rowtotal(v4-v7)
egen p = rowtotal(v8-v10)
egen s1 = rowtotal(v11-v13)
egen p1 = rowtotal(v14-v16)

foreach v of varlist SUst_* {
  replace `v' = . if (s==0 & s1==0)
}
foreach v of varlist hasSW_* {
  replace `v' = . if (p==0 & p1==0)
}
foreach v of varlist SUst_newly {
  replace `v' = . if s==0 & (s1>0|p1>0)
}

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

keep `mvar' clientid socdate SUst* hasSW*
destring `mvar' clientid, replace
assert `mvar'!=.
save "`file'_v2", replace
*------------------------------
local file "DB2_M1342 - 1350"
use "`file'", clear
rename v1 `mvar'
rename v2 clientid
rename v3 socdate

gen byte SWst_partial = (v4=="1"|v10=="1")
gen byte SWst_fully = (v5=="1"|v11=="1")
gen byte SWst_newly = (v6=="1"|v12=="1")
gen byte SWst_notheal = (v7=="1"|v13=="1")
gen byte skinlesion_n = (v8=="1"|v14=="1")
gen byte skinlesion_y = (v9=="1"|v15=="1")

lab var SWst_partial "C/C1:M1342 - Status Of Most Problematic Surgical Wound Early/Partial Granulation"
lab var SWst_fully "C/C1:M1342 - Status Of Most Problematic Surgical Wound Fully Granulating"
lab var SWst_newly "C/C1:M1342 - Status Of Most Problematic Surgical Wound Newly Epithelialized"
lab var SWst_notheal "C/C1:M1342 - Status Of Most Problematic Surgical Wound Not Healing"
lab var skinlesion_n "C/C1:M1350 - Skin Lesion Or Open Wound No"
lab var skinlesion_y "C/C1:M1350 - Skin Lesion Or Open Wound Yes"

*recode var to missing value if corresponding var in C (C1) has no values in all categories
drop if _n==1|_n==2
destring v4-v15, replace
egen s = rowtotal(v4-v7)
egen p = rowtotal(v8-v9)
egen s1 = rowtotal(v10-v13)
egen p1 = rowtotal(v14-v15)

foreach v of varlist SWst_* {
  replace `v' = . if (s==0 & s1==0)
}
foreach v of varlist skinlesion_* {
  replace `v' = . if (p==0 & p1==0)
}

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

keep `mvar' clientid socdate SWst* skinlesion*
destring `mvar' clientid, replace
assert `mvar'!=.
save "`file'_v2", replace
*------------------------------
local file "DB2_M1400 - 1410"
use "`file'", clear
rename v1 `mvar'
rename v2 clientid
rename v3 socdate

gen byte whendyspneic_rest = (v4=="1"|v13=="1")
gen byte whendyspneic_n = (v5=="1"|v14=="1")
gen byte whendyspneic_walk = (v6=="1"|v15=="1")
gen byte whendyspneic_minimal = (v7=="1"|v16=="1")
gen byte whendyspneic_moderate = (v8=="1"|v17=="1")
gen byte respira_airway = (v9=="1"|v18=="1")
gen byte respira_none = (v10=="1"|v19=="1")
gen byte respira_oxygen = (v11=="1"|v20=="1")
gen byte respira_ventil = (v12=="1"|v21=="1")

lab var whendyspneic_rest "C:M1400 - When Dyspneic At Rest"
lab var whendyspneic_n "C:M1400 - When Dyspneic Patient Is Not Short Of Breath"
lab var whendyspneic_walk "C:M1400 - When Dyspneic When Walking More Than 20 Feet"
lab var whendyspneic_minimal "C:M1400 - When Dyspneic With Minimal Exertion"
lab var whendyspneic_moderate "C:M1400 - When Dyspneic With Moderate Exertion"
lab var respira_airway "C:M1410 - Respiratory Treatments at Home Airway Pressure"
lab var respira_none "C:M1410 - Respiratory Treatments at Home None Of The Above"
lab var respira_oxygen "C:M1410 - Respiratory Treatments at Home Oxygen"
lab var respira_ventil "C:M1410 - Respiratory Treatments at Home Ventilator"

*recode var to missing value if corresponding var in C (C1) has no values in all categories
drop if _n==1|_n==2
destring v4-v21, replace
egen s = rowtotal(v4-v8)
egen p = rowtotal(v9-v12)
egen s1 = rowtotal(v13-v17)
egen p1 = rowtotal(v18-v21)

foreach v of varlist whendyspneic_* {
  replace `v' = . if (s==0 & s1==0)
}
foreach v of varlist respira_* {
  replace `v' = . if (p==0 & p1==0)
}

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

keep `mvar' clientid socdate whendyspneic* respira*
destring `mvar' clientid, replace
assert `mvar'!=.

save "`file'_v2", replace
*------------------------------
local file "DB2_M1500 - 1510"
use "`file'", clear
rename v1 `mvar'
rename v2 clientid
rename v3 socdate
drop v24

gen byte HFsympt_na = (v4=="1"|v14=="1")
gen byte HFsympt_n = (v5=="1"|v15=="1")
gen byte HFsympt_notassess = (v6=="1"|v16=="1")
gen byte HFsympt_y = (v7=="1"|v17=="1")
gen byte HFfu_er = (v8=="1"|v18=="1")
gen byte HFfu_na = (v9=="1"|v19=="1")
gen byte HFfu_edu = (v10=="1"|v20=="1")
gen byte HFfu_phys = (v11=="1"|v21=="1")
gen byte HFfu_treat = (v12=="1"|v22=="1")
gen byte HFfu_chng = (v13=="1"|v23=="1")

lab var HFsympt_na "C:M1500 - Symptoms In Heart Failure Patients  NA - Patient does not have diagnosis of heart failure"
lab var HFsympt_n "C:M1500 - Symptoms In Heart Failure Patients No"
lab var HFsympt_notassess "C:M1500 - Symptoms In Heart Failure Patients Not Assessed"
lab var HFsympt_y "C:M1500 - Symptoms In Heart Failure Patients Yes"
lab var HFfu_er "C:M1510 - Heart Failure Follow Up ER Treatment Advised"
lab var HFfu_na "C:M1510 - Heart Failure Follow Up No Action"
lab var HFfu_edu "C:M1510 - Heart Failure Follow Up Patient Education Or Other Clinical Interventions"
lab var HFfu_phys "C:M1510 - Heart Failure Follow Up Physician Contacted"
lab var HFfu_treat "C:M1510 - Heart Failure Follow Up Physician-Ordered Treatment Implemented"
lab var HFfu_chng "C:M1510 - Heart Failure Follow Up Change In Care Plan Orders Obtained"

*recode var to missing value if corresponding var in C (C1) has no values in all categories
drop if _n==1|_n==2
destring v4-v23, replace
egen s = rowtotal(v4-v7)
egen p = rowtotal(v8-v13)
egen s1 = rowtotal(v14-v17)
egen p1 = rowtotal(v18-v23)

foreach v of varlist HFsympt_* {
  replace `v' = . if (s==0 & s1==0)
}
foreach v of varlist HFfu_* {
  replace `v' = . if (p==0 & p1==0)
}

*recode some people with both HFsympt_na==1 & HFsympt_y==1 to HFsympt_na==0 & HFsympt_y==1 if they have a HF follow up
replace HFsympt_na=0 if HFsympt_na==1 & HFsympt_y==1 & (p>0|p1>0)

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

keep `mvar' clientid socdate HFsympt* HFfu*
destring `mvar' clientid, replace
assert `mvar'!=.

save "`file'_v2", replace
*------------------------------
 local file "DB2_M1600 - 1610"
use "`file'", clear
rename v1 `mvar'
rename v2 clientid
rename v3 socdate

gen byte treaturin_na = (v4=="1"|v11=="1")
gen byte treaturin_n = (v5=="1"|v12=="1")
gen byte treaturin_uk = (v6=="1"|v13=="1")
gen byte treaturin_y = (v7=="1"|v14=="1")
gen byte incontinence_n = (v8=="1"|v15=="1")
gen byte incontinence_y = (v9=="1"|v16=="1")
gen byte incontinence_cath = (v10=="1"|v17=="1")

lab var treaturin_na "C:M1600 - Treated for Urinary Tract Infection in Past 14 Days  N/A - Patient On Prophylactic Treatment"
lab var treaturin_n "C:M1600 - Treated for Urinary Tract Infection in Past 14 Days No"
lab var treaturin_uk "C:M1600 - Treated for Urinary Tract Infection in Past 14 Days Unknown"
lab var treaturin_y "C:M1600 - Treated for Urinary Tract Infection in Past 14 Days Yes"
lab var incontinence_n "C:M1610 - Urinary Incontinence or Urinary Catheter Present No Incontinence Or Catheter"
lab var incontinence_y "C:M1610 - Urinary Incontinence or Urinary Catheter Present Patient Is Incontinent"
lab var incontinence_cath "C:M1610 - Urinary Incontinence or Urinary Catheter Present Patient Requires Catheter"

*recode var to missing value if corresponding var in C (C1) has no values in all categories
drop if _n==1|_n==2
destring v4-v17, replace
egen s = rowtotal(v4-v7)
egen p = rowtotal(v8-v10)
egen s1 = rowtotal(v11-v14)
egen p1 = rowtotal(v15-v17)

foreach v of varlist treaturin_* {
  replace `v' = . if (s==0 & s1==0)
}
foreach v of varlist incontinence_* {
  replace `v' = . if (p==0 & p1==0)
}

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

keep `mvar' clientid socdate treaturin* incontinence*
destring `mvar' clientid, replace
assert `mvar'!=.

save "`file'_v2", replace
*------------------------------
 local file "DB2_M1615 - 1700"
use "`file'", clear
rename v1 `mvar'
rename v2 clientid
rename v3 socdate

gen byte whenincontin_dn = (v4=="1"|v14=="1")
gen byte whenincontin_d = (v5=="1"|v15=="1")
gen byte whenincontin_n = (v6=="1"|v16=="1")
gen byte whenincontin_str = (v7=="1"|v17=="1")
gen byte whenincontin_def = (v8=="1"|v18=="1")
gen byte cogni_alert = (v9=="1"|v19=="1")
gen byte cogni_assist = (v10=="1"|v20=="1")
gen byte cogni_lotsassist = (v11=="1"|v21=="1")
gen byte cogni_prompt = (v12=="1"|v22=="1")
gen byte cogni_depend = (v13=="1"|v23=="1")

lab var whenincontin_dn "C:M1615 - When Urinary Incontinence Occurs During The Day And Night"
lab var whenincontin_d "C:M1615 - When Urinary Incontinence Occurs During The Day Only"
lab var whenincontin_n "C:M1615 - When Urinary Incontinence Occurs During The Night Only"
lab var whenincontin_str "C:M1615 - When Urinary Incontinence Occurs Occasional Stress Incontinence"
lab var whenincontin_def "C:M1615 - When Urinary Incontinence Occurs Timed-Voiding Defers Incontinence"
lab var cogni_alert "C:M1700 - Cognitive Functioning Alert/Oriented"
lab var cogni_assist "C:M1700 - Cognitive Functioning Requires Assistance In Specific Situations"
lab var cogni_lotsassist "C:M1700 - Cognitive Functioning Requires Considerable Assistance In Routine Situations"
lab var cogni_prompt "C:M1700 - Cognitive Functioning Requires Prompting"
lab var cogni_depend "C:M1700 - Cognitive Functioning Totally Dependent"

*recode var to missing value if corresponding var in C (C1) has no values in all categories
drop if _n==1|_n==2
destring v4-v23, replace
egen s = rowtotal(v4-v8)
egen p = rowtotal(v9-v13)
egen s1 = rowtotal(v14-v18)
egen p1 = rowtotal(v19-v23)

foreach v of varlist whenincontin_* {
  replace `v' = . if (s==0 & s1==0)
}
foreach v of varlist cogni_* {
  replace `v' = . if (p==0 & p1==0)
}

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

*since `mvar' has missing values, create them by concatenating clientid and socdate
gen new = socdate_e +21916
tostring new, replace
replace `mvar' = clientid + new

keep `mvar' clientid socdate whenincontin* cogni*
destring `mvar' clientid, replace
assert `mvar'!=.

save "`file'_v2", replace
*------------------------------
local file "DB2_M1620 - 1630"
use "`file'", clear
rename v1 `mvar'
rename v2 clientid
rename v3 socdate

gen byte freqbowel_4to6 = (v4=="1"|v15=="1")
gen byte freqbowel_le1 = (v5=="1"|v16=="1")
gen byte freqbowel_gt1 = (v6=="1"|v17=="1")
gen byte freqbowel_na = (v7=="1"|v18=="1")
gen byte freqbowel_day = (v8=="1"|v19=="1")
gen byte freqbowel_1to3 = (v9=="1"|v20=="1")
gen byte freqbowel_uk = (v10=="1"|v21=="1")
gen byte freqbowel_rare = (v11=="1"|v22=="1")
gen byte ostomy_nr = (v12=="1"|v23=="1")
gen byte ostomy_r = (v13=="1"|v24=="1")
gen byte ostomy_n = (v14=="1"|v25=="1")

lab var freqbowel_4to6 "C:M1620 - Bowel Incontinence Frequency Four To Six Times Weekly"
lab var freqbowel_le1 "C:M1620 - Bowel Incontinence Frequency Less Than Once Weekly"
lab var freqbowel_gt1 "C:M1620 - Bowel Incontinence Frequency More Often Than Once Daily"
lab var freqbowel_na "C:M1620 - Bowel Incontinence Frequency N/A - Patient Has Ostomy For Bowel Elimination"
lab var freqbowel_day "C:M1620 - Bowel Incontinence Frequency On A Daily Basis"
lab var freqbowel_1to3 "C:M1620 - Bowel Incontinence Frequency One To Three Times Weekly"
lab var freqbowel_uk "C:M1620 - Bowel Incontinence Frequency Unknown"
lab var freqbowel_rare "C:M1620 - Bowel Incontinence Frequency Very Rarely Or Never Has Bowel Incontinence"
lab var ostomy_nr "C:M1630 - Ostomy for Bowel Elimination Ostomy Not Related To Inpatient Stay Or Change In Treatment"
lab var ostomy_r "C:M1630 - Ostomy for Bowel Elimination Ostomy Related To Inpatient Stay Of Change In Treatment"
lab var ostomy_n "C:M1630 - Ostomy for Bowel Elimination Patient Does Not Have An Ostomy For Bowel Elimination"

*recode var to missing value if corresponding var in C (C1) has no values in all categories
drop if _n==1|_n==2
destring v4-v25, replace
egen s = rowtotal(v4-v11)
egen p = rowtotal(v12-v14)
egen s1 = rowtotal(v15-v22)
egen p1 = rowtotal(v23-v25)

foreach v of varlist freqbowel_* {
  replace `v' = . if (s==0 & s1==0)
}
foreach v of varlist ostomy_* {
  replace `v' = . if (p==0 & p1==0)
}

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

keep `mvar' clientid socdate freqbowel* ostomy*
destring `mvar' clientid, replace
assert `mvar'!=.
save "`file'_v2", replace
*------------------------------
local file "DB2_M1710 - 1720"
use "`file'", clear
rename v1 `mvar'
rename v2 clientid
rename v3 socdate

gen byte whenconfuse_const = (v4=="1"|v15=="1")
gen byte whenconfuse_ampm = (v5=="1"|v16=="1")
gen byte whenconfuse_new = (v6=="1"|v17=="1")
gen byte whenconfuse_na = (v7=="1"|v18=="1")
gen byte whenconfuse_never = (v8=="1"|v19=="1")
gen byte whenconfuse_wake = (v9=="1"|v20=="1")

gen byte whenanxious_all = (v10=="1"|v21=="1")
gen byte whenanxious_day = (v11=="1"|v22=="1")
gen byte whenanxious_ltday = (v12=="1"|v23=="1")
gen byte whenanxious_na = (v13=="1"|v24=="1")
gen byte whenanxious_none = (v14=="1"|v25=="1")

lab var whenconfuse_const "C:M1710 - When Confused Constantly"
lab var whenconfuse_ampm "C:M1710 - When Confused During The Day And Evening, But Not Constantly"
lab var whenconfuse_new "C:M1710 - When Confused In New Or Complex Situations Only"
lab var whenconfuse_na "C:M1710 - When Confused N/A - Patient Nonresponsive"
lab var whenconfuse_never "C:M1710 - When Confused Never"
lab var whenconfuse_wake "C:M1710 - When Confused On Awakening Or At Night Only"
lab var whenanxious_all "C:M1720 - When Anxious All Of The Time"
lab var whenanxious_day "C:M1720 - When Anxious Daily, But Not Constantly"
lab var whenanxious_ltday "C:M1720 - When Anxious Less Often Than Daily"
lab var whenanxious_na "C:M1720 - When Anxious N/A - Patient Nonresponsive"
lab var whenanxious_none "C:M1720 - When Anxious None Of The Time"

*recode var to missing value if corresponding var in C (C1) has no values in all categories
drop if _n==1|_n==2
destring v4-v25, replace
egen s = rowtotal(v4-v9)
egen p = rowtotal(v10-v14)
egen s1 = rowtotal(v15-v20)
egen p1 = rowtotal(v21-v25)

foreach v of varlist whenconfuse_* {
  replace `v' = . if (s==0 & s1==0)
}
foreach v of varlist whenanxious_* {
  replace `v' = . if (p==0 & p1==0)
}

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

keep `mvar' clientid socdate whenconfuse* whenanxious*
destring `mvar' clientid, replace
assert `mvar'!=.
save "`file'_v2", replace
*------------------------------
local file "DB3_M1000"
use "`file'", clear
rename v1 `mvar'
rename v2 clientid
rename v3 socdate

gen byte inpatdc_rehab = (v4=="1"|v21=="1")
gen byte inpatdc_ltchosp = (v5=="1"|v22=="1")
gen byte inpatdc_ltnursing = (v6=="1"|v23=="1")
gen byte inpatdc_na = (v7=="1"|v24=="1")
gen byte inpatdc_oth = (v8=="1"|v25=="1")
gen byte inpatdc_psych = (v9=="1"|v26=="1")
gen byte inpatdc_acutehosp = (v10=="1"|v27=="1")
gen byte inpatdc_snf = (v11=="1"|v28=="1")
gen byte priorcond_disrupt = (v12=="1"|v29=="1")
gen byte priorcond_impdm = (v13=="1"|v30=="1")
gen byte priorcond_cath = (v14=="1"|v31=="1")
gen byte priorcond_pain = (v15=="1"|v32=="1")
gen byte priorcond_memloss = (v16=="1"|v33=="1")
gen byte priorcond_ndc = (v17=="1"|v34=="1")
gen byte priorcond_none = (v18=="1"|v35=="1")
gen byte priorcond_uk = (v19=="1"|v36=="1")
gen byte priorcond_incontn = (v20=="1"|v37=="1")

lab var inpatdc_rehab "C:M1000 - Inpatient Facility DC Last 14 Days Inpatient Rehabilitation Hospital Or Unit"
lab var inpatdc_ltchosp "C:M1000 - Inpatient Facility DC Last 14 Days Long-Term Care Hospital"
lab var inpatdc_ltnursing "C:M1000 - Inpatient Facility DC Last 14 Days Long-Term Nursing Facility"
lab var inpatdc_na "C:M1000 - Inpatient Facility DC Last 14 Days Na - Patient Was Not Discharged From An Inpatient Facility"
lab var inpatdc_oth "C:M1000 - Inpatient Facility DC Last 14 Days Other"
lab var inpatdc_psych "C:M1000 - Inpatient Facility DC Last 14 Days Psychiatric Hospital Or Unit"
lab var inpatdc_acutehosp "C:M1000 - Inpatient Facility DC Last 14 Days Short-Stay Acute Hospital"
lab var inpatdc_snf "C:M1000 - Inpatient Facility DC Last 14 Days Skilled Nursing Facility"
lab var priorcond_disrupt "C:M1018 - Prior Condition Disruptive Or Socially Inappropriate Behavior"
lab var priorcond_impdm "C:M1018 - Prior Condition Impaired Decision-Making"
lab var priorcond_cath "C:M1018 - Prior Condition Indwelling/Suprapubic Catheter"
lab var priorcond_pain "C:M1018 - Prior Condition Intractable Pain"
lab var priorcond_memloss "C:M1018 - Prior Condition Memory Loss, Supervision Required"
lab var priorcond_ndc "C:M1018 - Prior Condition No Inpatient Discharge, No Change In Regimen In Past 14 Days"
lab var priorcond_none "C:M1018 - Prior Condition None Of The Above"
lab var priorcond_uk "C:M1018 - Prior Condition Unknown"
lab var priorcond_incontn "C:M1018 - Prior Condition Urinary Incontinence"

*recode var to missing value if corresponding var in C (C1) has no values in all categories
drop if _n==1|_n==2
destring v4-v37, replace
egen s = rowtotal(v4-v11)
egen p = rowtotal(v12-v20)
egen s1 = rowtotal(v21-v28)
egen p1 = rowtotal(v29-v37)

foreach v of varlist inpatdc_* {
  replace `v' = . if (s==0 & s1==0)
}
foreach v of varlist priorcond_* {
  replace `v' = . if (p==0 & p1==0)
}

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

keep `mvar' clientid socdate inpatdc* priorcond*
destring `mvar' clientid, replace
assert `mvar'!=.
save "`file'_v2", replace
*------------------------------
local file "DB3_M1032 - 1033 - 1034"
use "`file'", clear
rename v1 `mvar'
rename v2 clientid
rename v3 socdate

gen byte riskhosp_frail = (v4=="1")
gen byte riskhosp_fall = (v5=="1"|v19=="1")
gen byte riskhosp_manyhosp = (v6=="1"|v21=="1")
gen byte riskhosp_none = (v7=="1"|v22=="1")
gen byte riskhosp_oth = (v8=="1"|v23=="1")
gen byte riskhosp_mental = (v9=="1"|v18=="1")
gen byte riskhosp_ge5med = (v10=="1"|v17=="1")
gen byte riskhosp_exhaust = (v16=="1")
gen byte riskhosp_manyer = (v20=="1")
gen byte riskhosp_diff = (v24=="1")
gen byte riskhosp_weightloss = (v25=="1")

gen byte overallst_bad = (v11=="1"|v26=="1")
gen byte overallst_vbad = (v12=="1"|v27=="1")
gen byte overallst_stable = (v13=="1"|v28=="1")
gen byte overallst_tempbad = (v14=="1"|v29=="1")
gen byte overallst_uk = (v15=="1"|v30=="1")

lab var riskhosp_frail "C:M1032 - Risk For Hospitalization Frailty Indicators"
lab var riskhosp_fall "C:M1032/C1:M1033 - Risk For Hospitalization History of Falls (2 or more in past 12 months)"
lab var riskhosp_manyhosp "C:M1032/C1:M1033 - Risk For Hospitalization Multiple Hospitalizations (2 or more in past 12 (6) months in C (C1))"
lab var riskhosp_none "C:M1032/C1:M1033 - Risk For Hospitalization None Of The Above"
lab var riskhosp_oth "C:M1032/C1:M1033 - Risk For Hospitalization Other"
lab var riskhosp_mental "C:M1032/C1:M1033 - Risk For Hospitalization Recent Decline In Mental, Emotional Or Behavior Status"
lab var riskhosp_ge5med "C:M1032/C1:M1033 - Risk For Hospitalization Taking Five Or More Medications"
lab var riskhosp_exhaust "C1:M1033 - Risk for Hospitalization Currently reports exhaustion (Only C1)"
lab var riskhosp_manyer "C1:M1033 - Risk for Hospitalization Multiple emergency department visits (2 or more) in the past 6 months"
lab var riskhosp_diff "C1:M1033 - Risk for Hospitalization Reported or observed history of difficulty complying with any medical instructions in the past 3 months"
lab var riskhosp_weightloss "C1:M1033 - Risk for Hospitalization Unintentional weight loss of a total of 10 pounds or more in the past 12 months"

lab var overallst_bad "C/C1:M1034 - Overall Status Likely to remain in Fragile Health and have Ongoing High Risks of Serious Complications and Death"
lab var overallst_vbad "C/C1:M1034 - Overall Status Serious Progressive Conditions that Could Lead to Death within a Year"
lab var overallst_stable "C:M1034 - Overall Status Stable with No Heightened Risk"
lab var overallst_tempbad "C:M1034 - Overall Status Temporarily Facing High Health Risks but is Likely to Return to Being Stable"
lab var overallst_uk "C:M1034 - Overall Status UK - The Patient?s Situation is Unknown or Unclear"

*recode var to missing value if corresponding var in C (C1) has no values in all categories
drop if _n==1|_n==2
destring v4-v30, replace
egen s = rowtotal(v4-v10)
egen p = rowtotal(v11-v15)
egen s1 = rowtotal(v16-v25)
egen p1 = rowtotal(v26-v30)

foreach v of varlist riskhosp_* {
  replace `v' = . if (s==0 & s1==0)
}
foreach v of varlist overallst_* {
  replace `v' = . if (p==0 & p1==0)
}

foreach v of varlist riskhosp_frail {
  replace `v' = . if (s1>0|p1>0)
}
foreach v of varlist riskhosp_exhaust riskhosp_manyer riskhosp_diff riskhosp_weightloss {
  replace `v' = . if substr(socdate,-2,.)!="15"
}

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

keep `mvar' clientid socdate riskhosp* overallst*
destring `mvar' clientid, replace
assert `mvar'!=.
save "`file'_v2", replace
*------------------------------
local file "DB3_M1036 - 1200"
use "`file'", clear
rename v1 `mvar'
rename v2 clientid
rename v3 socdate

gen byte hrfactor_alcohol = (v4=="1"|v13=="1")
gen byte hrfactor_drug = (v5=="1"|v14=="1")
gen byte hrfactor_smoke = (v6=="1"|v15=="1")
gen byte hrfactor_none = (v7=="1"|v16=="1")
gen byte hrfactor_obese = (v8=="1"|v17=="1")
gen byte hrfactor_uk = (v9=="1"|v18=="1")
gen byte vision_norm = (v10=="1"|v19=="1")
gen byte vision_partial = (v11=="1"|v20=="1")
gen byte vision_severe = (v12=="1"|v21=="1")

lab var hrfactor_alcohol "C:M1036 - High Risk Factor Alcohol Dependency"
lab var hrfactor_drug "C:M1036 - High Risk Factor Drug Dependency"
lab var hrfactor_smoke "C:M1036 - High Risk Factor Heavy Smoking"
lab var hrfactor_none "C:M1036 - High Risk Factor None Of The Above"
lab var hrfactor_obese "C:M1036 - High Risk Factor Obesity"
lab var hrfactor_uk "C:M1036 - High Risk Factor Unknown"
lab var vision_norm " C:M1200 - Vision Status Normal Vision"
lab var vision_partial " C:M1200 - Vision Status Partially Impaired"
lab var vision_severe " C:M1200 - Vision Status Severely Impaired"

*recode var to missing value if corresponding var in C (C1) has no values in all categories
drop if _n==1|_n==2
destring v4-v21, replace
egen s = rowtotal(v4-v9)
egen p = rowtotal(v10-v12)
egen s1 = rowtotal(v13-v18)
egen p1 = rowtotal(v19-v21)

foreach v of varlist hrfactor_* {
  replace `v' = . if (s==0 & s1==0)
}
foreach v of varlist vision_* {
  replace `v' = . if (p==0 & p1==0)
}

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

keep `mvar' clientid socdate hrfactor* vision*
destring `mvar' clientid, replace
assert `mvar'!=.
save "`file'_v2", replace
*------------------------------
local file "DB3_M1100"
use "`file'", clear
rename v1 `mvar'
rename v2 clientid
rename v3 socdate

gen byte living_alatc = (v4=="1"|v19=="1")
gen byte living_alna = (v5=="1"|v20=="1")
gen byte living_aloa = (v6=="1"|v21=="1")
gen byte living_alrd = (v7=="1"|v22=="1")
gen byte living_alrn = (v8=="1"|v23=="1")
gen byte living_coatc = (v9=="1"|v24=="1")
gen byte living_cona = (v10=="1"|v25=="1")
gen byte living_cooa = (v11=="1"|v26=="1")
gen byte living_cord = (v12=="1"|v27=="1")
gen byte living_corn = (v13=="1"|v28=="1")
gen byte living_othatc = (v14=="1"|v29=="1")
gen byte living_othna = (v15=="1"|v30=="1")
gen byte living_othoa = (v16=="1"|v31=="1")
gen byte living_othrd = (v17=="1"|v32=="1")
gen byte living_othrn = (v18=="1"|v33=="1")

lab var living_alatc "C:M1100 - Patient Living Situation Patient Lives Alone - Around The Clock"
lab var living_alna "C:M1100 - Patient Living Situation Patient Lives Alone - No Assistance Available"
lab var living_aloa "C:M1100 - Patient Living Situation Patient Lives Alone - Occasional / Short-Term Assistance"
lab var living_alrd "C:M1100 - Patient Living Situation Patient Lives Alone - Regular Daytime"
lab var living_alrn "C:M1100 - Patient Living Situation Patient Lives Alone - Regular Nighttime"
lab var living_coatc "C:M1100 - Patient Living Situation Patient Lives In Congregate Situation - Around The Clock"
lab var living_cona "C:M1100 - Patient Living Situation Patient Lives In Congregate Situation - No Assistance Available"
lab var living_cooa "C:M1100 - Patient Living Situation Patient Lives In Congregate Situation - Occasional / Short-Term Assistance"
lab var living_cord "C:M1100 - Patient Living Situation Patient Lives In Congregate Situation - Regular Daytime"
lab var living_corn "C:M1100 - Patient Living Situation Patient Lives In Congregate Situation - Regular Nightime"
lab var living_othatc "C:M1100 - Patient Living Situation Patient Lives With Others In Home - Around The Clock"
lab var living_othna "C:M1100 - Patient Living Situation Patient Lives With Others In Home - No Assistance Available"
lab var living_othoa "C:M1100 - Patient Living Situation Patient Lives With Others In Home - Occasional / Short-Term Assistance"
lab var living_othrd "C:M1100 - Patient Living Situation Patient Lives With Others In Home - Regular Daytime"
lab var living_othrn "C:M1100 - Patient Living Situation Patient Lives With Others In Home - Regular Nightime"

*recode var to missing value if corresponding var in C (C1) has no values in all categories
drop if _n==1|_n==2
destring v4-v33, replace
egen s = rowtotal(v4-v18)
egen s1 = rowtotal(v19-v33)

foreach v of varlist living_* {
  replace `v' = . if (s==0 & s1==0)
}

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

keep `mvar' clientid socdate living*
destring `mvar' clientid, replace
assert `mvar'!=.
save "`file'_v2", replace

*------------------------------
local file "Rev_M1820 - 1830"
use "`file'", clear
rename v1 `mvar'
rename v2 clientid
rename v3 socdate
drop v26

gen byte dresslb0 = (v4=="1" | v15=="1")
gen byte dresslb1 = (v5=="1" | v16=="1")
gen byte dresslb3 = (v6=="1" | v17=="1")
gen byte dresslb2 = (v7=="1" | v18=="1")

gen byte bath0 = (v8=="1" | v19=="1")
gen byte bath1 = (v14=="1" | v25=="1")
gen byte bath2 = (v9=="1" | v20=="1")
gen byte bath3 = (v10=="1" | v21=="1")
gen byte bath4 = (v13=="1" | v24=="1")
gen byte bath5 = (v12=="1" | v23=="1")
gen byte bath6 = (v11=="1" | v22=="1")

lab var dresslb0 "C:M1820 - Dress Lower Body Able To Dress"
lab var dresslb1 "C:M1820 - Dress Lower Body Able To Dress if clothing laid out"
lab var dresslb2 "C:M1820 - Dress Lower Body Someone Must Help"
lab var dresslb3 "C:M1820 - Dress Lower Body Depends Entirely On Someone Else"

lab var bath0 "C:M1830 - Bathing Able To Bathe self Independently"
lab var bath1 "C:M1830 - Bathing Able To Bathe With the use of devices"
lab var bath2 "C:M1830 - Bathing Able To Bathe With Intermittent Assistance"
lab var bath3 "C:M1830 - Bathing Able To Bathe but Requires Assistance Throughout"
lab var bath4 "C:M1830 - Bathing Unable to Use Shower/Tub but able in Bed or Chair"
lab var bath5 "C:M1830 - Bathing Unable to Use Shower/Tub but able at Bed w/ assist"
lab var bath6 "C:M1830 - Bathing Unable to participate effectively in bathing"

*recode var to missing value if corresponding var in C (C1) has no values in all categories
drop if _n==1|_n==2
destring v4-v25, replace
egen s = rowtotal(v4-v14)
egen s1 = rowtotal(v15-v25)

foreach v of varlist dress* bath* {
  replace `v' = . if (s==0 & s1==0)
}

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

keep `mvar' clientid socdate dress* bath*
destring `mvar' clientid, replace
assert `mvar'!=.

compress
save "`file'_v2", replace
*------------------------------

local file "Rev_M1840 - 1845"
use "`file'", clear
rename v1 `mvar'
rename v2 clientid
rename v3 socdate
drop v22-v26

gen byte toilet_able = (v4=="1"|v13=="1")
gen byte toilet_depend = (v5=="1"|v14=="1")
gen byte toilet_unable = (v6=="1"|v15=="1")
gen byte toilet_commode = (v7=="1"|v16=="1")
gen byte toilet_assist = (v8=="1"|v17=="1")
gen byte toilethyg_able = (v9=="1"|v18=="1")
gen byte toilethyg_ablesupply = (v10=="1"|v19=="1")
gen byte toilethyg_depend = (v11=="1"|v20=="1")
gen byte toilethyg_assist = (v12=="1"|v21=="1")

lab var toilet_able "C:M1840 - Toileting Able To Get To/From Toilet Independently"
lab var toilet_depend "C:M1840 - Toileting Totally Depended In Toileting"
lab var toilet_unable "C:M1840 - Toileting Unable To Get To/From Toilet, And Unable To Use Bedside Commode"
lab var toilet_commode "C:M1840 - Toileting Unable To Get To/From Toilet, But Able To Use Bedside Commode"
lab var toilet_assist "C:M1840 - Toileting When Reminded, Assisted Or Supervised, Able To Get To/From Toilet"
lab var toilethyg_able "C:M1845 - Toileting Hygiene(TH) Able To Manage TH And Clothing Management Without Assistance"
lab var toilethyg_ablesupply "C:M1845 - Toileting Hygiene Able to manage TH and clothing without assistance if supplies/implements are laid out for the patient"
lab var toilethyg_depend "C:M1845 - Toileting Hygiene Patient Depends Entirely Upon Another Person To Maintain TH"
lab var toilethyg_assist "C:M1845 - Toileting Hygiene Someone Must Help The Patient To Maintain TH And/Or Adjust Clothing"

*recode var to missing value if corresponding var in C (C1) has no values in all categories
drop if _n==1|_n==2
destring v4-v21, replace
egen s = rowtotal(v4-v8)
egen s1 = rowtotal(v13-v17)
egen p = rowtotal(v9-v12)
egen p1 = rowtotal(v18-v21)

foreach v of varlist toilet_* {
  replace `v' = . if (s==0 & s1==0)
}
foreach v of varlist toilethyg_* {
  replace `v' = . if (p==0 & p1==0)
}

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

keep `mvar' clientid socdate toilet* toilethyg*
destring `mvar' clientid, replace
assert `mvar'!=.
save "`file'_v2", replace
*------------------------------  RESUME HERE
local file "Rev_M1800 - 1810"
use "`file'", clear
rename v1 `mvar'
rename v2 clientid
rename v3 socdate
drop v20 v21

gen byte groom_able = (v4=="1"|v12=="1")
gen byte groom_uten = (v5=="1"|v13=="1")
gen byte groom_depend = (v6=="1"|v14=="1")
gen byte groom_assist = (v7=="1"|v15=="1")
gen byte dressub_able = (v8=="1"|v16=="1")
gen byte dressub_laid = (v9=="1"|v17=="1")
gen byte dressub_depend = (v10=="1"|v18=="1")
gen byte dressub_assist = (v11=="1"|v19=="1")

lab var groom_able "C:M1800 - Grooming Able To Groom Self Unaided"
lab var groom_uten "C:M1800 - Grooming Grooming Utensils Must Be Placed Within Reach"
lab var groom_depend "C:M1800 - Grooming Patient Depends Entirely On Someone Else"
lab var groom_assist "C:M1800 - Grooming Someone Must Assist The Patient"
lab var dressub_able "C:M1810 - Dress Upper Body Able To Dress Upper Body Without Assistance"
lab var dressub_laid "C:M1810 - Dress Upper Body Able To Dress Upper Body Without Assistance If Clothing Laid Out"
lab var dressub_depend "C:M1810 - Dress Upper Body Patient Depends Entirely On Someone Else"
lab var dressub_assist "C:M1810 - Dress Upper Body Someone Must Help Patient With Upper Body Clothing"

*recode var to missing value if corresponding var in C (C1) has no values in all categories
drop if _n==1|_n==2
destring v4-v19, replace
egen s = rowtotal(v4-v7)
egen s1 = rowtotal(v13-v15)
egen p = rowtotal(v8-v11)
egen p1 = rowtotal(v16-v19)

foreach v of varlist groom_* {
  replace `v' = . if (s==0 & s1==0)
}
foreach v of varlist dressub_* {
  replace `v' = . if (p==0 & p1==0)
}

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

keep `mvar' clientid socdate groom* dressub*
destring `mvar' clientid, replace
assert `mvar'!=.
save "`file'_v2", replace
*------------------------------
local file "Rev_M1740 - 1745"
use "`file'", clear
rename v1 `mvar'
rename v2 clientid
rename v3 socdate

gen byte behav_delus = (v4=="1"|v17=="1")
gen byte behav_impdm = (v5=="1"|v18=="1")
gen byte behav_memdeficit = (v6=="1"|v19=="1")
gen byte behav_none = (v7=="1"|v20=="1")
gen byte behav_physagg = (v8=="1"|v21=="1")
gen byte behav_socinapp = (v9=="1"|v22=="1")
gen byte behav_verbdisrup = (v10=="1"|v23=="1")
gen byte freqbehav_ge1day = (v11=="1"|v24=="1")
gen byte freqbehav_lt1mo = (v12=="1"|v25=="1")
gen byte freqbehav_never = (v13=="1"|v26=="1")
gen byte freqbehav_1mo = (v14=="1"|v27=="1")
gen byte freqbehav_manywk = (v15=="1"|v28=="1")
gen byte freqbehav_manymo = (v16=="1"|v29=="1")

lab var behav_delus "C:M1740 - Behavior Demonstrated Delusions"
lab var behav_impdm "C:M1740 - Behavior Demonstrated Impaired Decision-Making"
lab var behav_memdeficit "C:M1740 - Behavior Demonstrated Memory Deficit"
lab var behav_none "C:M1740 - Behavior Demonstrated None Of The Above"
lab var behav_physagg "C:M1740 - Behavior Demonstrated Physical Aggression"
lab var behav_socinapp "C:M1740 - Behavior Demonstrated Socially Inappropriate"
lab var behav_verbdisrup "C:M1740 - Behavior Demonstrated Verbal Disruption"
lab var freqbehav_ge1day "C:M1745 - Frequency of Behavior Problems At Least Daily"
lab var freqbehav_lt1mo "C:M1745 - Frequency of Behavior Problems Less Than Once A Month"
lab var freqbehav_never "C:M1745 - Frequency of Behavior Problems Never"
lab var freqbehav_1mo "C:M1745 - Frequency of Behavior Problems Once A Month"
lab var freqbehav_manywk "C:M1745 - Frequency of Behavior Problems Several Times A Week"
lab var freqbehav_manymo "C:M1745 - Frequency of Behavior Problems Several Times Each Month"

*recode var to missing value if corresponding var in C (C1) has no values in all categories
drop if _n==1|_n==2
destring v4-v29, replace
egen s = rowtotal(v4-v10)
egen s1 = rowtotal(v17-v23)
egen p = rowtotal(v11-v16)
egen p1 = rowtotal(v24-v29)

foreach v of varlist behav_* {
  replace `v' = . if (s==0 & s1==0)
}
foreach v of varlist freqbehav_* {
  replace `v' = . if (p==0 & p1==0)
}

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

keep `mvar' clientid socdate behav* freqbehav*
destring `mvar' clientid, replace
assert `mvar'!=.
save "`file'_v2", replace
*------------------------------
local file "Rev_M1730 - C1"
use "`file'", clear
rename v1 `mvar'
rename v2 clientid
rename v3 socdate

gen byte depress_n = v4=="1"
gen byte depress_yphq = v5=="1"
gen byte depress_ynomore = v6=="1"
gen byte depress_ymore = v7=="1"
gen byte phq2int_na = v8=="1"
gen byte phq2int_gthalf = v9=="1"
gen byte phq2int_every = v10=="1"
gen byte phq2int_not = v11=="1"
gen byte phq2int_several = v12=="1"
gen byte phq2down_gthalf = v13=="1"
gen byte phq2down_na = v14=="1"
gen byte phq2down_every = v15=="1"
gen byte phq2down_not = v16=="1"
gen byte phq2down_several = v17=="1"

lab var depress_n "C1:M1730 - Depression Screening No"
lab var depress_yphq "C1:M1730 - Depression Screening Yes, patient was screened using the PHQ-2? scale"
lab var depress_ynomore "C1:M1730 - Depression Screen Yes, pat was screened w/ a different std. assessment & doesn't meet criteria for further evaluation for depression"
lab var depress_ymore "C1:M1730 - Depression Screen Yes, with a different std. assessment-and the patient meets criteria for further evaluation for depression"

lab var phq2int_na "C1:M1730a - PHQ-2 Scale: Interest in Doing Things N/A - Unable to Respond"
lab var phq2int_gthalf "C1:M1730a - PHQ-2 Scale: Interest in Doing Things More Than Half Of The Days (7 - 11 Days)"
lab var phq2int_every "C1:M1730a - PHQ-2 Scale: Interest in Doing Things Nearly Every Day (12 - 14 Days)"
lab var phq2int_not "C1:M1730a - PHQ-2 Scale: Interest in Doing Things Not At All (0 - 1 Day)"
lab var phq2int_several "C1:M1730a - PHQ-2 Scale: Interest in Doing Things Several Days (2 - 6 Days)"
lab var phq2down_gthalf "C1:M1730b - PHQ-2 Scale:Feeling Down,Depresed, or Hopeless More Than Half Of The Days (7 - 11 Days)"
lab var phq2down_na "C1:M1730b - PHQ-2 Scale:Feeling Down,Depresed, or Hopeless N/A - Unable to respond"
lab var phq2down_every "C1:M1730b - PHQ-2 Scale:Feeling Down,Depresed, or Hopeless Nearly Every Day (12 - 14 Days)"
lab var phq2down_not "C1:M1730b - PHQ-2 Scale:Feeling Down,Depresed, or Hopeless Not At All (0 - 1 Day)"
lab var phq2down_several "C1:M1730b - PHQ-2 Scale:Feeling Down,Depresed, or Hopeless Several Days (2 - 6 Days)"

*recode var to missing value if corresponding var in C (C1) has no values in all categories
drop if _n==1|_n==2
destring v4-v17, replace
egen s = rowtotal(v4-v7)
egen s1 = rowtotal(v8-v12)
egen p = rowtotal(v13-v17)

foreach v of varlist depress_* {
  replace `v' = . if (s==0)
}
foreach v of varlist phq2int_* {
  replace `v' = . if (s1==0)
}
foreach v of varlist phq2down_* {
  replace `v' = . if (p==0)
}

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

keep `mvar' clientid socdate depress* phq2int* phq2down*
destring `mvar' clientid, replace
assert `mvar'!=.
save "`file'_v2", replace
*------------------------------
local file "Rev_M1730 - C"
use "`file'", clear
rename v1 `mvar'
rename v2 clientid
rename v3 socdate

gen byte depress_n = v4=="1"
gen byte depress_yphq = v5=="1"
gen byte depress_ynomore = v6=="1"
gen byte depress_ymore = v7=="1"
gen byte phq2int_na = v8=="1"
gen byte phq2int_gthalf = v9=="1"
gen byte phq2int_every = v10=="1"
gen byte phq2int_not = v11=="1"
gen byte phq2int_several = v12=="1"
gen byte phq2down_gthalf = v13=="1"
gen byte phq2down_na = v14=="1"
gen byte phq2down_every = v15=="1"
gen byte phq2down_not = v16=="1"
gen byte phq2down_several = v17=="1"

lab var depress_n "C1:M1730 - Depression Screening No"
lab var depress_yphq "C1:M1730 - Depression Screening Yes, patient was screened using the PHQ-2? scale"
lab var depress_ynomore "C1:M1730 - Depression Screen Yes, pat was screened w/ a different std. assessment & doesn't meet criteria for further evaluation for depression"
lab var depress_ymore "C1:M1730 - Depression Screen Yes, with a different std. assessment-and the patient meets criteria for further evaluation for depression"

lab var phq2int_na "C1:M1730a - PHQ-2 Scale: Interest in Doing Things N/A - Unable to Respond"
lab var phq2int_gthalf "C1:M1730a - PHQ-2 Scale: Interest in Doing Things More Than Half Of The Days (7 - 11 Days)"
lab var phq2int_every "C1:M1730a - PHQ-2 Scale: Interest in Doing Things Nearly Every Day (12 - 14 Days)"
lab var phq2int_not "C1:M1730a - PHQ-2 Scale: Interest in Doing Things Not At All (0 - 1 Day)"
lab var phq2int_several "C1:M1730a - PHQ-2 Scale: Interest in Doing Things Several Days (2 - 6 Days)"
lab var phq2down_gthalf "C1:M1730b - PHQ-2 Scale:Feeling Down,Depresed, or Hopeless More Than Half Of The Days (7 - 11 Days)"
lab var phq2down_na "C1:M1730b - PHQ-2 Scale:Feeling Down,Depresed, or Hopeless N/A - Unable to respond"
lab var phq2down_every "C1:M1730b - PHQ-2 Scale:Feeling Down,Depresed, or Hopeless Nearly Every Day (12 - 14 Days)"
lab var phq2down_not "C1:M1730b - PHQ-2 Scale:Feeling Down,Depresed, or Hopeless Not At All (0 - 1 Day)"
lab var phq2down_several "C1:M1730b - PHQ-2 Scale:Feeling Down,Depresed, or Hopeless Several Days (2 - 6 Days)"

*recode var to missing value if corresponding var in C (C1) has no values in all categories
drop if _n==1|_n==2
destring v4-v17, replace
egen s = rowtotal(v4-v7)
egen s1 = rowtotal(v8-v12)
egen p = rowtotal(v13-v17)

foreach v of varlist depress_* {
  replace `v' = . if (s==0)
}
foreach v of varlist phq2int_* {
  replace `v' = . if (s1==0)
}
foreach v of varlist phq2down_* {
  replace `v' = . if (p==0)
}

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

keep `mvar' clientid socdate depress* phq2int* phq2down*
destring `mvar' clientid, replace
assert `mvar'!=.

*append the depression screening data from OASIS-C and OASIS-C1
append using "Rev_M1730 - C1_v2"
save "Rev_M1730_v2", replace

*All DX Categories ------------------------------
local file "Rev_All DX Categories"
use "`file'", clear
drop grandtotal

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

*recode all diagnosis vars to zero from missing values (all vars except ID and SOC date vars)
foreach v of varlist * {
  replace `v'=0 if `v'==. & `mvar'!=clientid
}
save "`file'_v2", replace

* DC date ------------------------------
local file "Rev_DC Date DB"
use "`file'", clear
drop if `mvar'==.

gen yr = substr(socdate,-2,.)
tab yr
destring yr, replace
count if yr<=10
*668 obs have SOC year <= 2010; keep them for now

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

*convert discharge date to a date variable
split dcdate, p("/")
replace dcdate3 = "20"+dcdate3
destring dcdate?, replace float
gen dcdate_e = mdy(dcdate1, dcdate2, dcdate3)
format dcdate_e %d
drop dcdate? dcdate
label var dcdate_e "Discharge date"

drop yr
save "`file'_v2", replace
