*clean each individual data file from the client discharge DB

local stdatapath /home/hcmg/kunhee/Labor/Bayada_data
set linesize 200
cd `stdatapath'
local mvar admissionclientsocid

*first, work on  data ------------------------------
local file "DB7_M1860"
use "`file'", clear

rename v1 `mvar'
rename v2 clientid
rename v3 socdate

gen byte ambul_able = (v4=="1")
gen byte ambul_assist = (v5=="1")
gen byte ambul_bedfast = (v6=="1")
gen byte ambul_chairfastnowh = (v7=="1")
gen byte ambul_chairfastwh = (v8=="1")
gen byte ambul_onedevice = (v9=="1")
gen byte ambul_twodevice = (v10=="1")

lab var ambul_able "C:M1860 - Ambulation Able To Independently Walk On Even Or Uneven Surfaces"
lab var ambul_assist "C:M1860 - Ambulation Able To Walk Only With Supervision Or Assistance Of Another"
lab var ambul_bedfast "C:M1860 - Ambulation Bedfast, Unable To Ambulate Or Be Upright In Chair"
lab var ambul_chairfastnowh "C:M1860 - Ambulation Chairfast, Unable To Ambulate And Unable To Wheel Self"
lab var ambul_chairfastwh"C:M1860 - Ambulation Chairfast, Unable To Ambulate But Able To Wheel Self"
lab var ambul_onedevice "C:M1860 - Ambulation Requires Use of a one-handed Device to Walk Alone"
lab var ambul_twodevice "C:M1860 - Ambulation Requires Use of a two-handed Device to Walk Alone"

*recode var to missing value if corresponding var in C (C1) has no values in all categories
drop if _n==1|_n==2
destring v4-v10, replace
egen s = rowtotal(v4-v10)
foreach v of varlist ambul_* {
  replace `v' = . if s==0
}

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

keep `mvar' clientid socdate ambul*
destring `mvar' clientid, replace
assert `mvar'!=.
save "`file'_v2", replace

*------------------------------
local file "DB7_M1850"
use "`file'", clear

rename v1 `mvar'
rename v2 clientid
rename v3 socdate

gen byte trnsf0 = (v4=="1")
gen byte trnsf1 = (v5=="1")
gen byte trnsf2 = (v6=="1")
gen byte trnsf3 = (v7=="1")
gen byte trnsf4 = (v8=="1")
gen byte trnsf5 = (v9=="1")

lab var trnsf0 "C:M1850 - Bed-transfer Able To Independently Walk On Even Or Uneven Surfaces"
lab var trnsf1 "C:M1850 - Bed-transfer With Minimal Human Assistance"
lab var trnsf2 "C:M1850 - Bed-transfer Unable To Transfer Self But Able To Bear Weight"
lab var trnsf3 "C:M1850 - Bed-transfer Unable To Transfer Self And Unable To Bear Weight"
lab var trnsf4 "C:M1850 - Bed-transfer Bedfast, Unable To Transfer But Able To Turn And Position Self"
lab var trnsf5 "C:M1850 - Bed-transfer Bedfast, Unable To Transfer And Unable To Turn And Position Self"

*recode var to missing value if corresponding var in C (C1) has no values in all categories
drop if _n==1|_n==2
destring v4-v9, replace
egen s = rowtotal(v4-v9)
foreach v of varlist trnsf* {
  replace `v' = . if s==0
}

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

keep `mvar' clientid socdate trnsf*
destring `mvar' clientid, replace
assert `mvar'!=.

save "`file'_v2", replace

*------------------------------------
local file "DB6_M1400"
use "`file'", clear

rename v1 `mvar'
rename v2 clientid
rename v3 socdate

gen byte whendyspneic_rest = v4=="1"
gen byte whendyspneic_n = v5=="1"
gen byte whendyspneic_walk = v6=="1"
gen byte whendyspneic_minimal = v7=="1"
gen byte whendyspneic_moderate = v8=="1"

lab var whendyspneic_rest "C:M1400 - When Dyspneic At Rest"
lab var whendyspneic_n "C:M1400 - When Dyspneic Patient Is Not Short Of Breath"
lab var whendyspneic_walk "C:M1400 - When Dyspneic When Walking More Than 20 Feet"
lab var whendyspneic_minimal "C:M1400 - When Dyspneic With Minimal Exertion"
lab var whendyspneic_moderate "C:M1400 - When Dyspneic With Moderate Exertion"

*recode var to missing value if corresponding var in C (C1) has no values in all categories
drop if _n==1|_n==2
destring v4-v8, replace
egen s = rowtotal(v4-v8)

foreach v of varlist whendyspneic_* {
  replace `v' = . if s==0
}

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

keep `mvar' clientid socdate whendyspneic*
  destring `mvar' clientid, replace
assert `mvar'!=.

save "`file'_v2", replace
*------------------------------
local file "DB7_M1700"
use "`file'", clear

rename v1 `mvar'
rename v2 clientid
rename v3 socdate

gen byte cogni_alert = v4=="1"
gen byte cogni_assist = v5=="1"
gen byte cogni_lotsassist = v6=="1"
gen byte cogni_prompt = v7=="1"
gen byte cogni_depend = v8=="1"

lab var cogni_alert "C:M1700 - Cognitive Functioning Alert/Oriented"
lab var cogni_assist "C:M1700 - Cognitive Functioning Requires Assistance In Specific Situations"
lab var cogni_lotsassist "C:M1700 - Cognitive Functioning Requires Considerable Assistance In Routine Situations"
lab var cogni_prompt "C:M1700 - Cognitive Functioning Requires Prompting"
lab var cogni_depend "C:M1700 - Cognitive Functioning Totally Dependent"

*recode var to missing value if corresponding var in C (C1) has no values in all categories
drop if _n==1|_n==2
destring v4-v8, replace
egen p = rowtotal(v4-v8)
foreach v of varlist cogni_* {
  replace `v' = . if p==0
}

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

keep `mvar' clientid socdate cogni*
destring `mvar' clientid, replace
assert `mvar'!=.

save "`file'_v2", replace
*------------------------------
local file "DB7_M1710"
use "`file'", clear

rename v1 `mvar'
rename v2 clientid
rename v3 socdate

gen byte whenconfuse_const = v4=="1"
gen byte whenconfuse_ampm = v5=="1"
gen byte whenconfuse_new = v6=="1"
gen byte whenconfuse_na = v7=="1"
gen byte whenconfuse_never = v8=="1"
gen byte whenconfuse_wake = v9=="1"

lab var whenconfuse_const "C:M1710 - When Confused Constantly"
lab var whenconfuse_ampm "C:M1710 - When Confused During The Day And Evening, But Not Constantly"
lab var whenconfuse_new "C:M1710 - When Confused In New Or Complex Situations Only"
lab var whenconfuse_na "C:M1710 - When Confused N/A - Patient Nonresponsive"
lab var whenconfuse_never "C:M1710 - When Confused Never"
lab var whenconfuse_wake "C:M1710 - When Confused On Awakening Or At Night Only"

*recode var to missing value if corresponding var in C (C1) has no values in all categories
drop if _n==1|_n==2
destring v4-v9, replace
egen s = rowtotal(v4-v9)

foreach v of varlist whenconfuse_* {
  replace `v' = . if s==0
}

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

keep `mvar' clientid socdate whenconfuse*
destring `mvar' clientid, replace
assert `mvar'!=.
save "`file'_v2", replace
*------------------------------
local file "DB7_M1720"
use "`file'", clear

rename v1 `mvar'
rename v2 clientid
rename v3 socdate

gen byte whenanxious_all = v4=="1"
gen byte whenanxious_day = v5=="1"
gen byte whenanxious_ltday = v6=="1"
gen byte whenanxious_na = v7=="1"
gen byte whenanxious_none = v8=="1"
drop v9

lab var whenanxious_all "C:M1720 - When Anxious All Of The Time"
lab var whenanxious_day "C:M1720 - When Anxious Daily, But Not Constantly"
lab var whenanxious_ltday "C:M1720 - When Anxious Less Often Than Daily"
lab var whenanxious_na "C:M1720 - When Anxious N/A - Patient Nonresponsive"
lab var whenanxious_none "C:M1720 - When Anxious None Of The Time"

*recode var to missing value if corresponding var in C (C1) has no values in all categories
drop if _n==1|_n==2
destring v4-v8, replace
egen s = rowtotal(v4-v8)
foreach v of varlist whenanxious_* {
  replace `v' = . if s==0
}

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

keep `mvar' clientid socdate whenanxious*
destring `mvar' clientid, replace
assert `mvar'!=.
save "`file'_v2", replace
*------------------------------
local file "DB8_M2020"
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
egen s = rowtotal(oralmed*)
foreach v of varlist oralmed_* {
  replace `v' = . if s==0
}

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

keep `mvar' clientid socdate oralmed*
destring `mvar' clientid, replace
assert `mvar'!=.

foreach v of varlist oral* {
    replace `v' = 0 if `v'==.
}
save "`file'_v2", replace
*------------------------------------------
local file "DB7_M1830"
use "`file'", clear
rename v1 `mvar'
rename v2 clientid
rename v3 socdate

gen byte bath0 = v4=="1"
gen byte bath1 = v10=="1"
gen byte bath2 = v5=="1"
gen byte bath3 = v6=="1"
gen byte bath4 = v9=="1"
gen byte bath5 = v8=="1"
gen byte bath6 = v7=="1"

lab var bath0 "C:M1830 - Bathing Able To Bathe self Independently"
lab var bath1 "C:M1830 - Bathing Able To Bathe With the use of devices"
lab var bath2 "C:M1830 - Bathing Able To Bathe With Intermittent Assistance"
lab var bath3 "C:M1830 - Bathing Able To Bathe but Requires Assistance Throughout"
lab var bath4 "C:M1830 - Bathing Unable to Use Shower/Tub but able in Bed or Chair"
lab var bath5 "C:M1830 - Bathing Unable to Use Shower/Tub but able at Bed w/ assist"
lab var bath6 "C:M1830 - Bathing Unable to participate effectively in bathing"

loc x bath
drop if _n<3
destring v4-v10, replace
egen s = rowtotal(v4-v10)
foreach v of varlist `x'* {
  replace `v' = . if s==0
}

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

keep `mvar' clientid socdate `x'*
destring `mvar' clientid, replace
assert `mvar'!=.

save "`file'_v2", replace

*-------------------------------
local file "DB6_M1242"
use "`file'", clear
rename v1 `mvar'
rename v2 clientid
rename v3 socdate

gen byte freqpain_alltime = v4=="1"
gen byte freqpain_daily = v5=="1"
gen byte freqpain_ledaily = v6=="1"
gen byte freqpain_no = v7=="1"
gen byte freqpain_noaffect = v8=="1"

label var freqpain_alltime "C/C1:M1242 - Frequency of Pain: All Of The Time"
label var freqpain_daily "C/C1:M1242 - Frequency of Pain: Daily, But Not Constantly"
label var freqpain_ledaily "C/C1:M1242 - Frequency of Pain: Less Often Than Daily"
label var freqpain_no "C/C1:M1242 - Frequency of Pain:  No Pain"
label var freqpain_noaffect "C/C1:M1242 - Frequency of Pain:  Pain, but does not affect activity"

loc x freq
drop if _n<3
destring v4-v8, replace
egen s = rowtotal(v4-v8)
foreach v of varlist `x'* {
  replace `v' = . if s==0
}

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

keep `mvar' clientid socdate `x'*
destring `mvar' clientid, replace
assert `mvar'!=.
drop if `mvar'==.

save "`file'_v2", replace


/*
"DB6_M1230"
"DB6_M1230"
"DB6_M1242"
"DB6_M1306"
"DB6_M1307"
"DB6_M1308"
"DB6_M1309"
"DB6_M1320"
"DB6_M1322"
"DB6_M1324"
"DB6_M1330"
"DB6_M1332"
"DB6_M1334"
"DB6_M1340"
"DB6_M1342"
"DB6_M1350"
"DB6_M1410"
"DB6_M1500"
"DB6_M1510"
"DB7_M1600"
"DB7_M1610"
"DB7_M1615"
"DB7_M1620"
"DB7_M1740"
"DB7_M1745"
"DB7_M1800"
"DB7_M1810"
"DB7_M1820"
"DB7_M1830"
"DB7_M1840"
"DB7_M1845"
"DB7_M1850"
"DB7_M1870"
"DB7_M1880"
"DB7_M1890"
"DB8_M2004"
"DB8_M2015"
"DB8_M2030"
"DB8_M2100a & 2102a"
"DB8_M2100b & M2102b"
"DB8_M2100c & M2102c"
"DB8_M2100d & M2102d"
"DB8_M2100e & M2102e"
"DB8_M2100f & M2102f"
"DB8_M2100g & M2102g"
"DB8_M2110"
*/
