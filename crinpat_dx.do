*Create Stata data containing all inpatient diagnosis codes

loc path /home/hcmg/kunhee/Labor/Bayada_data
cd `path'/client_CSV

forval yr=2012/2014 {
    loc file "client\Inpatient DC Dates & Diagnoses_`yr' Inpt DX.csv"
    insheet using "`file'", comma names clear
    di "`yr'"
    des

    capture drop if inpat==. & clientid==. & socdate==""
    capture drop if inpat=="" & clientid==. & socdate==""

    *convert SOC date to a date variable
    split socdate, p("/")
    replace socdate3 = "20"+socdate3
    destring socdate?, replace float
    gen socdate_e = mdy(socdate1, socdate2, socdate3)
    format socdate_e %d
    drop socdate? socdate
    label var socdate_e "SOC date"

    capture drop if regexm(inpat, "NOICD") | regexm(inpat, "NOI.CD")
    capture destring inpati, replace
    rename inpatientdiag inpat_dx
    assert inpat_dx!=.
    tempfile dx`yr'
    save `dx`yr''
}

*for 2015, there are 1163 obs with string inpatient DX codes -> ICD-10 codes b/c CMS required using ICD-10 for OASIS starting from 10/1/2015
loc yr 2015
loc file "client\Inpatient DC Dates & Diagnoses_`yr' Inpt DX.csv"
insheet using "`file'", comma names clear

capture drop if inpat==. & clientid==. & socdate==""
capture drop if inpat=="" & clientid==. & socdate==""

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

*restrict to client-SOC date pairs that appear in our client sample
preserve
use `path'/client_chars, clear
keep clientid socdate_e
duplicates drop
tempfile unique
save `unique'
restore

merge m:1 clientid socdate_e using `unique'
*74,468 have _m=3 ; 180,352 have _m=2; 28,064 have _m=1
keep if _merge==3
drop _merge

*are there any ICD-10 codes (those start with alphabet)?
gen x = real(inpat)
*21 obs missing values in x
tab inpat if x==.

*what are the unique ICD-10 codes appearing in the 2015 inpatient DX data?
preserve
keep if x==.
rename inpatientdiag i10
keep i10
duplicates drop

*remove dot
gen i10_old = i10
replace i10 = subinstr(i10, ".", "", .)

tempfile uniquei10
save `uniquei10'
restore

*get ICD-9 codes for the ICD-10 we have
preserve
import delimit "`path'/ICD/DiagnosisGEMs-2014/2014_I10gem.txt", clear
split v1, p("")
drop v1
rename v11 i10
rename v12 i9
rename v13 flag

*merge with the list of unique ICD-10 codes appearing in the 2015 inpat DX data
merge m:1 i10 using `uniquei10'
*18 _m=3; 4 _m=2;

*if _m=2, manually fill in the ICD-9 codes
list i10 if _merge==2
replace i9 = "E929.3" if i10=="W19"
replace i9 = "174.9" if i10=="C5091"
replace i9 = "682.8" if i10=="L0381"
replace i9 = "789.60" if i10=="R1081"
assert i9!="" if _merge!=1
drop if _merge==1
drop _merge

bys i10: gen n = _N
*if there are >1 ICD-9 codes for each ICD-10 codes, then pick one that is exact match or a more general one
drop if n==2 & flag!="00000" & i10=="R188"
drop if n==2 & i10=="I2510" & i9!="4292"
drop n flag

*place a dot
gen l = length(i9)
gen yesdot = regexm(i9, "[.]")
gen x = ""
forval k = 4/5 {
    replace x = substr(i9,1,3) + "." + substr(i9,4,`k') if l==`k' & yesdot==0
}
replace x = i9 if yesdot==1 & x==""
drop i9 l yesdot
rename x i9
rename i10 i10_new
rename i10_old i10

tempfile i10gem
save `i10gem'
restore

*merge back for observations with ICD-10 inpat diagnosis
rename inpat i10
merge m:1 i10 using `i10gem'
assert _merge==3 if x==.
replace i9 = i10 if i9=="" & i10!=""
assert i9!=""
keep clientid socdate i9
replace i9 = "9" if i9=="009.0"

rename i9 inpat_dx

tempfile dx`yr'
save `dx`yr''

use `dx2012', clear
forval yr=2013/2014 {
    append using `dx`yr''
}
gen x = string(inpat)
drop inpat_dx
rename x inpat_dx
append using `dx2015'

tempfile data1
save `data1'

*--------------------
*merge with an inpatient diagnosis variable I received earlier for each admission

use DB4_M1010_v2, clear

*convert a few ICD-10 codes to ICD-9 codes

*are there any ICD-10 codes (those start with alphabet)?
replace inpatdiag = subinstr(inpatdiag," ","",.)
gen x = real(inpat)
*21 obs missing values in x
tab inpat if x==.

*what are the unique ICD-10 codes appearing in the 2015 inpatient DX data?
preserve
keep if x==.
rename inpatdiag i10
keep i10
duplicates drop

*remove dot
gen i10_old = i10
replace i10 = subinstr(i10, ".", "", .)
replace i10 = subinstr(i10, " ", "", .)
duplicates tag i10, gen(dup)
tab dup
drop dup
duplicates drop i10, force

tempfile uniquei10
save `uniquei10'
restore

preserve
import delimit "`path'/ICD/DiagnosisGEMs-2014/2014_I10gem.txt", clear
split v1, p("")
drop v1
rename v11 i10
rename v12 i9
rename v13 flag

*merge with the list of unique ICD-10 codes appearing in the 2015 inpat DX data
merge m:1 i10 using `uniquei10', keep(2 3)
*348 have _m=3; 91 have _m=2

*if _m=2, manually fill in the ICD-9 codes in http://www.icd10data.com/
list i10* if _merge==2
replace i9 = "001" if i10=="A00"
replace i9 = "038" if i10=="A41"
replace i9 = "174.0" if i10=="C5001"
replace i9 = "174.9" if i10=="C5091"
replace i9 = "198.4" if i10=="C793"
replace i9 = "233.0" if i10=="D05"
replace i9 = "282.62" if i10=="D570"
replace i9 = "249.90" if i10=="E08"
replace i9 = "249.70" if i10=="E085"
replace i9 = "249.80" if i10=="E136"
replace i9 = "249.20" if i10=="F039"
replace i9 = "305.00" if i10=="F101"
replace i9 = "291.81" if i10=="F1023"
replace i9 = "295.90" if i10=="F20"
replace i9 = "295.70" if i10=="F25"
replace i9 = "296.21" if i10=="F32"
replace i9 = "331.0" if i10=="G30"
replace i9 = "345.50" if i10=="G40"
replace i9 = "339.29" if i10=="G892"
replace i9 = "386.11" if i10=="H811"
replace i9 = "386.19" if i10=="H813"
replace i9 = "410.01" if i10=="I21"
replace i9 = "414.01" if i10=="I251"
replace i9 = "414.01" if i10=="I2511"
replace i9 = "415.19" if i10=="I26"
replace i9 = "415.12" if i10=="I269"
replace i9 = "427.31" if i10=="I48"
replace i9 = "428.20" if i10=="I50"
replace i9 = "428.20" if i10=="I502"
replace i9 = "428.30" if i10=="I503"
replace i9 = "440.30" if i10=="I7039"
replace i9 = "480.8" if i10=="J128"
replace i9 = "482.0" if i10=="J15"
replace i9 = "485" if i10=="J18"
replace i9 = "491.22" if i10=="J44"
replace i9 = "562.11" if i10=="K574"
replace i9 = "682.3" if i10=="L0311"
replace i9 = "715.09" if i10=="M15"
replace i9 = "715.15" if i10=="M16"
replace i9 = "715.16" if i10=="M17"
replace i9 = "719.90" if i10=="M25"
replace i9 = "719.41" if i10=="M2551"
replace i9 = "721.90" if i10=="M47"
replace i9 = "724.00" if i10=="M480"
replace i9 = "733.16" if i10=="M80071"
replace i9 = "730.20" if i10=="M86"
replace i9 = "730.05" if i10=="M8615"
replace i9 = "719.7" if i10=="R26"
replace i9 = "781.2" if i10=="R268"
replace i9 = "780.60" if i10=="R50"
replace i9 = "873.8" if i10=="S0190"
replace i9 = "852.21" if i10=="S065"
replace i9 = "873.8" if i10=="S098XX"
replace i9 = "952.00" if i10=="S14102"
replace i9 = "879.0" if i10=="S21009"
replace i9 = "807.02" if i10=="S224"
replace i9 = "805.4" if i10=="S32"
replace i9 = "808.41" if i10=="S32301"
replace i9 = "808.0" if i10=="S3246"
replace i9 = "808.2" if i10=="S32599"
replace i9 = "808.49" if i10=="S3289X"
replace i9 = "812.00" if i10=="S42202"
replace i9 = "812.01" if i10=="S42222"
replace i9 = "812.49" if i10=="S4249"
replace i9 = "890.1" if i10=="S71029"
replace i9 = "820.8" if i10=="S720"
replace i9 = "820.8" if i10=="S72009"
replace i9 = "820.09" if i10=="S72092"
replace i9 = "820.20" if i10=="S72109"
replace i9 = "820.21" if i10=="S7214"
replace i9 = "821.00" if i10=="S729"
replace i9 = "821.00" if i10=="S7290X"
replace i9 = "891.0" if i10=="S81"
replace i9 = "891.0" if i10=="S818"
replace i9 = "891.0" if i10=="S81809"
replace i9 = "823.20" if i10=="S82201"
replace i9 = "824.4" if i10=="S82841"
replace i9 = "824.8" if i10=="S82899"
replace i9 = "836.3" if i10=="S83"
replace i9 = "892.0" if i10=="S913"
replace i9 = "825.0" if i10=="S9203"
replace i9 = "825.29" if i10=="S9220"
replace i9 = "988.0" if i10=="T61781"
replace i9 = "998.01" if i10=="T8111"
replace i9 = "998.30" if i10=="T8130X"
replace i9 = "996.47" if i10=="T84093"
replace i9 = "996.2" if i10=="T85"
replace i9 = "E888.1" if i10=="W180"
replace i9 = "E888.9" if i10=="W19XXX"
replace i9 = "162.2" if i10=="C34"
replace i9 = "820.00" if i10=="S72019"
assert i9!=""
drop _merge

bys i10: gen n = _N
*if there are >1 ICD-9 codes for each ICD-10 codes, then pick one randomly
duplicates drop i10*, force
drop n flag

*place a dot
gen l = length(i9)
gen yesdot = regexm(i9, "[.]")
gen x = ""
forval k = 4/5 {
    replace x = substr(i9,1,3) + "." + substr(i9,4,`k') if l==`k' & yesdot==0
}
replace x = i9 if x==""
drop i9 l yesdot
rename x i9
rename i10 i10_new
rename i10_old i10

tempfile i10gem
save `i10gem'
restore

*merge back for observations with ICD-10 inpat diagnosis
rename inpat i10
merge m:1 i10 using `i10gem'
assert _merge==3 if x==.
replace i9 = i10 if i9=="" & i10!=""
assert i9!=""
keep `mvar' clientid socdate i9
replace i9 = "9" if i9=="009.0"

rename i9 inpat_dx

tempfile data2
save `data2'

*---------------------------
*append with the newly obtained inpatient DX data2
use `data1', clear
append using `data2'
drop `mvar'

*the old inpatient DX may be one of the DX codes in the new data, so drop if they're duplicates
duplicates drop


compress
save `path'/inpat_dx, replace
