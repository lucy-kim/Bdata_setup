*create data on date of transfer to another facility or death

loc path /home/hcmg/kunhee/Labor/Bayada_data
cd `path'/client_CSV
local mvar admissionclientsocid

insheet using "client\Deaths_Deaths.csv", comma clear names
rename uniqueid `mvar'
format `mvar' %11.0f
rename mo90 deathdate
keep `mvar' deathdate

*convert Episode start date to a date variable
gen death = 1
replace deathdate = "" if deathdate=="Unknown"
split deathdate, p("/")
replace deathdate3 = "20"+deathdate3
destring deathdate?, replace float
gen deathdate_e = mdy(deathdate1, deathdate2, deathdate3)
format deathdate_e %d
drop deathdate? deathdate
label var deathdate_e "Death date"

bys `mvar': gen n = _N
sort `mvar' deathdate
count if n > 1
list if n > 1 in 1/100
bys `mvar': gen last = _n==_N

preserve
keep if last==0
drop last n
save `path'/transfer, replace
restore

keep if last==1
duplicates tag `mvar', gen(dup)
assert dup==0
drop dup n last
save `path'/deaths, replace
