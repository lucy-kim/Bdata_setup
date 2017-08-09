*Create client recertification data showing episode counts in recertification for each client admission observation 

loc path /home/hcmg/kunhee/Labor/Bayada_data
cd `path'/client_CSV
local mvar admissionclientsocid

insheet using "visit\Visits by Office and Recertification Episodes_Recertifications.csv", comma clear names
drop v5-v8

*the concatenation of clietnid and socdate seems wrong because different clientid and socdate observations have the same admission-SOC ID
duplicates tag uniqueid, gen(dup)
count if dup > 0
format uniqueid %11.0f
list if dup > 0 in 1/100

count if socdate==""
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
drop socdate? socdate
format uniqueid %11.0f
list in 1/10

*revise the socdate number to the one used in excel to match the last 5 digits of uniqueid
gen soc2 = socdate_e + 21916
tostring soc2, replace
tostring clientid, replace

gen uniqueid2 = clientid + soc2
destring uniqueid2, replace 
format uniqueid2 %11.0f

*list in 1/10

rename uniqueid2 `mvar'
list in 1/30

rename episode epi_recert
duplicates tag `mvar', gen(dupp)
assert dupp==0

sort `mvar'
count if soc2=="."
*932 observations have no SOC date
assert epi_recert == . if soc2=="."
drop if soc2=="."

format socdate %d
drop soc2 dupp
/* destring socdate, replace */
/* format socdate %d */

keep `mvar' epi_recert socdate clientid
replace epi_recert = 0 if epi_recert==.

compress
save `path'/clientrecert, replace
