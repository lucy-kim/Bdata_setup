*create data on patient's ZIP code

set linesize 120
local path /home/hcmg/kunhee/Labor/Bayada_data
cd `path'/client_CSV

insheet using "Client Zip Codes.csv", comma clear names
keep allclients v2
rename allclients clientid
rename v2 zip
drop if _n<3
gen yr = 2015
tempfile y2015
save `y2015'

insheet using "Client Zip Codes.csv", comma clear names
keep v4 v5
rename v4 clientid
rename v5 zip
drop if _n<3
gen yr = 2014
tempfile y2014
save `y2014'

insheet using "Client Zip Codes.csv", comma clear names
keep v7 v8
rename v7 clientid
rename v8 zip
drop if _n<3
gen yr = 2013
tempfile y2013
save `y2013'

insheet using "Client Zip Codes.csv", comma clear names
keep v10 v11
rename v10 clientid
rename v11 zip
drop if _n<3
gen yr = 2012
tempfile y2012
save `y2012'

insheet using "Client Zip Codes.csv", comma clear names
keep v13 v14
rename v13 clientid
rename v14 zip
drop if _n<3
gen yr = 2011
tempfile y2011
save `y2011'

use `y2011', clear
forval y = 2012/2015 {
append using `y`y''
}
*for each year, if there is missing ZIP code and non-missing ZIP code, then fill the missing value
drop if clientid==""
drop if zip ==""
destring zip, replace
gsort yr clientid -zip
bys yr clientid: replace zip = zip[_n-1] if zip[_n-1]!=. & zip>=.
list if clientid=="10111"
duplicates drop
duplicates tag yr clientid, gen(dup)
sum dup
*2% of the clients have multiple ZIP codes within the same year
list if dup > 0 in 100000/105000
*for clients having multiple ZIP codes within a year, just choose one randomly for each client-year
drop dup 
duplicates drop client yr, force

rename zip zipcode
compress
save `path'/client_zip, replace
