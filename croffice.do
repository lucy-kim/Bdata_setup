*create office-level data (note that this is snapshot data) that contains all the offices that have been in business and contain xwalk between alphabet office ID (offid0, offid) and numeric office ID (offid_nu)

set linesize 120
local path /home/hcmg/kunhee/Labor/Bayada_data
cd `path'/office_CSV

*use office data that contains open and closed date
insheet using "office\Bayada Office Spreadsheat - 10-2015_Office Data.csv", comma names clear

keep closed primary* medicare officeabb officenumber officefullname officestatus officetype opendate provideshomehealth startuptype tax* valid

list officeabbre officefullname officenumber

lab var officestatus "Active, Closed, Closing, Pending"
lab var officetype "Budget, Service, Support"

rename primaryaddress1 addr_str
rename primaryaddress2 addr_bldg
rename primaryaddresscity addr_city
rename primaryaddressstate addr_st
rename primaryaddresszipcode addr_zip

*opening date
split opendate, p("/")
destring opendate?, replace
gen oo = mdy(opendate1, opendate2, opendate3)
format oo %d
drop opendate opendate?
rename oo opendate

*closed date
tab closed
split closed, p("/")
destring closeddate?, replace
gen oo = mdy(closeddate1, closeddate2, closeddate3)
format oo %d
drop closeddate closeddate?
rename oo closeddate

rename officeabbrev offid
rename officenumber offid_nu

tempfile office_historical
save `office_historical'

*-------------------
insheet using "office\Bayada Office Data_Bayada Office Data.csv", clear comma
drop if v1=="BAYADA HOME HEALTH CARE OFFICE DATA"

lab var v1 "Branch Code"
lab var v2 "Branch group"
lab var v3 "Agency provider number"
lab var v4 "Branch state"
lab var v5 "Branch zip code"
lab var v6 "Average length of episode"
lab var v7 "Average length of stay"
lab var v8 "Business hours - actual"
lab var v9 "Episode count - new admit"
lab var v10 "Office worker count"
lab var v11 "Gross margin %"
lab var v12 "Avg client age"
lab var v13 "Complaint log count"
lab var v14 "Days Between Order Date  And Signed By Physician"
lab var v15 "Days Between PT Add-on Visit And SOE Date"
lab var v16 "Avg Days Between SOC Date And F2F Signed Date"
lab var v17 "Episode Count - Admit"
lab var v18 "Episode Count - Re-Admit"
lab var v19 "Episode Count - Recertification"
lab var v20 "Episode Count"
lab var v21 "Episode Count - discharge"

rename v1 offid
rename v2 branch_gp
rename v3 provnum
rename v4 branch_st
rename v5 branch_zip
rename v6 avg_loepi
rename v7 avg_los
rename v8 bizhrs
rename v9 epicnt_newadmit
rename v10 worker_cnt
rename v11 grossmargin_perc
rename v12 avg_clientage
rename v13 complaint_cnt
rename v14 days_order_signed
rename v15 dats_pt_soe
rename v16 days_soc_f2f
rename v17 epicnt_admit
rename v18 epicnt_readmit
rename v19 epicnt_recert
rename v20 epicnt
rename v21 epicnt_disch
des

drop if offid=="Branch Code"

tempfile office1
save `office1'

use `office1', clear
merge 1:1 offid using `office_historical', keepusing (offid_nu) keep(1 3)
gen nuid = real(offid)
list offid nuid offid_nu _merge
replace offid_nu = nuid if nuid!=. & offid_nu==.
drop nuid
sort offid_nu
assert offid_nu!=.
*6 contradictions for those offices with alphabetic office ID that appear only in the original office-level data
list offid* branch_gp _merge if offid_nu==.
*list offid* _merge branch_gp numeric
tab offid_nu if offid_nu==92 | offid_nu==87 | offid_nu==163 | offid_nu==286
*the offices that appear only in the original office-level data have a numeric ID that corresponds to an existing office's numeric ID

*for alpha office IDs with no matched numeric office ID, create a new office ID var that equals the numeric ID noted in the branch group
gen numeric = substr(branch_gp, 6,3)
destring numeric, replace
replace offid_nu = numeric if _merge==1 & offid_nu==.
assert offid_nu!=.
count
drop numeric _m

rename offid offid0
lab var offid0 "Alphabet office ID in the first office-level data"

sort offid_nu offid0
bys offid_nu: gen noff = _N
list offid* branch_st branch_zip if noff > 1

egen tag = tag(offid_nu)
count if tag==1 & noff > 1

merge m:1 offid_nu using `office_historical'
*1 obs has _m==1; 32 obs has _m==2; 119 obs has _m==3

tab offid_nu if _m==1
tab offid_nu if _m==2

tab officetype if _m==2
tab officestatus if _m==2

list offid* branch_gp if _m==1
list offid* officefullname if _m==2

drop noff branch_zip branch_st tag

*if the office ID in the first data have missing values, then fill in the missing values in the office ID using the numeric office ID from the historical data
tostring offid_nu, gen(a)
count if offid0==""
replace offid0 = a if offid0==""
assert offid0!=""
duplicates tag offid0, gen(dup)
assert dup==0
drop dup a

*create additional observations for those offices that do not appear in the original office-level data showing both numeric and alphabetic office IDs so I can have a full xwalk between alphabetic office ID and numeric office ID
gen x = 2 if _merge==2
expand x
sort offid_nu offid0
tostring offid_nu, gen(a)
bys offid_nu: replace offid0 = a if _merge==2 & _n==1 & offid0==""
bys offid_nu: replace offid0 = offid if _merge==2 & _n==2 & offid0==""
assert offid0!=""
duplicates tag offid0, gen(dup)
assert dup==0
drop dup a x _merge

compress
save `path'/office, replace

*----------------------------------------
* create medicare provider number + address for each office
use office, clear
keep offid_nu provnum addr_str addr_city addr_st medicarecerti addr_zip
gen add = addr_str + ", " + addr_city + ", " + addr_st + " " + addr_zip
duplicates drop
drop addr_*
rename add addr
compress
outsheet using office_medprovnum.csv, replace comma names

*----------------------------------------
  *create only the office location data
use `path'/office, clear

keep offid_nu branch_st
duplicates drop

save `path'/existing_offid_nu, replace
saveold existing_offid_nu, replace v(12)

*----------------------------------------
  *create ZIP code data
use `path'/office, clear

keep branch_zip
duplicates drop
compress
destring branch_zip, replace
rename branch_zip zip

saveold office_zips, replace v(12)

*----------------------------------------
*get CBSA codes using the ZIP-CBSA xwalk
  use `path'/office, clear
rename branch_zip zip
destring zip, replace
merge m:1 zip using `path'/zip_cbsa_xwalk

, nogen

keep offid offid_nu branch_st cbsa zip
duplicates tag offid_nu, gen(dup)
sort branch_st offid_nu

*there are offid_nu's associated with different CBSA codes -- use the CBSA code for the ZIP code indicated for the numeric ID
list if dup > 0
bys offid_nu: egen mcbsa = mean(cbsa) if dup >0
gen cbsa_new = cbsa if dup > 0 & cbsa!=mcbsa& real(offid)!=.
drop if dup > 0 & real(offid)==.
replace cbsa_new = cbsa if cbsa_new==.
keep offid_nu branch_st cbsa_new zip
rename cbsa_new cbsa

saveold offid_st_cbsa, replace v(12)
