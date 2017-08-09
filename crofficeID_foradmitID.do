*crofficeID_foradmitID.do
*create office ID for each admission ID (i.e. clientID-SOC date pair)

local path /home/hcmg/kunhee/Labor/Bayada_data
cd `path'/client_CSV

forval yr = 2012/2015 {
    loc file "Clients 2012 to 2015 with Office ID_Office w Admit ID `yr'.csv"
    insheet using "`file'", clear comma
    drop if regexm(v1, "Bayada") | regexm(v1, "Branch")

    rename v1 offid0
    rename v2 branch_gp
    rename v3 clientid
    rename v4 socdate
    rename v5 admissionclientsocid

    *convert SOC date to a date variable
    split socdate, p("/")
    destring socdate?, replace float
    gen socdate_e = mdy(socdate1, socdate2, socdate3)
    format socdate_e %d
    drop socdate? socdate
    label var socdate_e "SOC date"

    foreach v of varlist clientid socdate {
        capture destring `v', replace
        drop if `v'==. & offid0==""
        assert `v'!=.
    }
    assert admissionclientsocid!=""

    *merge with office ID data
    merge m:1 offid0 using `path'/office, keep(1 3) keepusing(offid_nu) nogen

    des
    tempfile office_admitID`yr'
    save `office_admitID`yr''
}

use `office_admitID2012', clear
forval yr=2013/2015 {
    append using `office_admitID`yr''
}
count if offid_nu==.
*158 / 291K obs have missing numeric office ID; office ID 88 & 631 -> drop these
drop if offid_nu==.

compress
save `path'/officeID_foradmitID, replace

*there are multiple office IDs for each admission ID
cd `path'
use officeID_foradmitID, clear
duplicates tag admiss, gen(dup)
*merge with episode ID and choose the one with smaller episode IDs
keep if dup > 0
keep admiss
duplicates drop
merge 1:m admiss using epiid_admitID, keep(1 3) nogen
sort admiss epiid
bys admiss: gen i = _n==1
keep if i==1
drop i
merge 1:m admiss clientid socdate using officeID_foradmitID, nogen

*get office ID for the episode ID
preserve
keep if epiid!=.
keep epiid
duplicates drop
merge 1:1 epiid using episode_visitcnt_offid, keepusing(offid_nu) keep(1 3) nogen
rename offid_nu newoffid_nu
tempfile realoffice
save `realoffice'
restore

merge m:1 epiid using `realoffice'
drop if offid_nu!=newoffid_nu & _merge==3
drop offid0 branch_gp newoffid_nu _merge
duplicates drop
duplicates tag admiss, gen(dup)
assert dup==0
drop dup

compress
save `path'/officeID_foradmitID, replace
