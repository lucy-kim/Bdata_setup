*crepiid_admitID.do
*create xwalk data all episode IDs contained in each admission ID

local path /home/hcmg/kunhee/Labor/Bayada_data
cd `path'/client_CSV

forval yr = 2012/2015 {
    loc file "Episodes 2012 to 2015 with Client ID_Epi ID w Client ID `yr'.csv"
    insheet using "`file'", clear comma
    drop if regexm(v1, "Epi")

    rename v1 epiid
    rename v2 clientid
    rename v3 socdate
    rename v4 admissionclientsocid

    *convert SOC date to a date variable
    split socdate, p("/")
    destring socdate?, replace float
    gen socdate_e = mdy(socdate1, socdate2, socdate3)
    format socdate_e %d
    drop socdate? socdate
    label var socdate_e "SOC date"

    foreach v of varlist clientid socdate {
        capture destring `v', replace
        drop if `v'==. & epiid==""
        assert `v'!=.
    }
    assert admissionclientsocid!=""

    des
    tempfile epi_admitID`yr'
    save `epi_admitID`yr''
}

use `epi_admitID2012', clear
forval yr=2013/2015 {
    append using `epi_admitID`yr''
}
destring epiid, replace
format epiid %10.0g

compress
save `path'/epiid_admitID, replace
