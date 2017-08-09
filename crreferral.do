*create referral source data for each admission

loc path /home/hcmg/kunhee/Labor/Bayada_data
loc mvar admissionclie

forval y = 2012/2015 {
    cd `path'/client_CSV
    insheet using "Referring Facilities by Client by Year_`y'.csv", comma names clear

    *convert SOC date to a date variable
    split date, p("/")
    replace date3 = "20"+date3
    destring date?, replace float
    gen socdate_e = mdy(date1, date2, date3)
    format socdate_e %d
    drop date? date
    label var socdate_e "SOC date"

    keep if facility!=""
    tab type

    /* keep if type=="HOSPITAL" */
    replace facility = lower(facility)

    duplicates drop

    bys state type facility: gen nadm = _N

    gen yr = year(socdate)

    /*tab fac if state=="MA"*/

    /**keep only client admissions we have office data for
    merge 1:m clientid socdate using `path'/masterclientadmiss2, keepusing(offid_nu admissionclie) keep(1 3)
    *7K have _m=1; 22.8K have _m=3
    duplicates drop
    duplicates tag `mvar', gen(dup)
    tab dup*/

    compress
    tempfile referral`y'
    save `referral`y''
}

cd `path'
use `referral2012', clear
forval y = 2013/2015 {
    append using `referral`y''
}
compress

lab var nadm "# admissions by that facility"
lab var yr "Year of the SOC of the admission"
lab var type "facility type"
save referral, replace
