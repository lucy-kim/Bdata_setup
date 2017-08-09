*create a Stata data on payer source (i.e. insurance plan IDs) from the CSV files

loc path /home/hcmg/kunhee/Labor/Bayada_data
cd `path'/client_CSV

forval y = 2012/2015 {
    insheet using Payer_`y'.csv, clear comma names
    de
    drop if epiid==. | payorsource==""
    tempfile payer`y'
    save `payer`y''
}

*append across years
use `payer2012', clear
forval y = 2012/2015 {
    append using `payer`y''
}
tab payorsource, sort
rename payorsource payername
rename payororder payerorder

duplicates drop

*may have to standardize the string payer names before grouping into num ID
egen planid = group(payername)

bys epiid: gen n = _N
tab n
drop n

compress
save `path'/planid, replace
