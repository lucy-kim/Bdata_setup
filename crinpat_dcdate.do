*create a Stata file containing the inpatient discharge date for each admission from the raw CSV file

loc path /home/hcmg/kunhee/Labor/Bayada_data
cd `path'/client_CSV

loc file "client\Inpatient DC Dates & Diagnoses_Inpt DC Date.csv"
insheet using "`file'", comma names clear
rename m1005 inpat_dcdate

*convert Inpatient DC date to a date variable
drop if inpat_dcdate=="Unknown"
split inpat_dcdate, p("/")
replace inpat_dcdate3 = "20"+inpat_dcdate3
destring inpat_dcdate?, replace float
gen inpat_dcdate_e = mdy(inpat_dcdate1, inpat_dcdate2, inpat_dcdate3)
format inpat_dcdate_e %d
drop inpat_dcdate? inpat_dcdate
label var inpat_dcdate_e "Inpatient discharge date (M1005)"

*convert SOC date to a date variable
split socdate, p("/")
replace socdate3 = "20"+socdate3
destring socdate?, replace float
gen socdate_e = mdy(socdate1, socdate2, socdate3)
format socdate_e %d
drop socdate? socdate
label var socdate_e "SOC date"

compress
save `path'/inpat_dcdate, replace 
