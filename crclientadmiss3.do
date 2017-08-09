* convert additional client admission files (DB5)  in CSV to Stata files

local stdatapath /home/hcmg/kunhee/Labor/Bayada_data
cd `stdatapath'/client_CSV

*rename files
#delimit;
foreach file in
"DB5_M2250a"
"DB5_M2250b"
"DB5_M2250c"
"DB5_M2250d"
"DB5_M2250e"
"DB5_M2250f"
"DB5_M2250g"
{ ;
  insheet using "client\Client Data - UPenn Project `file'.csv", clear comma ;
  saveold "`stdatapath'/`file'", replace v(12) ;
} ;
                                        #delimit cr

