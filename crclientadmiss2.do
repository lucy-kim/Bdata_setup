* convert additional client admission files (DB4)  in CSV to Stata files

local stdatapath /home/hcmg/kunhee/Labor/Bayada_data
cd `stdatapath'/client_CSV

*rename files
#delimit;
foreach file in
"DB4_M1010"
"DB4_M1018"
"DB4_M1030"
"DB4_M1870 - 1880 - 1890"
"DB4_M1900"
"DB4_M1910"
"DB4_M2020"
"DB4_M2030"
"DB4_M2040"
"DB4_M2100"
"DB4_M2102"
"DB4_M2110"
*"DB4_M2250"
{ ;
  insheet using "client\Client Data - UPenn Project `file'.csv", clear comma ;
  saveold "`stdatapath'/`file'", replace v(12) ;
} ;
                                        #delimit cr

