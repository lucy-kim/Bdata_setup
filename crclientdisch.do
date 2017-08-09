* convert client discharge OASIS files (DB6-DB8)  in CSV to Stata files

local stdatapath /home/hcmg/kunhee/Labor/Bayada_data
cd `stdatapath'/client_CSV

*rename files
#delimit;
foreach file in
"DB6_M1230"
"DB6_M1242"
"DB6_M1306"
"DB6_M1307"
"DB6_M1308"
"DB6_M1309"
"DB6_M1320"
"DB6_M1322"
"DB6_M1324"
"DB6_M1330"
"DB6_M1332"
"DB6_M1334"
"DB6_M1340"
"DB6_M1342"
"DB6_M1350"
"DB6_M1400"
"DB6_M1410"
"DB6_M1500"
"DB6_M1510"
"DB7_M1600"
"DB7_M1610"
"DB7_M1615"
"DB7_M1620"
"DB7_M1700"
"DB7_M1710"
"DB7_M1720"
"DB7_M1740"
"DB7_M1745"
"DB7_M1800"
"DB7_M1810"
"DB7_M1820"
"DB7_M1830"
"DB7_M1840"
"DB7_M1845"
"DB7_M1850"
"DB7_M1860"
"DB7_M1870"
"DB7_M1880"
"DB7_M1890"
"DB8_M2004"
"DB8_M2015"
"DB8_M2020"
"DB8_M2030"
"DB8_M2100a & 2102a"
"DB8_M2100b & M2102b"
"DB8_M2100c & M2102c"
"DB8_M2100d & M2102d"
"DB8_M2100e & M2102e"
"DB8_M2100f & M2102f"
"DB8_M2100g & M2102g"
"DB8_M2110"
{ ;
  insheet using "client\Client Data - UPenn Project `file'.csv", clear comma ;
  saveold "`stdatapath'/`file'", replace v(12) ;
} ;
                                        #delimit cr


