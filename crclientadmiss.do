* convert client admission files in CSV to Stata files

local stdatapath /home/hcmg/kunhee/Labor/Bayada_data
cd `stdatapath'/client_CSV

*load and save CSV files
#delimit;
foreach file in
"Rev_All DX Categories"
"Rev_DC Date DB"
"Rev_Hosp & Reason"
"Rev_M1730 - C"
"Rev_M1730 - C1"
"Rev_M1740 - 1745"
"Rev_M1800 - 1810"
"Rev_M1820 - 1830"
"Rev_M1840 - 1845"
"Rev_M1850 - 1860"
"Rev_Master DB"
"Rev_Quality Incidents"
"Rev_Race"
"DB2_M1210 - 1220"
"DB2_M1230 - 1240"
"DB2_M1242 - 1300"
"DB2_M1302 - 1306"
"DB2_M1307 - 1320"
"DB2_M1322 - 1324"
"DB2_M1330 - 1332"
"DB2_M1334 - 1340"
"DB2_M1342 - 1350"
"DB2_M1400 - 1410"
"DB2_M1500 - 1510"
"DB2_M1600 - 1610"
"DB2_M1615 - 1700"
"DB2_M1620 - 1630"
"DB2_M1710 - 1720"
"DB3_M1000"
"DB3_M1032 - 1033 - 1034"
"DB3_M1036 - 1200"
"DB3_M1100"
{ ;
  insheet using "client\Client Data - UPenn Project `file'.csv", clear comma ;
  saveold "`stdatapath'/`file'", replace v(12) ;
} ;
                                        #delimit cr

