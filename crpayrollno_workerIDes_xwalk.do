*create xwalk data from payrollno to worker ID in the historical employment status / pay rate / quota data

loc path /home/hcmg/kunhee/Labor/Bayada_data

*merge with the workerID_es to payrollno xwalk
cd `path'/staff_CSV
loc f "Bayada Employee History File_EE Identifiers"
insheet using "`f'.csv", clear comma names
rename count workerID
rename v2 payrollno
rename v3 sysdes
lab var sysdes "Subsystem description"
drop if workerID=="WorkerId"

*there are duplicates in terms of workerID & payrollno but not in sysdes
duplicates drop workerID payrollno, force
compress
rename workerID workerID_es
drop sysdes
save `path'/payrollno_workerIDes_xwalk, replace
