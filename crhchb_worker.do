*create a stata dataset using the HCHB payroll data which contains the various information about workers (this may be a cross-sectional data)

loc path /home/hcmg/kunhee/Labor/Bayada_data
cd `path'/staff_CSV

loc f "staff\Bayada Workers from HCHB Payroll Report_HCHB Payroll Data.csv"
insheet using "`f'", clear comma names
drop v1 v2 
rename workerid workerID
rename v6 workertype
rename workercategory worker_category
rename v9 workquota
rename v10 fulltime
drop primaryjobcode
rename v12 primary_jobcode
rename v13 auth_soc_recert
rename v14 admiss_coordi
rename v15 holidaypay
rename employmenttype paymethod
rename v17 holidaypaymethod
rename v18 eval_productivity
drop v19
rename v20 pay_prodbonus
rename v22 payroll_dept
rename homeoffice homeoffid
rename v25 productivityfreq
rename v26 PDOpoints
rename v27 mileagepaymethod
rename v28 overtimepay
rename v29 contractor
rename v30 worker_dept
rename v32 effectivefrom


lab var workerID "Worker ID"
lab var worker_cate "Worker category"
lab var workertype "Worker type = Employee, contractual"
drop workerstat
lab var primaryworkercla "Primary Worker Class = field, office"
lab var workquota "Expected Hrs/Pay Period"
lab var fulltime "Full time or not"
lab var exempt "Exempt or not"
lab var primary_job "Primary Job Description"
lab var auth_soc "Has authority to do SOC/RECT assessment?"
lab var admiss_coordi "Admission Coordinator"
lab var holidaypay "Holiday pay y/n"
lab var paymethod "Standard Payment Method: per visit, hourly"
lab var holidaypaymethod "Holiday payment method"
lab var eval_productivity "Evaluate Productivity"
lab var pay_prodbonus "Pay Productivity Bonus y/n"
lab var payrollno "Payroll Number"
lab var payroll_dept "Payroll Department"
lab var homeoffid "Home Branch code"
lab var visitexpectedproduc "Expected Productivity"
lab var productivityfreq "Productivity Frequency"
lab var PDOpoints "PDO points"
lab var mileagepaymethod "Mileage Payment Method"
lab var overtimepay "overtime pay"
lab var contractor "Contractor"
lab var worker_dept "Worker department"
lab var jobtitle "job title"
lab var effectivefrom "effective employment date; =1/1/1900 if hired before 2011"

drop if _n==1
drop workquota
rename visitexpectedproduc workquota
destring workquota, replace
sum workquota
tab worker_cate if workquota > 0
tab worker_cate if workquota == 0

save `path'/hchb_worker, replace

