*create weekly pay data for each worker-week-office-discipline (previously named crpay_byworker_byweek.do)

set linesize 150
local path /home/hcmg/kunhee/Labor/Bayada_data
cd `path'/pay_CSV
loc f "Worker Pay Bonus & Expense Payments 2011-15.csv"
set seed 3872048
 
insheet using "`f'", clear comma names

*assert the service week ending date is sunday (di dow(mdy(9,14,2014)))
tostring serviceweek, replace
replace serviceweek = trim(serviceweek)
gen l = length(serviceweek)
sum l
gen yr = substr(serviceweek,1,4)
gen mo = substr(serviceweek,5,2)
gen day = substr(serviceweek,7,2)
destring yr mo day, replace
gen dow = dow( mdy(mo,day,yr) )
assert dow == 0
drop dow l

*create a week index & the start date and end date of the week index
gen enddate = mdy(mo,day,yr)
gen startdate = enddate - 6
format startdate %d
format enddate %d

*assert start date is always Monday
assert dow(startdate)==1
assert dow(enddate)==0
drop serviceweek yr mo day

rename officenum offid_nu
rename primarylevelid job
rename employeeid payrollno
*payroll no = gp id
sort payrollno startdate paytype

tempfile tmp
save `tmp'

/*restrict to payroll IDs that appear in the visit-level data
use `path'/staff_visit_office, clear
*get payrollno since several worker IDs map to one payrollno
merge m:1 workerID using `path'/staff_chars, keep(1 3) nogen keepusing(payrollno)
drop if payr==""
keep payr
duplicates drop
tempfile uniqpayr
save `uniqpayr'*/

/* *merge with the list of unique payrollno  */
/* use `tmp', clear */
/* merge m:1 payro using `uniqpayr', keep(3) nogen */
/* *most _m=2 workers are contractual workers with payrollno starting w/ C */

*merge with GP ID (payrollno) file to get worker ID
/* count if tag==1 */
/* drop tag */
/* merge m:1 payrollno using `path'/payrollno_workerID_xwalk */
/* egen tag = tag(payrollno) */
/* tab _m if tag==1 */
/* *I think _m=2 means worker-weeks during 2011 or earlier */
/* keep if _m==3 */
/* drop _m */

/* *get a sample of _m=1 one guys for Alan's review */
/* tab _m */
/* keep if _m==1 */
/* sample 5, by(offid) count */
/* sample 20 */
/* drop workerID _merge */
/* outsheet using `path'/nomatch_onGPid.csv, comma replace names */

/* *for now, drop the _m==1 guys - 6% of the weekly pay data have no matching payroll ID. */

tab paytype
*-----------------
*pay type definition
*regular pay = non-salary pay = per-visit pay beyond guaranteed salary for guaranteed pay workers/per-visit pay for per-diem workers
*salary = paid amount for salaried workers
*transportation = clinician travel mileage
*bonus = could be pay adjustments or transportation if transportation is missing (?)
*PTO = paid time off
*overtime = overtime pay
*VG no pay / VG pay = pay adjustments, likely from prior weeks.

*-----------------

*AW- if there are two bonus payments for the same clinician and for the same week, then I would treat the lower amount as transportation, if there is not a transportation payment for that week.  Likewise, if there is a small bonus payment - say below $10 - for a clinician and there is no transportation payment in the same week I would treat the bonus payment as transportation.

*recode bonus < $10 as transportation if bonus categories appear > once & there is no other transportation payment for each office-startdate-worker cell
gen bb = paytype=="BONUS"
gen tt = paytype=="TRANSPORTATION"
bys offid startdate payrollno: egen sbb = sum(bb)
bys offid startdate payrollno: egen stt = sum(tt)
bys offid startdate payrollno: gen i = sbb > 1 & stt==0
tab i
sum totalpaid if paytype=="BONUS" & i==1
*there are 167 obs with i = 1
bys offid startdate payrollno: egen minb = min(totalpa) if i==1 & paytype=="BONUS"
bys offid startdate payrollno: replace paytype = "TRANSPORTATION" if totalpaid==minb & i==1 & minb <= 10 & minb!=.
list payrollno offid startdate paytype totalpa minb if i==1
drop i minb *bb *tt

/* *I wonder if for office-startdate-worker cells that have no transportation payments and 1 bonus payment, I should recode the bonus pay to transportation pay - I don't think so after checking with visit-level data  */
/* *check a few examples of workers in the visit data */
/* gen bb = paytype=="BONUS" */
/* gen tt = paytype=="TRANSPORTATION" */
/* bys offid startdate payrollno: egen sbb = sum(bb) */
/* bys offid startdate payrollno: egen stt = sum(tt) */
/* bys offid startdate payrollno: gen j = stt==0 & sbb >= 1 */
/* tab j */
/* *5% of the obs have j = 1 */
/* list payrollno workerID offid startdate start end paytype totalpa if j==1 in 1/1300 */
/* list payrollno workerID offid startdate start end paytype totalpa primary if workerID==27020 in 1000/1300 */
/* drop *bb *tt j */

*----------------------------------------
*create startdately panel for each worker with pay amount for each payment type (bonus, holiday flat, overtime, PTO, regular -majority, salary, transportation, VG no pay, VG pay)

*if multiple pay entries exist for the same category, then sum them and keep one entry for the same worker-office-week-job-category
duplicates drop
bys payrollno offid_nu startdate end job paytype: egen stot = sum(totalpaid)
count if totalpa!=stot
list if totalpa!=stot
drop totalpai
duplicates drop
rename stot pay_

*keep one obs per worker-startdate-office-job
reshape wide pay_, i(payrollno offid_nu startdate end job) j(paytype) string
sort payrollno startdate offid job

/* *tag a worker as salary workers if the person has any non-missing + amount under salary */
/* gen salary = pay_SALARY!=. & pay_SALARY > 0 */
/* tab salary */
/* *only 2.3 % of the obs are salary - must not be true */
/* drop salary */

tempfile tmp2
save `tmp2'


* Issues:
*how many offices does a worker work for on each week on average?
*how many different jobs does a worker work for on each week on avg?


*some people get paid > once under different job titles on the same week by the same office
use `tmp2', clear

*tag worker-week cells in which worker had multiple job titles
sort payrollno startdate job offid
bys payrollno startdate job: gen i = _n==1
bys payrollno startdate: egen njobs = sum(i)
drop i
tab njobs
* 1.4% has 2-3 jobs listed on the same week

*tag worker-week cells in which worker worked in multiple offices
sort payrollno startdate offid job
bys payrollno startdate offid: gen i = _n==1
bys payrollno startdate: egen noffices = sum(i)
drop i
tab noffices
*20% has 2-3 offices (up to 7 offices) listed on the same week

*create a worker-week-office-job-level data
sort payrollno startd offid_nu job
rename job paydisc
lab var paydisc "Discipline classification in the weekly pay data: FS HHA LPN MSW OT PT RN ST"

tab paydisc
/**aggregate discipline to match the discipline categories showing up in visit-level data; LPN should be SN & RN should be SN
replace disc = "SN" if discip=="RN" | discip=="LPN"

*collapse to the newly defined discipline for each worker-week-office
collapse (sum) pay_*, by(payrollno offid_nu startdate njobs noffices paydisc )*/

save `path'/pay_bywwod, replace





*---------

*want to recode the job for each worker-week-office cell to one that earns more money
duplicates tag payrollno offid startdate, gen(dup)
tab dup

*list if payr=="204210"

sort offid payrollno startdate
*list payrollno workerID offid primary startdate *date pay* dup if dup > 0 in 1/100
*these dup > 1 guys appear > once on the same startdate-office under different priamry jobs -> the worker played different roles on that week; use the priamry job for which pay is higher
egen pay = rowtotal(pay_*)
bys payrollno offid startdate: egen mm = max(pay)
gen more = pay==mm
*gen jobtitle_paid = job
gsort payrollno offid startdate -more
bys payrollno offid startdate: replace job = job[_n-1] if more==0 & dup > 0
gen job_recoded = dup > 0
foreach v of varlist pay_* pay {
    bys payrollno offid startdate: egen s_`v' = sum(`v')
}
drop pay_* mm more dup pay
duplicates drop

*there are some guys who have same payment under different job titles
*manually recode the job title by looking up in the cross-sectional worker data
duplicates tag payrollno offid startdate, gen(dup)
tab dup
tab payr if dup > 0
*there are 48 obs with dup ==1
*list if dup > 0

*manually recode the job title
replace job = "LPN" if payr=="204210" & job!="LPN" & dup==1
replace job = "LPN" if payr=="100406" & job!="LPN" & dup==1
replace job = "LPN" if payr=="209571" & job!="LPN" & dup==1
replace job = "LPN" if payr=="215358" & job!="LPN" & dup==1
replace job = "LPN" if payr=="219197" & job!="LPN" & dup==1
replace job = "LPN" if payr=="221706" & job!="LPN" & dup==1
replace job = "LPN" if payr=="234058" & job!="LPN" & dup==1
replace job = "LPN" if payr=="322744" & job!="LPN" & dup==1
replace job = "LPN" if payr=="325434" & job!="LPN" & dup==1
replace job = "LPN" if payr=="326771" & job!="LPN" & dup==1
replace job = "LPN" if payr=="329117" & job!="LPN" & dup==1
replace job = "RN" if payr=="333900" & job!="RN" & dup==1
replace job = "LPN" if payr=="340209" & job!="LPN" & dup==1
replace job = "PT" if payr=="347974" & job!="PT" & dup==1
replace job = "PTA" if payr=="354630" & job!="PTA" & dup==1
replace job = "OTA" if payr=="356662" & job!="OTA" & dup==1
replace job = "OT" if payr=="379374" & job!="OT" & dup==1

duplicates drop
drop dup
duplicates tag payrollno offid startdate, gen(dup)
assert dup==0
drop dup


/* *merge with the up-to-date guaranteed pay amt and visit expected points data for each worker by GP ID */
/* merge m:1 payrollno using `path'/guaranteed_cs, keep(1 3) gen(m_guaranteed_cs) */

/* *merge with the start and end (if quit) of employment dates */
/* merge m:1 workerID using `path'/staff, keep(1 3) nogen */
/* *1220 obs has _m==2; 451 has _m=1; 0.4M has _m=3 */


sort payrollno startd offid_nu
order payrollno offid_nu paydisc startd enddate pay_REGUL pay_BON pay_TR
save `path'/pay_bywwod, replace

/* *aggregate the pay up to worker-week */
/* use `path'/pay_byworker_byweek, clear */



/* *----
*are there workers whose primary job title changed?
bys payrollno startd job: gen i = _n==1
bys payrollno startd: egen si = sum(i)
tab si if tag==1
*there are 5540 unique workers, & 7% of them have more than one primary job title
tab primary if si > 1 & tag==1
* I think the title means the role the worker played; I should ignore the primary job title .
drop i si tag

sort payrollno startd
list payrollno startd s_pay_* in 1/100 */
