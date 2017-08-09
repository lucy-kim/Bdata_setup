*analyze the relationship b/w weekly pay and tenure for (field) SN workers
*for pay, use 1) salary for salaried workers; 2) pay rate for piece-rate workers; 3) per-visit rate (= default visit rate for SA workers or pay rate for PR workers)
*fix a status, e.g. only compare among VFTs

loc path /home/hcmg/kunhee/Labor/Bayada_data
cd `path'
loc gph /home/hcmg/kunhee/Labor/gph

loc labVFT "Full time salaried"
loc labVPB "Part time salaried with benefit"
loc labVPC "Part time salaried without benefit"
loc labVPD "Piece-rate paid"


use daily_workerpanel, clear

keep if majordisc=="SN"

keep if status=="VFT" | status=="VPB" | status=="VPC" | status=="VPD"

keep status salary esd monday payrollno majordisc productivity
duplicates drop

*on each week , what is the tenure up to that point?
gen tenure = monday - esd + 1
replace tenure = tenure / 30

foreach f in "VFT" "VPB" "VPC" {
    binscatter salary tenure if tenure < 120, title("`lab`f''") line(qfit) by(status)
    graph save `gph'/bs_salary_`f', asis replace
}

foreach f in "VFT" "VPB" "VPC" {
    binscatter productivity tenure if status=="`f'" & tenure < 120, title("`lab`f''") line(qfit) by(majordisc)
    graph save `gph'/bs_prod_`f', asis replace
}

*pay rate
use daily_workerpanel, clear

keep if majordisc=="SN" | majordisc=="PT"

keep if status=="VFT" | status=="VPB" | status=="VPC" | status=="VPD"

collapse (mean) payrate , by(payrollno monday esd majordisc status)
duplicates drop

*on each week, what is the tenure up to that point?
gen tenure = monday - esd + 1
replace tenure = tenure / 30

foreach f in "VFT" "VPB" "VPC" "VPD" {
    binscatter payrate tenure if status=="`f'" & tenure < 120, title("`lab`f''") line(qfit) by(majordisc)
    graph save `gph'/bs_payrate_`f', asis replace
}

*correlation b/w pay rate & per-visit rate
use daily_workerpanel, clear

keep if majordisc=="SN" | majordisc=="PT"

keep if status=="VFT" | status=="VPB" | status=="VPC"

gen pervisit_rate = salary/productivity

collapse (mean) payrate pervisit_rate, by(payrollno monday esd majordisc status)
duplicates drop

bys status: corr payrate pervisit_rate

*--------------------------------------
*per-visit rate for VFT, VPC, VPD

use daily_workerpanel, clear

keep if majordisc=="SN" | majordisc=="PT"

keep if status=="VFT" | status=="VPB" | status=="VPC"

keep status salary esd monday payrollno majordisc productivity
duplicates drop

*on each week , what is the tenure up to that point?
gen tenure = monday - esd + 1
replace tenure = tenure / 30

*per-visit rate
gen pervisit_rate = salary/productivity

loc labVFT "Full time salaried"
loc labVPB "Part time salaried with benefit"
loc labVPC "Part time salaried without benefit"

foreach f in "VFT" "VPB" "VPC" {
    binscatter pervisit_rate tenure if status=="`f'" & tenure < 120, title("`lab`f''") line(qfit) by(majordisc)
    graph save `gph'/bs_pvr_`f', asis replace
}

*--- on my Mac

foreach f in "VFT" "VPB" "VPC" {
    foreach v in "prod" "salary" "pvr" {
        graph use bs_`v'_`f'.gph
        graph export bs_`v'_`f'.png, replace
    }
}

foreach f in "VFT" "VPB" "VPC" "VPD" {
    loc v "payrate"
    graph use bs_`v'_`f'.gph
    graph export bs_`v'_`f'.png, replace
}

*--------------------------------
*what is the productivity trend over time for salaried workers?
use daily_workerpanel, clear

keep if majordisc=="SN" | majordisc=="PT"

keep if status=="VFT" | status=="VPB" | status=="VPC" | status=="VPD"

collapse (sum) wnv = dnv , by(payrollno status monday esd salary productivity majordisc)

gen prod_met = 100*wnv / productivity
lab var prod_met "Percentage of visit productivity goals met"

gen yr = year(monday)
gen mo = month(monday)
gen ym = ym(yr, mo)
format ym %tm
drop if ym==ym(2011,12)

forval y = 2012/2015 {
    loc jan `jan' `=mdy(1,1,`y')'
    loc jul `jul' `=mdy(7,1,`y')'
}

loc tprod_met "Percentage of visits provided relative to productivity goal [%]"

foreach f in "VFT" "VPB" "VPC" {
    foreach v in "prod_met" "productivity" {
        binscatter `v' monday if status=="`f'", title("`lab`f''") yti("`t`v''", size(small)) xti("Week") line(qfit) by(majordisc) xlabel(`jul', format(%tdCY) noticks) xtick(`jan', tlength(*1.5))
        graph save `gph'/bs_`v'_`f'_trend, asis replace
    }
}

loc f "VPD"
foreach v in "wnv" {
    binscatter `v' monday if status=="`f'", title("`lab`f''") yti("Number of visits", size(small)) xti("Week") line(qfit) by(majordisc) xlabel(`jul', format(%tdCY) noticks) xtick(`jan', tlength(*1.5))
    graph save `gph'/bs_`v'_`f'_trend, asis replace
}

foreach f in "VFT" "VPB" "VPC" {
    foreach v in "prod_met" "productivity" {
        graph use bs_`v'_`f'_trend.gph
        graph export bs_`v'_`f'_trend.png, replace
    }
}
loc f "VPD"
loc v "wnv"
graph use bs_`v'_`f'_trend.gph
graph export bs_`v'_`f'_trend.png, replace

*-------------------------------
*number of people who quit or fired for salaried workers for each discipline
use daily_workerpanel, clear

keep if majordisc=="SN" | majordisc=="PT"

/*keep if status=="VFT" | status=="VPB" | status=="VPC" | status=="VPD" | status=="NOT EMPLOYEE-CONTRACTOR"*/

*for each office-day, how many people quit / fired
sort offid_nu visitdate payrollno
bys offid_nu visitdate payrollno: gen i = _n==1
replace i = . if worked==0

*tag all field workers by empl arrangement
replace status = "CT" if status=="NOT EMPLOYEE-CONTRACTOR"
foreach f in "VFT" "VPB" "VPC" "VPD" "CT" {
    gen nw_`f' = i
    replace nw_`f' = . if status!="`f'"
}

*tag all salaried & piece-rate workers
foreach f in "salaried" "piecerate" {
    gen nw_`f' = i
    replace nw_`f' = . if `f'!=0
}

*tag field new hires
foreach f in "VFT" "VPB" "VPC" "VPD" "CT" {
    gen nh_`f' = newhire==1 & visitdate==esd & status=="`f'"
}

*tag field quitters
foreach f in "VFT" "VPB" "VPC" "VPD" "CT" {
    gen quit_`f' = attrited==1 & visitdate==etd & status=="`f'" & fired==0
}

*tag field layoffs
foreach f in "VFT" "VPB" "VPC" "VPD" "CT" {
    gen layoff_`f' = attrited==1 & visitdate==etd & status=="`f'" & fired==1
}

*rate of absence:
* tag absent workers
foreach f in "VFT" "VPB" "VPC" "VPD" "CT" {
    gen absent_`f' = absent==1 & status=="`f'"
}

collapse (sum) nw_all = i nw_salaried nw_piecerate nw_VFT nw_VPB nw_VPC nw_VPD nw_CT quit_* layoff_* absent_* nh_* , by(offid_nu visitdate majordisc)

tempfile empl
save `empl'

*restrict to offices that stay throughout
use office_flow, clear
*keep offices that opened before 2012
drop if year(opendate) >= 2012
*keep offices that didn't close
keep if office_act=="Active"
count

drop addr_bld addr_str addr_ci addr_zip office_active closeddate
tempfile office
save `office'

use `empl', clear
merge m:1 offid_nu using `office', keep(3) nogen

*what is the first week and last week for each office?
bys offid_nu: egen last = max(visitdate)
bys offid_nu: egen first = min(visitdate)
format last %d
format first %d

preserve
keep offid_nu last first
duplicates drop
drop if last < mdy(9,7,2015) | first > mdy(1,30,2012)
keep offid_nu
tempfile insmpl
save `insmpl'
restore

*48 offices that stay throughout 1/30/2012 - 9/7/2015
merge m:1 offid_nu using `insmpl', keep(3) nogen
drop first last

keep if visitdate <= mdy(9,7,2015) & visitdate >= mdy(1,30,2012)

*get office-day # episodes
preserve
use visit_worker_chars, clear
assert visitdate!=.

*since I do not have office IDs for a majority of visits in 2015 Q4, drop Q4 2015
drop if yr==2015 & qtr==4

*fill in missing values in office ID for the worker if she has office ID for any visits
gsort payrollno visitdate -offid_nu
bys payrollno: replace offid_nu = offid_nu[_n-1] if offid_nu >= .
assert offid_nu!=.

sort payrollno offid_nu visitdate

tempfile t
save `t'

*for each office, first visit date & last visit date
bys offid_nu epiid: egen fvd = min(visitdate)
bys offid_nu epiid: egen lvd = max(visitdate)
gen g = lvd - fvd + 1
keep offid_nu epiid fvd lvd g
duplicates drop
expand g
sort offid_nu epiid
bys offid_nu epiid: gen visitdate_e = fvd + _n - 1

keep offid_nu visitdate epiid
duplicates drop
gen i = 1
collapse (sum) allepi = i, by(offid_nu visitdate)
tempfil od
save `od'

collapse (mean) allepi, by(offid_nu)
xtile qt = allepi, n(4)
tempfile qt
save `qt'
restore

merge m:1 offid_nu using `qt', nogen
assert qt!=.

foreach f in "VFT" "VPB" "VPC" "VPD" "CT" {
    gen ar_`f' = absent_`f'/nw_`f'
}

compress
save nw_byod, replace
outsheet using nw_byod.csv, comma names replace

forval y = 2012/2015 {
    loc jan `jan' `=mdy(1,1,`y')'
    loc jul `jul' `=mdy(7,1,`y')'
}

loc labCT "Contactor"
loc yt_layoff "Number of layoffs"
loc yt_quit "Number of quits"
loc yt_ar "Fraction of absent workers"

*"VPB" "VPC" "VPD" "CT"
foreach f in "VFT" {
    loc v layoff
    binscatter `v'_`f' visitdate if majordisc=="SN", title("`lab`f''") yti("`yt_`v''") by(qt) line(qfit) xti("Date") xlabel(`jul', format(%tdCY) noticks) xtick(`jan', tlength(*1.5))
    graph save `gph'/`v'_`f'_trend, asis replace
}

foreach f in "VFT" {
    loc v quit
    binscatter `v'_`f' visitdate if majordisc=="SN", title("`lab`f''") yti("`yt_`v''") by(qt) line(qfit) xti("Date") xlabel(`jul', format(%tdCY) noticks) xtick(`jan', tlength(*1.5))
    graph save `gph'/`v'_`f'_trend, asis replace
}

foreach f in "VPC" "VPB" "VPD" {
    loc v ar
    binscatter `v'_`f' visitdate if majordisc=="SN", title("`lab`f''") yti("`yt_`v''") by(qt) line(qfit) xti("Date") xlabel(`jul', format(%tdCY) noticks) xtick(`jan', tlength(*1.5))
    graph save `gph'/`v'_`f'_trend, asis replace
}

*"quit" "layoff"
foreach f in "VPC" "VPB" "VPD" {
    foreach v in "ar" {
        graph use `v'_`f'_trend.gph
        graph export `v'_`f'_trend.png, replace
    }
}



*pay by the month of employment for new hires




*for salaried workers, do workers with low productivity goal quit more?
