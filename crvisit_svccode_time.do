*create visit service code & time data for each visit from CSV files

loc path /home/hcmg/kunhee/Labor/Bayada_data
cd `path'/visit_CSV

*create service code description data from a CSV file
do crsvccode_desc

forval yr = 2012/2015 {
    forval q = 1/4 {
        if `yr'!=2014 | `q'!=1 {
            insheet using "Bayada Visit Time & Service Codes `yr'_Q`q' `yr'.csv", clear comma names

            capture drop if epiid=="Epi ID"
        
            rename date visitdate
            rename workerid workerID
            rename servicecode svccode
            rename time visittime
            rename totaltime tot_time
            rename visitinhouse visit_inhouse_hrs
            rename visitproductiv visit_points
            
            lab var epiid "Episode ID"
            lab var visitdate "Visit Date"
            lab var visittime "Visit time"
            
            foreach v of varlist epiid workerID visitnum tot_time visit_inhouse visit_points {
                capture destring `v', replace
            }
            drop if epiid==.
            
            gen qtr = `q'
            gen yr = `yr'
            
            des
            tempfile visit_Q`q'_`yr'
            save `visit_Q`q'_`yr''            
        }
        else {
            insheet using "Bayada Visit Time & Service Codes `yr' Q`q'_Q`q' `yr'.csv", clear comma names
            capture drop if epiid=="Epi ID"
        
            rename date visitdate
            rename workerid workerID
            rename servicecode svccode
            rename time visittime
            rename totaltime tot_time
            rename visitinhouse visit_inhouse_hrs
            rename visitproductiv visit_points
            
            lab var epiid "Episode ID"
            lab var visitdate "Visit Date"
            lab var visittime "Visit time"
            
            foreach v of varlist epiid workerID visitnum tot_time visit_inhouse visit_points {
                capture destring `v', replace
            }
            drop if epiid==.
            
            gen qtr = `q'
            gen yr = `yr'
            
            des
            tempfile visit_Q`q'_`yr'
            save `visit_Q`q'_`yr''         
        }
    }
}

*drop irrelevant vars 
use `visit_Q2_2014', clear
drop v6 v7
tempfile visit_Q2_2014
save `visit_Q2_2014'

use `visit_Q1_2014', clear
drop v5 v6
tempfile visit_Q1_2014
save `visit_Q1_2014'


*append data
use `visit_Q1_2012', clear
forval q = 2/4 {
    append using `visit_Q`q'_2012'
}
forval yr = 2013/2015 {
    forval q = 1/4 {
        append using `visit_Q`q'_`yr''
    }
}
*sort yr qtr visitdate workerID

lab var yr "Year of visit"
lab var qtr "Quarter of visit"
des

*convert visit date variable
split visitdate, p(/)
gen mo = visitdate1
gen day = visitdate2
gen yr2 = visitdate3
tab yr2
replace yr2 = "" if visitdate==""
destring mo day yr2, replace
gen visitdate_e = mdy(mo,day,yr2)
format visitdate_e %d
drop visitdate? visitdate mo day yr2
lab var visitdate_e "Visit date"

sort epiid visitdate
order epiid visitdate workerID

*drop visit sequence number (meaningless)
drop visitnumber

*Add in the service code description & discipline
rename svccode servicecode
merge m:1 servicecode using `path'/svccode_desc.dta, keep(1 3) nogen

*drop irrelevant vars
drop includehospice electronic excludefrommi subsequent death transfer roc fu averagevisit billingdes cptcode excludefromrapi pointcareformat active

*description, jobcode, discipline seem most relevant
tab jobcode
tab discipline
*discipline (in the decreasing order of % visits): SN, PT, OT, HHA, ST, HHA, ST, MSW, FS, RD
tab jobcode servicecode if discipline=="SN"

*create Stata time variable
gen s = 0
split visittime, p(":")
destring visittime?, replace

drop visittime_e
gen visittime_e = dhms(visitdate, visittime1, visittime2, s)
*gen visittime_e = hms(visittime1, visittime2, s)
format visittime_e %tc
drop visittime visittime? s

cd `path'
compress
save visit_svccode_time, replace
