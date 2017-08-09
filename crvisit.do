*create visit data from CSV files

set linesize 100
local stdatapath /home/hcmg/kunhee/Labor/Bayada_data
cd `stdatapath'/visit_CSV

*load and save CSV files
forval yr = 2012/2014 {
    forval q = 1/4 {
        loc file "Q`q' `yr'"      
        insheet using "visit\Bayada Visit Data_`file'.csv", clear comma 
        drop if regexm(v1, "Quarter" "Visit Data") 

        lab var v1 "Episode ID"
        lab var v2 "Visit Date"
        lab var v3 "Worker ID"
        lab var v4 "Distance Driven Miles"
        lab var v5 "Drive Time Hours"
        lab var v6 "Time From Leaving Home To Doc Completion In Hours"
        lab var v7 "Time To Synch From Doc Completion In Hours"
        lab var v8 "Time To Synch In Hours"
        lab var v9 "Total Time Hours"
        lab var v10 "Visit In-House Hours"
        lab var v11 "Visit Productivity Points"
        lab var v12 "Visit Total Cost"
        lab var v13 "Visit Travel Cost"
        
        rename v1 epiid
        rename v2 visitdate
        rename v3 workerID
        rename v4 distance_driven
        rename v5 time_driven
        rename v6 time2Doc_fromhome
        rename v7 time2sync_fromDoc
        rename v8 time2sync
        rename v9 tot_time
        rename v10 visit_inhouse_hrs
        rename v11 visit_points
        rename v12 visit_tot_cost
        rename v13 visit_travel_cost
        
        drop if regexm(epiid, "Epi ID")
        gen qtr = `q'
        gen yr = `yr'       
        
        tempfile visit_Q`q'_`yr'
        save `visit_Q`q'_`yr''
    }
}

loc yr = 2011
forval q = 3/4 {
    loc file "Q`q' `yr'"      
    insheet using "visit\Bayada Visit Data_`file'.csv", clear comma 
    drop if regexm(v1, "Quarter" "Visit Data") 
    
    lab var v1 "Episode ID"
    lab var v2 "Visit Date"
    lab var v3 "Worker ID"
    lab var v4 "Distance Driven Miles"
    lab var v5 "Drive Time Hours"
    lab var v6 "Time From Leaving Home To Doc Completion In Hours"
    lab var v7 "Time To Synch From Doc Completion In Hours"
    lab var v8 "Time To Synch In Hours"
    lab var v9 "Total Time Hours"
    lab var v10 "Visit In-House Hours"
    lab var v11 "Visit Productivity Points"
    lab var v12 "Visit Total Cost"
    lab var v13 "Visit Travel Cost"
    
    rename v1 epiid
    rename v2 visitdate
    rename v3 workerID
    rename v4 distance_driven
    rename v5 time_driven
    rename v6 time2Doc_fromhome
    rename v7 time2sync_fromDoc
    rename v8 time2sync
    rename v9 tot_time
    rename v10 visit_inhouse_hrs
    rename v11 visit_points
    rename v12 visit_tot_cost
    rename v13 visit_travel_cost
    
    drop if regexm(epiid, "Epi ID")
    gen qtr = `q'
    gen yr = `yr'
    
    tempfile visit_Q`q'_`yr'
    save `visit_Q`q'_`yr''
}

loc yr = 2015
forval q = 1/1 {
    loc file "Q`q' `yr'"      
    insheet using "visit\Bayada Visit Data_`file'.csv", clear comma 
    drop if regexm(v1, "Quarter" "Visit Data") 
    
    lab var v1 "Episode ID"
    lab var v2 "Visit Date"
    lab var v3 "Worker ID"
    lab var v4 "Distance Driven Miles"
    lab var v5 "Drive Time Hours"
    lab var v6 "Time From Leaving Home To Doc Completion In Hours"
    lab var v7 "Time To Synch From Doc Completion In Hours"
    lab var v8 "Time To Synch In Hours"
    lab var v9 "Total Time Hours"
    lab var v10 "Visit In-House Hours"
    lab var v11 "Visit Productivity Points"
    lab var v12 "Visit Total Cost"
    lab var v13 "Visit Travel Cost"
    
    rename v1 epiid
    rename v2 visitdate
    rename v3 workerID
    rename v4 distance_driven
    rename v5 time_driven
    rename v6 time2Doc_fromhome
    rename v7 time2sync_fromDoc
    rename v8 time2sync
    rename v9 tot_time
    rename v10 visit_inhouse_hrs
    rename v11 visit_points
    rename v12 visit_tot_cost
    rename v13 visit_travel_cost
    
    drop if regexm(epiid, "Epi ID")
    gen qtr = `q'
    gen yr = `yr'
    
    tempfile visit_Q`q'_`yr'
    save `visit_Q`q'_`yr''
}

*replace the old 2015 Q2 data with the new one and adding new 2015 Q3 data
loc yr = 2015
forval q = 2/3 {
    loc file "Q`q' `yr'"  
    insheet using "visit\Bayada Visit Data Q2 & Q3 2015_`file'.csv", clear comma
    drop if regexm(v1, "Quarter" "Visit Data") 
    
    lab var v1 "Episode ID"
    lab var v2 "Visit Date"
    lab var v3 "Worker ID"
    lab var v4 "Distance Driven Miles"
    lab var v5 "Drive Time Hours"
    lab var v6 "Time From Leaving Home To Doc Completion In Hours"
    lab var v7 "Time To Synch From Doc Completion In Hours"
    lab var v8 "Time To Synch In Hours"
    lab var v9 "Total Time Hours"
    lab var v10 "Visit In-House Hours"
    lab var v11 "Visit Productivity Points"
    lab var v12 "Visit Total Cost"
    lab var v13 "Visit Travel Cost"
    
    rename v1 epiid
    rename v2 visitdate
    rename v3 workerID
    rename v4 distance_driven
    rename v5 time_driven
    rename v6 time2Doc_fromhome
    rename v7 time2sync_fromDoc
    rename v8 time2sync
    rename v9 tot_time
    rename v10 visit_inhouse_hrs
    rename v11 visit_points
    rename v12 visit_tot_cost
    rename v13 visit_travel_cost
    
    drop if regexm(epiid, "Epi ID")
    gen qtr = `q'
    gen yr = `yr'

    tempfile visit_Q`q'_`yr'
    save `visit_Q`q'_`yr''
}

*create appended visit data that have visits from all the quarters Q3 2011 - Q3 2015
cd `stdatapath'
use `visit_Q1_2012'
forval q = 2/4 {
    append using `visit_Q`q'_2012'
}
forval yr = 2013/2014 {
    forval q = 1/4 {
        append using `visit_Q`q'_`yr''
    }
}
loc yr = 2011
forval q = 3/4 {
    append using `visit_Q`q'_`yr''
}
loc yr = 2015
forval q = 1/3 {
    append using `visit_Q`q'_`yr''
}
    
sort yr qtr visitdate workerID

capture drop  v14
*v14 some variable that has no value but entered accidentally from excel 

lab var yr "Year of visit"
lab var qtr "Quarter of visit"

*destring variables
destring distance_driven time_driven time2Doc time2sync* tot_time visit_*, replace ig("," "$")

*epiid is numeric in the client admissions file
destring epiid, replace

*convert visit date variable
split visitdate, p(/)
gen mo = visitdate1
gen day = visitdate2
gen yr2 = visitdate3
tab yr2
replace yr2 = "20"+yr2
replace yr2 = "" if visitdate==""
destring mo day yr2, replace
gen visitdate_e = mdy(mo,day,yr2)
format visitdate_e %d
drop visitdate? visitdate mo day yr2
lab var visitdate_e "Visit date"

duplicates tag, gen(dup)
assert dup==0
drop dup

sort yr qtr visitdate workerID

compress
save visit_allqtrs, replace
