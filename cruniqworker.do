*what is the unique worker list for workers appearing in the visit data?

set linesize 100
local stdatapath /home/hcmg/kunhee/Labor/Bayada_data
cd `stdatapath'

use visit_allqtrs, clear
gen i = 1
collapse (sum) num = i, by(workerID)

compress
save uniqworker, replace
