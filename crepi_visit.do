*merge the patient episode-hopsitalization date-admission-level data with the visit-level data -> beforehand, will have to collapse the patient data to episode-admission level data

loc path /home/hcmg/kunhee/Labor/Bayada_data
cd `path'

use client_chars2, clear

*merge with episode-visit level data that contain worker status & job title information
merge 1:m epiid using visit_worker_chars, keep(1 3) nogen
*4,332,163 have _m=3; 1,023,116 have _m=2; none have _m=1 (may be an artifact of my restriction earlier to patients who appear in visit data)

*1 obs has _m==1
count if visitdate==.
drop if visitdate==.
assert visitdate!=.

compress
save epi_visit, replace
