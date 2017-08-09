*create discharge date data 

loc path /home/hcmg/kunhee/Labor/Bayada_data
cd `path'
local mvar admissionclientsocid

use "Rev_DC Date DB_v2", clear
format `mvar' %11.0f
duplicates tag `mvar', gen(dup)
count if dup > 0
*there are 53K obs that appear twice, once with missing DC date and once without missing DC date -- double check this
bys `mvar': gen nm = dcdate!=.
bys `mvar': egen nms = sum(nm)
*list if dup==1 in 1/100
assert nms==1 if dup==1
sum nms

*there are 42 obs that have two different DC dates -- drop them
drop if nms>1 & dup==1
assert nms==1 if dup==1
*drop the missing DC date obs if the corresponding client-SOC ID has a nonmissing DC date
drop if dup==1 & nm==0
drop nm nms

*there are 50 obs that appear 3 times, once with missing DC date and twice with different DC dates (sometimes the DC dates are different by 1 day but for others different by months) - drop them 
/* list if dup==2 */
drop if dup==2

drop dup
duplicates tag `mvar', gen(dup)
assert dup==0
*now, the DC date data have one obs per admission-SOC ID
drop dup

drop clientid socdate_e
save dischrg, replace
