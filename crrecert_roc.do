*create data on recertifications and ROCs that each client had

loc path /home/hcmg/kunhee/Labor/Bayada_data
cd `path'/client_CSV

insheet using "Recertifications & ROC Episodes.csv", clear comma names
rename date socdate
rename v3 epidate

*convert date vars into Stata date vars
foreach n in socdate epidate {
  split `n', p("/")
  replace `n'3 = "20"+`n'3
  destring `n'?, replace float
  gen `n'_e = mdy(`n'1, `n'2, `n'3)
  format `n'_e %d
  drop `n'? `n'
}
rename epiid epiid_new
rename oasistype oasistype_new

*there are some duplicates in terms of all values other than the oasis type -> drop duplicates randomly
duplicates drop clientid epiid_ socd epid, force

duplicates tag clientid socdate epidate, gen(dup)
assert dup==0
drop dup

*drop if the SOC date is before 2011
drop if year(socdate) < 2012

save `path'/recert_roc, replace
