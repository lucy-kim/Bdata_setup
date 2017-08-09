*create hospital panel data for each calendar year

*append all years of Cost report data
use hospcr2011, clear
forval y = 2012/2016 {
    append using hospcr`y'
}
assert prov_num!=""
assert fyear!=.
duplicates tag prov_num fyear, gen(dup)
tab dup

*to have one report per fiscal year, keep the record whose fi_rcpt_dt is later; if same, whose fi_creat_dt is later
foreach v of varlist fi_rcpt_dt fi_creat_dt{
    bys prov_num fyear: egen m_`v' = max(`v') if dup > 0
}
drop if m_fi_rcpt_dt!=fi_rcpt_dt & dup > 0
drop if dup > 0 & m_fi_rcpt_dt==fi_rcpt_dt & m_fi_creat_dt!=fi_creat_dt

drop dup m_*
duplicates tag prov_num fyear, gen(dup)
assert dup==0
drop dup

*merge with state name by state SSA code
rename state ssa_state_cd
destring ssa, replace
merge m:1 ssa_st using states4hospcr, keep(3) nogen

*drop variables that are missing in all obs
drop fy_end_dt fy_bgn_dt adr_vndr_cd fi_num trnsmtl_num rpt_stus_cd

compress
save hospcr_panel, replace
