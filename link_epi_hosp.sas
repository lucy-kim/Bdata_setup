*link the episode-level data with hospitalization data;

libname tmp "/home/hcmg/kunhee/Labor/Bayada_data";

options mprint;

options nocenter nodate nonumber linesize=max pagesize=max;

x "cd /home/hcmg/kunhee/Labor/Bayada_data";

* Convert to SAS data using Stat-transfer;
x "qrsh st hosp.dta hosp.sas7bdat -y";
x "qrsh st masterclientadmiss2.dta masterclientadmiss2.sas7bdat -y";
x "qrsh st hosp.dta hosp.sas7bdat -y";

proc contents data=tmp.masterclientadmiss2;
proc contents data=tmp.hosp;
proc print data=tmp.masterclientadmiss2 (obs=20);
run;

data hosp;
    set tmp.hosp (keep= clientid hospdate_e);
run;

*merge the episode-level data with the hospitliation date data by whether hosp date falls between the episode end date;
proc sql;
    create table test as
        select a.*, b.hospdate_e
        from tmp.masterclientadmiss2 a
        left join hosp b
        on (a.clientid = b.clientid and a.epidate2 <= b.hospdate_e and a.epienddate >= b.hospdate_e);
    create table tmp.epi_hosp as
        select distinct * from test;
    quit;

proc contents data=tmp.epi_hosp;
proc print data=tmp.epi_hosp (obs=20);
run;

* Convert back to Stata data;
x "qrsh st epi_hosp.sas7bdat epi_hosp.dta -y";
