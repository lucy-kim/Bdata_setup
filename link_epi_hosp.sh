#!/bin/bash
#$ -N sasjob01
#$ -j y
#$ -m e
sas -nodms -noterminal link_epi_hosp.sas
