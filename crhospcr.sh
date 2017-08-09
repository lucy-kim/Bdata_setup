#!/bin/bash
#$ -N sasjob01
#$ -j y
#$ -m e
sas -nodms -noterminal crhospcr.sas
