

# Script to process remaining files in msrawfiles, which have not been processed yet.

# WARNING: Make a copy of this document for changes and testing, do not edit this script 
# (or even better, create a branch)

# The script will create batches from the new files by splitting the data by 
# directory
# If no new blanks are in the batches, these will be added to using the blanks
# that are already in the directory from which the files were added

# If files need to be reprocessed, this can be done by removing the field
# "dbas_last_eval" from the msrawfiles doc. This means that any data from
# this file will be removed in dbas index and the file will be reprocessed 
# (including ingest).

# usage (from ntsportal):
# nohup Rscript scripts/eval-new-rawfiles-dbas.R &> /scratch/nts/logs/$(date +%F)_dbas_eval.log &
# tail -f /scratch/nts/logs/$(date +%F)_dbas_eval.log

# If there is an error in processing a file, you can use the function 
# ntsportal::reset_eval, which will remove the last processing date and
# and therefore the file will be processed again

# Variables ####



ntsportal::screeningProcessAllStepsDbas()

