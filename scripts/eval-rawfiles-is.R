

# nohup Rscript scripts/eval-rawfiles-is.R &> ~/scratch_nts/logs/$(date +%y%m%d)_is_eval.log &
# tail -f ~/scratch_nts/logs/$(date +%y%m%d)_is_eval.log
# see crontab -e for processing

logger::log_info("Processing files for IS (process_is_all)")
library(ntsportal)
source("~/connect-ntsp.R")
#debugonce(process_is_all)
process_is_all(
  escon = escon, 
  rfindex = "g2_msrawfiles",
  isindex = "ntsp_alias_is_dbas_bfg",
  ingestpth = "~Jewell/projects/ntsportal/scripts/ingest.sh",
  configfile = "~/config.yml",
  tmpPath = "/scratch/nts/tmp", 
  numCores = 10
)

logger::log_info("Completed process_is_all")

