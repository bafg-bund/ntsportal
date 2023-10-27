

# nohup Rscript scripts/eval-rawfiles-is.R &> ~/scratch_nts/logs/$(date +%y%m%d)_is_eval.log &
# tail -f ~/scratch_nts/logs/$(date +%y%m%d)_is_eval.log
# see crontab -e for processing

source("~/connect-ntsp.R")
logger::log_info("Processing files for IS (process_is_all)")

ntsportal::process_is_all(
  escon = escon, 
  rfindex = "g2_msrawfiles",
  isindex = "g2_dbas_is_bfg",
  ingestpth = "/scratch/nts/ntsautoeval/ingest.sh",
  configfile = "~/config.yml",
  tmpPath = "/scratch/nts/tmp", 
  numCores = 10
)

