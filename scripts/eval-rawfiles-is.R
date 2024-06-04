

# nohup Rscript scripts/eval-rawfiles-is.R &> ~/scratch_nts/logs/$(date +%y%m%d)_is_eval.log &
# tail -f ~/scratch_nts/logs/$(date +%y%m%d)_is_eval.log
# see crontab -e for processing

logger::log_info("Processing files for IS (process_is_all)")
library(ntsportal)
source("~/connect-ntsp.R")

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

# Copyright 2016-2024 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
# ntsportal is free software: you can redistribute it and/or modify it under the 
# terms of the GNU General Public License as published by the Free Software 
# Foundation, either version 3 of the License, or (at your option) any 
# later version.
# 
# ntsportal is distributed in the hope that it will be useful, but WITHOUT ANY 
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS 
# FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License along 
# with ntsportal. If not, see <https://www.gnu.org/licenses/>.