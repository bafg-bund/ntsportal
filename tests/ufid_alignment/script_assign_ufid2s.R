
# Process all indices to assign ufid2s to all features

# usage 
# cd tests/ufid_alignment
# nohup Rscript script_assign_ufid2s.R &> /scratch/nts/logs/$(date +%F)_ufid2_assignment.log &
# nohup Rscript ../script_assign_ufid2s.R &> $(date +%F)_ufid2_all_assignment.log &

index <- "g2_nts_bfg,g2_nts_upb,g2_nts_lanuv"
#index <- "g2_nts_upb"
config_path <- "~/config.yml"
path_ufid_db <- "~/sqlite_local/ufid1.sqlite"

source("~/connect-ntsp.R")

# first clear current ufid2s
logger::log_info("Clearing previous ufid2s")
clear_ufids <- function() {
  elastic::docs_update_by_query(escon, index, refresh = "true", body =
'
{
  "query": {
    "exists": {
      "field": "ufid2"
    }
  },
  "script": {
    "source": "ctx._source.remove(\'ufid2\')",
    "lang": "painless"
  }
}
'
  )
}
repeat {
  failure <- FALSE
  tryCatch(
    res <- clear_ufids(),
    error = function(cnd) {
      logger::log_error("Error when clearing ufid2: {conditionMessage(cnd)}")
      failure <<- TRUE
      # wait 5 min and try again
      Sys.sleep(5)
    }
  )
  if (!failure && exists("res") && !is.null(res))
    break
}


logger::log_info("Cleared ufid2 of {res$total} docs")
# ufid2 alignment
logger::log_info("Starting ufid2_alignment")
ntsportal::ufid2_alignment(escon, index, rtTol = 2, mzTolmDa = 5, minPoints = 5, numCores = 20)

# run tests (ufid and ufid2)
# udb <- DBI::dbConnect(RSQLite::SQLite(), path_ufid_db)
# ntsportal::es_test_fpfn(escon, udb, index)
# DBI::dbDisconnect(udb)

logger::log_info("Completed script_assign_ufid2s")

