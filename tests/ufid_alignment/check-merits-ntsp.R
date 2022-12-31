
# check quality of alignment

library(logger)

VERSION <- "2022-11-13"
index <- commandArgs(TRUE)
config_path <- "~/config.yml"

# Check args
if (!is.character(index) || length(index) != 1 || !grepl("^g2_nts", index))
  stop("Incorrect index specification")

ec <- config::get("elastic_connect", file = config_path)
escon <- elastic::connect(
  host = 'elastic.dmz.bafg.de', 
  port = 443, user=ec$user, 
  pwd  = ec$pwd,
  transport_schema = "https"
)

log_info("----- check-merits-ntsp.R v{VERSION} -----")
log_info("Checking merits on {index}")

# do some checks
if (escon$ping()$cluster_name != "bfg-elastic-cluster") {
  stop("Connection to es-db not established")
}

# run tests (ufid and ufid2)
ntsportal::es_test_fpfn(escon, index)

