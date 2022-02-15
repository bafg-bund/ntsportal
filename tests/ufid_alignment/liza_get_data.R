library(ntsportal)
index <- "g2_nts_expn"
path_ufid_db <- "~/HRMS_Z/sw_entwicklung/ntsportal/ufid1.sqlite"
config_path <- "~/config.yml"
ec <- config::get("elastic_connect", file = config_path)
escon <- elastic::connect(host = '10.140.73.204', user=ec$user, pwd=ec$pwd)

test <- get_time_series(escon, index = "g2_nts_expn",
                station = "KOMO",
                startRange = c("2021-01-01", "2022-01-01"),
                responseField = "intensity",
                ufidLevel = 2,
                form = "long")
