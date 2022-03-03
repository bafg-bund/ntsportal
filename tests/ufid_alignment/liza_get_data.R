library(ntsportal)
index <- "g2_nts_expn"
path_ufid_db <- "~/projects/ufid/tests/ufid1.sqlite"
config_path <- "tests/ufid_alignment/config.yml"
path_ufid_db <- "~/HRMS_Z/sw_entwicklung/ntsportal/ufid1.sqlite"
#config_path <- "~/config.yml"
ec <- config::get("elastic_connect", file = config_path)

escon <- elastic::connect(host = 'elastic-mn-01.hpc.bafg.de', port = 9200, user=ec$user, pwd=ec$pwd, transport_schema = "https")
escon$ping()

test <- get_time_series(escon, index = "g2_nts_expn",
                station = "KOMO",
                startRange = c("2021-01-01", "2022-01-01"),
                responseField = "intensity",
                ufidLevel = 2,
                form = "long")
