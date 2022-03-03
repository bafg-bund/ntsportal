# will clear and reassign both ucid types

index <- "g2_nts*"
config_path <- "~/config.yml"
ec <- config::get("elastic_connect", file = config_path)
escon <- elastic::connect(host = '10.140.73.204', user=ec$user, pwd=ec$pwd)

#message("Level 1 assignment on ", date())

#ntsportal::es_assign_ucids(escon, index = "g2_nts*", rtlim = 0.2)

message("Level 2 assignment on ", date())

ntsportal::es_assign_ucids(escon, index = "g2_nts*", rtlim = 0.2, ufidLevel = 2)

message("Done on ", date())
