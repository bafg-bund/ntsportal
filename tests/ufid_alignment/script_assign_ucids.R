# will clear and reassign both ucid types

index <- "g2_nts*"
config_path <- "~/config.yml"
ec <- config::get("elastic_connect", file = config_path)
escon <- elastic::connect(host = 'elastic.dmz.bafg.de', port = 443, user=ec$user, pwd=ec$pwd,
                          transport_schema = "https", ssl_verifypeer = FALSE)

#message("Level 1 assignment on ", date())

#ntsportal::es_assign_ucids(escon, index = "g2_nts*", rtlim = 0.2, ufidLevel = 1)

message("Level 2 assignment on ", date())

ntsportal::es_assign_ucids(escon, index = "g2_nts*", rtlim = 0.2, ufidLevel = 2)

message("Done on ", date())
