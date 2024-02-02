

source("~/connect-csl.R")
source("~/connect-ntsp.R")

library(dplyr)

tbl(csl, "compound") %>% filter(name == "Eprosartan")

es_add_identifiers(escon = escon, sdb = csl, "g2_dbas_sachsen", compoundLimit = "Eprosartan")
