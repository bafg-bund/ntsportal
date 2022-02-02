# addition of inchy-key

library(DBI)
library(dplyr)
index <- "g2_dbas_v5_upb"
config_path <- "~/projects/config.yml"
ec <- config::get("elastic_connect", file = config_path)
escon <- elastic::connect(host = '10.140.73.204', user=ec$user, pwd=ec$pwd)

db <- DBI::dbConnect(RSQLite::SQLite(), "~/sqlite_local/MS2_db_v7.db")
dbListFields(db, "compound")

# first create lookup table, name -> inchikey

comps <- tbl(db, "compound") %>% select(name, SMILES, CAS) %>% collect()

webchem::cs_check_key()
webchem::cs_convert(comps$SMILES[2], "smiles", "inchi")
webchem::cs_convert(test, "inchi", "inchikey")

inchis <- webchem::cs_convert(comps$SMILES, "smiles", "inchi")
inchis <- character(nrow(comps))

for (i in 486:500) {
  inchis[i] <- webchem::cs_convert(comps$SMILES[i], "smiles", "inchi")
}

tb <- cbind(comps$name, comps$SMILES, inchis)

saveRDS(tb, "table_inchis_211001.RDS")
write.csv(tb, "table_smiles_inchis.csv")

comps$inchikey <- webchem::cs_convert(comps$SMILES, "SMILES", "InChiKey")

inchikeys <- vector("list", nrow(comps))

ik <- webchem::cts_convert(comps$CAS[3], "CAS", "InChIKey", "all")

for (i in 1:nrow(comps)) {
  inchikeys[[i]] <- webchem::cts_convert(comps$CAS[i], "CAS", "InChIKey", "all")
}

names(inchikeys) <- comps$name
saveRDS(inchikeys, "inchikeys_list.RDS")

inchikeys2 <- sapply(inchikeys, function(x) x[[1]])
saveRDS(inchikeys2, "inchikeys2_list.RDS")
DBI::dbDisconnect(db)
