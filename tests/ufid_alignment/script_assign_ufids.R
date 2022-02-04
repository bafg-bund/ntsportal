# process the entire index to assign ufid to all features


library(ntsportal)
index <- "g2_nts_expn"
path_ufid_db <- "~/projects/ufid/tests/ufid1.sqlite"
config_path <- "~/projects/config.yml"
ec <- config::get("elastic_connect", file = config_path)
escon <- elastic::connect(host = '10.140.73.204', user=ec$user, pwd=ec$pwd)

# check for duplicates
if (!es_no_duplicates(escon, index))
  stop("Duplicate features found")

udb <- DBI::dbConnect(RSQLite::SQLite(), path_ufid_db)

message("------------started pos at ", Sys.time())

#message("initial pass through ufid-db")
tryCatch(es_assign_ufids(escon, udb, index, "pos"),
         error = function(e) {
           message(e, " error at ", Sys.time())
           DBI::dbDisconnect(udb)
         })


#new_ufid <- ubd_new_ufid(udb, escon, index, "pos")
message("looking for new clusters")
success <- tryCatch(ubd_new_ufid(udb, escon, index, "pos"),
                   error = function(e) {
                     message(e, " error at ", Sys.time())
                     DBI::dbDisconnect(udb)
                   })

if (success) {
  # sometimes we see a disconnection of DB, so just connect again
  udb <- DBI::dbConnect(RSQLite::SQLite(), path_ufid_db)
  message("no more features found to assign")
  message("final pass through ufid-db")
  # pass through all ufids one more time for good measure
  tryCatch(es_assign_ufids(escon, udb, index, "pos"),
           error = function(e) {
             message(e, " error at ", Sys.time())
             DBI::dbDisconnect(udb)
           })
}

message("----------ended pos without error at ", Sys.time())


message("------------started neg at ", Sys.time())
message("initial pass through ufid-db")
udb <- DBI::dbConnect(RSQLite::SQLite(), path_ufid_db)
tryCatch(es_assign_ufids(escon, udb, index, "neg"),
         error = function(e) {
           message(e, " error at ", Sys.time())
           DBI::dbDisconnect(udb)
         })
message("looking for new clusters")
success <- tryCatch(ubd_new_ufid(udb, escon, index, "neg"),
                       error = function(e) {
                         message(e, " error at ", Sys.time())
                         DBI::dbDisconnect(udb)
                       })

if (success) {
  udb <- DBI::dbConnect(RSQLite::SQLite(), path_ufid_db)
  message("no more features found to assign")
  message("final pass through ufid-db")
  # pass through all ufids one more time for good measure
  tryCatch(es_assign_ufids(escon, udb, index, "neg"),
           error = function(e) {
             message(e, " error at ", Sys.time())
             DBI::dbDisconnect(udb)
           })
}

message("----------ended neg without error at ", Sys.time())

DBI::dbDisconnect(udb)

message("Disconnected DB")
