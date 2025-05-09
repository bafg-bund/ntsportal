

inspectAnnotations <- function(knownAnnotationTablePath, dbTableName) {
  knowns <- read.csv(knownAnnotationTablePath)
  queries <- apply(knowns, 1, buildQueryRow)
  dbComm <- getDbComm()
  found <- map_lgl(queries, \(q) getNrow(dbComm, dbTableName, q) == 1)
  if (any(!found)) {
    fails <- knowns[!found, ]
    for (i in 1:nrow(fails)) {
      cli_alert_danger("Compound {fails[i, 'name']} not found once in file {fails[i, 'filename']}")
    }
  } else {
    cli_alert_success("All compounds found once.")
  }
}

buildQueryRow <- function(r) {
  list(
    query = list(
      bool = list(
        must = list(
          list(term = list(name = unname(r["name"]))), 
          list(term = list(filename = unname(r["filename"])))
        )
      )
    )
  )
}
