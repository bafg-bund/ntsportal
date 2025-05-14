

inspectAnnotations(file.path("tests", "annotationInspector", "dbas_upb.csv"), "ntsp25.1_dbas_upb")

library(ntsworkflow)
runPeakPicking()

dbComm <- getDbComm()
recs <- getTableAsRecords(dbComm, "ntsp25.1_msrawfiles", searchBlock = list(query = list(regexp = list(filename = "Des_.._.._pos.mzXML"))), newMsrawfilesRecord)
recsBlanks <- getTableAsRecords(dbComm, "ntsp25.1_msrawfiles", searchBlock = list(query = list(regexp = list(path = ".*mud_pos/BW.*"))), newMsrawfilesRecord)
res <- scanBatchDbas(c(recs, recsBlanks), "Methyltriphenylphosphonium")
featureRecs <- convertToRecord(res, c(recs, recsBlanks))




