

records <- getOneSampleRecords()
result <- scanBatchDbas(records)
saveRDS(result, test_path("fixtures", "screening-convertToRecord", "oneSampleDbasResult.RDS"))

