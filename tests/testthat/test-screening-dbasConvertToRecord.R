

test_that("A small result can be reformated to a dbasRecord object", {
  rr <- getOneSampleDbasResultAndRecords()
  featRecs <- convertToRecord(rr$dbasResult, rr$msrRecords)
  featRecWithSpec <- featRecs[map_lgl(featRecs, \(x) x$name == rr$compWithSpec)][[1]]
  expect_contains(names(featRecWithSpec), c("mz", "rt", "intensity", "ms2", "feature_table_alias", "csl_experiment_id"))
  expect_s3_class(featRecs[[1]], "featureRecord")
  expect_true(validateRecord(featRecs[[1]]))
  differenceIntArea <- featRecs[[1]]$area - featRecs[[1]]$intensity
  expect_gt(differenceIntArea, 100)
  checkForAlias(featRecs[[1]])
  level1Feat <- featRecs[sapply(featRecs, \(x) length(x) == 25)][[1]]
  expect_length(level1Feat$compound_annotation, 1)
  annotationTableCols <- names(level1Feat$compound_annotation[[1]])
  expect_contains(annotationTableCols, "score_ms2_match")
  expect_contains(annotationTableCols, "mz_diff_lib")
})

test_that("Sample data is added to features", {
  featureRecMsrawfileRec <- getFeatureRecordAndMsrawfileRecordNoSampleData()
  featureWithData <- addSampleInfo(featureRecMsrawfileRec$featRecs, featureRecMsrawfileRec$msrRecords)
  expect_contains(names(featureWithData[[1]]), c("path"))
  expect_true(validateRecord(newFeatureRecord(featureWithData[[1]])))
})

test_that("Record is reduced to selected fields", {
  fields <- fieldsToMergeFromMsrawfiles()
  records <- getOneSampleRecords("dbas")
  recordReduced <- reduceRecordToFields(records[[1]], fields)
  expect_equal(length(recordReduced), 1)
  expect_contains(names(recordReduced), "feature_table_alias")
})

test_that("Spectra area added to feature", {
  rr <- getOneSampleDbasResultAndRecords()
  featRecs <- convertToRecord(rr$dbasResult, rr$msrRecords)
  featRecWithSpec <- featRecs[map_lgl(featRecs, \(x) x$name == rr$compWithSpec)][[1]]
  featRecNoSpec <- removeFieldsFromRecord(featRecWithSpec, c("ms1", "ms2", "eic"))
  featRecAddedSpec <- addSpectraToFeatures(rr$dbasResult, list(featRecNoSpec))[[1]]
  expect_contains(names(featRecAddedSpec), "ms2")
  expect_contains(names(featRecAddedSpec), "score_ms2_match")
})

test_that("MS2 matching score can be read for a peak ID", {
  scanResult <- getOneSampleDbasResultAndRecords()$dbasResult
  peakId <- scanResult$peakList[1, "peakID"]
  matchScore <- getScoreMs2Match(peakId, scanResult)
  expect_gt(matchScore, 300)
  expect_true(is.integer(matchScore))
})

test_that("An empty result is converted to an empty record", {
  record <- getEmptyFeatureRecord()
  expect_equal(length(record), 1)
  expect_s3_class(record[[1]], "featureRecord")
  expect_match(record[[1]]$path, "msrawfiles/unit_tests/.*\\.mzX?ML$")
  expect_match(record[[1]]$feature_table_alias,"^ntsp")
})

test_that("Internal standard name, intensity and area is added to record", {
  resultAndRecords <- getOneSampleDbasResultAndRecords()
  testRecordList <- convertToRecord(resultAndRecords$dbasResult, resultAndRecords$msrRecords)
  
  expect_contains(names(testRecordList[[1]]), "internal_standard")
  expect_equal(testRecordList[[1]]$internal_standard, "Olmesartan-d6")
  expect_contains(names(testRecordList[[1]]), "area_internal_standard")
  expect_contains(names(testRecordList[[1]]), "intensity_internal_standard")
})

test_that("If the internal standard is not found, there is no addition of area and intensity to the doc", {
  resultAndRecords <- getOneSampleDbasResultAndRecords()
  resultAndRecords$msrRecords[[1]]$internal_standard <- "Foobar"
  testRecordList <- convertToRecord(resultAndRecords$dbasResult, resultAndRecords$msrRecords)
  
  expect_contains(names(testRecordList[[1]]), "internal_standard")
  expect_equal(testRecordList[[1]]$internal_standard, "Foobar")
  expect_false(is.element("area_internal_standard", names(testRecordList[[1]])))
})

test_that("Annotation quality information is added to records", {
  scanRes1 <- getDbasScanResultDuplicatePeaks()
  msrBatch <- getDbasBatchDuplicatePeaks()
  recs <- convertToRecord(scanRes1, msrBatch)
  expect_gt(recs[[1]]$score_ms2_match, 400)
  expect_lt(recs[[1]]$mz_diff_lib, 50)
  expect_lt(recs[[1]]$rt_diff_lib, 1.5)
  
  # case 2, gap filled peaks
  dbasResult <- dbasScanResultWithMultiHitGapFilledPeaks()
  sr <- dbasResult$scanResult
  msrBatch <- dbasResult$batch
  recs2 <- convertToRecord(sr, msrBatch)
  gapFilledRecs <- recs2[map_lgl(recs2, \(x) grepl("foobar", x$path))]
  expect_disjoint(names(gapFilledRecs[[1]]), "mz_diff_lib")
})

test_that("If a peak has duplicate annotations, they are grouped as multihits and all annotations are included", {
  # Case 1) 2 duplicate, 1 unitary
  scanRes1 <- getDbasScanResultDuplicatePeaks()
  msrBatch <- getDbasBatchDuplicatePeaks()
  recs1 <- convertToRecord(scanRes1, msrBatch)
  expect_length(recs1, 3)
  expect_length(recs1[[1]]$name, 1)
  expect_length(recs1[[2]]$compound_annotation, 2)
  expect_contains(names(recs1[[2]]), "multi_hit_id")
  expect_type(recs1[[2]]$multi_hit_id, "character")
  
  # Case 2) 2 unitary
  scanRes2 <- getDbasScanResultDuplicatePeaks()
  scanRes2$peakList <- scanRes2$peakList[c(1,3), ]
  scanRes2$peakList <- transform(scanRes2$peakList, duplicate = NA)
  scanRes2$reintegrationResults <- scanRes2$reintegrationResults[c(1,3), ]
  recs2 <- convertToRecord(scanRes2, msrBatch)
  expect_length(recs2, 2)
  expect_length(recs2[[1]]$name, 1)
  expect_true("multi_hit_id" %in% names(recs2[[1]]))
  expect_length(recs2[[1]]$compound_annotation, 1)
  
  # Case 3) 3 duplicate, 1 unitary
  scanRes3 <- getDbasScanResultDuplicatePeaks()
  scanRes3$reintegrationResults <- scanRes3$reintegrationResults[c(1,2,3,1), ]
  scanRes3$reintegrationResults[4, "comp_name"] <- "Diclofenac"
  recs3 <- convertToRecord(scanRes3, msrBatch)
  expect_length(recs3, 4)
  expect_length(recs3[[2]]$compound_annotation, 3)
  expect_contains(map_chr(recs3[[2]]$compound_annotation, \(x) x$name), "Diclofenac")
  
  # Case 4) 2 duplicate, 2 duplicate, 1 unitary
  scanRes4 <- getDbasScanResultDuplicatePeaks()
  scanRes4$reintegrationResults <- scanRes4$reintegrationResults[c(1,2,3,1,2), ]
  scanRes4$reintegrationResults[4, "comp_name"] <- "Diclofenac"
  scanRes4$reintegrationResults[5, "comp_name"] <- "Sitagliptin"
  scanRes4$reintegrationResults[4, "real_mz"] <- 100.0001
  scanRes4$reintegrationResults[5, "real_mz"] <- 100.0001
  recs4 <- convertToRecord(scanRes4, msrBatch)
  expect_length(recs4, 5)
  expect_length(recs4[[1]]$name, 1)
  expect_length(recs4[[2]]$name, 1)
  expect_length(recs4[[4]]$name, 1)
  multiHitIds <- unique(map_chr(recs4, \(x) x$multi_hit_id))
  expect_length(multiHitIds, 3)
  artificialDup <- recs4[[which(sapply(recs4, \(x) x$name == "Diclofenac"))]]
  expect_contains(map_chr(artificialDup$compound_annotation, \(x) x$name), "Diclofenac")
  expect_contains(map_chr(artificialDup$compound_annotation, \(x) x$name), "Sitagliptin")
})

test_that("If there are gap-filled multihit peaks these are also linked", {
  dbasResult <- dbasScanResultWithMultiHitGapFilledPeaks()
  sr <- dbasResult$scanResult
  msrBatch <- dbasResult$batch
  recs <- convertToRecord(sr, msrBatch)
  expect_length(recs, 5)
  multihits <- unique(map_chr(recs, \(x) x$multi_hit_id))
  expect_length(multihits, 3)
})
