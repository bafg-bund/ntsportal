
#' Convert the results of DBAS processing to a list of feature records
#' @description The results of DBAS file scanning (`dbasResult`) are converted to the NTSPortal `featureRecord` format.
#' Compound and sample metadata are collected from the `msrawfileRecords` argument and the spectral library (CSL).
#' @rdname convertToRecord
#' @examples
#' \dontrun{
#' dbComm <- getDbComm()
#' recs <- getTableAsRecords(
#'   dbComm, 
#'   "ntsp25.3_msrawfiles", 
#'   searchBlock = list(query = list(regexp = list(filename = "Des_.._.._pos.mzXML"))), 
#'   newDbasMsrawfilesRecord
#' )
#' recsBlanks <- getTableAsRecords(
#'   dbComm, 
#'   "ntsp25.3_msrawfiles", 
#'   searchBlock = list(query = list(regexp = list(path = ".*mud_pos/BW.*"))), 
#'   newDbasMsrawfilesRecord
#' )
#' dbasBatch <- recordsToOneBatch(c(recs, recsBlanks))
#' dbasRes <- scanBatch(dbasBatch, "Methyltriphenylphosphonium")
#' featureRecs <- convertToRecord(dbasRes, dbasBatch)
#' }
#' @export
convertToRecord.dbasResult <- function(scanResult, msrawfilesBatch) {
  
  msrawfilesBatch <- nameRecordsByPath(msrawfilesBatch)
  
  features <- getAreasOfFeatures(scanResult)
  
  if (length(features) == 0)
    return(makeEmptyRecordWithPath(msrawfilesBatch))
    
  specLibPath <- getField(msrawfilesBatch, "spectral_library_path")[1]
  features <- addCompoundInfo(features, specLibPath)
  features <- addAnnotationTable(features)
  features <- addIntStdData(features, scanResult, msrawfilesBatch)
  features <- addSampleInfo(features, msrawfilesBatch)
  features <- addSpectraToFeatures(scanResult, features)
  features <- cleanUpFeatures(features)
  features
}

#' @export
getAreasOfFeatures.dbasResult <- function(scanResult) {
  featuresTable <- scanResult$reintegrationResults[, c("samp", "comp_name", "adduct", "isotopologue",
                                                  "int_h", "int_a", "real_mz", "real_rt_min")]
  featuresTable <- cleanUpFeaturesTable(featuresTable)
  featuresTableWithPath <- addPathToFeaturesTable(featuresTable, scanResult)
  featuresTableWithMulti <- addMultiHitGroupToFeaturesTable(featuresTableWithPath, scanResult)
  featuresTableWithQc <- addAnnotationQualityToFeaturesTable(featuresTableWithMulti, scanResult$peakList)
  createFeaturesListFromFeaturesTable(featuresTableWithQc)
}

addAnnotationQualityToFeaturesTable <- function(ft, pl) {
  pl <- rename(pl, name = comp_name, rt_diff_lib = rt_error_min, mz_diff_lib = mz_error_mDa, score_ms2_match = score)
  ft <- mutate(ft, samp = basename(path))
  ft <- addUniquePeakSpecToTable(ft)
  pl <- addUniquePeakSpecToTable(pl)
  left_join(ft, select(pl, peakSpec, score_ms2_match, rt_diff_lib, mz_diff_lib), by = "peakSpec") |> 
    select(-peakSpec, -samp)
}

addUniquePeakSpecToTable <- function(df) {
  mutate(df, peakSpec = paste(name, adduct, isotopologue, samp, sep = "|"))
}

cleanUpFeaturesTable <- function(features) {
  features$real_mz <- round(features$real_mz, 4)
  colnames(features) <- gsub("^comp_name$", "name", colnames(features))
  colnames(features) <- gsub("^int_a$", "area", colnames(features))
  colnames(features) <- gsub("^int_h$", "intensity", colnames(features))
  colnames(features) <- gsub("^samp$", "filename", colnames(features))
  colnames(features) <- gsub("^real_mz$", "mz", colnames(features))
  colnames(features) <- gsub("^real_rt_min$", "rt", colnames(features))
  rownames(features) <- NULL
  features$intensity <- as.integer(features$intensity)
  features
}

addPathToFeaturesTable <- function(features, scanResult) {
  allFilePaths <- data.frame(path = scanResult$rawFilePaths, filename = basename(scanResult$rawFilePaths))
  featuresWithPath <- merge(features, allFilePaths, by = "filename")
  featuresWithPath$filename <- NULL
  featuresWithPath
}

addMultiHitGroupToFeaturesTable <- function(featsTbl, scanRes) {
  featsTbl <- mergeFeatsTblPeakListForMultiHits(featsTbl, scanRes$peakList)
  featsTbl <- addMultiHitIdCol(featsTbl)
  as.data.frame(featsTbl)
}

mergeFeatsTblPeakListForMultiHits <- function(ft, pl) {
  pl <- rename(pl, name = comp_name)
  ft <- addIonSpecToTable(ft)
  pl <- addIonSpecToTable(pl)
  left_join(ft, select(pl, ionSpec, duplicate), by = "ionSpec") |> select(-ionSpec)
}

addIonSpecToTable <- function(df) {
  mutate(df, ionSpec = paste(name, adduct, isotopologue, sep = "|"))
}

addMultiHitIdCol <- function(ft) {
  ft <- mutate(ft, multi_hit_id = ifelse(is.na(duplicate), NA, paste0(shortHash(path), duplicate))) |> 
    select(-duplicate)
  numUnitary <- sum(is.na(ft$multi_hit_id))
  if (numUnitary > 0)
    ft$multi_hit_id[is.na(ft$multi_hit_id)] <- randomString(numUnitary)
  ft
}

randomString <- function(x) {
  stringi::stri_rand_strings(n = x, length = 10)
}

shortHash <- function(x) {
  substr(Vectorize(rlang::hash)(x), 1, 9)
}

createFeaturesListFromFeaturesTable <- function(featuresWithPath) {
  featureList <- split(featuresWithPath, seq_len(nrow(featuresWithPath))) 
  featureList <- lapply(featureList, as.list)
  unname(featureList)
}

addCompoundInfo <- function(features, specLibPath) {
  specLibConn <- connectSqlite(specLibPath)
  comptab <- tbl(specLibConn, "compound") %>% collect()
  grouptab <- tbl(specLibConn, "compound") %>% 
    select(name, compound_id) %>% 
    left_join(tbl(specLibConn, "compound_group_map"), by = "compound_id") %>% 
    left_join(tbl(specLibConn, "compound_group"), by = "compound_group_id") %>% 
    select(name.x, name.y) %>% 
    rename(compname = name.x, groupname = name.y) %>% 
    collect()
  DBI::dbDisconnect(specLibConn)
  
  features <- lapply(features, function(doc) {
    if ("name" %in% names(doc) && !is.na(doc$name)) {
      doc[["cas"]] <- filter(comptab, name == !!doc$name) %>% pull(cas)
      doc[["inchi"]] <- filter(comptab, name == !!doc$name) %>% pull(inchi)
      doc[["inchikey"]] <- filter(comptab, name == !!doc$name) %>% pull(inchikey)
      doc[["formula"]] <- filter(comptab, name == !!doc$name) %>% pull(formula)
      cg <- filter(grouptab, compname == !!doc$name) %>% pull(groupname)
      cg <- cg[!is.element(cg, spectrumSourcesToRemoveFromCompGroup())]
      doc[["comp_group"]] <- cg
    }
    doc
  })
  
  features
}

addAnnotationTable <- function(features) {
  allGroups <- map_chr(features, \(x) x$multi_hit_id)
  multiHitsInGroups <- split(features, allGroups)
  multiHitsInGroups <- map(multiHitsInGroups, addAnnotationTableToMultiHitGroup)
  unname(unlist(multiHitsInGroups, recursive = FALSE))
}

addAnnotationTableToMultiHitGroup <- function(multiHitGroup) {
  annotList <- dfToList(createMultiHitAnnotationTable(multiHitGroup))
  map(multiHitGroup, \(rec) {rec$compound_annotation <- annotList; rec})
}

createMultiHitAnnotationTable <- function(multiHitGroup) {
  compNames <- map_chr(multiHitGroup, \(rec) rec$name)
  cass <- map_chr(multiHitGroup, \(rec) rec$cas)
  inchis <- map_chr(multiHitGroup, \(rec) rec$inchi)
  inchikeys <- map_chr(multiHitGroup, \(rec) rec$inchikey)
  adducts <- map_chr(multiHitGroup, \(rec) rec$adduct)
  isotops <- map_chr(multiHitGroup, \(rec) rec$isotopologue)
  formulas <- map_chr(multiHitGroup, \(rec) rec$formula)
  ms2scores <- map_dbl(multiHitGroup, \(rec) rec$score_ms2_match)
  mzDiffs <- map_dbl(multiHitGroup, \(rec) rec$mz_diff_lib)
  rtDiffs <- map_dbl(multiHitGroup, \(rec) rec$rt_diff_lib)
  data.frame(name = compNames, cas = cass, inchi = inchis, inchikey = inchikeys, adduct = adducts, 
             isotopologue = isotops, formula = formulas, score_ms2_match = ms2scores, mz_diff_lib = mzDiffs, 
             rt_diff_lib = rtDiffs)
}

addIntStdData <- function(features, scanResult, msrawfileRecords) {
  features <- addIntStdName(features, msrawfileRecords)
  addResponseIntStd(features, scanResult)
}

addIntStdName <- function(features, msrawfileRecords) {
  nameIntStd <- getField(msrawfileRecords, "internal_standard")[1]
  lapply(features, function(rec) {rec$internal_standard <- nameIntStd; rec})
}

addResponseIntStd <- function(features, scanResult) {
  resp <- scanResult$intStdResults
  lapply(features, function(rec) {
    if (!is.element(rec$internal_standard, resp$compound_name)) {
      return(rec)
    } else {
      fileName <- basename(rec$path)
      rec$area_internal_standard <- resp[resp$filename == fileName & resp$compound_name == rec$internal_standard, "area"]
      rec$intensity_internal_standard <- resp[resp$filename == fileName & resp$compound_name == rec$internal_standard, "intensity"]
      rec
    }
  })
}

addSampleInfo <- function(features, msrawfileRecords) {
  msrawfileRecords <- purrr::map_at(
    msrawfileRecords, 
    names(msrawfileRecords), 
    reduceRecordToFields, 
    fields = fieldsToMergeFromMsrawfiles()
  )
  recordsPerFeature <- msrawfileRecords[sapply(features, "[[", i = "path")]
  purrr::map2(features, recordsPerFeature, c)
}

normalizeArea <- function(feature) {
  normalizedArea <- signif(feature[["area"]] / feature$area_is, 3)
  normalizedIntensity <- signif(feature[["intensity"]] / feature$intensity_is, 3)
  feature$area_normalized <- normalizedArea
  feature$norm_a <- normalizedArea
  feature$intensity_normalized <- normalizedIntensity
  feature
}

#' @export
addSpectraToFeatures.dbasResult <- function(scanResult, features) { 
  for (featNum in seq_along(features)) {
    peakId <- getPeakIdForFeature(features[[featNum]], scanResult$peakList)
    if (length(peakId) == 0 || !is.numeric(peakId))
      next
    if (length(peakId) > 1) { 
      peakId <- choosePeakWithHighestArea(peakId, scanResult$peakList)
      features[[featNum]]$comment <- paste(features[[featNum]]$comment, "Isomers in dbas found, highest peak used")
    } 
    if (isSpecChromAvailable(peakId, scanResult$eicTable))
      features[[featNum]]$eic <- getEicFromScanResult(scanResult, peakId)
    
    if (isSpecChromAvailable(peakId, scanResult$ms1Table)) 
      features[[featNum]]$ms1 <- getMs1FromScanResult(scanResult, peakId)
    
    if (isSpecChromAvailable(peakId, scanResult$ms2Table)) {
      features[[featNum]]$ms2 <- getMs2FromScanResult(scanResult, peakId)
      #features[[featNum]]$score_ms2_match <- getScoreMs2Match(peakId, scanResult)
      features[[featNum]]$csl_experiment_id <- getLibExpId(peakId, scanResult)
    }
  }
  features
}




makeEmptyRecordWithPath <- function(msrawfileRecords) {
  list(
    newFeatureRecord(
      list(
        path = msrawfileRecords[[1]]$path,
        feature_table_alias = msrawfileRecords[[1]]$feature_table_alias
      )
    )
  )
}



nameRecordsByPath <- function(msrawfileRecords) {
  names(msrawfileRecords) <- getField(msrawfileRecords, "path")
  msrawfileRecords
}



reduceRecordToFields <- function(record, fields) {
  availableFields <- fields[fields %in% names(record)]
  record[availableFields]
}

spectrumSourcesToRemoveFromCompGroup <- function() {
  c("BfG", "LfU", "UBA", "bfg", "lfuby", "uba")
}


fieldsToMergeFromMsrawfiles <- function() {
  c("feature_table_alias")
}


getPeakIdForFeature <- function(feature, peakList) {
  subset(
    peakList, 
    comp_name == feature$name & 
      samp == basename(feature$path) &
      adduct == feature$adduct &
      isotopologue == feature$isotopologue, 
    peakID, 
    drop = TRUE
  )
}

choosePeakWithHighestArea <- function(peakId, peakList) {
  best <- which.max(subset(dbas$peakList, peakID %in% peakId, int_a, drop = T))
  subset(dbas$peakList, peakID %in% peakId, peakID, drop = T)[best]
}

#' @export
getEicFromScanResult.dbasResult <- function(scanResult, peakId) {
  eic <- subset(scanResult$eicTable, peakID == peakId, c(time, int))
  eic$time <- as.integer(round(eic$time))  # in seconds
  eic$int <- round(eic$int, 4)
  rownames(eic) <- NULL
  dfToList(eic)
}

#' @export
getMs1FromScanResult.dbasResult <- function(scanResult, peakId) {
  ms1 <- subset(scanResult$ms1Table, peakID == peakId, c(mz, int))
  ms1 <- removeNoiseMsSpec(ms1, 0.01)
  ms1 <- convertSpecToRelativeIntensities(ms1, getAnalyteIntensity(peakId, scanResult$peakList))
  ms1 <- limitSizeMsSpec(ms1)
  dfToList(ms1)
}

#' @export
getMs2FromScanResult.dbasResult <- function(scanResult, peakId) {
  ms2 <- subset(scanResult$ms2Table, peakID == peakId, c(mz, int))
  ms2 <- removeNoiseMsSpec(ms2, 0.01)
  ms2 <- convertSpecToRelativeIntensities(ms2, max(ms2$int))
  ms2 <- limitSizeMsSpec(ms2)
  dfToList(ms2)
}

getScoreMs2Match <- function(peakId, scanResult) {
  pl <- scanResult$peakList
  as.integer(pl[pl$peakID == peakId, "score", drop = TRUE])
}

getLibExpId <- function(peakId, scanResult) {
  pl <- scanResult$peakList
  as.integer(pl[pl$peakID == peakId, "expID", drop = TRUE])
}

isSpecChromAvailable <- function(peakId, scanResultTable) {
  peakId %in% scanResultTable$peakID
}

getAnalyteMz <- function(peakId, peakList) {
  peakList[peakList$peakID == peakId, "mz", drop = T]
}

getAnalyteIntensity <- function(peakId, peakList) {
  peakList[peakList$peakID == peakId, "int_h", drop = T]
}

# Copyright 2025 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
