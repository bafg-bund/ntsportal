# Copyright 2016-2024 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal

convertToRecord.dbasResult <- function(scanResult, msrawfileRecords) {
  
  msrawfileRecords <- nameRecordsByFilename(msrawfileRecords)
  
  features <- getAreasOfFeatures(scanResult)
  
  if (length(features) == 0)
    return(makeEmptyRecordWithPath(msrawfileRecords))
    
  specLibPath <- getField(msrawfileRecords, "dbas_spectral_library")[1]
  features <- addCompoundInfo(features, specLibPath)
  
  features <- addIntStdData(features, scanResult, msrawfileRecords)
  
  features <- lapply(features, normalizeArea)
  features <- addSampleInfo(features, msrawfileRecords)
  
  features <- lapply(features, addSpectraToFeature, scanResult = scanResult)
  
  lapply(features, cleanFeature)
}


getAreasOfFeatures.dbasResult <- function(scanResult) {
  features <- scanResult$reintegrationResults[, c("samp", "comp_name", "adduct", "isotopologue",
                                                  "int_h", "int_a", "real_mz", "real_rt_min")]
  features$samp <- basename(features$samp)
  features$int_a <- signif(features$int_a, 3)
  features$int_a <- signif(features$int_h, 3)
  features$real_mz <- round(features$real_mz, 4)
  colnames(features) <- gsub("^comp_name$", "name", colnames(features))
  colnames(features) <- gsub("^int_a$", "area", colnames(features))
  colnames(features) <- gsub("^int_h$", "intensity", colnames(features))
  colnames(features) <- gsub("^samp$", "filename", colnames(features))
  colnames(features) <- gsub("^real_mz$", "mz", colnames(features))
  colnames(features) <- gsub("^real_rt_min$", "rt", colnames(features))
  rownames(features) <- NULL
  featureList <- split(features, seq_len(nrow(features))) 
  featureList <- lapply(featureList, as.list)
  featureList <- unname(featureList)
  featureList
}

addCompoundInfo <- function(features, specLibPath) {
  specLibConn <- connectSqlite(specLibPath)
  comptab <- tbl(specLibConn, "compound") %>% collect()
  grouptab <- tbl(specLibConn, "compound") %>% 
    select(name, compound_id) %>% 
    left_join(tbl(specLibConn, "compGroupComp"), by = "compound_id") %>% 
    left_join(tbl(specLibConn, "compoundGroup"), by = "compoundGroup_id") %>% 
    select(name.x, name.y) %>% 
    rename(compname = name.x, groupname = name.y) %>% 
    collect()
  
  features <- lapply(features, function(doc) {
    if("name" %in% names(doc) && !is.na(doc$name)) {
      doc[["cas"]] <- filter(comptab, name == !!doc$name) %>% pull(CAS)
      doc[["inchi"]] <- filter(comptab, name == !!doc$name) %>% pull(inchi)
      doc[["inchikey"]] <- filter(comptab, name == !!doc$name) %>% pull(inchikey)
      doc[["formula"]] <- filter(comptab, name == !!doc$name) %>% pull(formula)
      cg <- filter(grouptab, compname == !!doc$name) %>% pull(groupname)
      cg <- cg[!is.element(cg, spectrumSourcesToRemoveFromCompGroup())]
      doc[["comp_group"]] <- cg
    }
    doc
  })
  DBI::dbDisconnect(specLibConn)
  features
}

addIntStdData <- function(features, scanResult, msrawfileRecords) {
  features <- addAreasIntStd(features, scanResult)
  addPrimaryIntStdName(features, msrawfileRecords)
}

addAreasIntStd <- function(features, scanResult) {
  intStdAreas <- getAreasIntStds(scanResult)
  lapply(features, function(rec) {rec$internal_standard <- intStdAreas[[rec$filename]]; rec})
}

getAreasIntStds.dbasResult <- function(scanResult) {
  newIsResults <- scanResult$isResults[, c("samp", "IS", "int_h", "int_a")]
  newIsResults <- dplyr::rename(newIsResults, filename = samp, compound_name = IS, intensity = int_h, area = int_a)
  newIsResults <- dataframeToList(newIsResults)
  names(newIsResults) <- vapply(newIsResults, function(x) x$filename, character(1))
  newIsResults <- lapply(newIsResults, function(x) {x$filename <- NULL; x})
  lapply(newIsResults, as.list)
}

addPrimaryIntStdName <- function(features, msrawfileRecords) {
  nameIntStd <- getField(msrawfileRecords, "dbas_is_name")[1]
  lapply(features, function(rec) {rec$primary_is_name <- nameIntStd; rec})
}

addSampleInfo <- function(features, msrawfileRecords) {
  msrawfileRecords <- purrr::map_at(
    msrawfileRecords, 
    names(msrawfileRecords), 
    reduceRecordToFields, 
    fields = fieldsToMergeFromMsrawfiles()
  )
  recordsPerFeature <- msrawfileRecords[sapply(features, "[[", i = "filename")]
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

addSpectraToFeature <- function(feature, scanResult) { 
  
  peakId <- getPeakIdForFeature(feature, scanResult$peakList)
  
  if (length(peakId) == 0 || !is.numeric(peakId))
    return(feature) 
  
  if (length(peakId) > 1) { 
    peakId <- choosePeakWithHighestArea(peakId, scanResult$peakList)
    feature$comment <- paste(feature$comment, "Isomers in dbas found, highest peak used")
  } 
  
  if (isSpecChromAvailable(peakId, scanResult$eicTable))
    feature$eic <- getEicFromScanResult(peakId, scanResult)
  
  if (isSpecChromAvailable(peakId, scanResult$ms1Table)) 
    feature$ms1 <- getMs1FromScanResult(peakId, scanResult)
  
  if (isSpecChromAvailable(peakId, scanResult$ms2Table)) 
    feature$ms2 <- getMs2FromScanResult(peakId, scanResult)
  
  feature
}

cleanFeature <- function(doc) {
  doc <- lapply(doc, function(field) {
    if (is.vector(field)) {
      field <- field[!is.na(field)]
      field <- field[!(field == "NA")]
      if (length(field) == 0)
        return(NULL) else return(field)
    } else if (is.data.frame(field)) {
      keep <- apply(field, 1, function(fieldEntry) {
        !any(is.na(fieldEntry) | fieldEntry == "NA") 
      })
      field <- field[keep, ]
      if (nrow(field) == 0)
        return(NULL) else return(field)
    } else if (is.list(field)) {
      field <- lapply(field, function(fieldEntry) {
        fieldEntry <- fieldEntry[!any(is.na(fieldEntry))]
        fieldEntry <- fieldEntry[!any(fieldEntry == "NA")]
        fieldEntry
      })
      if (length(field) == 0)
        return(NULL) else return(field)
    } else {
      stop("unknown case when removing NAs")
    }
  })
  newFeatureRecord(Filter(Negate(is.null), doc))
}



makeEmptyRecordWithPath <- function(msrawfileRecords) {
  list(
    newFeatureRecord(
      list(
        path = msrawfileRecords[[1]]$path,
        dbas_alias_name = msrawfileRecords[[1]]$dbas_alias_name
      )
    )
  )
}



nameRecordsByFilename <- function(msrawfileRecords) {
  names(msrawfileRecords) <- getField(msrawfileRecords, "filename")
  msrawfileRecords
}



reduceRecordToFields <- function(record, fields) {
  availableFields <- fields[fields %in% names(record)]
  record[availableFields]
}

spectrumSourcesToRemoveFromCompGroup <- function() {
  c("BfG", "LfU", "UBA")
}


fieldsToMergeFromMsrawfiles <- function() {
  c(
    "start",
    "pol",
    "matrix",
    "data_source",
    "sample_source",
    "license",
    "chrom_method",
    "duration",
    "path",
    "date_measurment",
    "river",
    "km",
    "gkz",
    "tag",
    "comment",
    "dbas_alias_name",
    "station",
    "loc"
  )
}



newFeatureRecord <- function(feature) {
  stopifnot(is.list(feature))
  structure(feature, class = "featureRecord")
}

getPeakIdForFeature <- function(feature, peakList) {
  subset(
    peakList, 
    comp_name == feature$name & 
      samp == feature$filename &
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

getEicFromScanResult <- function(peakId, scanResult) {
  eic <- subset(scanResult$eicTable, peakID == peakId, c(time, int))
  eic$time <- round(eic$time)  # in seconds
  eic$int <- round(eic$int, 4)
  rownames(eic) <- NULL
  dataframeToList(eic)
}

getMs1FromScanResult <- function(peakId, scanResult) {
  ms1 <- subset(scanResult$ms1Table, peakID == peakId, c(mz, int))
  ms1 <- normalizeMs1(ms1, getAnalyteIntensity(peakId, scanResult$peakList))
  ms1 <- removeNoise(ms1, 0.01)
  ms1 <- roundSpectrumTo4(ms1)
  dataframeToList(ms1)
}

getMs2FromScanResult <- function(peakId, scanResult) {
  ms2 <- subset(scanResult$ms2Table, peakID == peakId, c(mz, int))
  ms2 <- normalizeMs2(ms2)
  ms2 <- removeNoise(ms2, 0.01)
  rownames(ms2) <- NULL
  ms2 <- roundSpectrumTo4(ms2)
  dataframeToList(ms2)
}

dataframeToList <- function(df) {
  l <- split(df, seq_len(nrow(df)))
  unname(l)
}


roundSpectrumTo4 <- function(msSpectrum) {
  msSpectrum$mz <- round(msSpectrum$mz, 4)
  msSpectrum$int <- round(msSpectrum$int, 4)
  msSpectrum
}

isSpecChromAvailable <- function(peakId, scanResultTable) {
  peakId %in% scanResultTable$peakID
}

normalizeMs2 <- function(msSpectrum) {
  data.frame(mz = msSpectrum$mz, int = msSpectrum$int / max(msSpectrum$int))
}

normalizeMs1 <- function(msSpectrum, analyteIntensity) {
  data.frame(mz = msSpectrum$mz, int = msSpectrum$int / analyteIntensity)
}

getAnalyteMz <- function(peakId, peakList) {
  peakList[peakList$peakID == peakId, "mz", drop = T]
}
getAnalyteIntensity <- function(peakId, peakList) {
  peakList[peakList$peakID == peakId, "int_h", drop = T]
}

removeNoise <- function(msSpectrum, noiseLevel) {
  msSpectrum[msSpectrum$int >= noiseLevel, , drop = F]
}

# Define generics

convertToRecord <- function(scanResult, msrawfileRecords) {
  UseMethod("convertToRecord")
}

getAreasOfFeatures <- function(scanResult) {
  UseMethod("getAreasOfFeatures")
}

getAreasIntStds <- function(scanResult) {
  UseMethod("getAreasIntStds")
}

.S3method("convertToRecord", "dbasResult", convertToRecord.dbasResult)
.S3method("getAreasOfFeatures", "dbasResult", getAreasOfFeatures.dbasResult)
.S3method("getAreasIntStds", "dbasResult", getAreasIntStds.dbasResult)

