# Copyright 2016-2024 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal

# Validate records in msrawfiles ####


#' Check an msrawfile index for validity
#'
#' @description
#' This function connects to elasticsearch and checks the given msrawfiles 
#' index according to the current validation routine.
#' 
#'
#' @param indexName default: ntsp_msrawfiles
#'
#' @return TRUE
#' @export
checkMsrawfiles <- function(indexName = "ntsp_msrawfiles") {
  allRecords <- getAllMsrawfilesRecords(indexName)
  validateRecordsMsrawfiles(allRecords)
}

validateRecordsMsrawfiles <- function(records) {
  passed <- all(
    purrr::map_lgl(records, validateRecord),
    noDuplicatedFilenames(records)  
  )
  if (!passed) {
    stop("Validation failed, see warnings")
  } else {
    invisible(TRUE)
  }
}

validateRecord.msrawfileRecord <- function(record) {
  all(
    fieldsExistForSampleType(record),
    filesExist(record),
    correctRawfileLocation(record$path),
    correctIsTablePolarity(record),
    correctReplicateRegex(record),
    correctBlankRegex(record)
  )
}


noDuplicatedFilenames <- function(records) {
  !any(duplicated(basename(getField(records, "path"))))
}

fieldsExistForSampleType <- function(rec) {
  fieldsFound <- fieldsExist(defineRequiredFieldsAnySample(), rec)
  if (!rec$blank) {
    fieldsFoundEnv <- fieldsExist(defineRequiredFieldsEnvSample(), rec)
    fieldsFound <- append(fieldsFound, fieldsFoundEnv)
  }
  all(fieldsFound)  
}

filesExist <- function(rec) {
  fields <- defineRequiredFilesAnySample()
  all(purrr::map_lgl(fields, checkFileExists, rec = rec))
}

correctRawfileLocation <- function(path) {
  allowedPaths <- c(
    "G2/HRMS/Messdaten",
    "G2/3-Arbeitsgruppen_G2/3.5-NTS-Gruppe/db/ntsp/unit_tests/meas_files",
    "tests/testthat/fixtures/msrawfiles-addRecord",
    "/beegfs/nts/ntsportal/msrawfiles"
  )
  if (any(purrr::map_lgl(allowedPaths, grepl, x = path))) {
    TRUE
  } else {
    warning("File ", path, " not saved in allowed location")
    FALSE
  }
}

correctIsTablePolarity <- function(rec) {
  tablePath <- basename(rec$dbas_is_table)
  if (grepl(rec$pol, tablePath)) {
    TRUE
  } else {
    warning("IS Table does not match polarity in file ", rec$path)
  }
}

correctReplicateRegex <- function(rec) {
  if ("dbas_replicate_regex" %in% names(rec)) {
    if (bracketsReplicateRegex(rec$dbas_replicate_regex))
      patternFoundReplicateRegex(rec) else FALSE
  } else {
    TRUE
  }
}

correctBlankRegex <- function(rec) {
  regexMatch <- grepl(rec$dbas_blank_regex, basename(rec$path))
  isBlank <- rec$blank
  if ((regexMatch && isBlank) || (!regexMatch && !isBlank)) {
    TRUE
  } else {
    warning("File blank regex and filename mismatch: ", rec$path, " and ", rec$dbas_blank_regex)
  }
}

fieldsExist <- function(fields, rec) {
  all(purrr::map_lgl(fields, checkFieldExists, rec = rec))
}

checkFieldExists <- function(field, rec) {
  if (field %in% names(rec)) {
    TRUE
  } else {
    warning(field, " doesn't exist in file ", rec$path)
    FALSE
  }
}

checkFileExists <- function(field, rec) {
  if (file.exists(rec[[field]])) {
    TRUE
  } else {
    warning(field, " in record, ", rec$filename, " with value ", rec[[field]], " not found")
    FALSE
  }
}

bracketsReplicateRegex <- function(replicateRegex) {
  if (grepl("\\(.*\\)", replicateRegex)) {
    TRUE
  } else {
    warning("Brackets not found in ", replicateRegex)
    FALSE
  }
}

patternFoundReplicateRegex <- function(rec) {
  regex <- rec$dbas_replicate_regex
  fileName <- basename(rec$path)
  reducedFileName <- stringr::str_replace(fileName, regex, "\\1")
  if (nchar(reducedFileName) < nchar(fileName)) {
    TRUE
  } else {
    warning("Pattern not found in dbas_replicate_regex for file ", rec$path)
    FALSE
  }
}

defineRequiredFieldsAnySample <- function() {
  c(
    "path",
    "dbas_spectral_library",
    "dbas_is_table",
    "dbas_area_threshold",
    "dbas_rttolm",
    "dbas_mztolu",
    "dbas_mztolu_fine",
    "dbas_ndp_threshold",
    "dbas_rtTolReinteg",
    "date_measurement",
    "dbas_ndp_m",
    "dbas_ndp_n",
    "dbas_instr",
    "pol",
    "chrom_method",
    "matrix",
    "data_source",
    "dbas_index_name",
    "blank",
    "sample_source"
  )
}

defineRequiredFieldsEnvSample <- function() {
  c(
    "duration"
  )
}

defineRequiredFilesAnySample <- function() {
  c(
    "dbas_spectral_library",
    "dbas_is_table",
    "path"
  )
}


# Check batch quality ####
checkQualityBatchList <- function(recordsInBatches) {
  batchesOk <- purrr::map_lgl(recordsInBatches, checkQualityBatch)
  if (!all(batchesOk))
    stop("Quality check batches failed")
  invisible(batchesOk)
}

checkQualityBatch <- function(records) {
  batchOk <- all(
    validateRecordsMsrawfiles(records),
    allFieldsUniform(records),
    blanksPresent(records),
    enoughSamplesForMinFeatures(records)
  )
  batchOk
}

allFieldsUniform <- function(records) {
  uniformAll <- eachFieldUniform(records, fieldsToCheckUniformAllSamples())
  
  recordsEnvSamples <- purrr::discard(records, getField(records, "blank"))
  uniformEnv <- eachFieldUniform(recordsEnvSamples, fieldsToCheckUniformEnvSamples()) 
  
  uniform <- c(uniformAll, uniformEnv)
  
  if (!all(uniform)) {
    badFields <- names(uniform)[which(!uniform)] 
    for (f in badFields)
      ununiformFieldWarning(records, f)
    FALSE
  } else {
    TRUE
  }
}

blanksPresent <- function(records) {
  if (!any(getField(records, "blank"))) {
    badBatchWarning(records, "No blank found")
    FALSE
  } else {
    TRUE
  }
}

enoughSamplesForMinFeatures <- function(records) {
  minFeat <- getField(records, "nts_alig_filter_min_features")[1]
  if (minFeat > length(records)) {
    badBatchWarning(records, "Not enough samples for minimum features filter")
    FALSE
  } else {
    TRUE
  }
}

eachFieldUniform <- function(records, fields) {
  vapply(fields, fieldUniform, logical(1), records = records)
}

fieldUniform <- function(records, fieldToCheck) {
  if (!all(fieldExistsRecords(records, fieldToCheck)))
    return(TRUE)
  allEntries <- getField(records, fieldToCheck)
  length(unique(allEntries)) == 1
}

fieldExistsRecords <- function(records, fieldToCheck) {
  suppressWarnings(purrr::map_lgl(records, checkFieldExists, field = fieldToCheck))
}

fieldsToCheckUniformAllSamples <- function() {
  c(
    # Sampling
    "duration",
    "matrix",
    
    # Data acquisition
    "pol",
    "data_source",
    "chrom_method",
    
    # dbas Processing
    "dbas_blank_regex",
    "dbas_is_table",
    "dbas_is_name",
    "dbas_index_name",
    "dbas_alias_name",
    "dbas_minimum_detections",
    "dbas_fp",
    
    # nts processing
    "nts_spectral_library"
  )
}

fieldsToCheckUniformEnvSamples <- function() {
  c(
    # dbas Processing
    "dbas_replicate_regex",
    "dbas_build_averages",
    
    # nts processing
    "nts_alig_filter_min_features",
    "nts_alig_filter_type"
  )
}

ununiformFieldWarning <- function(records, field) {
  badBatchWarning(records, "field not uniform")
  warning("Error in field ", field)
}

badBatchWarning <- function(records, reason) {
  batchName <- dirname(getField(records, "path")[1])
  warning(reason, " in batch ", batchName)
}


# Check feature quality

validateRecord.featureRecord <- function(record) {
  all(
    isNestedFieldAList(record, "ms1"), 
    isNestedFieldAList(record, "ms2"),
    isNestedFieldAList(record, "eic"),
    isNestedFieldAList(record, "loc"),
    checkFieldsAllowed(record)
  )
}

isNestedFieldAList <- function(record, field) {
  if (field %in% names(record)) {
    if (is.list(record[[field]]) && !is.data.frame(record[[field]])) {
      TRUE
    } else {
      warning("Badly formated nested field ", field)
      FALSE
    }
  } else {
    TRUE
  }
}

checkFieldsAllowed <- function(record) {
  fieldsOk <- names(record) %in% allowedFieldsFeature()
  if (!all(fieldsOk)) {
    badNames <- paste(names(record)[which(!fieldsOk)], collapse = ", ")
    warning("Fields which should not be there: ", badNames)
    FALSE
  } else {
    TRUE
  }
}

allowedFieldsFeature <- function() {
  allowed <- union(
    names(jsonlite::read_json(fs::path_package("ntsportal", "extdata", "dbas_index_mappings.json"))$mappings$properties),
    names(jsonlite::read_json(fs::path_package("ntsportal", "extdata", "nts_index_mappings.json"))$mappings$properties)
  )
  c(allowed, "dbas_alias_name")
}




# Old Integrity checks ####


check_integrity_msrawfiles_nts <- function(msrawfilesDocsList) {
  
  dl <- msrawfilesDocsList
  ds <- lapply(dl, "[[", i = "_source")
  # takes too long
  #df <- data.table::rbindlist(lapply(ds, as.data.frame), fill = T)
  

  # Fields that must be present in all docs
  fieldsPresentAll <- c(
    "nts_mz_step",
    "nts_spectral_library"
  )
  purrr::walk(ds, function(doc) {
    purrr::walk(fieldsPresentAll, function(x) {
      if (!is.element(x, names(doc)))
        stop("Field ", x, " not found in doc ", doc$path)
    })
  })

  # Check that files exist
  pth1 <- unique(gf(ds, "nts_spectral_library", character(1)))
  pth2 <- gf(ds, "path", character(1))
  if (!all(file.exists(pth1, pth2)))
    stop("Not all files found")
  
  # Check that nts_mz_min is always lower than nts_mz_max
  
  mzCheck <- all(
    as.numeric(
      purrr::compact(
        sapply(ds, "[[", i = "nts_mz_min")
        )
      ) < 
      as.numeric(
        purrr::compact(
          sapply(ds, "[[", i = "nts_mz_max")
        )
      )
  )
  if (!mzCheck) {
    stop("nts_mz_min is not always lower than nts_mz_max")
  }
 
  invisible(TRUE)
}

validateRecord <- function(record) {
  UseMethod("validateRecord")
}

.S3method("validateRecord", "msrawfileRecord", validateRecord.msrawfileRecord)
.S3method("validateRecord", "featureRecord", validateRecord.featureRecord)
