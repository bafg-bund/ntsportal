

# Validate records in msrawfiles ####

#' Check `msrawfiles` for validity
#' @description Checks the `msrawfiles` table according to the current validation routine.
#' @param indexName default: ntspXX.X_msrawfiles
#' @return TRUE if checks passed
#' @export
checkMsrawfiles <- function(indexName = "ntsp25.2_msrawfiles") {
  if (!grepl("msrawfiles", indexName))
    stop("This function is intended only for msrawfiles-type indices")
  runMsrawfileChecksForProcessingType("dbas")
  runMsrawfileChecksForProcessingType("nts")
}

runMsrawfileChecksForProcessingType <- function(screeningType) {
  allRecords <- getTableAsRecords(
    getDbComm(), msrawfilesIndex, 
    searchBlock = list(query = list(terms = list(batchname = batchNames))), sortField = "start", 
    fields = c(msrawfilesFieldsForProcessing(screeningType), msrawfileFieldsForValidation()),
    recordConstructor = switch(screeningType, dbas = newDbasMsrawfilesRecord, nts = newNtsMsrawfilesRecord)
  )
  validateRecordsMsrawfiles(allRecords)
  recordsInBatches <- recordsToBatches(allRecords)
  checkQualityBatchList(recordsInBatches)
}

msrawfileFieldsForValidation <- function() {
    "blank_regex"
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

#' Validate that a `*record` is correctly formatted
#' @description Checks are run for consistency and the associated files (if any) are present on disc.
#' @export
validateRecord <- function(record) {
  UseMethod("validateRecord")
}

#' Check validity of `dbasMsrawfilesRecord`
#' @rdname validateRecord
#' @export
validateRecord.dbasMsrawfilesRecord <- function(record) {
  all(correctIsTablePolarity(record), NextMethod())
}

#' Check validity of `msrawfilesRecord`
#' @rdname validateRecord
#' @export
validateRecord.msrawfilesRecord <- function(record) {
  all(
    fieldsExistForSampleType(record),
    filesExist(record),
    correctRawfileLocation(record$path),
    correctReplicateRegex(record),
    correctBlankRegex(record)
  )
}

noDuplicatedFilenames <- function(records) {
  !any(duplicated(basename(getField(records, "path"))))
}

fieldsExistForSampleType <- function(rec) {
  fieldsFound <- fieldsExist(defineRequiredFieldsAnySample(rec), rec)
  all(fieldsFound)  
}

fieldsExist <- function(fields, rec) {
  all(purrr::map_lgl(fields, checkFieldExists, rec = rec))
}


filesExist <- function(rec) {
  fields <- defineRequiredFilesAnySample(rec)
  all(purrr::map_lgl(fields, checkFileExists, rec = rec))
}

checkFieldExists <- function(field, rec) {
  if (field %in% names(rec)) {
    TRUE
  } else {
    warning(field, " doesn't exist in file ", rec$path)
    FALSE
  }
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
    FALSE
  }
}

correctReplicateRegex <- function(rec) {
  if ("replicate_regex" %in% names(rec)) {
    if (bracketsReplicateRegex(rec$replicate_regex))
      patternFoundReplicateRegex(rec) else FALSE
  } else {
    TRUE
  }
}

correctBlankRegex <- function(rec) {
  regexMatch <- grepl(rec$blank_regex, basename(rec$path))
  isBlank <- rec$blank
  if ((regexMatch && isBlank) || (!regexMatch && !isBlank)) {
    TRUE
  } else {
    warning("File blank regex and filename mismatch: ", rec$path, " and ", rec$blank_regex)
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
  regex <- rec$replicate_regex
  fileName <- basename(rec$path)
  reducedFileName <- stringr::str_replace(fileName, regex, "\\1")
  if (nchar(reducedFileName) < nchar(fileName)) {
    TRUE
  } else {
    warning("Pattern not found in replicate_regex for file ", rec$path)
    FALSE
  }
}

#' @export
defineRequiredFieldsAnySample <- function(record) {
  UseMethod("defineRequiredFieldsAnySample")
}

#' @export
defineRequiredFieldsAnySample.ntsMsrawfilesRecord <- function(record) {
  c(
    "nts_alig_filter_min_features",
    "nts_mz_min",
    "nts_mz_max",
    "nts_mz_step",
    "nts_rt_min",
    "nts_rt_max",
    "nts_sn",
    "nts_int_threshold",
    "nts_peak_noise_scans",
    "nts_precursor_mz_tol",
    "nts_peak_width_min",
    "nts_peak_width_max",
    "nts_max_num_peaks",
    "nts_componentization_ppm",
    "nts_componentization_rt_tol",
    "nts_componentization_rt_tol_l",
    "nts_componentization_rt_tol_r",
    "nts_componentization_rt_tol_sum",
    "nts_componentization_dynamic_tolerance",
    "nts_alig_delta_mz",
    "nts_alig_delta_rt",
    "nts_annotation_threshold_dp_score",
    "nts_annotation_mz_tol",
    "nts_annotation_rt_tol",
    "nts_annotation_ce_min",
    "nts_annotation_ce_max",
    "nts_annotation_ces_min",
    "nts_annotation_ces_max",
    "nts_annotation_ms2_mz_tol",
    "nts_annotation_rt_offset",
    "nts_annotation_int_cutoff",
    NextMethod()
  )
}

#' @export
defineRequiredFieldsAnySample.dbasMsrawfilesRecord <- function(record) {
  c(
    "dbas_is_table",
    "dbas_area_threshold",
    "dbas_rttolm",
    "dbas_mztolu",
    "dbas_mztolu_fine",
    "dbas_ndp_threshold",
    "dbas_rtTolReinteg",
    "dbas_ndp_m",
    "dbas_ndp_n",
    NextMethod()
  )
}

#' @export
defineRequiredFieldsAnySample.msrawfilesRecord <- function(record) {
  c(
    "csl_instruments_allowed",
    "path",
    "spectral_library_path",
    "pol",
    "chrom_method",
    "feature_table_alias",
    "internal_standard",
    "blank"
  )
}

#' @export
defineRequiredFilesAnySample <- function(record) {
  UseMethod("defineRequiredFilesAnySample")
}

#' @export
defineRequiredFilesAnySample.dbasMsrawfilesRecord <- function(record) {
  c(
    "dbas_is_table",
    NextMethod()
  )
}

#' @export
defineRequiredFilesAnySample.msrawfilesRecord <- function(record) {
  c(
    "spectral_library_path",
    "path"
  )
}


#' Check `featureRecord` validity
#' @rdname validateRecord
#' @method validateRecord featureRecord
#' @export
validateRecord.featureRecord <- function(record) {
  all(
    isNestedFieldAList(record, "ms1"), 
    isNestedFieldAList(record, "ms2"),
    isNestedFieldAList(record, "eic"),
    isNestedFieldAList(record, "loc"),
    isNestedFieldAList(record, "compound_annotation"),
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
  all(checkUnnestedFields(record), checkNestedFields(record))
}

checkUnnestedFields <- function(record) {
  fieldsOk <- names(record) %in% allowedFieldsFeature()
  if (!all(fieldsOk)) {
    warnBadFields(names(record)[!fieldsOk])
  }
  all(fieldsOk)
}

checkNestedFields <- function(record) {
  nestedFields <- getNestedFieldsFeatureTable()
  all(vapply(nestedFields, checkNestedField, logical(1), record = record))
}


checkNestedField <- function(record, fieldName) {
  nestedNames <- names(getMappingProperties("feature")[[fieldName]]$properties)
  fieldsOk <- vapply(names(record[[fieldName]]), function(x) x %in% nestedNames, logical(1))
  if (!all(fieldsOk)) {
    warnBadFields(names(record[[fieldName]])[!fieldsOk], parentField = fieldName)
  }
  all(fieldsOk)
}

getNestedFieldsFeatureTable <- function() {
  mappings <- getMappingProperties("feature")
  names(purrr::keep(mappings, function(x) x$type == "nested"))
}

getAllTableTypes <- function() {
  allMappings <- list.files(fs::path_package("ntsportal", "mappings"))
  stringr::str_match(allMappings, "^(.*)_index_mappings.json")[,2]
}

getAllNestedFields <- function() {
  fieldsAllMappings <- map(getAllTableTypes(), getMappingProperties)
  nestedFieldsAllMappings <- map(fieldsAllMappings, \(fields) names(keep(fields, \(x) x$type == "nested")))
  unique(unlist(nestedFieldsAllMappings))
}

allowedFieldsFeature <- function() {
  names(getMappingProperties("feature"))
}

warnBadFields <- function(badFields, parentField = "top-level") {
  badFieldsString <- paste(badFields, collapse = ", ")
  warning("Fields in ", parentField, " field which should not be there: ", badFieldsString)
}

# Copyright 2025 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
