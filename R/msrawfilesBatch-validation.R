
# Check batch quality ####
checkQualityBatchList <- function(recordsInBatches) {
  batchesOk <- purrr::map_lgl(recordsInBatches, checkQualityBatch)
  if (!all(batchesOk))
    stop("Quality check batches failed")
  invisible(batchesOk)
}

checkQualityBatch <- function(msrawfilesBatch) {
  batchOk <- all(
    allFieldsUniform(msrawfilesBatch),
    blanksPresent(msrawfilesBatch),
    enoughSamplesForMinFeatures(msrawfilesBatch)
  )
  batchOk
}

allFieldsUniform <- function(records) {
  uniformAll <- eachFieldUniform(records, fieldsToCheckUniformAllSamples(records))
  
  recordsEnvSamples <- purrr::discard(records, getField(records, "blank"))
  uniformEnv <- eachFieldUniform(recordsEnvSamples, fieldsToCheckUniformEnvSamples(records)) 
  
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
  minFeat <- getField(records, "nts_alig_filter_min_features", quiet = TRUE)[1]
  if (is.na(minFeat))
    return(TRUE)
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

#' @export
fieldsToCheckUniformAllSamples <- function(msrawfilesBatch) {
  UseMethod("fieldsToCheckUniformAllSamples")
}
#' @export
fieldsToCheckUniformAllSamples.dbasMsrawfilesBatch <- function(msrawfilesBatch) {
  c(
    "dbas_is_table",
    "dbas_minimum_detections",
    "dbas_fp",
    NextMethod()
  )
}
#' @export
fieldsToCheckUniformAllSamples.ntsMsrawfilesBatch <- function(msrawfilesBatch) {
  c(
    "nts_spectral_library",
    "nts_alig_delta_mz",
    "nts_alig_delta_rt",
    "nts_alig_mz_tol_units",
    "nts_blank_correction_factor",
    NextMethod()
  )
}
#' @export
fieldsToCheckUniformAllSamples.msrawfilesBatch <- function(msrawfilesBatch) {
  c(
    "blank_regex",
    "duration",
    "matrix",
    "pol",
    "data_source",
    "chrom_method",
    "internal_standard",
    "feature_table_alias"
  )
}
#' @export
fieldsToCheckUniformEnvSamples <- function(msrawfilesBatch) {
  UseMethod("fieldsToCheckUniformEnvSamples")
}
#' @export
fieldsToCheckUniformEnvSamples.msrawfilesBatch <- function(msrawfilesBatch) {
  c(
    "replicate_regex"
  )
}
#' @export
fieldsToCheckUniformEnvSamples.ntsMsrawfilesBatch <- function(msrawfilesBatch) {
  c(
    "nts_alig_filter_type",
    "nts_alig_filter_num_consecutive",
    "nts_alig_filter_min_features",
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
ununiformFieldWarning <- function(records, field) {
  badBatchWarning(records, "field not uniform")
  warning("Error in field ", field)
}

badBatchWarning <- function(records, reason) {
  batchName <- dirname(getField(records, "path")[1])
  warning(reason, " in batch ", batchName)
}
# Copyright 2025 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal