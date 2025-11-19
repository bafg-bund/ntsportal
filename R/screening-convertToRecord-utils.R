#' Convert the result of file scanning to a list of feature records
#' Compound and sample metadata, including spectra are collected from the `scanResult`, from the linked raw measurement
#' files, or from `msrawfileRecords`
#' @param scanResult results of a file scanning algorithm (e.g. dbasResult, ntsResult)
#' @param msrawfilesBatch msrawfilesBatch used to create the scanResult
#' @export
convertToRecord <- function(scanResult, msrawfilesBatch) {
  UseMethod("convertToRecord")
}

getAreasOfFeatures <- function(scanResult) {
  UseMethod("getAreasOfFeatures")
}

getEicFromScanResult <- function(scanResult, peakId, ...) {
  UseMethod("getEicFromScanResult")
}

getMs1FromScanResult <- function(scanResult, peakId, ...) {
  UseMethod("getMs1FromScanResult")
}

getMs2FromScanResult <- function(scanResult, peakId, ...) {
  UseMethod("getMs2FromScanResult")
}

cleanUpFeatures <- function(features) {
  for (featNum in seq_along(features)) {
    features[[featNum]]$sampleId <- NULL
    features[[featNum]]$peakId <- NULL
    features[[featNum]]$componentId <- NULL
    features[[featNum]]$alignmentId <- NULL
    features[[featNum]]$ms2scan <- NULL
    features[[featNum]] <- removeNasFromRecord(features[[featNum]])
    features[[featNum]] <- newFeatureRecord(features[[featNum]])
  }
  features
}

removeNasFromRecord <- function(rec) {
  for (field in names(rec)) {
    if (is.vector(rec[[field]])) {
      rec[[field]] <- rec[[field]][!is.na(rec[[field]])]
      rec[[field]] <- rec[[field]][!(rec[[field]] == "NA")]
      if (length(rec[[field]]) == 0)
        rec[[field]] <- NULL
    } else if (is.data.frame(rec[[field]])) {
      warning("data.frame found in record ", rec, " in field ", field)
      rec[[field]] <- NULL
    } else if (is.list(field)) {
      rec[[field]] <- lapply(rec[[field]], function(fieldEntry) {
        fieldEntry <- fieldEntry[!any(is.na(fieldEntry))]
        fieldEntry <- fieldEntry[!any(fieldEntry == "NA")]
        fieldEntry
      })
      if (length(field) == 0)
        rec[[field]] <- NULL
    } else if (is.null(rec[[field]])) {
      rec[[field]] <- NULL
    } else {
      stop("unknown case when removing NAs")
    }
  }
  rec
}

limitSizeMsSpec <- function(spec) {
  spec <- slice_max(spec, int, n = 50)
  spec$mz <- round(spec$mz, 4)
  spec$int <- signif(spec$int, 5)
  spec
}

convertSpecToRelativeIntensities <- function(spec, referenceIntensity) {
  spec$int <- round(spec$int / referenceIntensity, 3)
  spec
}

removeNoiseMsSpec <- function(spec, noiseLevel) {
  spec[spec$int >= noiseLevel, , drop = F]
}

dfToList <- function(tbldf) {
  l <- split(tbldf, seq_len(nrow(tbldf)))
  l <- lapply(l, as.list)
  unname(l)
}