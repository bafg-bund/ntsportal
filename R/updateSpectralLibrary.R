
#' Update the CSL spectral library on ntsportal
#' @description The `spectral_library` table is a copy of the CSL to use as a reference in the Spectral Library dashboard 
#' and Spectra and Chromatograms dashboard. This does not change the library used for DBAS processing. After
#' reprocessing with a new CSL version, this function is run to update the reference table.
#' @param rfTableName Name of the msrawfiles table
#' @param specLibTableName Name for the new spectral library table (old table will be overwritten)
#' @export
updateSpectralLibrary <- function(rfTableName, specLibTableName) {
  testConnection()
  specLibPath <- getSpectralLibraryPath(rfTableName)
  message("Converting ", specLibPath)
  records <- SpecLibRecords$new(specLibPath)
  message("Ingesting...")
  ingestSpectralLibrary(records, specLibTableName)
}

getSpectralLibraryPath <- function(rfIndexName) {
  dbComm <- getDbComm()
  pth <- getUniqueValues(dbComm, rfIndexName, "dbas_spectral_library")
  stopifnot(length(pth) == 1)
  pth
}

SpecLibRecords <- R6Class(
  "SpecLibRecords", 
  public = list(
    specLibPath = character(),
    records = list(),
    initialize = function(spectralLibraryPath) {
      stopifnot(file.exists(spectralLibraryPath))
      self$specLibPath <- spectralLibraryPath
      self$records <- private$addExperimentRecords()
      private$addMs2Spectra()
      private$addRetentionTimes()
      private$addExperimentGroups()
      private$addCompoundGroups()
      private$cleanUpFields()
      invisible(self)
    },
    print = function(...) {
      cat("SpecLibRecords (R6) with path ", self$specLibPath, " and ", length(self$recs), " record(s).", sep = "")
    }
  ),
  private = list(
    addExperimentRecords = function() {
      sdb <- DBI::dbConnect(RSQLite::SQLite(), self$specLibPath)
      exps <- tbl(sdb, "experiment") %>%
        left_join(tbl(sdb, "compound"), by = "compound_id") %>%
        left_join(tbl(sdb, "parameter"), by = "parameter_id") %>%
        collect()
      DBI::dbDisconnect(sdb)
      exps <- split(exps, seq_len(nrow(exps)))
      exps <- unname(exps)
      lapply(exps, newNtspRecord)
    },
    addMs2Spectra = function() {
      sdb <- DBI::dbConnect(RSQLite::SQLite(), self$specLibPath)
      ms2tbl <- tbl(sdb, "fragment") %>% collect()
      DBI::dbDisconnect(sdb)
      cli_progress_bar("addMs2Spectra", total = length(self$records))
      for (i in seq_along(self$records)) {
        expId <- self$records[[i]]$experiment_id
        ms2 <- ms2tbl %>% filter(experiment_id == !!expId) %>%
          select(mz, int)
        ms2$int <- ms2$int / max(ms2$int)  # make spectrum relative to max int
        ms2 <- split(ms2, seq_len(nrow(ms2)))
        ms2 <- unname(ms2)
        ms2 <- lapply(ms2, as.list)
        self$records[[i]]$ms2 <- ms2
        cli_progress_update()
      }
      cli_progress_done()
    },
    addRetentionTimes = function() {
      sdb <- DBI::dbConnect(RSQLite::SQLite(), self$specLibPath)
      rttbl <- tbl(sdb, "retentionTime") %>% collect()
      DBI::dbDisconnect(sdb)
      cli_progress_bar("addRetentionTimes", total = length(self$records))
      for (i in seq_along(self$records)) {
        compId <- self$records[[i]]$compound_id
        rtt <- rttbl %>% filter(compound_id == !!compId) %>%
          select(rt, chrom_method, predicted) 
        colnames(rtt) <- sub("chrom_method", "method", colnames(rtt))
        rtt$doi <- rtt$method
        rtt$predicted <- as.logical(rtt$predicted)
        rtt[!grepl("^dx.doi", rtt$doi), "doi"] <- NA
        rtt <- subset(rtt, !is.na(method))
        if (nrow(rtt) == 0)
          next
        rtt <- split(rtt, seq_len(nrow(rtt)))
        rtt <- unname(rtt)
        rtt <- lapply(rtt, as.list)
        self$records[[i]]$rtt <- rtt
        cli_progress_update()
      }
      cli_progress_done()
    },
    addExperimentGroups = function() {
      sdb <- DBI::dbConnect(RSQLite::SQLite(), self$specLibPath)
      egetbl <- tbl(sdb, "expGroupExp") %>% collect()
      egtbl <- tbl(sdb, "experimentGroup") %>% collect()
      DBI::dbDisconnect(sdb)
      cli_progress_bar("addExperimentGroups", total = length(self$records))
      for (i in seq_along(self$records)) {
        expId <- self$records[[i]]$experiment_id
        expGroup <- egetbl %>%
          filter(experiment_id == !!expId) %>%
          left_join(egtbl, by = "experimentGroup_id") 
        self$records[[i]]$data_source <- expGroup$name  # the experiment groups are used to show data source
        cli_progress_update()
      }
      cli_progress_done()
    },
    addCompoundGroups = function() {
      sdb <- DBI::dbConnect(RSQLite::SQLite(), self$specLibPath)
      cgctbl <- tbl(sdb, "compGroupComp") %>% collect()
      cgtbl <- tbl(sdb, "compoundGroup") %>% collect()
      DBI::dbDisconnect(sdb)
      cli_progress_bar("addCompoundGroups", total = length(self$records))
      for (i in seq_along(self$records)) {
        compId <- self$records[[i]]$compound_id
        compGroup <- cgctbl %>%
          filter(compound_id == !!compId) %>%
          left_join(cgtbl, by = "compoundGroup_id") 
        compGroup <- subset(compGroup, Negate(is.element)(name, c("BfG", "LfU", "UBA")))
        if (nrow(compGroup) == 0) 
          next
        self$records[[i]]$comp_group <- compGroup$name
        cli_progress_update()
      }
      cli_progress_done()
    },
    cleanUpFields = function() {
      cli_progress_bar("cleanUpFields", total = 11)
      private$changeFieldName("CAS", "cas")
      cli_progress_update()
      private$changeFieldName("SMILES", "smiles")
      cli_progress_update()
      private$changeFieldName("polarity", "pol")
      cli_progress_update()
      private$changeFieldName("^CE$", "ce")
      cli_progress_update()
      private$changeFieldName("^CES$", "ces")
      cli_progress_update()
      private$changeFieldName("col_type", "frag_type")
      cli_progress_update()
      private$changeFieldName("isotope", "isotopologue")
      cli_progress_update()
      private$removeField("chem_list_id")
      cli_progress_update()
      private$changeFieldName("experiment_id", "csl_experiment_id")
      cli_progress_update()
      private$removeField("parameter_id")
      cli_progress_update()
      private$removeField("compound_id")
      cli_progress_update()
      private$correctDateFormat()
      cli_progress_done()
    },
    changeFieldName = function(oldNameRegex, newName) {
      for (i in seq_along(self$records)) 
        names(self$records[[i]]) <- sub(oldNameRegex, newName, names(self$records[[i]]))
    },
    removeField = function(fieldName) {
      for (i in seq_along(self$records))
        self$records[[i]][[fieldName]] <- NULL
    },
    correctDateFormat = function() {
      for (i in seq_along(self$records)) {
        thisDate <- lubridate::ymd_hms(self$records[[i]]$time_added, tz = "Europe/Berlin")  # local time is used in CSL
        self$records[[i]]$time_added <- format(thisDate, "%F %T", tz = "GMT")  # Elasticsearch needs UTC
      }
    }
  )
)

ingestSpectralLibrary <- function(specLibRecords, specLibTableName) {
  dbComm <- getDbComm()
  deleteTable(dbComm, specLibTableName)
  createNewTable(dbComm, specLibTableName, "spectral_library")
  appendRecords(dbComm, specLibTableName, specLibRecords$records)
}

# Copyright 2025 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
