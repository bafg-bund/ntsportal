
#' @export
newNtspRecord <- function(rec = list(), ..., class = character()) {
  stopifnot(is.list(rec))
  structure(rec, ..., class = c(class, "ntspRecord", "list"))
}

#' @export
newMsrawfilesRecord <- function(rec = list(), ..., class = character()) {
  newNtspRecord(rec, class = c(class, "msrawfilesRecord"))
}

#' @export
newDbasMsrawfilesRecord <- function(rec = list()) {
  newMsrawfilesRecord(rec, class = "dbasMsrawfilesRecord")
}

#' @export
newNtsMsrawfilesRecord <- function(rec = list()) {
  newMsrawfilesRecord(rec, class = "ntsMsrawfilesRecord")
}

#' @export
newFeatureRecord <- function(feature) {
  newNtspRecord(feature, class = "featureRecord")
}

#' @export
print.ntspRecord <- function(x, ...) {
  n <- names(x)
  cat(glue("ntspRecord with {length(n)} names:"), "\n", paste(n, collapse = ", "), sep = "")
  invisible(x)
}

#' @export
print.msrawfilesRecord <- function(x, ...) {
  cat("msrawfilesRecord\n")
  cat("file path: ", x$path, "\n", sep = "")
  NextMethod()
}

#' @export
print.dbasMsrawfilesRecord <- function(x, ...) {
  cat("msrawfilesRecord for DBAS processing\n")
  NextMethod()
}

#' @export
print.ntsMsrawfilesRecord <- function(x, ...) {
  cat("msrawfilesRecord for NTS processing\n")
  NextMethod()
}

#' @export
print.featureRecord <- function(x, ...) {
  cat("featureRecord\n")
  cat("mz: ", x$mz, ", rt: ", x$rt, "\n", sep = "")
  NextMethod()
}

# Copyright 2025 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
