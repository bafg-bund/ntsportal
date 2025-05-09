
#' @export
newNtspRecord <- function(rec = list(), ..., class = character()) {
  stopifnot(is.list(rec))
  structure(rec, ..., class = c(class, "ntspRecord", "list"))
}

#' @export
newMsrawfilesRecord <- function(rec = list()) {
  newNtspRecord(rec, class = "msrawfilesRecord")
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
print.featureRecord <- function(x, ...) {
  cat("featureRecord\n")
  cat("mz: ", x$mz, ", rt: ", x$rt, "\n", sep = "")
  NextMethod()
}

