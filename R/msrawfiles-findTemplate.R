

#' Get ID based on search parameters
#'
#' @param rfIndex index name for rawfiles index 
#' @param isBlank boolean default is FALSE
#' @param pol Either "pos" or "neg", default "pos"
#' @param station Station ID name 
#' @param matrix character default is "spm"
#' @param duration numeric, default 365
#'
#' @return ES-ID of template record in msrawfiles index
#' @export
#'
findTemplateId <- function(rfIndex, blank = FALSE, pol = "pos", station = "rhein_ko_l", matrix = "spm", duration = 365) {
  queryBody <- list(
    query = list(
      bool = list(
        must = list(
          list(term = list(station = station)),
          list(term = list(pol = pol)),
          list(term = list(matrix = matrix)),
          list(term = list(blank = blank)),
          list(term = list(duration = duration))
        )
      )
    )
  )
  tempID <- elastic::Search(escon, rfIndex, body = queryBody)
  if (tempID$hits$total$value == 0) {
    warning("Search returned no hits")
    return("")
  }
  tempID$hits$hits[[1]]$`_id`
}
