

#' Get ID based on search parameters
#'
#' @param escon elastic connection object created by elastic::connect
#' @param rfindex index name for rawfiles index 
#' @param isBlank boolean default is FALSE
#' @param polarity Either "pos" or "neg"
#' @param station Station ID name 
#' @param matrix character default is "spm"
#'
#' @return string templateID
#' @export
#'
find_templateid <- function(escon, rfindex, isBlank = FALSE, polarity, station, matrix = "spm", duration) {
  tempID <- elastic::Search(
    escon, rfindex, body = 
      sprintf('
        {
          "query": {
            "bool": {
              "must": [
                {
                  "term": {
                     "station": {
                      "value": "%s"
                    }
                  }
                },
                {
                  "term": {
                    "pol": {
                      "value": "%s"
                    }
                  }
                },
                {
                  "term": {
                    "matrix": {
                      "value": "%s"
                    }
                  }
                },
                {
                  "term": {
                    "blank": {
                      "value": %s
                    }
                  }
                },
                {
                  "term": {
                    "duration": {
                      "value": %s
                    }
                  }
                }
              ]
            }
          },
          "_source": false,
          "size": 1
        }
        ', 
              station, polarity, matrix, ifelse(isBlank, "true", "false"), duration
      )
  )
  if (tempID$hits$total$value == 0) {
    warning("search have no hits")
    return(NULL)
  }
  tempID$hits$hits[[1]]$`_id`
}
