


#' Get data from elastic
#'
#' @param escon connection objected created with elastic::connect
#' @param index name of index must be one of "g2_nts_expn", "g2_nts_bfg"
#' @param station can be one of "KOMO", "FAN", "WIN", "rhein_ko_l"
#' @param startRange 2 element character vector with date range e.g. c("2021-01-01", "2022-01-01")
#' @param responseField can be one of "intensity", "intensity_normalized", "area", "area_normalized"
#' @param form format of output table, can be one of "long", "wide"
#'
#' @return data.frame
#' @export
#'
#' @examples 
#' \dontrun{
#' config_path <- "~/projects/config.yml"
#' ec <- config::get("elastic_connect", file = config_path)
#' escon <- elastic::connect(host = '10.140.73.204', user=ec$user, pwd=ec$pwd)
#' index <- "g2_nts_expn"
#' get_time_series(
#'   escon, 
#'   station = "KOMO",
#'   startRange = c("2021-01-01", "2022-01-01"),
#'   responseField = "intensity"
#' )
#' }
#' 
#' @import lubridate
get_time_series <- function(
  escon,
  index = "g2_nts_expn",
  station = "KOMO",
  startRange = c("2021-01-01", "2022-01-01"),
  responseField = "intensity",
  form = "long") {
  
  stopifnot(length(startRange) == 2)
  if (!all(grepl("^\\d{4}-\\d{2}-\\d{2}$", startRange)))
    stop("'start_range' must be length 2 character with the format 'yyyy-mm-dd'")
  stopifnot(length(responseField) == 1, is.element(responseField, c("intensity", "intensity_normalized", "area", "area_normalized")))
  stopifnot(length(station) == 1,is.element(station, c("KOMO", "FAN", "WIN", "rhein_ko_l")))
  stopifnot(length(form) == 1, is.element(form, c("long", "wide")))
  stopifnot(length(index) == 1, is.element(index, c("g2_nts_expn", "g2_nts_bfg")))
  stopifnot(inherits(escon, "Elasticsearch"))
  
  # need to partition results since bucket overload may occur
  # get number of ufids
  
  numUfids <- elastic::Search(escon, index, body = sprintf(
    '
    {
  "query": {
    "bool": {
      "filter": [
        {
          "range": {
            "start": {
              "gte": "%s",
              "lte": "%s"
            }
          }
        },
        {
          "term": {
            "station": "%s"
          }
        }
      ]
    }
  },
  "size": 0, 
  "aggs": {
    "ufid_num": {
      "cardinality": {
        "field": "ufid"
      }
    }
  }
}
    ', startRange[1], startRange[2], station
  ))$aggregations$ufid_num$value
  
  # get number of days
  
  numDays <- interval(ymd(startRange[1]), ymd(startRange[2])) / ddays(1)
  
  # estimate number of partitions to ufids so that max buckets < 50000
  numPartitions <- ceiling(numUfids/(50000/numDays))
  message("Request split into ", numPartitions, " partitions.")
  
  get_data_partition <- function(part) {  # part <- 1
    message("Partition ", part)
    ans <- elastic::Search(escon, index, body = sprintf(
      '
    {
      "query": {
        "bool": {
          "filter": [
            {
              "term": {
                "station": "%s"
              }
            },
            {
              "range": {
                "start": {
                  "gte": "%s",
                  "lte": "%s"
                }
              }
            }
          ]
        }
      },
      "size": 0,
      "aggs": {
        "ufids": {
          "terms": {
            "field": "ufid",
            "include": {
               "partition": %i,
               "num_partitions": %i
            },
            "size": 10000
          },
          "aggs": {
            "mz": {
              "avg": {
                "field": "mz"
              }
            },
            "rt": {
              "avg": {
                "field": "rt"
              }
            },
            "names": {
              "terms": {
                "field": "name",
                "size": 10
              }
            },
            "polarity": {
              "terms": {
                "field": "pol",
                "size": 2
              }
            },
            "ucid": {
              "terms": {
                "field": "ucid",
                "size": 10
              }
            },
            "over_time": {
              "date_histogram": {
                "field": "start",
                "calendar_interval": "day"
              },
              "aggs": {
                "response": {
                  "avg": {
                    "field": "%s"
                  }
                }
              }
            }
          }
        }
      }
    }
    ', station, startRange[1], startRange[2], part - 1, numPartitions, responseField
    ))
    
    message(ans$hits$total$value, " features returned")
    ufids <- ans$aggregations$ufids$buckets
    message("Grouped into ", length(ufids), " ufids")
    df <- data.frame(
      ufid = vapply(ufids, "[[", numeric(1), i = "key"),
      mz = vapply(ufids, function(x) x$mz$value, numeric(1)),
      rt = vapply(ufids, function(x) x$rt$value, numeric(1)),
      ucid = vapply(ufids, function(x) {
        ucb <- x$ucid$buckets
        if (length(ucb) == 0) {
          return(0)
        } else if (length(ucb) == 1) {
          return(ucb[[1]]$key)
        } else {
          stop("more than one ucid for ufid ", x$key)
        }
      }, numeric(1)),
      name = vapply(ufids, function(x) {
        nb <- x$names$buckets
        if (length(nb) == 0) {
          return("")
        } else if (length(nb) == 1) {
          return(nb[[1]]$key)
        } else {
          return(paste(vapply(nb, "[[", character(1), i = "key"), collapse = ", "))
        }
      }, character(1)),
      pol = vapply(ufids, function(x) {
        nb <- x$polarity$buckets
        if (length(nb) == 0) {
          return("")
        } else if (length(nb) == 1) {
          return(nb[[1]]$key)
        } else {
          return(paste(vapply(nb, "[[", character(1), i = "key"), collapse = ", "))
        }
      }, character(1))
    ) 
    
    resp <- lapply(ufids, function(x) {
      u <- x$key
      tb <- x$over_time$buckets
      data.frame(
        ufid = u,
        start = vapply(tb, "[[", character(1), i = "key_as_string"),
        response = vapply(tb, function(y) {
          rVal <- y$response$value
          if (is.null(rVal))
            0 else rVal
        }, numeric(1))
      )
    })
    resp <- do.call("rbind", resp)
    resp$start <- substr(resp$start, 1, 10)
    
    merge(df, resp, by = "ufid")
  }
  
  aligTables <- lapply(seq_len(numPartitions), get_data_partition)
  
  # long form
  aligTable <- do.call("rbind", aligTables) 
  
  # wide form
  if (form == "wide")
    aligTable <- tidyr::pivot_wider(aligTable, names_from = "start", values_from = "response", values_fill = 0)
  
  aligTable
}
