# Index creation methods
# These are the mappings and they must remain synchronized with the mappings at
# https://gitlab.lan.bafg.de/nts/ntsportal/-/wikis/Create-index


#' Send index creation request for dbas index 
#'
#' @param eson elasticsearch connection object created with elastic::connect
#' @param index Name of the index you wish to create
#'
#' @return
#' @export
put_dbas_index <- function(eson, index) {
  elastic::index_create(
    escon, index, body = 
      '
    {
      "mappings" : {
        "dynamic": "strict",
        "properties" : {
          "area" : {"type" : "float"},
          "area_is" : {"type" : "float"},
          "area_normalized" : {"type" : "float"},
          "intensity" : {"type" : "float"},
          "intensity_is" : {"type" : "float"},
          "intensity_normalized" : {"type" : "float"},
          "cas" : {"type" : "keyword"},
          "comment" : {"type" : "text"},
          "comp_group" : {
            "type" : "keyword"
          },
          "conc" : {"type" : "float"},  
          "tag": {"type": "keyword"},
          "data_source" : {"type" : "keyword"},
          "sample_source" : {"type" : "keyword"},
          "licence" : {"type" : "keyword"}
          "start" : {
            "type" : "date",
            "format" : "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||yyyy-MM-dd HH:mm"
          },
          "km": { "type" : "float" },
          "gkz" : { "type" : "integer" },
          "river" : { "type" : "keyword" },
          "duration" : {"type" : "float"},
          "date_import" : {
            "type" : "date",
            "format" : "epoch_second"
          },
          "eic" : {
            "type" : "nested",
            "properties" : {
              "int" : {
                "type" : "float"
              },
              "time" : {
                "type" : "short"
              }
            }
          },
          "chrom_method" : {"type" : "keyword"},
          "instrument": { "type" : "text"},
          "loc" : {"type" : "geo_point"},
          "matrix" : {"type" : "keyword"},
          "ms1" : {                           
            "type" : "nested",                
            "properties" : {
              "int" : {
                "type" : "float"
              },
              "mz" : {
                "type" : "float"
              }
            }
          },
          "ms2" : {
            "type" : "nested",
            "properties" : {
              "int" : {
                "type" : "float"
              },
              "mz" : {
                "type" : "float"
              }
            }
          },
          "mz" : {"type" : "float"},
          "name" : {"type" : "keyword"},
          "norm_a" : {"type" : "float"},
          "pol" : {"type" : "keyword"},
          "rt" : {"type" : "float"},
          "rtt" : {
            "type" : "nested",
            "properties" : {
              "method" : {
                "type" : "keyword"
              },
              "predicted" : {
                "type" : "boolean"
              },
              "rt" : {
                "type" : "float"
              }
            }
          },
          "filename" : {"type" : "keyword"},
          "station" : {"type" : "keyword"},
          "inchikey" : {"type" : "keyword"},
          "inchi" : {"type" : "keyword"},
          "smiles" : {"type" : "keyword"},
          "mw" : {"type" : "float"},
          "adduct" : {"type" : "keyword"},
          "formula" : {"type" : "keyword"}
        }
      }
    }
    ')
}
