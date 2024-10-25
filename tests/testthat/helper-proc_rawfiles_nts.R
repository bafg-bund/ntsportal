

getRecordsDes_07 <- function() {
  rx <- elastic::Search(
    escon, 
    "ntsp_index_msrawfiles_unit_tests",
    body = list(
      query = list(
        bool = list(
          must = list(
            list(regexp = list(path = "/srv/cifs-mounts/g2/G/G2/3-Arbeitsgruppen_G2/3.5-NTS-Gruppe/db/ntsp/unit_tests/meas_files/olmesartan-d6-bisoprolol/.*")),
            list(term = list(blank = FALSE))
          )
        )
      ),
      sort = list(list(path = "asc"))
    )
  )$hits$hits
  jsonlite::write_json(rx, test_path("fixtures", "doc_source-Des_07-batch.json"), pretty = T, auto_unbox = T)
}