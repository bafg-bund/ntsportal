

test_that("You can compute the linear regression for one compound", {
  qb = list(bool = list(must = list(
    list(term = list(duration = "P1Y")),
    list(nested = list(path = "compound_annotation", query = list(exists = list(field = "compound_annotation.name")))),
    list(term = list(name = "Sitagliptin"))
  )))
  records <- computeLinearRegression("ntsp25.3_feature_upb", queryBlock = qb)
  tb <- convertRecordsToTibble(records)
  expect_gte(nrow(tb), 2)
  expect_equal(tb[1, "name", drop = T], "Sitagliptin")
  #updateLinearRegressionTable("ntsp25.3_feature_upb")
})

# Copyright 2026 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal