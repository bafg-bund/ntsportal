
test_that("You can create and delete an ingest pipeline", {
  pipelineName <- "ingest-feature-unit-tests"
  if (testForPipeline(pipelineName))
    deleteIngestPipeline(pipelineName)
  createIngestPipeline(pipelineName)
  expect_true(testForPipeline(pipelineName))
  deleteIngestPipeline(pipelineName)
})

test_that("You can read an ingest pipeline", {
  pipelineName <- "ingest-feature-unit-tests"
  x <- readPipelineProcessors(pipelineName)
  expect_length(x, 2)
})

# Copyright 2025 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal