
test_that("You can create and delete an ingest pipeline", {
  pipelineName <- "ingest-feature-unit-tests"
  if (testForPipeline(pipelineName))
    deleteIngestPipeline(pipelineName)
  createIngestPipeline(pipelineName)
  expect_true(testForPipeline(pipelineName))
})

test_that("You can read an ingest pipeline", {
  pipelineName <- "ingest-feature-unit-tests"
  x <- readPipelineProcessors(pipelineName)
  expect_length(x, 2)
})

test_that("A non-existant pipeline is not found", {
  pipelineName <- "foobar"
  expect_false(testForPipeline(pipelineName))
})

# Copyright 2025 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal