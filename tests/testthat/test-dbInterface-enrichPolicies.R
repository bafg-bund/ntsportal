

test_that("You can create a test enrich policy and execute it", {
  testPolicyName <- "date-import-policy-unit-tests"
  if (testForEnrichPolicy(testPolicyName)) 
    deleteEnrichPolicy(testPolicyName)
  createEnrichPolicy(testPolicyName)
  
  expect_true(testForEnrichPolicy(testPolicyName))
  taskId <- executeEnrichPolicy(testPolicyName)
  cancelTask(taskId)
  
  Sys.sleep(1)
  deleteEnrichPolicy(testPolicyName)
  expect_false(testForEnrichPolicy(testPolicyName))
})

test_that("You can get all policy names", {
  policyNames <- getEnrichPolicyNames()
  expect_gt(length(policyNames), 1)
})

# Copyright 2025 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal
