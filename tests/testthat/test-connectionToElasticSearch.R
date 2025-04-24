



test_that("You can connect to elasticsearch from the keychain and create escon", {
  # The test user "ntsportal_test_user" was created for this test
  if (haveRing("test"))
    clearRing("test")
  if (haveRing("system"))
    clearRing("system")
  
  if (exists("escon", where = .GlobalEnv))
    rm(escon, pos = .GlobalEnv)
  
  ntsportal::setCredNonInteractive(ring = "test", usr = "ntsportal_test_user", pwd = "test_user")

  expect_true(haveCred("test"))
  
  createEscon(ring = "test")
  
  expect_true(exists("escon", where = .GlobalEnv))
  rm(escon, pos = .GlobalEnv)
  clearRing("test")
})




