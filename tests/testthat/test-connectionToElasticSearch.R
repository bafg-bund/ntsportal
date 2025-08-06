



test_that("You can connect to elasticsearch from the keychain and create escon", {
  # The test user "ntsportal_test_user" was created for this test
  if (haveRing("test")) {
    keyring::key_delete("ntsportal-user", keyring = "test")
    keyring::key_delete("ntsportal-pwd", keyring = "test")
    clearRing("test")
  }
    
  if (haveRing("system"))
    clearRing("system")
  
  if (exists("escon", where = .GlobalEnv))
    rm(escon, pos = .GlobalEnv)
  
  ntsportal::setCredNonInteractive(ring = "test", usr = "ntsportal_test_user", pwd = "test_user")

  expect_true(haveCred("test"))
  
  createEscon(ring = "test")
  
  expect_true(exists("escon", where = .GlobalEnv))
  rm(escon, pos = .GlobalEnv)
  keyring::key_delete("ntsportal-user", keyring = "test")
  keyring::key_delete("ntsportal-pwd", keyring = "test")
  clearRing("test")
})


# Copyright 2025 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal

