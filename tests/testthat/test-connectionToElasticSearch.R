



test_that("You can connect to elasticsearch from the keychain", {
  # The test user "ntsportal_test_user" was created for this test
  if (haveRing("test")) {
    keyring::key_delete("ntsportal-user", keyring = "test")
    keyring::key_delete("ntsportal-pwd", keyring = "test")
    clearRing("test")
  }
    
  if (haveRing("system"))
    clearRing("system")

  setCredNonInteractive(ring = "test", usr = "ntsportal_test_user", pwd = "test_user")

  expect_true(haveCred("test"))

  keyring::key_delete("ntsportal-user", keyring = "test")
  keyring::key_delete("ntsportal-pwd", keyring = "test")
  clearRing("test")
})

# Copyright 2026 Bundesanstalt für Gewässerkunde
# This file is part of ntsportal

