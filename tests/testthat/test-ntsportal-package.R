

test_that("Imaginary keychain is not found", {
  answer <- haveCred(ring = "foobar")
  expect_false(answer)
})


test_that("You can set, check and retrieve a username and password from a keychain", {
  keyring::keyring_create("test", password = "test")
  setCred(ring = "test", usr = "ntsportal_test_user", pwd = "test_user")
  answer <- getCred(ring = "test")
  expect_contains(answer, c("ntsportal_test_user", "test_user"))
  
  keyring::keyring_delete("test")
})


test_that("You can connect to elasticsearch from the keychain", {
  # The test user "ntsportal_test_user" was created for this test
  keyring::keyring_create("test", password = "test")
  keyring::key_set_with_value("ntsportal-user", keyring = "test", password = "ntsportal_test_user")
  keyring::key_set_with_value("ntsportal-pwd", keyring = "test", password = "test_user")
  
  if (exists("escon", where = .GlobalEnv))
    rm(escon, pos = .GlobalEnv)
  
  createEscon(ring = "test")
  
  expect_true(exists("escon", where = .GlobalEnv))
  rm(escon, pos = .GlobalEnv)
  keyring::keyring_delete("test")
})
