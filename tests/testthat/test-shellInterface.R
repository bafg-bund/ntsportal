
test_that("Shell interface can be installed and executed", {
  tempDir <- withr::local_tempdir()
  withr::with_path(tempDir, {
    installShellInterface(tempDir)
    res <- system2("ntsp", args = c("process", "-h"), stdout = TRUE, stderr = TRUE)
    expect_true(any(grepl("usage:", res)))
  })
})
