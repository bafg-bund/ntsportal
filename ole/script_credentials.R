# install.packages('reticulate')
# reticulate::py_install('keyring')
# reticulate::py_install('secretstorage')
# reticulate::py_install('dbus-python')

reticulate::source_python("~/projects/ntsportal/tests/ole/get_credentials.py")


cred = get_credentials()