#' @importFrom reticulate py_available virtualenv_python py_install install_python
#' @importFrom reticulate use_virtualenv virtualenv_create py_exe use_python
#' @importFrom reticulate import r_to_py virtualenv_list py_list_packages
#' @importFrom purrr map map_chr map_lgl flatten walk
#' @importFrom methods new
#' @importFrom utils head
#' @import rscontract
#' @import dbplyr
#' @import glue
#' @import DBI
#' @import cli


sparklyr2_env <- new.env()
sparklyr2_env$vars$python_deps <- c(
  "pyspark", "pandas", "PyArrow", "grpcio", "google-api-python-client",
  "grpcio_status", "databricks-connect"
)
