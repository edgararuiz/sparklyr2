#' @importFrom reticulate import r_to_py
#' @importFrom purrr map map_chr flatten
#' @importFrom methods new
#' @import rscontract
#' @import dbplyr
#' @import glue
#' @import DBI
#' @import cli
sparklyr2_env <- new.env()
