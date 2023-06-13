#' @importFrom reticulate import r_to_py
#' @importFrom purrr map map_chr map_lgl flatten
#' @importFrom methods new
#' @importFrom utils head
#' @import rscontract
#' @import dbplyr
#' @import glue
#' @import DBI
#' @import cli
sparklyr2_env <- new.env()
