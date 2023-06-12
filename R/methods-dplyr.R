#' @importFrom dplyr copy_to
#' @export
copy_to.sparklyr2_connection <- function(dest,
                                         df,
                                         name = deparse(substitute(df)),
                                         overwrite = FALSE,
                                         ...
                                         ) {
  df_copy <- dest$python$createDataFrame(r_to_py(df))
  df_copy$createTempView(name)
  df_copy
  #TODO: implement override
}

#' @importFrom dplyr tbl
#' @export
tbl.sparklyr2_connection <- function(src, from, ...) {
  pyspark_obj <- src$python$table(from)
  #src$con <- simulate_hive()
  vars <- pyspark_obj$columns
  out <- tbl_sql("sparklyr2", src, as.sql(from, con = src$con), vars = vars)
  out
}

#' @importFrom dplyr collect
#' @export
collect.tbl_sparklyr2 <- function(x, ...) {
  sc <- x[[1]]
  res <- sc$python$sql(remote_query(x))
  res$toPandas()
}
