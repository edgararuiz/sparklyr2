#' @importFrom dplyr copy_to
#' @export
copy_to.sparklyr2_connection <- function(dest,
                                         df,
                                         name = deparse(substitute(df)),
                                         overwrite = FALSE,
                                         ...) {
  df_copy <- dest$python$createDataFrame(r_to_py(df))
  df_copy$createTempView(name)
  tbl(src = dest, from = name)
  # TODO: implement override
}

#' @importFrom dplyr tbl
#' @export
tbl.sparklyr2_connection <- function(src, from, ...) {
  pyspark_obj <- src$python$table(from)
  vars <- pyspark_obj$columns
  tbl_sql(
    subclass = "sparklyr2",
    src = src,
    from = as.sql(from, con = src$con),
    vars = vars
  )
}

#' @importFrom dplyr collect
#' @export
collect.tbl_sparklyr2 <- function(x, ...) {
  sc <- x[[1]]
  res <- sc$python$sql(remote_query(x))
  res$toPandas()
}
