#' @importFrom dplyr copy_to
#' @export
copy_to.sparklyr2_connection <- function(dest,
                                         df,
                                         name = deparse(substitute(df)),
                                         overwrite = FALSE,
                                         ...) {
  if(dest$python$catalog$tableExists(name)) {
    if(overwrite) {
      dest$python$catalog$dropTempView(name)
    } else {
      cli_abort(
        "Temp table {name} already exists, use `overwrite = TRUE` to replace"
        )
    }
  }
  df_copy <- dest$python$createDataFrame(r_to_py(df))
  df_copy$createTempView(name)
  rstudio_update_connection(dest)
  tbl(src = dest, from = name)
}

#' @importFrom dplyr tbl
#' @export
tbl.sparklyr2_connection <- function(src, from, ...) {
  sql_from <- as.sql(from, con = src$con)
  pyspark_obj <- src$python$table(sql_from)
  vars <- pyspark_obj$columns
  tbl_sql(
    subclass = "sparklyr2",
    src = src,
    from = sql_from,
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
