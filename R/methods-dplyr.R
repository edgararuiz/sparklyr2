#' @importFrom dplyr copy_to
#' @export
copy_to.sparklyr2_connection <- function(dest,
                                         df,
                                         name = deparse(substitute(df)),
                                         overwrite = FALSE,
                                         ...
                                         ) {
  df_copy <- sc$createDataFrame(r_to_py(df))
  df_copy$createTempView(name)
  df_copy
}
