#' sparklyr2 DB connection class
#' @keywords internal
#' @export
setClass("sparklyr2_connection",
  contains = "DBIConnection",
  slots = list()
)

#' sparklyr2 DB results class
#'
#' @keywords internal
#' @export
setClass("sparklyr2_result",
  contains = "DBIResult",
  slots = list(
    sql = "character",
    conn = "sparklyr2_connection"
  )
)

#' Send a query to Spark using sparklyr2
#' @param conn A DBIConnection object, as returned by dbConnect().
#' @param statement	A character string containing SQL.
#' @param ...	Other parameters passed on to methods.
#' @export
setMethod("dbSendQuery", "sparklyr2_connection", function(conn, statement, ...) {
  new("sparklyr2_result", conn = conn, sql = statement)
})

#' Clears sparklyr2 result
#' @param res	An object inheriting from DBIResult.
#' @param ...	Other arguments passed on to methods.
#' @export
setMethod("dbClearResult", "sparklyr2_result", function(res, ...) {
  TRUE
})

#' Retrieve records from a Spark query
#' @param res	An object inheriting from DBIResult, created by dbSendQuery().
#' @param n	 maximum number of records to retrieve per fetch. Use n = -1
#'  or n = Inf to retrieve all pending records. Some implementations may
#'  recognize other special values.
#' @param ...	 Other arguments passed on to methods. description
#' @export
setMethod("dbFetch", "sparklyr2_result", function(res, n = -1, ...) {
  sql_df <- res@conn$python$sql(res@sql)
  sql_df$toPandas()
})
