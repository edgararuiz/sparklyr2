setClass("sparklyr2_connection",
  contains = "DBIConnection",
  slots = list()
)

#' DBI connection and results methods
#' @name DBIMethods
setClass("sparklyr2_result",
  contains = "DBIResult",
  slots = list(
    sql = "character",
    conn = "sparklyr2_connection"
  )
)

#' @inheritParams DBI::dbSendQuery
#' @rdname DBIMethods
#' @export
setMethod("dbSendQuery", "sparklyr2_connection", function(conn, statement, ...) {
  new("sparklyr2_result", conn = conn, sql = statement)
})

#' @inheritParams DBI::dbClearResult
#' @rdname DBIMethods
#' @export
setMethod("dbClearResult", "sparklyr2_result", function(res, ...) {
  TRUE
})

#' @inheritParams DBI::dbFetch
#' @rdname DBIMethods
#' @export
setMethod("dbFetch", "sparklyr2_result", function(res, n = -1, ...) {
  sql_df <- res@conn$python$sql(res@sql)
  sql_df$toPandas()
})

# -----------------------------------Hive Reexports ----------------------------

#' Re-exports Hive DBI methods from `odbc`
#' @name HiveReexports

setClass("Hive")

#' @inheritParams DBI::dbQuoteString
#' @rdname HiveReexports
#' @export
setMethod(
  "dbQuoteString", signature("Hive", "character"),
  function(conn, x, ...) {
    if (is(x, "SQL")) {
      return(x)
    }
    x <- gsub("'", "\\\\'", enc2utf8(x))
    if (length(x) == 0L) {
      DBI::SQL(character())
    } else {
      str <- paste0("'", x, "'")
      str[is.na(x)] <- "NULL"
      DBI::SQL(str)
    }
  }
)

#' @inheritParams DBI::dbQuoteIdentifier
#' @rdname HiveReexports
#' @export
setMethod(
  "dbQuoteIdentifier", c("Hive", "character"),
  function(conn, x, ...) {
    conn_quote <- "`"
    if (length(x) == 0L) {
      return(DBI::SQL(character()))
    }
    if (any(is.na(x))) {
      stop("Cannot pass NA to dbQuoteIdentifier()", call. = FALSE)
    }
    if (nzchar(conn_quote)) {
      x <- gsub(conn_quote, paste0(conn_quote, conn_quote), x, fixed = TRUE)
    }
    nms <- names(x)
    res <- DBI::SQL(paste(conn_quote, encodeString(x), conn_quote, sep = ""))
    names(res) <- nms
    res
  }
)
